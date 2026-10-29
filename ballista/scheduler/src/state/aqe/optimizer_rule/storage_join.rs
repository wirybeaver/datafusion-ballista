// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Align provider-owned file groups before AQE creates exchanges.

use crate::state::aqe::execution_plan::{DynamicJoinSelectionExec, JoinSelectionAction};
use ballista_core::config::BallistaConfig;
use ballista_core::storage_partitioning::iceberg_bucket_count;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{JoinType, Result};
use datafusion::physical_expr::GroupedPartitioning;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, Partitioning};
use std::sync::Arc;

/// Query-local decisions collected only while planning EXPLAIN. Recording at
/// the rule's decision points keeps diagnostics and optimization in agreement.
#[derive(Debug, Clone, Default)]
pub(crate) struct StorageJoinDiagnostics(Arc<parking_lot::Mutex<Vec<String>>>);

impl StorageJoinDiagnostics {
    pub(crate) fn render(&self) -> String {
        self.0.lock().join("\n")
    }
}

#[derive(Debug, Default)]
pub struct StoragePartitionJoinRule {
    pub(crate) diagnostics: Option<StorageJoinDiagnostics>,
}

impl PhysicalOptimizerRule for StoragePartitionJoinRule {
    fn name(&self) -> &str {
        "StoragePartitionJoin"
    }
    fn schema_check(&self) -> bool {
        true
    }
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let options = config;
        let Some(config) = options.extensions.get::<BallistaConfig>() else {
            return Ok(plan);
        };
        if !config.storage_partition_join_enabled() && self.diagnostics.is_none() {
            return Ok(plan);
        }
        plan.transform_up(|plan| {
            align_join(plan, config, options, self.diagnostics.as_ref())
        })
        .data()
    }
}

fn align_join(
    plan: Arc<dyn ExecutionPlan>,
    config: &BallistaConfig,
    options: &ConfigOptions,
    diagnostics: Option<&StorageJoinDiagnostics>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(join) = plan.downcast_ref::<DynamicJoinSelectionExec>() else {
        return Ok(Transformed::no(plan));
    };
    let record = |reason: &str, grouping: Option<&GroupedPartitioning>| {
        if diagnostics.is_none() && !log::log_enabled!(log::Level::Debug) {
            return;
        }
        let mut decision =
            format!("StoragePartitionJoin: join={}, {reason}", join.plan_id);
        if let Some(grouping) = grouping {
            decision.push_str(&format!(
                ", scheme={}, effective_groups={}",
                grouping.scheme(),
                grouping.group_ids().len()
            ));
            if let Some(buckets) = iceberg_bucket_count(grouping.scheme()) {
                decision.push_str(&format!(", effective_buckets={buckets}"));
            }
        }
        log::debug!("{decision}");
        if let Some(diagnostics) = diagnostics {
            diagnostics.0.lock().push(decision);
        }
    };
    let unchanged = |reason: &str| {
        record(reason, None);
        Ok(Transformed::no(Arc::clone(&plan)))
    };
    if !config.storage_partition_join_enabled() {
        return unchanged("disabled");
    }
    if join.join_type != JoinType::Inner
        || join.null_aware
        || join.null_equality != datafusion::common::NullEquality::NullEqualsNothing
    {
        return unchanged("fallback: unsupported join semantics");
    }
    let (Partitioning::Grouped(left), Partitioning::Grouped(right)) = (
        join.left.output_partitioning(),
        join.right.output_partitioning(),
    ) else {
        return unchanged("fallback: inputs do not expose storage groups");
    };
    // Distribution satisfaction treats every singleton as key-partitioned.
    // Regrouping needs the actual storage keys, even after scan pruning.
    let left_eq = join.left.equivalence_properties().eq_group();
    let right_eq = join.right.equivalence_properties().eq_group();
    let keys_match = join.on.len() == left.keys().len()
        && join.on.len() == right.keys().len()
        && join
            .on
            .iter()
            .zip(left.keys().iter().zip(right.keys()))
            .all(|((left_key, right_key), (left_group, right_group))| {
                left_eq
                    .normalize_expr(left_key.clone())
                    .eq(&left_eq.normalize_expr(left_group.clone()))
                    && right_eq
                        .normalize_expr(right_key.clone())
                        .eq(&right_eq.normalize_expr(right_group.clone()))
            });
    if !keys_match {
        return unchanged("fallback: join keys do not match storage keys");
    }
    if left.compatible_layout(right) && !join.upstream_resolved() {
        record(
            "deferred: compatible storage groups; upstream join selection or execution pending",
            Some(left),
        );
        return Ok(Transformed::no(plan));
    }
    // Preserve the existing broadcast decision before regrouping the probe.
    // Source partitioning has not already paid for a shuffle.
    let selection = join.to_actual_join(options)?;
    if matches!(selection, JoinSelectionAction::CollectLeft(_)) {
        return unchanged("policy: broadcast selected; preserve probe parallelism");
    }
    if left.compatible_layout(right) {
        match selection {
            JoinSelectionAction::StageBuildSide { .. } => {
                return unchanged("policy: measure build before deciding broadcast");
            }
            JoinSelectionAction::Repartition(_) => {
                return unchanged(
                    "policy: unmeasured join output requires repartitioning",
                );
            }
            _ => {}
        }
        record("selected: compatible storage groups", Some(left));
        return Ok(Transformed::no(plan));
    }
    unchanged("fallback: storage layouts differ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::aqe::planner::AdaptivePlanner;
    use ballista_core::config::{
        BALLISTA_HASH_JOIN_MAX_BUILD_PARTITION_BYTES, BALLISTA_STORAGE_PARTITION_JOIN,
    };
    use ballista_core::extension::SessionConfigExt;
    use ballista_core::storage_partitioning::{
        BucketFile, BucketedParquetManifest, BucketedParquetTable,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn source(buckets: u32, num_rows: Option<usize>) -> Arc<BucketedParquetTable> {
        Arc::new(
            BucketedParquetTable::try_new(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
                BucketedParquetManifest {
                    object_store_url: "file://".into(),
                    key: "id".into(),
                    buckets,
                    files: vec![BucketFile {
                        path: "planning-only.parquet".into(),
                        size: 1024,
                        bucket: Some(0),
                        num_rows,
                    }],
                },
            )
            .unwrap(),
        )
    }

    async fn planned(
        left: u32,
        right: u32,
        sort: bool,
        num_rows: Option<usize>,
    ) -> String {
        let config = SessionConfig::new_with_ballista()
            .with_target_partitions(16)
            .set_bool(BALLISTA_STORAGE_PARTITION_JOIN, true)
            .set_u64(
                BALLISTA_HASH_JOIN_MAX_BUILD_PARTITION_BYTES,
                if sort { 1 } else { 0 },
            )
            .with_ballista_broadcast_join_threshold_bytes(0)
            .set_u64(
                "datafusion.optimizer.hash_join_single_partition_threshold",
                0,
            )
            .set_u64(
                "datafusion.optimizer.hash_join_single_partition_threshold_rows",
                0,
            );
        let ctx = SessionContext::new_with_config(config);
        ctx.register_table("a", source(left, num_rows)).unwrap();
        ctx.register_table("b", source(right, num_rows)).unwrap();
        let logical = ctx
            .sql("select a.id from a join b on a.id=b.id")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        let planner = AdaptivePlanner::try_new(&ctx, &logical, "spj".into())
            .await
            .unwrap();
        let executed = displayable(planner.current_plan())
            .indent(false)
            .to_string();
        let explained =
            crate::state::distributed_explain::generate_distributed_explain_plan(
                &"spj".into(),
                &ctx,
                Arc::new(logical),
            )
            .await
            .unwrap();
        assert!(explained.ends_with(&executed), "{explained}\n{executed}");
        explained
    }

    #[tokio::test]
    async fn unmeasured_grouped_join_outputs_keep_the_broadcast_guard() {
        use datafusion::catalog::TableProvider;
        use datafusion::physical_expr::{PhysicalExpr, expressions::Column};
        use datafusion::physical_plan::joins::{
            HashJoinExec, HashJoinExecBuilder, PartitionMode,
        };

        let config = SessionConfig::new_with_ballista()
            .set_bool(BALLISTA_STORAGE_PARTITION_JOIN, true)
            .with_ballista_broadcast_join_threshold_bytes(16 * 1024)
            .with_ballista_broadcast_join_threshold_rows(2_000);
        let ctx = SessionContext::new_with_config(config.clone());
        let small = source(4, Some(1_000))
            .scan(&ctx.state(), None, &[], None)
            .await
            .unwrap();
        let large = source(4, Some(1_000_000_000))
            .scan(&ctx.state(), None, &[], None)
            .await
            .unwrap();
        let probe = source(4, Some(300_000_000))
            .scan(&ctx.state(), None, &[], None)
            .await
            .unwrap();
        let key = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let on = vec![(key.clone(), key)];
        // Without distinct counts, this join's small estimate cannot bound its
        // fan-out. Storage grouping does not make that estimate measured.
        let inner: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExecBuilder::new(small, large, on.clone(), JoinType::Inner)
                .with_partition_mode(PartitionMode::Partitioned)
                .with_projection(Some(vec![1]))
                .build()
                .unwrap(),
        );
        assert!(matches!(
            inner.output_partitioning(),
            Partitioning::Grouped(_)
        ));
        let delayed: Arc<dyn ExecutionPlan> = DynamicJoinSelectionExec::from_hash_join(
            inner.downcast_ref::<HashJoinExec>().unwrap(),
            1,
        )
        .unwrap();

        let outer = |build| {
            let hash = HashJoinExecBuilder::new(
                build,
                probe.clone(),
                on.clone(),
                JoinType::Inner,
            )
            .with_partition_mode(PartitionMode::Partitioned)
            .build()
            .unwrap();
            DynamicJoinSelectionExec::from_hash_join(&hash, 2).unwrap()
        };
        let join = outer(inner);
        for (stage, reason) in [
            (true, "policy: measure build before deciding broadcast"),
            (
                false,
                "policy: unmeasured join output requires repartitioning",
            ),
        ] {
            let options = config
                .clone()
                .set_bool("ballista.optimizer.stage_build_side", stage);
            let action = join.to_actual_join(options.options()).unwrap();
            if stage {
                assert!(matches!(action, JoinSelectionAction::StageBuildSide { .. }));
            } else {
                assert!(matches!(action, JoinSelectionAction::Repartition(_)));
            }
            let diagnostics = StorageJoinDiagnostics::default();
            StoragePartitionJoinRule {
                diagnostics: Some(diagnostics.clone()),
            }
            .optimize(join.clone(), options.options())
            .unwrap();
            assert!(
                diagnostics.render().contains(reason),
                "{}",
                diagnostics.render()
            );
        }
        // Before child join selection, the parent has no size estimate yet.
        // Explain the deferral instead of claiming SPJ has already been chosen.
        let diagnostics = StorageJoinDiagnostics::default();
        StoragePartitionJoinRule {
            diagnostics: Some(diagnostics.clone()),
        }
        .optimize(outer(delayed), config.options())
        .unwrap();
        assert!(
            diagnostics
                .render()
                .contains("deferred: compatible storage groups"),
            "{}",
            diagnostics.render()
        );
    }

    #[tokio::test]
    async fn production_planner_preserves_storage_groups_before_exchanges() {
        let equal = planned(4, 4, false, Some(32)).await;
        assert!(equal.contains("HashJoinExec"), "{equal}");
        assert!(!equal.contains("ExchangeExec"), "{equal}");
        assert!(
            equal.contains("selected: compatible storage groups"),
            "{equal}"
        );
        assert!(
            equal.contains("effective_groups=5, effective_buckets=4"),
            "{equal}"
        );
        let sorted = planned(4, 4, true, Some(32)).await;
        assert!(sorted.contains("SortMergeJoinExec"), "{sorted}");
        assert!(!sorted.contains("ExchangeExec"), "{sorted}");
        let unknown_size = planned(4, 4, true, None).await;
        assert!(unknown_size.contains("SortMergeJoinExec"), "{unknown_size}");
        assert!(!unknown_size.contains("ExchangeExec"), "{unknown_size}");
        let fallback = planned(8, 4, false, Some(32)).await;
        assert!(fallback.contains("ExchangeExec"), "{fallback}");
        assert!(
            fallback.contains("fallback: storage layouts differ"),
            "{fallback}"
        );
    }
    #[tokio::test]
    async fn tiny_storage_build_keeps_broadcast_and_probe_buckets() {
        let config = SessionConfig::new_with_ballista()
            .with_target_partitions(16)
            .set_bool(BALLISTA_STORAGE_PARTITION_JOIN, true)
            .with_ballista_broadcast_join_threshold_bytes(1024)
            .set_u64(
                "datafusion.optimizer.hash_join_single_partition_threshold",
                0,
            )
            .set_u64(
                "datafusion.optimizer.hash_join_single_partition_threshold_rows",
                0,
            );
        let ctx = SessionContext::new_with_config(config);
        ctx.register_table("probe", source(8, Some(10_000)))
            .unwrap();
        ctx.register_table("build", source(2, Some(1))).unwrap();
        let logical = ctx
            .sql("select probe.id from probe join build on probe.id=build.id")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        let explained = AdaptivePlanner::explain(&ctx, &logical, "broadcast_spj".into())
            .await
            .unwrap();
        assert!(explained.contains("mode=CollectLeft"), "{explained}");
        assert!(explained.contains("buckets=8"), "{explained}");
        assert!(!explained.contains("partitioning=Hash"), "{explained}");
        assert!(
            explained.contains("policy: broadcast selected; preserve probe parallelism"),
            "{explained}"
        );
    }
}
