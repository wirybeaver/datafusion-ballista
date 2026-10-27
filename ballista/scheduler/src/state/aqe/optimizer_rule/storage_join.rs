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
use ballista_core::storage_partitioning::{
    bucket_group_id, iceberg_bucket_count, with_group_statistics,
};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{JoinType, Result};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::GroupedPartitioning;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, Partitioning, replace_children_if_necessary,
};
use std::collections::{BTreeSet, HashMap};
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
    let (Some(left_buckets), Some(right_buckets)) = (
        iceberg_bucket_count(left.scheme()),
        iceberg_bucket_count(right.scheme()),
    ) else {
        return unchanged("fallback: no compatible transform adapter");
    };
    let target = left_buckets.min(right_buckets);
    let prefix_left = left.scheme().rsplit_once(":buckets=").unwrap().0;
    let prefix_right = right.scheme().rsplit_once(":buckets=").unwrap().0;
    if prefix_left != prefix_right {
        return unchanged("fallback: incompatible bucket schemes");
    }
    if !left_buckets.max(right_buckets).is_multiple_of(target) {
        return unchanged("fallback: non-divisor bucket counts");
    }
    if left_buckets != right_buckets && !config.storage_partition_join_divisor_enabled() {
        return unchanged("policy: divisor alignment disabled");
    }
    if left_buckets != right_buckets
        && (target as usize) < config.storage_partition_join_min_buckets()
    {
        return unchanged(&format!(
            "policy: effective_buckets={target} below minimum_buckets={}",
            config.storage_partition_join_min_buckets()
        ));
    }
    let scheme = format!("{prefix_left}:buckets={target}");
    let ids = left
        .group_ids()
        .iter()
        .chain(right.group_ids())
        .map(|id| normalized_id(id, target))
        .collect::<Result<BTreeSet<_>>>()?
        .into_iter()
        .collect::<Vec<_>>();
    let Some(new_left) = regroup(&join.left, &scheme, &ids, target)? else {
        return unchanged("fallback: left input cannot be regrouped");
    };
    let Some(new_right) = regroup(&join.right, &scheme, &ids, target)? else {
        return unchanged("fallback: right input cannot be regrouped");
    };
    if let Partitioning::Grouped(grouping) = new_left.output_partitioning() {
        record(
            &format!("selected: aligned {left_buckets}/{right_buckets} buckets"),
            Some(grouping),
        );
    }
    Ok(Transformed::yes(replace_children_if_necessary(
        plan,
        vec![new_left, new_right],
    )?))
}

fn normalized_id(id: &[u8], buckets: u32) -> Result<Vec<u8>> {
    if id.is_empty() {
        return Ok(bucket_group_id(None));
    }
    let bytes: [u8; 4] = id.try_into().map_err(|_| {
        datafusion::common::plan_datafusion_err!("Invalid Iceberg bucket group label")
    })?;
    Ok(bucket_group_id(Some(u32::from_be_bytes(bytes) % buckets)))
}

fn regroup(
    plan: &Arc<dyn ExecutionPlan>,
    scheme: &str,
    ids: &[Vec<u8>],
    buckets: u32,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(scan) = plan.downcast_ref::<DataSourceExec>() {
        let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>() else {
            return Ok(None);
        };
        let Some(Partitioning::Grouped(grouped)) = &config.output_partitioning else {
            return Ok(None);
        };
        // Per-partition row limits cannot move below a bucket merge: merging
        // first would select a different set of rows.
        if config.limit.is_some() {
            return Ok(None);
        }
        let slots: HashMap<_, _> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| (id.clone(), i))
            .collect();
        let mut files: Vec<FileGroup> =
            ids.iter().map(|_| FileGroup::default()).collect();
        for (id, group) in grouped.group_ids().iter().zip(&config.file_groups) {
            let index = slots[&normalized_id(id, buckets)?];
            for file in group.iter() {
                files[index].push(file.clone());
            }
        }
        let files = files
            .into_iter()
            .map(|group| {
                with_group_statistics(
                    group,
                    config.file_source().table_schema().table_schema(),
                )
            })
            .collect();
        let partitioning = Partitioning::Grouped(GroupedPartitioning::try_new(
            grouped.keys().to_vec(),
            scheme.to_owned(),
            ids.to_vec(),
        )?);
        // Concatenating old groups does not preserve cross-file sort order.
        let config = FileScanConfigBuilder::from(config.clone())
            .with_file_groups(files)
            .with_output_ordering(vec![])
            .with_output_partitioning(Some(partitioning))
            .build();
        return Ok(Some(DataSourceExec::from_data_source(config)));
    }
    let preserves = plan.is::<FilterExec>()
        || plan.is::<ProjectionExec>()
        || plan
            .downcast_ref::<SortExec>()
            .is_some_and(|sort| sort.preserve_partitioning() && sort.fetch().is_none());
    if !preserves {
        return Ok(None);
    }
    let children = plan.children();
    let Some(child) = regroup(children[0], scheme, ids, buckets)? else {
        return Ok(None);
    };
    Ok(Some(replace_children_if_necessary(
        Arc::clone(plan),
        vec![child],
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::aqe::planner::AdaptivePlanner;
    use ballista_core::config::{
        BALLISTA_HASH_JOIN_MAX_BUILD_PARTITION_BYTES, BALLISTA_STORAGE_PARTITION_JOIN,
        BALLISTA_STORAGE_PARTITION_JOIN_DIVISOR,
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
        divisor: bool,
        sort: bool,
        num_rows: Option<usize>,
    ) -> String {
        let config = SessionConfig::new_with_ballista()
            .with_target_partitions(16)
            .set_bool(BALLISTA_STORAGE_PARTITION_JOIN, true)
            .set_bool(BALLISTA_STORAGE_PARTITION_JOIN_DIVISOR, divisor)
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
    async fn singleton_scans_are_not_regrouped_on_non_bucket_keys() {
        use crate::state::task_builder::restrict_plan_to_partitions;
        use datafusion::catalog::TableProvider;
        use datafusion::physical_expr::{PhysicalExpr, expressions::Column};
        use datafusion::physical_plan::joins::{HashJoinExecBuilder, PartitionMode};

        let config = SessionConfig::new_with_ballista()
            .set_bool(BALLISTA_STORAGE_PARTITION_JOIN, true)
            .with_ballista_broadcast_join_threshold_bytes(0);
        let ctx = SessionContext::new_with_config(config.clone());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let mut scans = Vec::new();
        // Valid source-pruned layouts: one remaining bucket on each side.
        // Their storage key is id, while the join below compares v.
        for bucket in [3, 0] {
            let provider = BucketedParquetTable::try_new(
                schema.clone(),
                BucketedParquetManifest {
                    object_store_url: "file://".into(),
                    key: "id".into(),
                    buckets: 4,
                    files: vec![BucketFile {
                        path: format!("planning-bucket-{bucket}.parquet"),
                        size: 128,
                        bucket: Some(bucket),
                        num_rows: Some(1),
                    }],
                },
            )
            .unwrap();
            let scan = provider.scan(&ctx.state(), None, &[], None).await.unwrap();
            scans.push(restrict_plan_to_partitions(scan, &[bucket as usize]).unwrap());
        }
        let key = Arc::new(Column::new("v", 1)) as Arc<dyn PhysicalExpr>;
        let hash = HashJoinExecBuilder::new(
            scans[0].clone(),
            scans[1].clone(),
            vec![(key.clone(), key)],
            JoinType::Inner,
        )
        .with_partition_mode(PartitionMode::Partitioned)
        .build()
        .unwrap();
        let join = DynamicJoinSelectionExec::from_hash_join(&hash, 0).unwrap();
        let diagnostics = StorageJoinDiagnostics::default();
        let optimized = StoragePartitionJoinRule {
            diagnostics: Some(diagnostics.clone()),
        }
        .optimize(join, config.options())
        .unwrap();
        for (input, bucket) in optimized.children().into_iter().zip([3, 0]) {
            let Partitioning::Grouped(grouped) = input.output_partitioning() else {
                panic!("source grouping was lost");
            };
            assert_eq!(grouped.group_ids(), &[bucket_group_id(Some(bucket))]);
        }
        assert!(
            diagnostics
                .render()
                .contains("join keys do not match storage keys")
        );
    }

    #[tokio::test]
    async fn production_planner_preserves_storage_groups_before_exchanges() {
        let equal = planned(4, 4, false, false, Some(32)).await;
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
        let divisor = planned(8, 4, true, true, Some(32)).await;
        assert!(divisor.contains("SortMergeJoinExec"), "{divisor}");
        assert!(!divisor.contains("ExchangeExec"), "{divisor}");
        assert!(!divisor.contains("buckets=8"), "{divisor}");
        assert!(
            divisor.contains("selected: aligned 8/4 buckets"),
            "{divisor}"
        );
        assert!(
            divisor.contains("effective_groups=5, effective_buckets=4"),
            "{divisor}"
        );
        let unknown_size = planned(4, 4, false, true, None).await;
        assert!(unknown_size.contains("SortMergeJoinExec"), "{unknown_size}");
        assert!(!unknown_size.contains("ExchangeExec"), "{unknown_size}");
        let fallback = planned(8, 4, false, false, Some(32)).await;
        assert!(fallback.contains("ExchangeExec"), "{fallback}");
        assert!(
            fallback.contains("policy: divisor alignment disabled"),
            "{fallback}"
        );
        let incompatible = planned(6, 4, true, false, Some(32)).await;
        assert!(
            incompatible.contains("fallback: non-divisor bucket counts"),
            "{incompatible}"
        );
        assert!(incompatible.contains("ExchangeExec"), "{incompatible}");
    }
    #[tokio::test]
    async fn tiny_storage_build_keeps_broadcast_and_probe_buckets() {
        let config = SessionConfig::new_with_ballista()
            .with_target_partitions(16)
            .set_bool(BALLISTA_STORAGE_PARTITION_JOIN, true)
            .set_bool(BALLISTA_STORAGE_PARTITION_JOIN_DIVISOR, true)
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
    #[tokio::test]
    async fn divisor_policy_can_preserve_join_parallelism() {
        use ballista_core::config::BALLISTA_STORAGE_PARTITION_JOIN_MIN_BUCKETS;
        let config = SessionConfig::new_with_ballista()
            .with_target_partitions(16)
            .set_bool(BALLISTA_STORAGE_PARTITION_JOIN, true)
            .set_bool(BALLISTA_STORAGE_PARTITION_JOIN_DIVISOR, true)
            .set_u64(BALLISTA_STORAGE_PARTITION_JOIN_MIN_BUCKETS, 8)
            .with_ballista_broadcast_join_threshold_bytes(0);
        let ctx = SessionContext::new_with_config(config);
        ctx.register_table("a", source(8, Some(32))).unwrap();
        ctx.register_table("b", source(4, Some(32))).unwrap();
        let logical = ctx
            .sql("select a.id from a join b on a.id=b.id")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        let explained = AdaptivePlanner::explain(&ctx, &logical, "parallel_spj".into())
            .await
            .unwrap();
        assert!(explained.contains("ExchangeExec"), "{explained}");
        assert!(explained.contains("buckets=8"), "{explained}");
        assert!(
            explained.contains("policy: effective_buckets=4 below minimum_buckets=8"),
            "{explained}"
        );
    }
}
