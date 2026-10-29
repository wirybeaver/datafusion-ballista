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

//! Choose the smaller known build input before planting taps. AQE can still
//! choose the join algorithm later; the existing shuffles deliver the reports.

use super::SelectJoinRule;
use crate::state::aqe::execution_plan::ExchangeExec;
use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::runtime_filter::supports_key;
use ballista_core::execution_plans::{RuntimeFilterApplyExec, RuntimeFilterBuildExec};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{JoinType, Result};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::{PhysicalExpr, expressions::Column};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, replace_children_if_necessary};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
pub struct RuntimeFilterRule {
    ids: Arc<AtomicUsize>,
}

impl RuntimeFilterRule {
    pub fn new(ids: Arc<AtomicUsize>) -> Self {
        Self { ids }
    }
}

impl PhysicalOptimizerRule for RuntimeFilterRule {
    fn name(&self) -> &str {
        "RuntimeFilterRule"
    }
    fn schema_check(&self) -> bool {
        true
    }
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = config
            .extensions
            .get::<BallistaConfig>()
            .cloned()
            .unwrap_or_default();
        if !config.runtime_filter_enabled() {
            return Ok(plan);
        }
        // swap_inputs may wrap an inner join in a schema-restoring projection.
        // Normalize first, then visit the resulting joins to attach the taps.
        let plan = plan
            .transform_up(|node| {
                let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                    return Ok(Transformed::no(node));
                };
                if supports_join(join)
                    && SelectJoinRule::supports_swap_join_order(
                        join.left().as_ref(),
                        join.right().as_ref(),
                    )?
                {
                    Ok(Transformed::yes(join.swap_inputs(*join.partition_mode())?))
                } else {
                    Ok(Transformed::no(node))
                }
            })?
            .data;
        plan.transform_up(|node| {
            let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            if !supports_join(join) {
                return Ok(Transformed::no(node));
            }
            let Some((left_key, right_key)) = join.on().first() else {
                return Ok(Transformed::no(node));
            };
            let data_type = left_key.data_type(join.left().schema().as_ref())?;
            if !supports_key(&data_type)
                || data_type != right_key.data_type(join.right().schema().as_ref())?
                || !left_key.is::<Column>()
                || !right_key.is::<Column>()
            {
                return Ok(Transformed::no(node));
            }
            let id = self.ids.fetch_add(1, Ordering::Relaxed);
            let build: Arc<dyn ExecutionPlan> = Arc::new(RuntimeFilterBuildExec::new(
                join.left().clone(),
                id as u64,
                left_key.clone(),
            ));
            // Let the existing AQE/distribution rules choose the shuffle
            // boundary. Adding one here would serialize otherwise independent
            // probe stages even when the filter wait is disabled.
            let probe = push_apply(join.right().clone(), id as u64, right_key.clone())?;
            Ok(Transformed::yes(replace_children_if_necessary(
                node,
                vec![build, probe],
            )?))
        })
        .map(|t| t.data)
    }
}

fn supports_join(join: &HashJoinExec) -> bool {
    !join.null_aware
        && matches!(
            join.join_type(),
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
        )
}

// Move only through operators that preserve key values and do not select a
// limited row set. In particular, never cross LIMIT, aggregates or other joins.
fn push_apply(
    plan: Arc<dyn ExecutionPlan>,
    id: u64,
    key: Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>()
        && let Some(column) = key.downcast_ref::<Column>()
    {
        let projected = &projection.expr()[column.index()].expr;
        if projected.is::<Column>() {
            let child = push_apply(projection.input().clone(), id, projected.clone())?;
            return replace_children_if_necessary(plan, vec![child]);
        }
    }
    let pass = plan.is::<FilterExec>()
        || plan.is::<RepartitionExec>()
        || plan.is::<CoalescePartitionsExec>()
        || plan.is::<ExchangeExec>()
        || plan
            .downcast_ref::<SortExec>()
            .is_some_and(|sort| sort.fetch().is_none());
    if pass {
        let child = push_apply(plan.children()[0].clone(), id, key)?;
        return replace_children_if_necessary(plan, vec![child]);
    }
    Ok(Arc::new(RuntimeFilterApplyExec::try_new(
        plan, id, key, None,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::aqe::execution_plan::{
        DynamicJoinSelectionExec, JoinSelectionAction,
    };
    use ballista_core::extension::SessionConfigExt;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion::prelude::SessionConfig;

    fn source(name: &str, rows: i64) -> Arc<dyn ExecutionPlan> {
        let schema =
            Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..rows))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    #[test]
    fn runtime_filter_preserves_small_build_selection_after_swap() {
        let (large, small) = (source("large_key", 1000), source("small_key", 2));
        let join: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                large.clone(),
                small.clone(),
                vec![(
                    Arc::new(Column::new("large_key", 0)),
                    Arc::new(Column::new("small_key", 0)),
                )],
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        );
        let config = SessionConfig::new_with_ballista()
            .set_bool("ballista.planner.runtime_filter.enabled", true);
        let plan = RuntimeFilterRule::new(Arc::new(AtomicUsize::new(0)))
            .optimize(join.clone(), config.options())
            .unwrap();
        assert_eq!(
            plan.schema(),
            join.schema(),
            "swap must preserve output order"
        );
        let mut checked = false;
        plan.apply(|node| {
            if let Some(join) = node.downcast_ref::<HashJoinExec>() {
                let build = join
                    .left()
                    .downcast_ref::<RuntimeFilterBuildExec>()
                    .unwrap();
                let probe = join
                    .right()
                    .downcast_ref::<RuntimeFilterApplyExec>()
                    .unwrap();
                assert!(Arc::ptr_eq(build.children()[0], &small));
                assert!(Arc::ptr_eq(probe.children()[0], &large));
                let stats =
                    datafusion::physical_plan::statistics::StatisticsContext::new()
                        .compute(
                            join.right().as_ref(),
                            &datafusion::physical_plan::StatisticsArgs::new(),
                        )?;
                assert_eq!(
                    stats.num_rows,
                    datafusion::common::stats::Precision::Exact(1000),
                    "an unpopulated filter must preserve probe statistics"
                );
                let dynamic = DynamicJoinSelectionExec::from_hash_join(join, 10)?;
                assert!(matches!(
                    dynamic.to_actual_join(config.options())?,
                    JoinSelectionAction::CollectLeft(_)
                ));
                checked = true;
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        assert!(checked);
    }
}
