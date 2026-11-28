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

//! Add runtime filters only to executable stage copies. The AQE tree stays
//! untouched so scan recognition, join selection and stage boundaries do not
//! depend on whether runtime filtering is enabled.

use super::execution_plan::{
    AdaptiveDatafusionExec, DynamicJoinSelectionExec, ExchangeExec,
};
use super::optimizer_rule::SelectJoinRule;
use ballista_core::execution_plans::runtime_filter::supports_key;
use ballista_core::execution_plans::{RuntimeFilterBuildExec, ShuffleReaderExec};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{JoinType, Result, internal_err};
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::{PhysicalExpr, expressions::Column};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, replace_children_if_necessary};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

#[derive(Clone)]
struct Filter {
    id: u64,
    key: Arc<dyn PhysicalExpr>,
}

/// A scan occurrence in an executable stage. Counting occurrences keeps the
/// two sides of a self-join distinct even if they share the same scan Arc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProbeBinding {
    pub scan_index: usize,
    pub filter_id: u64,
    pub column: usize,
}

#[derive(Clone, Default)]
pub(super) struct RuntimeFilterPlanner {
    builds: BTreeMap<(usize, usize), Filter>,
    probes: BTreeMap<usize, Vec<Filter>>,
    planned: HashSet<usize>,
}

impl RuntimeFilterPlanner {
    /// Discover summaries before their producer stages are submitted. A pending
    /// join may still swap sides, so only the final join binds a scan consumer.
    /// Independent shuffle consumers can be bound now: filtering either input
    /// is safe for the supported join types, even if the join later swaps.
    pub fn discover(&mut self, plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
        plan.apply(|node| {
            if node
                .downcast_ref::<ExchangeExec>()
                .is_some_and(|e| e.shuffle_created())
            {
                return Ok(TreeNodeRecursion::Jump);
            }
            let (left, right, on, kind, null_aware) = if let Some(join) =
                node.downcast_ref::<HashJoinExec>()
            {
                (
                    join.left(),
                    join.right(),
                    join.on(),
                    *join.join_type(),
                    join.null_aware,
                )
            } else if let Some(join) = node.downcast_ref::<DynamicJoinSelectionExec>() {
                (
                    &join.left,
                    &join.right,
                    join.on.as_slice(),
                    join.join_type,
                    join.null_aware,
                )
            } else {
                return Ok(TreeNodeRecursion::Continue);
            };
            if supported_join(kind, null_aware)
                && let Some((left_key, right_key)) = on.first()
            {
                if left.is::<ExchangeExec>() {
                    self.discover_pair(left, right, left_key, right_key)?;
                } else if node.is::<DynamicJoinSelectionExec>() {
                    self.discover_pair(right, left, right_key, left_key)?;
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }

    fn discover_pair(
        &mut self,
        build: &Arc<dyn ExecutionPlan>,
        probe: &Arc<dyn ExecutionPlan>,
        build_key: &Arc<dyn PhysicalExpr>,
        probe_key: &Arc<dyn PhysicalExpr>,
    ) -> Result<()> {
        let Some(exchange) = build.downcast_ref::<ExchangeExec>() else {
            return Ok(());
        };
        let Some(stage) = exchange.stage_id() else {
            return Ok(());
        };
        let Some(column) = build_key.downcast_ref::<Column>() else {
            return Ok(());
        };
        let data_type = build_key.data_type(build.schema().as_ref())?;
        if !supports_key(&data_type)
            || data_type != probe_key.data_type(probe.schema().as_ref())?
        {
            return Ok(());
        }
        let probe_exchange = probe.downcast_ref::<ExchangeExec>();
        let (probe_input, probe_stage) = if let Some(probe) = probe_exchange {
            let Some(id) = probe.stage_id() else {
                return Ok(());
            };
            // Both boundaries have been made runnable in this pass. Never
            // wait for a future stage, or create a boundary to obtain a filter.
            if probe.shuffle_created() || self.planned.contains(&id) {
                return Ok(());
            }
            (probe.input().clone(), Some(id))
        } else {
            (probe.clone(), None)
        };
        if probe_column(
            probe_input.as_ref(),
            probe_key.as_ref(),
            probe_stage.is_some(),
        )
        .is_none()
        {
            return Ok(());
        }
        let producer_key = (stage, column.index());
        // A fallback probe can be created after its producer completed. Reuse
        // that producer's existing summary; only new taps need an unplanned stage.
        if !self.builds.contains_key(&producer_key) {
            if exchange.shuffle_created() || self.planned.contains(&stage) {
                return Ok(());
            }
            // Unfinished exchanges have unknown statistics. Compare their inputs
            // to avoid collecting a known larger prospective build side.
            if SelectJoinRule::supports_swap_join_order(
                exchange.input().as_ref(),
                probe_input.as_ref(),
            )? {
                return Ok(());
            }
            self.builds.insert(
                producer_key,
                Filter {
                    id: self.builds.len() as u64,
                    key: build_key.clone(),
                },
            );
        }
        let filter = &self.builds[&producer_key];
        if let Some(probe_stage) = probe_stage {
            let probes = self.probes.entry(probe_stage).or_default();
            if !probes.iter().any(|p| p.id == filter.id) {
                probes.push(Filter {
                    id: filter.id,
                    key: probe_key.clone(),
                });
            }
        }
        Ok(())
    }

    pub fn instrument(
        &mut self,
        boundary: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let stage_id = if let Some(exchange) = boundary.downcast_ref::<ExchangeExec>() {
            exchange.stage_id()
        } else if let Some(adaptive) = boundary.downcast_ref::<AdaptiveDatafusionExec>() {
            adaptive.stage_id()
        } else {
            return internal_err!(
                "Unexpected runtime filter stage boundary: {}",
                boundary.name()
            );
        };
        let Some(stage) = stage_id else {
            return internal_err!(
                "Missing stage ID for runtime filter boundary {}",
                boundary.name()
            );
        };
        self.planned.insert(stage);
        let mut input = boundary.children()[0].clone();
        for (_, filter) in self.builds.range((stage, 0)..=(stage, usize::MAX)) {
            input = Arc::new(RuntimeFilterBuildExec::new(
                input,
                filter.id,
                filter.key.clone(),
            ));
        }
        replace_children_if_necessary(boundary, vec![input])
    }

    pub fn probe_bindings(
        &self,
        stage: usize,
        writer: &dyn ExecutionPlan,
    ) -> Vec<ProbeBinding> {
        let input = writer.children()[0];
        let mut bindings = vec![];
        for filter in self.probes.get(&stage).into_iter().flatten() {
            if let Some(column) = probe_column(input.as_ref(), filter.key.as_ref(), true)
            {
                bindings.push(ProbeBinding {
                    scan_index: 0,
                    filter_id: filter.id,
                    column,
                });
            }
        }
        let mut next_scan = 0;
        self.collect_join_probes(input.as_ref(), &mut next_scan, &mut bindings);
        bindings
    }

    fn collect_join_probes(
        &self,
        node: &dyn ExecutionPlan,
        next_scan: &mut usize,
        bindings: &mut Vec<ProbeBinding>,
    ) {
        if node.is::<DataSourceExec>() {
            *next_scan += 1;
            return;
        }
        if let Some(join) = node.downcast_ref::<HashJoinExec>()
            && supported_join(*join.join_type(), join.null_aware)
            && let Some(reader) = join.left().downcast_ref::<ShuffleReaderExec>()
            && let Some((left, right)) = join.on().first()
            && let Some(build_key) = left.downcast_ref::<Column>()
            && let Some(filter) = self.builds.get(&(reader.stage_id, build_key.index()))
            && let Some(column) =
                probe_column(join.right().as_ref(), right.as_ref(), false)
        {
            // The build reader is a leaf and the eligible probe is unary.
            // Its scan is therefore the next scan visited in this stage.
            bindings.push(ProbeBinding {
                scan_index: *next_scan,
                filter_id: filter.id,
                column,
            });
        }
        for child in node.children() {
            self.collect_join_probes(child.as_ref(), next_scan, bindings);
        }
    }
}

fn supported_join(kind: JoinType, null_aware: bool) -> bool {
    !null_aware
        && matches!(
            kind,
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
        )
}

/// Find a scan key without constructing a speculative plan. Only cross
/// value-preserving unary operators; a predicate or shuffle must benefit.
fn probe_column(
    mut plan: &dyn ExecutionPlan,
    key: &dyn PhysicalExpr,
    mut useful: bool,
) -> Option<usize> {
    let mut column = key.downcast_ref::<Column>()?.index();
    loop {
        if let Some(scan) = plan.downcast_ref::<DataSourceExec>() {
            // An ordinary filter can already be inside the Parquet reader.
            let filtered_parquet = scan
                .data_source()
                .downcast_ref::<FileScanConfig>()
                .is_some_and(|files| {
                    files.limit.is_none()
                        && files.file_source.downcast_ref::<ParquetSource>().is_some()
                        && files.file_source.filter().is_some()
                });
            return (useful || filtered_parquet).then_some(column);
        }
        // LIMIT may be fused into a filter or coalesce. Never change which
        // rows it selects by inserting a runtime filter below it.
        if plan.fetch().is_some() {
            return None;
        }
        if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
            column = projection.expr()[column]
                .expr
                .downcast_ref::<Column>()?
                .index();
            plan = projection.input().as_ref();
        } else if let Some(filter) = plan.downcast_ref::<FilterExec>() {
            column = filter.projection().as_ref().map_or(column, |p| p[column]);
            useful = true;
            plan = filter.input().as_ref();
        } else if let Some(build) = plan.downcast_ref::<RuntimeFilterBuildExec>() {
            plan = build.children()[0].as_ref();
        } else if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
            plan = repartition.input().as_ref();
        } else {
            let coalesce = plan.downcast_ref::<CoalescePartitionsExec>()?;
            plan = coalesce.input().as_ref();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::{col, lit};
    use datafusion::physical_plan::filter::FilterExecBuilder;
    use datafusion::physical_plan::{displayable, joins::PartitionMode};

    fn source(values: impl IntoIterator<Item = i64>) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(values))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    #[test]
    fn runtime_filter_skips_larger_producer_behind_unfinished_exchange() {
        let exchange = Arc::new(ExchangeExec::new(source(0..1000), None, 0));
        exchange.set_stage_id(0);
        let probe = Arc::new(FilterExec::try_new(lit(true), source(0..2)).unwrap());
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let join: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                exchange.clone(),
                probe,
                vec![(key.clone(), key)],
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        );
        let mut filters = RuntimeFilterPlanner::default();
        filters.discover(&join).unwrap();
        let stage = filters.instrument(exchange).unwrap();
        assert!(
            !displayable(stage.as_ref())
                .indent(false)
                .to_string()
                .contains("RuntimeFilterBuildExec"),
            "the exchange has unknown output statistics, but its input is already known to exceed the probe"
        );
    }

    #[test]
    fn runtime_filter_maps_keys_through_scan_projections() {
        let batch = RecordBatch::try_from_iter([
            (
                "value",
                Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
            ),
            ("k", Arc::new(Int64Array::from(vec![0, 1, 2])) as ArrayRef),
        ])
        .unwrap();
        let scan = MemorySourceConfig::try_new_exec(
            &[vec![batch.clone()]],
            batch.schema(),
            None,
        )
        .unwrap();
        let filter = FilterExecBuilder::new(lit(true), scan)
            .apply_projection(Some(vec![1, 0]))
            .unwrap()
            .build()
            .unwrap();
        let schema = filter.schema();
        let probe = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (col("value", &schema).unwrap(), "value".into()),
                    (col("k", &schema).unwrap(), "join_key".into()),
                ],
                Arc::new(filter),
            )
            .unwrap(),
        );
        let key = Column::new("join_key", 1);
        assert_eq!(probe_column(probe.as_ref(), &key, false), Some(1));
    }

    #[test]
    fn runtime_filter_preserves_limits_fused_into_unary_operators() {
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let probes: [Arc<dyn ExecutionPlan>; 2] = [
            Arc::new(
                FilterExecBuilder::new(lit(true), source(0..4))
                    .with_fetch(Some(2))
                    .build()
                    .unwrap(),
            ),
            Arc::new(CoalescePartitionsExec::new(source(0..4)).with_fetch(Some(2))),
        ];
        for probe in probes {
            // The limited input is [0, 1]. Filtering before its limit would
            // instead retain key 2 and incorrectly add a row to the join.
            assert!(probe_column(probe.as_ref(), key.as_ref(), true).is_none());
        }
    }
}
