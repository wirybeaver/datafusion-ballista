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

//! Job-scoped runtime filter hub. Keeping it in the execution graph gives
//! `(job_id, filter_id)` isolation and ties cleanup to the job's existing lifetime.
//! Reports stay in TaskInfo: task/stage retry and executor-loss handling already
//! invalidate those statuses, so a separate partial-report cache cannot go stale.

use super::runtime_filter_planner::ProbeBinding;
use crate::physical_optimizer::filter_pushdown::FilterPushdown;
use crate::state::execution_stage::ExecutionStage;
use ballista_core::execution_plans::runtime_filter::RuntimeFilter;
use ballista_core::execution_plans::runtime_filter::RuntimeFilterBloomExpr;
use ballista_core::execution_plans::{RuntimeFilterApplyExec, RuntimeFilterBuildExec};
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::{RuntimeFilterPayload, task_status};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetSource};
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::physical_expr::expressions::{BinaryExpr, InListExpr, IsNullExpr, lit};
use datafusion::physical_expr::{PhysicalExpr, conjunction, split_conjunction};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::filter_pushdown::PushedDown;
use datafusion::prelude::SessionConfig;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub(super) struct RuntimeFilterRegistry {
    producers: HashMap<u64, usize>,
    // Consumed once before dispatch; retries keep the specialized plan.
    probes: HashMap<usize, Vec<ProbeBinding>>,
}

impl RuntimeFilterRegistry {
    pub fn register(
        &mut self,
        stage_id: usize,
        plan: &dyn ExecutionPlan,
        probes: Vec<ProbeBinding>,
    ) {
        if !probes.is_empty() {
            self.probes.insert(stage_id, probes);
        }
        self.register_builds(stage_id, plan);
    }

    fn register_builds(&mut self, stage_id: usize, plan: &dyn ExecutionPlan) {
        if let Some(build) = plan.downcast_ref::<RuntimeFilterBuildExec>() {
            self.producers.insert(build.filter_id, stage_id);
        }
        for child in plan.children() {
            self.register_builds(stage_id, child.as_ref());
        }
    }

    /// Freeze available complete filters before this stage's first dispatch.
    /// Bypass missing, unfinished, and nearly saturated filters.
    pub fn prepare(
        &mut self,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        stages: &HashMap<usize, ExecutionStage>,
        config: &SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Binding is attempted once, including when the caller falls back
        // after an invalid report. Later tasks must keep the same stage plan.
        let Some(bindings) = self.probes.remove(&stage_id) else {
            return Ok(plan);
        };
        let mut next_scan = 0;
        let mut probe_scans = Vec::new();
        let plan = plan
            .transform_up(|node| {
                // Let DataFusion move eligible ordinary predicates down the
                // same probe pipeline, preserving projection and limit semantics.
                if let Some(parent) = node.downcast_ref::<FilterExec>()
                    && leads_to_probe_scan(parent.input(), &probe_scans)
                    && !parent
                        .predicate()
                        .exists(|expr| Ok(expr.is_volatile_node()))?
                {
                    let plan = FilterPushdown::new().optimize(node, config.options())?;
                    return Ok(Transformed::yes(order_reader_predicates(
                        plan,
                        false,
                        &mut probe_scans,
                    )?));
                }
                if !node.is::<DataSourceExec>() {
                    return Ok(Transformed::no(node));
                }
                let scan_index = next_scan;
                next_scan += 1;
                let mut scan = node.clone();
                let mut fallback = Vec::new();
                for binding in bindings
                    .iter()
                    .filter(|probe| probe.scan_index == scan_index)
                {
                    let Some(payload) = completed_filter(
                        binding.filter_id,
                        self.producers
                            .get(&binding.filter_id)
                            .and_then(|id| stages.get(id)),
                    )?
                    else {
                        continue;
                    };
                    let key: Arc<dyn PhysicalExpr> =
                        Arc::new(datafusion::physical_expr::expressions::Column::new(
                            scan.schema().field(binding.column).name(),
                            binding.column,
                        ));
                    if let Some(reader) =
                        push_into_parquet(&scan, &key, &payload, config)?
                    {
                        scan = order_reader_predicates(reader, true, &mut probe_scans)?;
                    } else {
                        fallback.push((key, payload));
                    }
                }
                for (key, payload) in fallback {
                    scan = Arc::new(RuntimeFilterApplyExec::try_new(
                        scan,
                        payload.filter_id,
                        key,
                        Some(payload),
                    )?);
                }
                Ok(if Arc::ptr_eq(&scan, &node) {
                    Transformed::no(node)
                } else {
                    Transformed::yes(scan)
                })
            })?
            .data;
        Ok(plan)
    }
}

fn leads_to_probe_scan(
    mut input: &Arc<dyn ExecutionPlan>,
    scans: &[Arc<dyn ExecutionPlan>],
) -> bool {
    while !scans.is_empty() {
        if scans.iter().any(|scan| Arc::ptr_eq(scan, input)) {
            return true;
        }
        let children = input.children();
        let [child] = children.as_slice() else { break };
        input = *child;
    }
    false
}

fn push_into_parquet(
    child: &Arc<dyn ExecutionPlan>,
    key: &Arc<dyn PhysicalExpr>,
    payload: &RuntimeFilterPayload,
    config: &SessionConfig,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(exec) = child.downcast_ref::<DataSourceExec>() else {
        return Ok(None);
    };
    let Some(files) = exec.data_source().downcast_ref::<FileScanConfig>() else {
        return Ok(None);
    };
    let Some(parquet) = files.file_source.downcast_ref::<ParquetSource>() else {
        return Ok(None);
    };
    let reader_schema = files
        .file_source
        .table_schema()
        .schema_without_virtual_columns();
    if files.limit.is_some()
        || files
            .file_source
            .filter()
            .map(|p| p.exists(|e| {
                Ok(e.is_volatile_node() || e.downcast_ref::<datafusion::physical_expr::expressions::Column>()
                    .is_some_and(|col| reader_schema.index_of(col.name()).is_err()))
            }))
            .transpose()?
            .unwrap_or(false)
    {
        return Ok(None);
    }
    // Only this scan opts into decoding predicates. FileScanConfig handles key
    // remapping for projected scans and reports any unsupported predicates.
    let Some(predicate) = probe_predicate(key, payload, child.schema().as_ref(), config)?
    else {
        return Ok(None);
    };
    let mut files = files.clone();
    files.file_source = Arc::new(
        parquet
            .clone()
            .with_pushdown_filters(true)
            .with_reorder_filters(false),
    );
    let result = files.try_pushdown_filters(vec![predicate], config.options())?;
    match (result.filters.as_slice(), result.updated_node) {
        ([PushedDown::Yes], Some(source)) => {
            Ok(Some(Arc::new(exec.clone().with_data_source(source))))
        }
        _ => Ok(None),
    }
}

fn probe_predicate(
    key: &Arc<dyn PhysicalExpr>,
    payload: &RuntimeFilterPayload,
    schema: &datafusion::arrow::datatypes::Schema,
    config: &SessionConfig,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    let predicate: Arc<dyn PhysicalExpr> =
        if payload.has_in_set && payload.in_values.len() == payload.in_hashes.len() {
            let values = payload
                .in_values
                .iter()
                .map(|value| Ok(ScalarValue::try_from(value)?))
                .collect::<Result<Vec<_>>>()?;
            if values.is_empty() {
                if payload.has_null {
                    Arc::new(IsNullExpr::new(key.clone()))
                } else {
                    lit(false)
                }
            } else {
                let exact = Arc::new(InListExpr::try_new(
                    key.clone(),
                    values.into_iter().map(lit).collect(),
                    false,
                    schema,
                )?);
                if payload.has_null {
                    Arc::new(BinaryExpr::new(
                        exact,
                        datafusion::logical_expr::Operator::Or,
                        Arc::new(IsNullExpr::new(key.clone())),
                    ))
                } else {
                    exact
                }
            }
        } else {
            // Every complete summary carries Bloom bits, even when exact literals were omitted.
            let bloom: Arc<dyn PhysicalExpr> = Arc::new(RuntimeFilterBloomExpr::try_new(
                key.clone(),
                payload.clone(),
            )?);
            if datafusion_proto::physical_plan::to_proto::serialize_physical_expr(
                &bloom,
                config.ballista_physical_extension_codec().as_ref(),
            )
            .is_err()
            {
                return Ok(None);
            }
            bloom
        };

    Ok(Some(predicate))
}

fn order_reader_predicates(
    plan: Arc<dyn ExecutionPlan>,
    added_runtime_predicate: bool,
    probe_scans: &mut Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        let Some(exec) = node.downcast_ref::<DataSourceExec>() else {
            return Ok(Transformed::no(node));
        };
        let files = exec.data_source().downcast_ref::<FileScanConfig>().unwrap();
        let parquet = files.file_source.downcast_ref::<ParquetSource>().unwrap();
        let Some(predicate) = files.file_source.filter() else {
            return Ok(Transformed::no(node));
        };
        let mut predicates = split_conjunction(&predicate);
        // ParquetSource appends the remapped runtime predicate to the scan's
        // existing conjunction. Give a new exact set first use of the rows.
        if added_runtime_predicate
            && predicates
                .last()
                .is_some_and(|p| !p.is::<RuntimeFilterBloomExpr>())
        {
            predicates.rotate_right(1);
        }
        // Stable ordering keeps SQL predicates (including SQL IN lists) in
        // their original order and also handles a later ordinary pushdown.
        predicates.sort_by_key(|p| p.is::<RuntimeFilterBloomExpr>());
        let mut seen = HashSet::new();
        let predicates = predicates
            .into_iter()
            .filter(|p| seen.insert((*p).clone()))
            .cloned();
        let mut files = files.clone();
        files.file_source = Arc::new(parquet.with_predicate(conjunction(predicates)));
        let scan: Arc<dyn ExecutionPlan> =
            Arc::new(exec.clone().with_data_source(Arc::new(files)));
        probe_scans.push(scan.clone());
        Ok(Transformed::yes(scan))
    })
    .map(|t| t.data)
}

fn completed_filter(
    id: u64,
    stage: Option<&ExecutionStage>,
) -> Result<Option<RuntimeFilterPayload>> {
    let tasks = match stage {
        Some(ExecutionStage::Successful(stage)) => &stage.task_infos,
        Some(ExecutionStage::Running(stage)) if stage.is_successful() => {
            &stage.task_infos
        }
        _ => return Ok(None),
    };
    let mut merged = RuntimeFilter::default();
    for task in tasks {
        // Existing retry/loss handling invalidates old successful slices.
        if let task_status::Status::Successful(success) = &task.task_status {
            let Some(report) = success.runtime_filters.iter().find(|p| p.filter_id == id)
            else {
                return Ok(None);
            };
            merged.merge(&RuntimeFilter::from_proto(report)?);
        }
    }
    // Decide once before serializing a payload for the probe tasks.
    if merged.is_nearly_saturated() {
        return Ok(None);
    }
    merged.to_proto(id).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::execution_graph::create_task_info;
    use crate::state::execution_stage::{RunningStage, TaskInfo};
    use ballista_core::execution_plans::runtime_filter::collect_reports;
    use ballista_core::serde::protobuf::SuccessfulTask;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn source(values: impl IntoIterator<Item = i64>) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(values))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    async fn report(values: impl IntoIterator<Item = i64>) -> RuntimeFilterPayload {
        let build: Arc<dyn ExecutionPlan> = Arc::new(RuntimeFilterBuildExec::new(
            source(values),
            3,
            Arc::new(Column::new("k", 0)),
        ));
        collect(build.clone(), SessionContext::new().task_ctx())
            .await
            .unwrap();
        collect_reports(&build).unwrap().pop().unwrap()
    }

    #[tokio::test]
    async fn runtime_filter_orders_join_predicates_around_existing_scan_filters() {
        use datafusion::datasource::physical_plan::FileScanConfigBuilder;
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion::logical_expr::Operator;

        let schema = source([]).schema();
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let range: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(key.clone(), Operator::Gt, lit(0_i64)));
        let sql_in: Arc<dyn PhysicalExpr> = Arc::new(
            InListExpr::try_new(
                key.clone(),
                vec![lit(2_i64), lit(3_i64)],
                false,
                &schema,
            )
            .unwrap(),
        );
        let parquet = ParquetSource::new(schema.clone())
            .with_pushdown_filters(true)
            .with_predicate(conjunction([range.clone(), sql_in.clone()]));
        let files = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(parquet),
        )
        .build();
        let mut scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(files);
        let config = SessionConfig::new();
        let mut small = report([2, 98]).await;
        small.filter_id = 4;
        let expected_in = probe_predicate(&key, &small, &schema, &config)
            .unwrap()
            .unwrap();
        for payload in [report(0..160).await, small] {
            let reader = push_into_parquet(&scan, &key, &payload, &config)
                .unwrap()
                .unwrap();
            scan = order_reader_predicates(reader, true, &mut vec![]).unwrap();
        }
        let exec = scan.downcast_ref::<DataSourceExec>().unwrap();
        let files = exec.data_source().downcast_ref::<FileScanConfig>().unwrap();
        let predicate = files.file_source.filter().unwrap();
        let expressions = split_conjunction(&predicate);
        assert_eq!(expressions.len(), 4);
        assert_eq!(&expressions[..3], [&expected_in, &range, &sql_in]);
        assert!(expressions[3].is::<RuntimeFilterBloomExpr>());
    }

    fn task(
        id: usize,
        partition: usize,
        payload: Option<RuntimeFilterPayload>,
    ) -> TaskInfo {
        let mut task = create_task_info("e".into(), id);
        task.global_input_partition_ids = vec![partition];
        if let Some(payload) = payload {
            task.task_status = task_status::Status::Successful(SuccessfulTask {
                runtime_filters: vec![payload],
                ..Default::default()
            });
        }
        task
    }

    async fn keys(plan: Arc<dyn ExecutionPlan>) -> Vec<i64> {
        let batches = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect()
    }

    #[tokio::test]
    async fn runtime_filter_hub_uses_only_complete_current_attempts() {
        let build: Arc<dyn ExecutionPlan> = Arc::new(RuntimeFilterBuildExec::new(
            source([]),
            3,
            Arc::new(Column::new("k", 0)),
        ));
        let mut stage = RunningStage::new(
            1,
            0,
            build,
            2,
            vec![],
            HashMap::new(),
            Arc::new(SessionConfig::new()),
        );
        stage.pending.next_slice(2);
        stage.task_infos = vec![task(0, 0, Some(report([1]).await)), task(1, 1, None)];
        let mut stages = HashMap::from([(1, ExecutionStage::Running(stage))]);
        let probe = source([1, 2, 3, 4]);
        let mut hub = RuntimeFilterRegistry::default();
        hub.register(1, stages[&1].plan(), vec![]);
        let binding = ProbeBinding {
            scan_index: 0,
            filter_id: 3,
            column: 0,
        };
        hub.register(2, probe.as_ref(), vec![binding.clone()]);
        let bypass = hub
            .prepare(2, probe.clone(), &stages, &Default::default())
            .unwrap();
        assert!(Arc::ptr_eq(&bypass, &probe));
        assert_eq!(keys(bypass.clone()).await, [1, 2, 3, 4]);
        let ExecutionStage::Running(stage) = stages.get_mut(&1).unwrap() else {
            unreachable!()
        };
        // Drop the lost attempt's key 1; only replacement slices may contribute.
        stage.reset_task_info(0);
        stage.task_infos[1] = task(1, 1, Some(report([2]).await));
        stage.pending.next_slice(1);
        stage.task_infos.push(task(2, 0, Some(report([3]).await)));
        // A stage already dispatched without a filter stays unchanged.
        let frozen = hub
            .prepare(2, bypass, &stages, &Default::default())
            .unwrap();
        assert_eq!(keys(frozen).await, [1, 2, 3, 4]);
        // Only the targeted occurrence of a shared scan should be filtered.
        let union = datafusion::physical_plan::union::UnionExec::try_new(vec![
            probe.clone(),
            probe.clone(),
        ])
        .unwrap();
        hub.register(
            3,
            union.as_ref(),
            vec![ProbeBinding {
                scan_index: 1,
                ..binding
            }],
        );
        let plan = hub.prepare(3, union, &stages, &Default::default()).unwrap();
        assert!(Arc::ptr_eq(plan.children()[0], &probe));
        assert_eq!(keys(plan.children()[1].clone()).await, [2, 3]);
    }

    #[tokio::test]
    async fn runtime_filter_hub_skips_a_nearly_saturated_payload() {
        let payload = report(0..120_000).await;
        assert!(!RuntimeFilter::from_proto(&payload).unwrap().is_saturated());
        let mut stage = RunningStage::new(
            1,
            0,
            source([]),
            1,
            vec![],
            HashMap::new(),
            Arc::new(SessionConfig::new()),
        );
        stage.pending.next_slice(1);
        stage.task_infos = vec![task(0, 0, Some(payload))];

        assert!(
            completed_filter(3, Some(&ExecutionStage::Running(stage)))
                .unwrap()
                .is_none()
        );
    }
}
