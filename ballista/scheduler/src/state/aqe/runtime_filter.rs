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

use crate::state::execution_stage::ExecutionStage;
use ballista_core::execution_plans::runtime_filter::RuntimeFilter;
use ballista_core::execution_plans::{RuntimeFilterApplyExec, RuntimeFilterBuildExec};
use ballista_core::serde::protobuf::{RuntimeFilterPayload, task_status};
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::ExecutionPlan;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Clone, Default)]
pub(super) struct RuntimeFilterRegistry {
    wait: Duration,
    producers: HashMap<u64, usize>,
    waiting: HashMap<usize, FilterWait>,
    prepared: HashSet<usize>,
}

#[derive(Debug, Clone)]
struct FilterWait {
    deadline: Instant,
    wake_pending: bool,
}

impl RuntimeFilterRegistry {
    pub fn new(wait: Duration) -> Self {
        Self {
            wait,
            producers: HashMap::new(),
            waiting: HashMap::new(),
            prepared: HashSet::new(),
        }
    }

    pub fn register(&mut self, stage_id: usize, plan: &dyn ExecutionPlan) {
        if let Some(build) = plan.downcast_ref::<RuntimeFilterBuildExec>() {
            self.producers.insert(build.filter_id, stage_id);
        }
        for child in plan.children() {
            self.register(stage_id, child.as_ref());
        }
    }

    pub fn take_deadlines(&mut self) -> Vec<Instant> {
        self.waiting
            .values_mut()
            .filter_map(|wait| {
                std::mem::take(&mut wait.wake_pending).then_some(wait.deadline)
            })
            .collect()
    }

    /// None means hold this probe stage. A returned plan is frozen before its
    /// first task is bound, including when the deadline wins and filters bypass.
    pub fn prepare(
        &mut self,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        stages: &HashMap<usize, ExecutionStage>,
        now: Instant,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if self.prepared.contains(&stage_id) {
            return Ok(Some(plan));
        }
        let mut ids = HashSet::new();
        let mut local_builds = HashSet::new();
        plan.apply(|node| {
            if let Some(apply) = node.downcast_ref::<RuntimeFilterApplyExec>() {
                ids.insert(apply.filter_id);
            }
            if let Some(build) = node.downcast_ref::<RuntimeFilterBuildExec>() {
                local_builds.insert(build.filter_id);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        // A build in the same task cannot report before this task starts.
        // Such local joins need no distributed wait and simply bypass.
        ids.retain(|id| !local_builds.contains(id));
        let mut ready = HashMap::new();
        let mut pending = false;
        for id in &ids {
            let producer = self.producers.get(id).and_then(|id| stages.get(id));
            match filter_status(*id, producer)? {
                FilterStatus::Ready(payload) => {
                    ready.insert(*id, *payload);
                }
                FilterStatus::Pending => pending = true,
                FilterStatus::Bypass => {}
            }
        }
        // Missing, failed, or dependent (unresolved) producers cannot deliver
        // a useful report while this stage is held. Bypass them immediately.
        if pending {
            let deadline = self
                .waiting
                .entry(stage_id)
                .or_insert_with(|| FilterWait {
                    deadline: now + self.wait,
                    wake_pending: true,
                })
                .deadline;
            if now < deadline {
                return Ok(None);
            }
        }
        let plan = plan
            .transform_up(|node| {
                let Some(apply) = node.downcast_ref::<RuntimeFilterApplyExec>() else {
                    return Ok(Transformed::no(node));
                };
                let Some(payload) = ready.get(&apply.filter_id) else {
                    return Ok(Transformed::yes(apply.children()[0].clone()));
                };
                Ok(Transformed::yes(Arc::new(
                    apply.with_payload(payload.clone())?,
                )))
            })?
            .data;
        self.mark_prepared(stage_id);
        Ok(Some(plan))
    }

    pub fn mark_prepared(&mut self, stage_id: usize) {
        self.waiting.remove(&stage_id);
        self.prepared.insert(stage_id);
    }
}

enum FilterStatus {
    Ready(Box<RuntimeFilterPayload>),
    Pending,
    Bypass,
}

fn filter_status(id: u64, stage: Option<&ExecutionStage>) -> Result<FilterStatus> {
    let (tasks, complete) = match stage {
        Some(ExecutionStage::Successful(stage)) => (&stage.task_infos, true),
        Some(ExecutionStage::Running(stage)) => {
            (&stage.task_infos, stage.is_successful())
        }
        Some(ExecutionStage::Resolved(_)) => return Ok(FilterStatus::Pending),
        _ => return Ok(FilterStatus::Bypass),
    };
    let mut merged = RuntimeFilter::default();
    for task in tasks {
        // Retry/loss handling invalidates old statuses. Only successful slices
        // contribute, and only complete coverage can supply a pruning payload.
        if let task_status::Status::Successful(success) = &task.task_status {
            let Some(report) = success.runtime_filters.iter().find(|p| p.filter_id == id)
            else {
                return Ok(FilterStatus::Bypass);
            };
            merged.merge(&RuntimeFilter::from_proto(report)?);
        }
    }
    if complete {
        // Keep complete bounds, even when the Bloom cannot prune rows: scan
        // pruning may still use them.
        merged
            .to_proto(id)
            .map(|payload| FilterStatus::Ready(Box::new(payload)))
    } else if merged.is_saturated() {
        // Union can only set more bits. An already saturated partial summary
        // cannot justify holding an independent probe stage. Its bounds are
        // incomplete, so bypass rather than publishing the partial payload.
        Ok(FilterStatus::Bypass)
    } else {
        Ok(FilterStatus::Pending)
    }
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

    fn source(values: Vec<i64>) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    async fn report(value: i64) -> RuntimeFilterPayload {
        let build: Arc<dyn ExecutionPlan> = Arc::new(RuntimeFilterBuildExec::new(
            source(vec![value]),
            3,
            Arc::new(Column::new("k", 0)),
        ));
        collect(build.clone(), SessionContext::new().task_ctx())
            .await
            .unwrap();
        collect_reports(&build).unwrap().pop().unwrap()
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
    async fn runtime_filter_hub_waits_for_coverage_and_discards_lost_attempts() {
        let build: Arc<dyn ExecutionPlan> = Arc::new(RuntimeFilterBuildExec::new(
            source(vec![]),
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
        stage.task_infos = vec![task(0, 0, Some(report(1).await)), task(1, 1, None)];
        let mut stages = HashMap::from([(1, ExecutionStage::Running(stage))]);
        let probe: Arc<dyn ExecutionPlan> = Arc::new(
            RuntimeFilterApplyExec::try_new(
                source(vec![1, 2, 3, 4]),
                3,
                Arc::new(Column::new("k", 0)),
                None,
            )
            .unwrap(),
        );
        let wait = Duration::from_millis(500);
        let now = Instant::now();
        let mut hub = RuntimeFilterRegistry::new(wait);
        // A producer not yet created may depend on this consumer. It must
        // never impose the timeout on the current stage (TPC-H Q8).
        assert!(
            hub.prepare(4, probe.clone(), &stages, now)
                .unwrap()
                .is_some()
        );
        assert!(hub.take_deadlines().is_empty());
        hub.register(1, stages[&1].plan());
        assert!(
            hub.prepare(2, probe.clone(), &stages, now)
                .unwrap()
                .is_none()
        );
        assert_eq!(hub.take_deadlines().len(), 1);
        assert!(
            hub.take_deadlines().is_empty(),
            "each wait needs only one timer"
        );
        let ExecutionStage::Running(stage) = stages.get_mut(&1).unwrap() else {
            unreachable!()
        };
        // Lost executor's earlier value 1 must not participate after retry.
        stage.reset_task_info(0);
        stage.task_infos[1] = task(1, 1, Some(report(2).await));
        assert!(
            hub.prepare(2, probe.clone(), &stages, now)
                .unwrap()
                .is_none()
        );
        // An independently runnable consumer times out while producer coverage
        // is still partial. It must retain all rows, including the lost key.
        assert!(
            hub.prepare(3, probe.clone(), &stages, now)
                .unwrap()
                .is_none()
        );
        let bypass = hub
            .prepare(3, probe.clone(), &stages, now + wait)
            .unwrap()
            .unwrap();
        assert_eq!(keys(bypass).await, [1, 2, 3, 4]);
        let ExecutionStage::Running(stage) = stages.get_mut(&1).unwrap() else {
            unreachable!()
        };
        stage.pending.next_slice(1);
        stage.task_infos.push(task(2, 0, Some(report(3).await)));
        let plan = hub
            .prepare(2, probe.clone(), &stages, now)
            .unwrap()
            .unwrap();
        assert_eq!(keys(plan).await, [2, 3]);
    }

    #[tokio::test]
    async fn runtime_filter_saturated_partial_report_releases_probe() {
        let build: Arc<dyn ExecutionPlan> = Arc::new(RuntimeFilterBuildExec::new(
            source((0..1_000_000).collect()),
            3,
            Arc::new(Column::new("k", 0)),
        ));
        collect(build.clone(), SessionContext::new().task_ctx())
            .await
            .unwrap();
        let payload = collect_reports(&build).unwrap().pop().unwrap();
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
        stage.task_infos = vec![task(0, 0, Some(payload)), task(1, 1, None)];
        let stages = HashMap::from([(1, ExecutionStage::Running(stage))]);
        let probe: Arc<dyn ExecutionPlan> = Arc::new(
            RuntimeFilterApplyExec::try_new(
                source(vec![-1, 2_000_000]),
                3,
                Arc::new(Column::new("k", 0)),
                None,
            )
            .unwrap(),
        );
        let mut hub = RuntimeFilterRegistry::new(Duration::from_millis(500));
        hub.register(1, stages[&1].plan());
        let plan = hub
            .prepare(2, probe, &stages, Instant::now())
            .unwrap()
            .expect("a saturated partial Bloom cannot justify waiting");
        // Partial bounds must not be published: the unfinished partition may
        // contain keys outside the completed partition's range.
        assert!(matches!(
            filter_status(3, stages.get(&1)).unwrap(),
            FilterStatus::Bypass
        ));
        assert_eq!(keys(plan).await, [-1, 2_000_000]);
        assert!(hub.take_deadlines().is_empty());
    }

    #[tokio::test]
    async fn runtime_filter_hub_uses_only_complete_current_attempts() {
        let now = Instant::now();
        let build: Arc<dyn ExecutionPlan> = Arc::new(RuntimeFilterBuildExec::new(
            source(vec![]),
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
        stage.task_infos = vec![task(0, 0, Some(report(1).await)), task(1, 1, None)];
        let mut stages = HashMap::from([(1, ExecutionStage::Running(stage))]);
        let probe: Arc<dyn ExecutionPlan> = Arc::new(
            RuntimeFilterApplyExec::try_new(
                source(vec![1, 2, 3, 4]),
                3,
                Arc::new(Column::new("k", 0)),
                None,
            )
            .unwrap(),
        );
        let mut hub = RuntimeFilterRegistry::default();
        hub.register(1, stages[&1].plan());
        let bypass = hub
            .prepare(2, probe.clone(), &stages, now)
            .unwrap()
            .unwrap();
        assert_eq!(keys(bypass.clone()).await, [1, 2, 3, 4]);
        let ExecutionStage::Running(stage) = stages.get_mut(&1).unwrap() else {
            unreachable!()
        };
        // Drop the lost attempt's key 1; only replacement slices may contribute.
        stage.reset_task_info(0);
        stage.task_infos[1] = task(1, 1, Some(report(2).await));
        stage.pending.next_slice(1);
        stage.task_infos.push(task(2, 0, Some(report(3).await)));
        // A stage already dispatched without a filter stays unchanged.
        let frozen = hub.prepare(2, bypass, &stages, now).unwrap().unwrap();
        assert_eq!(keys(frozen).await, [1, 2, 3, 4]);
        let plan = hub.prepare(3, probe, &stages, now).unwrap().unwrap();
        assert_eq!(keys(plan).await, [2, 3]);
    }
}
