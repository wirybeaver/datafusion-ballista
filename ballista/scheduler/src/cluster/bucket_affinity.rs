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

//! Advisory placement of complete storage groups; task bookkeeping is shared
//! with the ordinary scheduler, including retries and resource accounting.

use super::{BoundTask, bind_one, stage_has_input_collapse};
use crate::state::task_manager::JobInfoCache;
use ballista_core::{
    JobId,
    serde::protobuf::{AvailableVcores, job_status},
};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::{physical_plan::FileScanConfig, source::DataSourceExec};
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use std::collections::HashMap;
use std::sync::Arc;

pub(super) async fn bind_tasks(
    mut budgets: Vec<&mut AvailableVcores>,
    jobs: Arc<HashMap<JobId, JobInfoCache>>,
    members: Vec<String>,
) -> Vec<BoundTask> {
    if budgets.iter().all(|budget| budget.vcores == 0) {
        return vec![];
    }
    budgets.sort_by(|a, b| a.executor_id.cmp(&b.executor_id));
    let mut bound = vec![];
    for (job_id, info) in jobs.iter() {
        if !matches!(info.status, Some(job_status::Status::Running(_))) {
            continue;
        }
        let mut graph = info.execution_graph.write().await;
        let session_id = graph.session_id().to_owned();
        while let Some(stage) = graph.fetch_running_stage(&[]) {
            let groups = stage
                .bucket_affinity
                .get_or_init(|| {
                    if stage_has_input_collapse(&stage.plan) {
                        None
                    } else {
                        affinity_groups(&stage.plan, stage.pending.total_partitions())
                            .map(Arc::from)
                    }
                })
                .clone();
            while let Some(partition) = stage.pending.front() {
                let key = groups.as_ref().map(|keys| keys[partition].as_slice());
                let preferred = key.and_then(|key| {
                    members
                        .iter()
                        .max_by_key(|id| (score(key, id.as_bytes()), id.as_str()))
                        .cloned()
                });
                let index = budgets
                    .iter()
                    .enumerate()
                    .filter(|(_, b)| b.vcores > 0)
                    .max_by_key(|(_, b)| match key {
                        Some(key) => {
                            (score(key, b.executor_id.as_bytes()), b.executor_id.as_str())
                        }
                        None => (u64::from(b.vcores), b.executor_id.as_str()),
                    })
                    .map(|(i, _)| i);
                let Some(index) = index else {
                    return bound;
                };
                let reason = match &preferred {
                    Some(preferred) if *preferred == budgets[index].executor_id => {
                        "preferred"
                    }
                    Some(preferred)
                        if budgets.iter().any(|b| b.executor_id == *preferred) =>
                    {
                        "preferred-capacity-exhausted"
                    }
                    Some(_) => "preferred-ineligible",
                    None => "no-storage-affinity",
                };
                log::debug!(
                    "bucket-affinity job={job_id} stage={} group={} preferred={preferred:?} executor={} reason={reason}",
                    stage.stage_id,
                    partition,
                    budgets[index].executor_id
                );
                let limit = if key.is_some() { 1 } else { usize::MAX };
                if let Some(task) =
                    bind_one(stage, &session_id, job_id, &mut *budgets[index], limit)
                {
                    bound.push(task);
                } else {
                    break;
                }
            }
        }
    }
    bound
}

/// A fixed, version-independent rendezvous score. Length prefixes keep the
/// namespace unambiguous; tie breaking uses the executor identifier.
fn score(group: &[u8], executor: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for part in [group, executor] {
        for byte in (part.len() as u64).to_le_bytes().iter().chain(part) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    // Avalanche the shared source prefix before comparing rendezvous scores.
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
    hash ^ (hash >> 33)
}

fn affinity_groups(plan: &Arc<dyn ExecutionPlan>, count: usize) -> Option<Vec<Vec<u8>>> {
    let mut ids: Option<Vec<Vec<u8>>> = None;
    let mut sources = Vec::new();
    let mut valid = true;
    plan.apply(|node| {
        if let Some(scan) = node.downcast_ref::<DataSourceExec>() {
            let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>() else {
                valid = false;
                return Ok(TreeNodeRecursion::Stop);
            };
            let Some(Partitioning::Grouped(grouped)) = &config.output_partitioning else {
                valid = false;
                return Ok(TreeNodeRecursion::Stop);
            };
            if grouped.partition_count() != count
                || ids
                    .as_ref()
                    .is_some_and(|ids| ids.as_slice() != grouped.group_ids())
            {
                valid = false;
                return Ok(TreeNodeRecursion::Stop);
            }
            if ids.is_none() {
                ids = Some(grouped.group_ids().to_vec());
            }
            sources.push(format!("scheme:{}", grouped.scheme()));
            for group in &config.file_groups {
                for file in group.iter() {
                    sources.push(format!(
                        "{}:{}:{}",
                        config.object_store_url,
                        file.object_meta.location,
                        file.object_meta.size
                    ));
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("affinity plan visitor is infallible");
    if !valid {
        return None;
    }
    sources.sort();
    let mut namespace = Vec::new();
    for source in sources {
        namespace.extend_from_slice(&(source.len() as u64).to_le_bytes());
        namespace.extend_from_slice(source.as_bytes());
    }
    let namespace = score(&namespace, &[]).to_le_bytes();
    Some(
        ids?.into_iter()
            .map(|id| {
                let mut key = namespace.to_vec();
                key.extend_from_slice(&(id.len() as u64).to_le_bytes());
                key.extend_from_slice(&id);
                key
            })
            .collect(),
    )
}
