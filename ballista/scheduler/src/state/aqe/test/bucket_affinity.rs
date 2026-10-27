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

use crate::cluster::memory::InMemoryClusterState;
use crate::cluster::{BoundTask, ClusterState};
use crate::config::TaskDistributionPolicy;
use crate::state::aqe::AdaptiveExecutionGraph;
use crate::state::execution_graph::{ExecutionGraph, TaskDescription};
use crate::state::task_builder::restrict_plan_to_partitions;
use crate::state::task_manager::JobInfoCache;
use crate::test_utils::mock_completed_task;
use ballista_core::JobId;
use ballista_core::config::{
    BALLISTA_HASH_JOIN_MAX_BUILD_PARTITION_BYTES, BALLISTA_STORAGE_PARTITION_JOIN,
    BALLISTA_STORAGE_PARTITION_JOIN_DIVISOR,
};
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::BallistaPhysicalExtensionCodec;
use ballista_core::serde::protobuf::TaskStatus;
use ballista_core::serde::scheduler::{
    ExecutorData, ExecutorMetadata, ExecutorSpecification,
};
use ballista_core::storage_partitioning::{
    BucketFile, BucketedParquetManifest, BucketedParquetTable,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::{physical_plan::FileScanConfig, source::DataSourceExec};
use datafusion::physical_plan::Partitioning;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use std::collections::HashMap;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

fn metadata(id: &str) -> ExecutorMetadata {
    ExecutorMetadata {
        id: id.into(),
        host: "localhost".into(),
        port: 0,
        grpc_port: 0,
        specification: ExecutorSpecification::default().with_vcores(16),
        os_info: Default::default(),
    }
}

async fn cluster(workers: &[(&str, u32)]) -> InMemoryClusterState {
    let cluster = InMemoryClusterState::default();
    for &(id, available_vcores) in workers {
        cluster
            .register_executor(
                metadata(id),
                ExecutorData {
                    executor_id: id.into(),
                    total_vcores: 16,
                    available_vcores,
                },
            )
            .await
            .unwrap();
    }
    cluster
}

async fn job(id: &JobId) -> Arc<HashMap<JobId, JobInfoCache>> {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new_with_ballista()
            .set_bool(BALLISTA_STORAGE_PARTITION_JOIN, true)
            .set_bool(BALLISTA_STORAGE_PARTITION_JOIN_DIVISOR, true)
            .set_u64(BALLISTA_HASH_JOIN_MAX_BUILD_PARTITION_BYTES, 0)
            .with_ballista_broadcast_join_threshold_bytes(0),
    );
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    for (name, buckets) in [("a", 8), ("b", 4)] {
        // Planning descriptors exercise AQE, binding, codecs and attempt
        // accounting. The process harness separately verifies actual rows.
        let files = (0..buckets)
            .map(|bucket| BucketFile {
                path: format!("snapshot/{name}/{bucket}.parquet"),
                size: 4096,
                bucket: Some(bucket),
                num_rows: Some(32),
            })
            .collect();
        ctx.register_table(
            name,
            Arc::new(
                BucketedParquetTable::try_new(
                    schema.clone(),
                    BucketedParquetManifest {
                        object_store_url: "file://".into(),
                        key: "id".into(),
                        buckets,
                        files,
                    },
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }
    let logical = ctx
        .sql("select a.id from a join b on a.id=b.id")
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();
    let mut graph =
        AdaptiveExecutionGraph::try_new("scheduler", id, "", &ctx, &logical, 0)
            .await
            .unwrap();
    assert!(graph.revive());
    Arc::new(HashMap::from([(
        id.clone(),
        JobInfoCache::new(Box::new(graph)),
    )]))
}

fn placements(tasks: &[BoundTask]) -> BTreeMap<usize, String> {
    let placements: BTreeMap<_, _> = tasks
        .iter()
        .map(|(worker, task)| {
            assert_eq!(task.global_input_partition_ids.len(), 1);
            (task.global_input_partition_ids[0], worker.clone())
        })
        .collect();
    assert_eq!(placements.len(), tasks.len(), "a group was bound twice");
    placements
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct InputGroup {
    scheme: String,
    ids: Vec<Vec<u8>>,
    files: Vec<(String, String, u64)>,
}

fn serialized_inputs(task: &TaskDescription) -> Vec<InputGroup> {
    let codec = BallistaPhysicalExtensionCodec::default();
    let restricted =
        restrict_plan_to_partitions(task.plan.clone(), &task.global_input_partition_ids)
            .unwrap();
    let bytes = PhysicalPlanNode::try_from_physical_plan(restricted, &codec)
        .unwrap()
        .encode_to_vec();
    let decoded = PhysicalPlanNode::decode(bytes.as_slice())
        .unwrap()
        .try_into_physical_plan(&SessionContext::new().task_ctx(), &codec)
        .unwrap();
    let mut inputs = Vec::new();
    decoded
        .apply(|node| {
            if let Some(scan) = node.downcast_ref::<DataSourceExec>() {
                let config = scan.data_source().downcast_ref::<FileScanConfig>().unwrap();
                let Some(Partitioning::Grouped(grouped)) = &config.output_partitioning
                else {
                    panic!("serialized task lost its source grouping");
                };
                assert_eq!(grouped.partition_count(), 1);
                inputs.push(InputGroup {
                    scheme: grouped.scheme().into(),
                    ids: grouped.group_ids().to_vec(),
                    files: config
                        .file_groups
                        .iter()
                        .flat_map(|g| g.iter())
                        .map(|f| {
                            (
                                config.object_store_url.to_string(),
                                f.object_meta.location.to_string(),
                                f.object_meta.size,
                            )
                        })
                        .collect(),
                });
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
    assert_eq!(inputs.len(), 2);
    assert_eq!(inputs[0].ids, inputs[1].ids);
    assert_eq!(inputs[0].scheme, inputs[1].scheme);
    inputs.sort();
    inputs
}

#[tokio::test]
async fn divisor_affinity_respects_capacity_eligibility_and_recovery() {
    let policy = TaskDistributionPolicy::BucketAffinity;
    let id: JobId = "affinity-original".into();
    let jobs = job(&id).await;
    let original_cluster = cluster(&[("worker-a", 16), ("worker-b", 16)]).await;
    let original = original_cluster
        .bind_schedulable_tasks(policy.clone(), jobs.clone(), None)
        .await
        .unwrap();
    let expected = placements(&original);
    assert_eq!(
        expected.keys().copied().collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4]
    );
    let inputs: BTreeMap<_, _> = original
        .iter()
        .map(|(_, task)| (task.global_input_partition_ids[0], serialized_inputs(task)))
        .collect();

    // Job IDs and membership iteration order do not change preferences.
    let repeated = cluster(&[("worker-b", 16), ("worker-a", 16)])
        .await
        .bind_schedulable_tasks(
            policy.clone(),
            job(&"affinity-repeat".into()).await,
            None,
        )
        .await
        .unwrap();
    assert_eq!(placements(&repeated), expected);

    let nonempty = *inputs
        .iter()
        .find(|(_, scans)| scans.iter().all(|scan| !scan.files.is_empty()))
        .unwrap()
        .0;
    let preferred = &expected[&nonempty];
    let other = if preferred == "worker-a" {
        "worker-b"
    } else {
        "worker-a"
    };
    let saturated = cluster(&[(preferred, 0), (other, 16)])
        .await
        .bind_schedulable_tasks(
            policy.clone(),
            job(&"affinity-saturated".into()).await,
            None,
        )
        .await
        .unwrap();
    assert_eq!(placements(&saturated).len(), 5);
    assert!(saturated.iter().all(|(worker, _)| worker == other));
    let eligible = cluster(&[(preferred, 16), (other, 16)])
        .await
        .bind_schedulable_tasks(
            policy.clone(),
            job(&"affinity-eligible".into()).await,
            Some(HashSet::from([other.to_owned()])),
        )
        .await
        .unwrap();
    assert_eq!(placements(&eligible).len(), 5);
    assert!(eligible.iter().all(|(worker, _)| worker == other));
    let expanded = cluster(&[("worker-c", 16), ("worker-a", 16), ("worker-b", 16)])
        .await
        .bind_schedulable_tasks(
            policy.clone(),
            job(&"affinity-expanded".into()).await,
            None,
        )
        .await
        .unwrap();
    let expanded = placements(&expanded);
    assert_eq!(expanded.len(), 5);
    for (group, worker) in expanded {
        if worker != "worker-c" {
            assert_eq!(worker, expected[&group]);
        }
    }

    // Exercise the existing graph's loss/retry path, preserving the complete
    // normalized input group through task specialization and physical serde.
    original_cluster.remove_executor(preferred).await.unwrap();
    {
        let mut graph = jobs[&id].execution_graph.write().await;
        let (reset, _) = graph.reset_stages_on_lost_executor(preferred).unwrap();
        assert!(!reset.is_empty());
        graph.revive();
    }
    let retried = original_cluster
        .bind_schedulable_tasks(policy, jobs.clone(), None)
        .await
        .unwrap();
    let lost: BTreeMap<_, _> = original
        .iter()
        .filter(|(worker, _)| worker == preferred)
        .map(|(_, task)| (task.global_input_partition_ids[0], task))
        .collect();
    assert_eq!(
        placements(&retried).keys().collect::<Vec<_>>(),
        lost.keys().collect::<Vec<_>>()
    );
    for (worker, task) in &retried {
        let group = task.global_input_partition_ids[0];
        assert_eq!(worker, other);
        assert_eq!(serialized_inputs(task), inputs[&group]);
        // Lost executors do not consume the execution-failure budget. The
        // replacement gets a distinct task identity for stale-result rejection.
        assert!(
            task.key.task_id != lost[&group].key.task_id
                || task.stage_attempt_num != lost[&group].stage_attempt_num
        );
    }
    // A stale successful message takes the actual protobuf path but cannot
    // publish output or overwrite the current attempts' task statuses.
    let late_task = lost.values().next().unwrap();
    let bytes = mock_completed_task((*late_task).clone(), preferred).encode_to_vec();
    let late = TaskStatus::decode(bytes.as_slice()).unwrap();
    let mut graph = jobs[&id].execution_graph.write().await;
    let statuses = |graph: &dyn ExecutionGraph| {
        graph.stages()[&late_task.key.stage_id]
            .task_infos()
            .unwrap()
            .iter()
            .map(|task| task.task_status.clone())
            .collect::<Vec<_>>()
    };
    let before = statuses(graph.as_ref());
    graph
        .update_task_status(&metadata(preferred), vec![late], 4, 4)
        .unwrap();
    assert_eq!(statuses(graph.as_ref()), before);
}
