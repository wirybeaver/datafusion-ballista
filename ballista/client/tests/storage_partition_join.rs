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

//! File-backed SPJ result parity through provider transport and actual tasks.

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_core::config::{
    BALLISTA_HASH_JOIN_MAX_BUILD_PARTITION_BYTES, BALLISTA_STORAGE_PARTITION_JOIN,
};
use ballista_core::storage_partitioning::{
    BucketFile, BucketedParquetManifest, BucketedParquetTable,
};
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::SessionStateBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

// Independent fixture implementation of the Iceberg integer hash. Integers are
// promoted to the eight-byte little-endian representation, including Int32.
fn hash_integer(value: i64) -> u32 {
    let mut h = 0u32;
    for bytes in value.to_le_bytes().chunks_exact(4) {
        let k = u32::from_le_bytes(bytes.try_into().unwrap())
            .wrapping_mul(0xcc9e2d51)
            .rotate_left(15)
            .wrapping_mul(0x1b873593);
        h ^= k;
        h = h.rotate_left(13).wrapping_mul(5).wrapping_add(0xe6546b64);
    }
    h ^= 8;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^ (h >> 16)
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn table(
    dir: &Path,
    name: &str,
    buckets: u32,
    missing: u32,
    rows: i64,
) -> Arc<BucketedParquetTable> {
    assert_eq!(hash_integer(34), 2017239379);
    let mut parts: BTreeMap<(Option<u32>, usize), Vec<Option<i64>>> = BTreeMap::new();
    for (i, key) in (0..rows)
        .map(Some)
        .chain([Some(3), Some(3), None, None])
        .enumerate()
    {
        let bucket = key.map(|k| (hash_integer(k) & 0x7fffffff) % buckets);
        if bucket == Some(missing) {
            continue;
        }
        parts.entry((bucket, i % 2)).or_default().push(key);
    }
    let mut files = vec![];
    for ((bucket, split), values) in parts {
        let path = dir.join(format!("{name}-{bucket:?}-{split}.parquet"));
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(values.clone())),
                Arc::new(Int64Array::from_iter_values(
                    (0..values.len()).map(|i| i as i64),
                )),
            ],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        files.push(BucketFile {
            path: path.to_str().unwrap().into(),
            size: path.metadata().unwrap().len(),
            bucket,
            num_rows: Some(values.len()),
        });
    }
    Arc::new(
        BucketedParquetTable::try_new(
            schema(),
            BucketedParquetManifest {
                object_store_url: "file://".into(),
                key: "id".into(),
                buckets,
                files,
            },
        )
        .unwrap(),
    )
}

fn input_directory() -> tempfile::TempDir {
    match std::env::var("BALLISTA_SPJ_DATA_ROOT") {
        Ok(root) => tempfile::tempdir_in(root).unwrap(),
        Err(_) => tempfile::tempdir().unwrap(),
    }
}

fn remote_table(provider: &Arc<BucketedParquetTable>) -> Arc<BucketedParquetTable> {
    use datafusion::catalog::TableProvider;
    let Ok(url) = std::env::var("BALLISTA_SPJ_HTTP_URL") else {
        return provider.clone();
    };
    let root = std::env::var("BALLISTA_SPJ_DATA_ROOT").unwrap();
    let mut manifest = provider.manifest().clone();
    manifest.object_store_url = url;
    for file in &mut manifest.files {
        file.path = Path::new(&file.path)
            .strip_prefix(&root)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
    }
    Arc::new(BucketedParquetTable::try_new(provider.schema(), manifest).unwrap())
}

fn config(enabled: bool, sort: bool) -> SessionConfig {
    SessionConfig::new_with_ballista()
        .with_target_partitions(12)
        .set_bool(BALLISTA_STORAGE_PARTITION_JOIN, enabled)
        .set_u64(
            BALLISTA_HASH_JOIN_MAX_BUILD_PARTITION_BYTES,
            if sort { 1 } else { 0 },
        )
        .with_ballista_broadcast_join_threshold_bytes(0)
        .with_ballista_broadcast_join_threshold_rows(0)
        .set_u64(
            "datafusion.optimizer.hash_join_single_partition_threshold",
            0,
        )
        .set_u64(
            "datafusion.optimizer.hash_join_single_partition_threshold_rows",
            0,
        )
        .set_bool("datafusion.optimizer.prefer_hash_join", !sort)
}

async fn ballista_context(config: SessionConfig) -> SessionContext {
    let state = SessionStateBuilder::new_with_default_features()
        .with_config(config)
        .build();
    if let Ok(url) = std::env::var("BALLISTA_SPJ_SCHEDULER") {
        SessionContext::remote_with_state(&url, state)
            .await
            .unwrap()
    } else {
        SessionContext::standalone_with_state(state).await.unwrap()
    }
}

async fn run(ctx: &SessionContext, query: &str) -> String {
    let batches = ctx.sql(query).await.unwrap().collect().await.unwrap();
    pretty_format_batches(&batches).unwrap().to_string()
}

#[tokio::test]
async fn storage_partition_join_matches_shuffle() {
    let dir = input_directory();
    let a = table(dir.path(), "a", 8, 1, 40);
    let b = table(dir.path(), "b", 8, 2, 40);
    let c = table(dir.path(), "c", 8, 3, 40);
    let incompatible = table(dir.path(), "incompatible", 6, 2, 40);
    let empty = Arc::new(
        BucketedParquetTable::try_new(
            schema(),
            BucketedParquetManifest {
                object_store_url: "file://".into(),
                key: "id".into(),
                buckets: 4,
                files: vec![],
            },
        )
        .unwrap(),
    );
    let baseline = SessionContext::new_with_config(config(false, false));
    for (name, provider) in [
        ("a", a.clone()),
        ("b", b.clone()),
        ("c", c.clone()),
        ("e", empty.clone()),
        ("incompatible", incompatible.clone()),
    ] {
        baseline.register_table(name, provider).unwrap();
    }
    let queries = [
        "SELECT a.id, a.v AS av, b.v AS bv FROM a JOIN b ON a.id=b.id WHERE a.v>=0 ORDER BY 1,2,3",
        "SELECT b.id, b.v AS bv, a.v AS av FROM b JOIN a ON b.id=a.id ORDER BY 1,2,3",
        "SELECT a.id, count(*) AS n FROM a JOIN c ON a.id=c.id GROUP BY a.id ORDER BY 1",
        "SELECT v FROM a ORDER BY v",
        "SELECT x.id, count(*) AS n FROM (SELECT a.id FROM a JOIN b ON a.id=b.id) x JOIN (VALUES (3), (7), (19)) d(id) ON x.id=d.id GROUP BY x.id ORDER BY 1",
        "SELECT a.id FROM a JOIN b ON a.id=b.id WHERE a.id < 0 ORDER BY 1",
        "SELECT count(*) AS n FROM e JOIN e AS f ON e.id=f.id",
        "SELECT a.id, count(*) AS n FROM a JOIN incompatible b ON a.id=b.id GROUP BY a.id ORDER BY 1",
        "SELECT a.id, count(*) AS n FROM a LEFT JOIN b ON a.id=b.id GROUP BY a.id ORDER BY 1",
    ];
    let mut expected = vec![];
    for query in queries {
        expected.push(run(&baseline, query).await);
    }
    for (sort, dynamic_filters) in [(false, true), (true, true), (false, false)] {
        let ctx = ballista_context(
            config(true, sort)
                .with_ballista_coalesce_enabled(!dynamic_filters)
                .with_ballista_coalesce_target_partition_bytes(1024)
                .set_bool(
                    "datafusion.optimizer.enable_dynamic_filter_pushdown",
                    dynamic_filters,
                ),
        )
        .await;
        for (name, provider) in [
            ("a", a.clone()),
            ("b", b.clone()),
            ("c", c.clone()),
            ("e", empty.clone()),
            ("incompatible", incompatible.clone()),
        ] {
            ctx.register_table(name, remote_table(&provider)).unwrap();
        }
        for (query, expected) in queries.iter().zip(&expected) {
            let actual = run(&ctx, query).await;
            assert_eq!(
                &actual, expected,
                "sort={sort} dynamic_filters={dynamic_filters}: {query}"
            );
        }
        // Exercise SQL EXPLAIN through provider transport and the scheduler's
        // real AQE path, including the storage-group selection reason.
        let explained = run(&ctx, "EXPLAIN SELECT a.id FROM a JOIN b ON a.id=b.id").await;
        assert!(
            explained.contains("selected: compatible storage groups"),
            "{explained}"
        );
        assert!(
            explained.contains("effective_groups=9, effective_buckets=8"),
            "{explained}"
        );
        let fallback =
            run(&ctx, "EXPLAIN SELECT a.id FROM a LEFT JOIN b ON a.id=b.id").await;
        assert!(
            fallback.contains("fallback: unsupported join semantics"),
            "{fallback}"
        );
    }
    // An eligible tiny build must retain broadcast, without changing rows or
    // reducing the probe's original eight-bucket execution domain.
    let tiny = table(dir.path(), "tiny", 2, u32::MAX, 1);
    baseline.register_table("tiny", tiny.clone()).unwrap();
    let query = "SELECT a.id, a.v AS av, t.v AS tv FROM a JOIN tiny t ON a.id=t.id ORDER BY 1,2,3";
    let expected = run(&baseline, query).await;
    let ctx = ballista_context(
        config(true, false).with_ballista_broadcast_join_threshold_bytes(128),
    )
    .await;
    ctx.register_table("a", remote_table(&a)).unwrap();
    ctx.register_table("tiny", remote_table(&tiny)).unwrap();
    assert_eq!(run(&ctx, query).await, expected);
}

#[tokio::test]
async fn storage_join_dynamic_filters_use_the_proven_partition_domain() {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::physical_plan::joins::HashJoinExec;
    use datafusion::physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, Partitioning, collect,
    };
    let dir = input_directory();
    let a = table(dir.path(), "filter-a", 8, 1, 40);
    let b = table(dir.path(), "filter-b", 8, 3, 40);
    let query =
        "SELECT a.id, count(*) AS n FROM a JOIN b ON a.id=b.id GROUP BY a.id ORDER BY 1";
    let mut result = None;
    let mut native_result = None;
    for enabled in [true, false] {
        // This is a direct DataFusion context: its real FilterPushdown runs,
        // unlike the deliberately restricted main AQE optimizer chain.
        let ctx = SessionContext::new_with_config(config(true, false).set_bool(
            "datafusion.optimizer.enable_dynamic_filter_pushdown",
            enabled,
        ));
        ctx.register_table("a", a.clone()).unwrap();
        ctx.register_table("b", b.clone()).unwrap();
        let plan = ctx
            .sql(query)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let mut grouped = 0;
        plan.apply(|node| {
            if let Some(join) = node.downcast_ref::<HashJoinExec>()
                && matches!(join.left().output_partitioning(), Partitioning::Grouped(_))
            {
                grouped += 1;
                assert!(
                    join.dynamic_expressions_produced().is_empty(),
                    "storage grouping cannot use native partition-routed filters"
                );
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        assert_eq!(grouped, 1);
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let actual = pretty_format_batches(&batches).unwrap().to_string();
        if let Some(expected) = &result {
            assert_eq!(expected, &actual);
        }
        result = Some(actual);

        let native_query = "SELECT x.id, count(*) AS n FROM (SELECT a.id FROM a JOIN b ON a.id=b.id) x JOIN (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) d(id) ON x.id=d.id GROUP BY x.id ORDER BY 1";
        let plan = ctx
            .sql(native_query)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let mut routed_filters = 0;
        plan.apply(|node| {
            if let Some(join) = node.downcast_ref::<HashJoinExec>()
                && matches!(join.left().output_partitioning(), Partitioning::Hash(_, _))
                && !join.dynamic_expressions_produced().is_empty()
            {
                routed_filters += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        if enabled {
            assert!(
                routed_filters > 0,
                "native routing must actually be exercised"
            );
        }
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        assert!(batches.iter().map(RecordBatch::num_rows).sum::<usize>() > 0);
        let actual = pretty_format_batches(&batches).unwrap().to_string();
        if let Some(expected) = &native_result {
            assert_eq!(expected, &actual);
        }
        native_result = Some(actual);
    }
}

#[derive(Debug)]
struct RotatedGroups {
    inner: Arc<BucketedParquetTable>,
    preserve_scheme: bool,
}

#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for RotatedGroups {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }
    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>>
    {
        use datafusion::datasource::{
            physical_plan::{FileScanConfig, FileScanConfigBuilder},
            source::DataSourceExec,
        };
        use datafusion::physical_expr::GroupedPartitioning;
        use datafusion::physical_plan::Partitioning;
        let plan = self.inner.scan(state, projection, filters, limit).await?;
        let source = plan.downcast_ref::<DataSourceExec>().unwrap();
        let config = source
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .unwrap();
        let Some(Partitioning::Grouped(grouped)) = &config.output_partitioning else {
            unreachable!()
        };
        let mut files = config.file_groups.clone();
        let n = self.inner.manifest().buckets as usize;
        files[..n].rotate_left(1);
        let mut ids = grouped.group_ids().to_vec();
        let scheme = if self.preserve_scheme {
            // Same function, different partition-index ordering.
            ids[..n].rotate_left(1);
            grouped.scheme().to_owned()
        } else {
            // Different function: non-null labels are (iceberg_bucket - 1) mod N.
            format!("test.offset-minus-one:{}", grouped.scheme())
        };
        Ok(DataSourceExec::from_data_source(
            FileScanConfigBuilder::from(config.clone())
                .with_file_groups(files)
                .with_output_partitioning(Some(Partitioning::Grouped(
                    GroupedPartitioning::try_new(grouped.keys().to_vec(), scheme, ids)?,
                )))
                .build(),
        ))
    }
}

#[tokio::test]
async fn incompatible_storage_domains_repartition_and_preserve_rows() {
    let dir = input_directory();
    let a = table(dir.path(), "domain-a", 8, 1, 40);
    let b = table(dir.path(), "domain-b", 8, 3, 40);
    let query = "SELECT a.id, b.v FROM a JOIN b ON a.id=b.id ORDER BY 1,2";
    let oracle = SessionContext::new_with_config(config(false, false));
    oracle.register_table("a", a.clone()).unwrap();
    oracle.register_table("b", b.clone()).unwrap();
    let expected = run(&oracle, query).await;
    for preserve_scheme in [false, true] {
        let ctx = SessionContext::new_with_config(config(true, false));
        ctx.register_table("a", a.clone()).unwrap();
        ctx.register_table(
            "b",
            Arc::new(RotatedGroups {
                inner: b.clone(),
                preserve_scheme,
            }),
        )
        .unwrap();
        let plan = ctx
            .sql(query)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let rendered = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(false)
            .to_string();
        assert!(rendered.contains("RepartitionExec"), "{rendered}");
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&batches).unwrap().to_string(),
            expected
        );
    }
}

#[tokio::test]
async fn unsupported_planner_modes_keep_ordinary_partitioning() {
    use ballista_core::config::{
        BALLISTA_ADAPTIVE_JOIN_ENABLED, BALLISTA_ADAPTIVE_PLANNER_ENABLED,
    };
    let dir = input_directory();
    let a = table(dir.path(), "mode-a", 4, 1, 40);
    let b = table(dir.path(), "mode-b", 4, 2, 40);
    let query = "SELECT a.id, a.v, b.v FROM a JOIN b ON a.id=b.id ORDER BY 1,2,3";
    let baseline = SessionContext::new_with_config(config(false, false));
    baseline.register_table("a", a.clone()).unwrap();
    baseline.register_table("b", b.clone()).unwrap();
    let expected = run(&baseline, query).await;

    for (disabled, reason) in [
        (
            BALLISTA_ADAPTIVE_PLANNER_ENABLED,
            "adaptive planner disabled",
        ),
        (
            BALLISTA_ADAPTIVE_JOIN_ENABLED,
            "adaptive join selection disabled",
        ),
    ] {
        let ctx = ballista_context(config(true, false).set_bool(disabled, false)).await;
        ctx.register_table("a", remote_table(&a)).unwrap();
        ctx.register_table("b", remote_table(&b)).unwrap();
        assert_eq!(run(&ctx, query).await, expected, "{disabled}");
        let explained = run(&ctx, &format!("EXPLAIN {query}")).await;
        assert!(!explained.contains("Grouped"), "{disabled}: {explained}");
        assert!(explained.contains("Hash(["), "{disabled}: {explained}");
        assert!(explained.contains(reason), "{disabled}: {explained}");
    }
}
