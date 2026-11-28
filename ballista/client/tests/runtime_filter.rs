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

#![cfg(feature = "standalone")]

use ballista::prelude::SessionContextExt;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::*;
use std::sync::Arc;

async fn run(ctx: &SessionContext, sql: &str) -> String {
    let batches = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        ctx.sql(sql).await.unwrap().collect().await.unwrap()
    })
    .await
    .expect("runtime filter must not deadlock the query");
    pretty_format_batches(&batches).unwrap().to_string()
}

#[tokio::test]
async fn distributed_runtime_filter_matches_unfiltered_joins() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["build", "probe"] {
        std::fs::create_dir(dir.path().join(name)).unwrap();
    }
    std::fs::write(dir.path().join("build/a.csv"), "k,v\n2,1\n,2\n").unwrap();
    std::fs::write(dir.path().join("build/b.csv"), "k,v\n98,3\n2,4\n").unwrap();
    for part in 0..4 {
        let mut csv = "k,v\n,0\n".to_string();
        for k in 0..25 {
            csv.push_str(&format!("{},{}\n", part * 25 + k, k));
        }
        std::fs::write(dir.path().join(format!("probe/{part}.csv")), csv).unwrap();
    }
    let ctx = SessionContext::standalone().await.unwrap();
    for setting in [
        "ballista.planner.adaptive.enabled = true",
        "datafusion.optimizer.prefer_hash_join = true",
        "datafusion.execution.target_partitions = 4",
    ] {
        run(&ctx, &format!("SET {setting}")).await;
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    // Parquet provides build cardinality, allowing AQE to choose a broadcast
    // stage whose completion naturally precedes the probe scan.
    let build_parquet = dir.path().join("build_parquet");
    let local =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    local
        .read_csv(
            dir.path().join("build").to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await
        .unwrap()
        .write_parquet(build_parquet.to_str().unwrap(), Default::default(), None)
        .await
        .unwrap();
    ctx.register_parquet(
        "build",
        build_parquet.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    ctx.register_csv(
        "probe",
        dir.path().join("probe").to_str().unwrap(),
        CsvReadOptions::new().schema(&schema),
    )
    .await
    .unwrap();
    let queries = [
        "SELECT b.k, b.v, p.v FROM build b JOIN probe p ON b.k = p.k WHERE b.v > 0 AND p.v >= 0 ORDER BY b.k, b.v, p.v",
        "SELECT b.k, b.v, p.v FROM build b JOIN probe p ON b.k IS NOT DISTINCT FROM p.k WHERE b.v > 0 AND p.v >= 0 ORDER BY b.k, b.v, p.v",
        "SELECT p.k FROM build b RIGHT JOIN probe p ON b.k = p.k ORDER BY p.k",
        "SELECT b.k, p.k FROM build b JOIN (SELECT k FROM probe ORDER BY k LIMIT 3) p ON b.k = p.k ORDER BY b.k, p.k",
    ];
    for query in queries {
        run(&ctx, "SET ballista.planner.runtime_filter.enabled = false").await;
        let expected = run(&ctx, query).await;
        run(&ctx, "SET ballista.planner.runtime_filter.enabled = true").await;
        assert_eq!(run(&ctx, query).await, expected, "{query}");
    }
    let plan = run(&ctx, &format!("EXPLAIN ANALYZE {}", queries[0])).await;
    assert!(plan.contains("RuntimeFilterBuildExec"), "{plan}");
    assert!(
        plan.lines()
            .any(|line| line.contains("RuntimeFilterApplyExec")
                && line.contains("ready=true")),
        "CSV keeps the bound Apply: {plan}"
    );
    // The build keys (2 and 98) bracket the two middle row groups. Only the
    // exact IN predicate can prune those groups by statistics.
    let parquet_dir = dir.path().join("parquet_probe");
    let local =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let mut writer_options = datafusion::common::config::TableParquetOptions::default();
    writer_options.global.max_row_group_size = 25;
    std::fs::create_dir(&parquet_dir).unwrap();
    // Two files preserve parallel probe input and the existing broadcast
    // dependency. Each file contains two disjoint row groups.
    for (part, lower) in [0, 50].into_iter().enumerate() {
        local
            .read_csv(
                dir.path().join("probe").to_str().unwrap(),
                CsvReadOptions::new().schema(&schema),
            )
            .await
            .unwrap()
            .filter(col("k").between(lit(lower), lit(lower + 49)))
            .unwrap()
            .sort(vec![col("k").sort(true, false)])
            .unwrap()
            .write_parquet(
                parquet_dir
                    .join(format!("part-{part}.parquet"))
                    .to_str()
                    .unwrap(),
                datafusion::dataframe::DataFrameWriteOptions::new()
                    .with_single_file_output(true),
                Some(writer_options.clone()),
            )
            .await
            .unwrap();
    }
    ctx.register_parquet(
        "parquet_probe",
        parquet_dir.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    run(
        &ctx,
        "SET datafusion.optimizer.enable_join_dynamic_filter_pushdown = false",
    )
    .await;
    let parquet_query = queries[0].replace("JOIN probe", "JOIN parquet_probe");
    let filtered = run(&ctx, &parquet_query).await;
    let plan = run(&ctx, &format!("EXPLAIN ANALYZE {parquet_query}")).await;
    let scan_predicate = plan
        .lines()
        .filter_map(|line| line.split_once(", predicate="))
        .filter_map(|(_, tail)| tail.split_once(", pruning_predicate="))
        .map(|(predicate, _)| predicate)
        .find(|p| p.contains(" IN "))
        .unwrap_or_else(|| panic!("missing IN predicate: {plan}"));
    assert!(
        scan_predicate
            .find("v@")
            .is_some_and(|pos| scan_predicate.find(" IN ").unwrap() < pos),
        "{plan}"
    );
    assert!(!plan.contains("RuntimeFilterApplyExec"), "{plan}");
    assert!(plan.split("row_groups_pruned_statistics=").skip(1).any(|suffix| {
        let mut parts = suffix.split_whitespace();
        let total = parts.next().and_then(|value| value.parse::<usize>().ok());
        let matched = parts.nth(2).and_then(|value| value.parse::<usize>().ok());
        matches!((total, matched), (Some(total), Some(matched)) if total > matched)
    }), "runtime IN must skip Parquet row groups: {plan}");
    run(&ctx, "SET ballista.planner.runtime_filter.enabled = false").await;
    assert_eq!(filtered, run(&ctx, &parquet_query).await);
    run(&ctx, "SET ballista.planner.runtime_filter.enabled = true").await;
    // Independently runnable partitioned stages must preserve results even
    // when the producer has not reported before the probe is dispatched.
    for setting in [
        "ballista.optimizer.broadcast_join_threshold_bytes = 0",
        "ballista.optimizer.broadcast_join_threshold_rows = 0",
        "ballista.optimizer.stage_build_side = false",
    ] {
        run(&ctx, &format!("SET {setting}")).await;
    }
    let partitioned = run(&ctx, queries[0]).await;
    let plan = run(&ctx, &format!("EXPLAIN ANALYZE {}", queries[0])).await;
    assert!(plan.contains("mode=Partitioned"), "{plan}");
    run(&ctx, "SET ballista.planner.runtime_filter.enabled = false").await;
    assert_eq!(partitioned, run(&ctx, queries[0]).await);
}

#[tokio::test]
async fn parquet_reader_runs_join_bloom_after_scan_predicates() {
    let dir = tempfile::tempdir().unwrap();
    let local =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
    let ctx = SessionContext::standalone().await.unwrap();
    for (table, parts) in [
        (
            "dimension",
            vec![
                "SELECT value * 16 AS k FROM range(80)",
                "SELECT value * 16 AS k FROM range(80, 160)",
            ],
        ),
        (
            "fact",
            vec![
                "SELECT value AS k, value % 3 AS v, value AS amount FROM range(4096)",
                "SELECT value AS k, value % 3 AS v, value AS amount FROM range(4096, 8192)",
            ],
        ),
    ] {
        let path = dir.path().join(table);
        std::fs::create_dir(&path).unwrap();
        for (part, sql) in parts.into_iter().enumerate() {
            local
                .sql(sql)
                .await
                .unwrap()
                .write_parquet(
                    path.join(format!("{part}.parquet")).to_str().unwrap(),
                    datafusion::dataframe::DataFrameWriteOptions::new()
                        .with_single_file_output(true),
                    None,
                )
                .await
                .unwrap();
        }
        ctx.register_parquet(
            table,
            path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
    }
    for setting in [
        "ballista.planner.adaptive.enabled = true",
        "datafusion.execution.target_partitions = 4",
        "datafusion.execution.parquet.pushdown_filters = true",
        "datafusion.optimizer.hash_join_single_partition_threshold = 0",
        "datafusion.optimizer.hash_join_single_partition_threshold_rows = 0",
        "datafusion.optimizer.prefer_hash_join = true",
        "datafusion.optimizer.enable_join_dynamic_filter_pushdown = false",
        "ballista.planner.runtime_filter.enabled = false",
    ] {
        run(&ctx, &format!("SET {setting}")).await;
    }
    let query = "SELECT SUM(f.amount) FROM dimension d JOIN fact f ON d.k = f.k WHERE d.k >= 0 AND f.v = 1";
    let expected = run(&ctx, query).await;
    run(&ctx, "SET ballista.planner.runtime_filter.enabled = true").await;
    assert_eq!(run(&ctx, query).await, expected);
    let plan = run(&ctx, &format!("EXPLAIN ANALYZE {query}")).await;
    let reader = plan
        .lines()
        .filter_map(|line| line.split_once("predicate=").map(|(_, p)| p))
        .find(|p| p.contains("runtime_bloom("))
        .unwrap_or_else(|| panic!("missing reader Bloom: {plan}"));
    assert!(
        reader
            .find("v@")
            .is_some_and(|i| i < reader.find("runtime_bloom(").unwrap()),
        "{plan}"
    );
    assert!(!plan.contains("RuntimeFilterApplyExec"), "{plan}");
    // The ordinary predicate alone passes 2,731 rows. Check the reader's
    // output, since checking join output would also pass with no Bloom probe.
    let output_rows: usize = reader
        .split_once("metrics=[output_rows=")
        .and_then(|(_, metrics)| metrics.split(',').next())
        .and_then(|rows| rows.parse().ok())
        .unwrap_or_else(|| panic!("expected a small reader output: {reader}"));
    assert!((53..160).contains(&output_rows), "{reader}");
}
