<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Storage partition join validation

This personal validation branch builds on the complete feature stack. It is
not a prerequisite for any feature PR. Provider setup and supported semantics
are described in the [user guide](../../user-guide/storage-partition-joins.md).

The process harness exercises real provider/task transport, both join algorithms,
and executor loss with row comparison against ordinary-shuffle execution.

## Build the local stack

With the DataFusion prerequisite checked out beside this repository:

```bash
uv run dev/configure_storage_partition_join.py ../datafusion-spj-56
cargo check --config .local/spj-cargo.toml -p ballista-core -p ballista-scheduler
cargo build --config .local/spj-cargo.toml --profile ci \
  -p ballista-scheduler -p ballista-executor --bins
cargo test --config .local/spj-cargo.toml --profile ci \
  -p ballista --test storage_partition_join
uv run dev/test_storage_partition_join.py
uv run dev/test_storage_partition_join.py --kill-executor
```

The harness builds and resolves all executables from Cargo's JSON artifact
messages, including `Fresh` artifacts; it never selects a binary by modification
time. The generated Cargo override contains local paths and stays in `.local`. The
DataFusion prerequisite uses its own pinned Rust toolchain. The cluster test
starts one scheduler and two executor processes on available loopback ports,
keeps their logs and shuffle artifacts in `.local`, and stops only the processes
it created. The failure run kills one executor after binding begins and checks
that the complete query still matches the ordinary-shuffle result.

## Measurements

For measurements, build the binaries and integration test using
`--profile release-nonlto` and run:

```bash
uv run dev/test_storage_partition_join.py --profile release-nonlto --benchmark --policy bias
uv run dev/test_storage_partition_join.py --profile release-nonlto --benchmark --policy bucket-affinity
```

The benchmark alternates SPJ off/on visits on identical input files and reports
elapsed time, tasks bound, and actual consolidated hash-shuffle data-file
bytes. It also captures analyzed plans and checks an ordinary scan control.
These are warm local shared-filesystem measurements: they do not establish
cold object-store performance or cache-hit improvements. Do not interpret zero
join-shuffle bytes as zero storage or network I/O.

## Remote reads and TPC-H Q12

The process harness can serve inputs through a local read-only HTTP range store:

```bash
uv run dev/test_storage_partition_join.py --profile release-nonlto --http
uv run dev/test_storage_partition_join.py --profile release-nonlto --http --kill-executor
```

For a larger query, generate `orders` and `lineitem` with the pinned generator,
then prepare immutable layouts. The preparation preserves every row, projects
only Q12's columns, writes orders at 16/32 buckets and lineitem at 32 buckets,
and computes an independent DuckDB oracle over the original files.

```bash
uv tool run --from tpchgen-cli==4.0.0 tpchgen-cli parquet \
  --scale-factor 10 --tables orders,lineitem --parts 16 --num-threads 4 \
  --output-dir .local/spj-tpch-sf10/raw --no-progress
uv run dev/prepare_spj_tpch.py .local/spj-tpch-sf10/raw .local/spj-tpch-sf10/bucketed
uv run dev/test_storage_partition_join.py --profile release-nonlto \
  --tpch-data .local/spj-tpch-sf10/bucketed --http
```

Use `--policy bias` for the placement control and
`--executor-cache-capacity 0` to disable session-runtime reuse. Per-visit
records include server response bytes and GET counts; the full server counters
are saved in `http-metrics.json`. First and repeated visits are recorded
separately. Fresh executors or disabled runtime reuse do not make the server's
OS page cache cold, and this test does not emulate cold S3 storage.

On macOS, add `--storage-cache cold` or `--storage-cache warm` to the HTTP
benchmark command to prepare and verify the server's OS page cache before
each timed query. The helper affects only the test inputs and rejects a run
if page residency does not match the requested state. It does not flush
disk-controller caches or change executor runtime reuse. The Q12 benchmark
also tests a 32/2 layout that reduces join parallelism below the four available
executor slots. `job-stages.json` retains the existing scheduler's task-duration
and input-row distributions for each job; visit records include job IDs for
correlation. See the [controlled cache results](storage-partition-join-cache.md).
