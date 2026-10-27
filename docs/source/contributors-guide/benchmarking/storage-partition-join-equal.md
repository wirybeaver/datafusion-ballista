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

# Equal-layout SPJ acceptance checkpoint

Validated on 2026-10-27 at Ballista
`5cd266b28c4692925fca44b139c642bdeb0c63c1`, with DataFusion
`5c977d74d7188e8445a3dd6f4a76c4f533fe6b0d`. This is the B4 checkpoint;
divisor grouping and affinity are not implemented in this branch.

The Apple M5 / 24 GiB host ran two native executors with two vcores and a
1 GB memory pool each, using bias placement and `release-nonlto` artifacts.
The 500,000-key synthetic workload uses 8/8 buckets over HTTP and an
unbucketed local-file scan control. SF10 Q12 uses 32/32 buckets over HTTP
and an independent DuckDB oracle over the original orders/lineitem files.
Broadcast is disabled and the hash-build budget is zero for measurements;
separate real-client correctness coverage exercises Sort-Merge Join.

Each workload visits off/on/on/off on identical files. Before every timed
query, the macOS helper either invalidates the input files' OS page caches
and verifies zero residency, or preloads them and verifies full residency.
Preparation and HTTP counter reads are outside timing. This does not flush
disk-controller caches or establish cold S3 behavior. Runs are serial, and
compilation completes before queries start. Every result matches its oracle.

| Workload | Server page cache | Mean off (s) | Mean on (s) |
| --- | --- | ---: | ---: |
| synthetic / equal | cold | 0.049524 | 0.026205 |
| synthetic / unbucketed-control | cold | 0.007855 | 0.007814 |
| synthetic / equal | warm | 0.057284 | 0.023047 |
| synthetic / unbucketed-control | warm | 0.012973 | 0.010350 |
| q12 / equal | cold | 1.529613 | 1.122581 |
| q12 / equal | warm | 1.377962 | 0.841131 |

There are only two observations per arm. These timings are bounded local
evidence, not an official TPC result or a production performance guarantee.
The raw [CSV](storage-partition-join-equal.csv) records every visit, shuffle
file bytes, HTTP payload bytes, task counts, and join-stage duration/input-row
distributions from the scheduler API. The scan control has no HTTP payload
because it uses local files. SPJ removes the join's shuffle but still reads
storage; Q12's aggregation exchange remains.

Peak combined executor RSS is sampled every 100 ms over each entire run,
including both feature settings. It can miss short peaks and cannot establish
a per-query memory improvement. Full metrics and plans remain in these run
directories in the stack worktree:

- `.local/spj-cluster-1793118499506171000`: synthetic cold, peak RSS 283,712 KiB.
- `.local/spj-cluster-1793118607316428000`: synthetic warm, peak RSS 256,752 KiB.
- `.local/spj-cluster-1793118665414711000`: q12 cold, peak RSS 696,576 KiB.
- `.local/spj-cluster-1793118675661908000`: q12 warm, peak RSS 736,560 KiB.

The two-process HTTP correctness runs passed with bias and round-robin.
The bias failure run killed an executor after task binding, observed lost-task
recovery, and still passed all three client tests. Direct task-codec coverage
also replays a manually constructed grouped join over real files, including
multiple files per input, empty groups, duplicates, and nulls.

Reproduce with the [SPJ build and provider instructions](../../user-guide/storage-partition-joins.md), then run:

```bash
uv run dev/test_storage_partition_join.py --profile release-nonlto \
  --benchmark --http --storage-cache cold --policy bias
```

Repeat with `--storage-cache warm`. For Q12, replace `--benchmark` with
`--tpch-data .local/spj-tpch-sf10/bucketed`. The controlled cache mode currently
requires macOS. This checkpoint does not complete the roadmap: divisor
alignment, bucket affinity, and their combined acceptance remain mandatory.
