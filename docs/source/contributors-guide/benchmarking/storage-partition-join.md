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

Subsequent validation: [SF10 Q12 over HTTP and implementation audit](storage-partition-join-sf10.md).

## Experiment

Validated the local implementation on 2026-10-25 (America/Los_Angeles), using
DataFusion prerequisite commit `5c977d74d` and Ballista implementation commit
`d6d66d48d1df07ff2e082d264f8e8d872b3dbeb1`. The build used Rust 1.99.1 and
`release-nonlto` on an Apple M5 (10 physical cores, 24 GiB RAM), macOS Darwin
25.6.0 arm64.

One scheduler and two separate executor processes shared immutable Parquet
files on the local filesystem. Each executor advertised two vcores and a 1 GB
memory pool. The dataset generator attempted 500,000 integer keys per table,
plus duplicate and null keys, and omitted different buckets on each side.
Each bucket had up to two files. The workloads were an 8/8 join and an 8/4
join, returning count/sum aggregates. An ordinary ListingTable scan without
bucket metadata provided the unrelated-query control.

Each scheduler policy ran in a fresh cluster. Both feature settings were
warmed before measured visits. Within each workload the visit order was
**off → on → on → off**, using identical files, session settings, and cluster
size. Broadcast was disabled and the hash-build budget was zero so the timed
joins used Hash Join. Separate correctness coverage exercised Sort-Merge Join.

Timings cover client query submission, execution, result collection, and
comparison with the baseline result. Hash-shuffle bytes are the measured sizes
of the completed consolidated hash-shuffle data files; they exclude result
files, index files, and storage reads. Task counts come from actual scheduler
bindings. The harness sampled combined executor RSS every 100 ms.

## Key results

| Scheduler       | Workload           | Mean off (ms) | Mean on (ms) | Latency change | Hash-shuffle bytes with SPJ |
| --------------- | ------------------ | ------------: | -----------: | -------------: | --------------------------: |
| bias            | equal              |        28.807 |        9.718 |         -66.3% |                           0 |
| bias            | divisor            |        38.418 |        8.597 |         -77.6% |                           0 |
| bias            | unbucketed-control |         5.118 |        5.277 |          +3.1% |                           0 |
| bucket-affinity | equal              |        26.953 |        8.567 |         -68.2% |                           0 |
| bucket-affinity | divisor            |        26.827 |        7.605 |         -71.7% |                           0 |
| bucket-affinity | unbucketed-control |         5.237 |        5.352 |          +2.2% |                           0 |

Latency change is `mean(on) / mean(off) - 1`. Each arm has only two visits;
the raw values below are the evidence, not a statistical performance guarantee.
The first bias/divisor baseline visit was noticeably slower than its second;
it is retained rather than discarded. The control varied by a few percent and
had unchanged task counts and no hash shuffle in either setting.

SPJ eliminated the joins' roughly 7–8 MB of hash-shuffle data files. Source
reads remained: the analyzed SPJ plans reported `bytes_scanned` of `8.64 M`
and `7.40 M` for the two equal-count inputs, and `8.64 M` and `7.48 M` for
the divisor inputs. These are the engine's displayed counters, not a claim of
remote network traffic. The storage was local shared filesystem, not S3.

- Sampled peak combined executor RSS, bias run: **258.9 MiB**. This covers the entire run, including both SPJ settings.
- Sampled peak combined executor RSS, bucket-affinity run: **247.5 MiB**. This covers the entire run, including both SPJ settings.

All query results matched their ordinary-shuffle oracle. The real-process
failure test killed one executor after binding storage-group tasks; the
scheduler reset lost tasks and completed the queries on the survivor. The
normal process run also checked that reversing join input order did not change
preferred placement for the same source inventories and normalized groups.

## Raw visits

| Scheduler       | Workload           | Visit | SPJ | Time (ms) | Hash-shuffle data bytes | Tasks |
| --------------- | ------------------ | ----: | --- | --------: | ----------------------: | ----: |
| bias            | equal              |     1 | off |    29.813 |                 7706912 |    19 |
| bias            | equal              |     2 | on  |    10.288 |                       0 |     6 |
| bias            | equal              |     3 | on  |     9.148 |                       0 |     6 |
| bias            | equal              |     4 | off |    27.802 |                 7706784 |    19 |
| bias            | unbucketed-control |     1 | off |     5.057 |                       0 |     7 |
| bias            | unbucketed-control |     2 | on  |     5.313 |                       0 |     7 |
| bias            | unbucketed-control |     3 | on  |     5.240 |                       0 |     7 |
| bias            | unbucketed-control |     4 | off |     5.179 |                       0 |     7 |
| bias            | divisor            |     1 | off |    50.816 |                 7056808 |    19 |
| bias            | divisor            |     2 | on  |     8.867 |                       0 |     4 |
| bias            | divisor            |     3 | on  |     8.327 |                       0 |     4 |
| bias            | divisor            |     4 | off |    26.019 |                 7056680 |    19 |
| bucket-affinity | equal              |     1 | off |    27.715 |                 7707040 |    19 |
| bucket-affinity | equal              |     2 | on  |     8.878 |                       0 |    10 |
| bucket-affinity | equal              |     3 | on  |     8.255 |                       0 |    10 |
| bucket-affinity | equal              |     4 | off |    26.192 |                 7706848 |    19 |
| bucket-affinity | unbucketed-control |     1 | off |     5.135 |                       0 |     7 |
| bucket-affinity | unbucketed-control |     2 | on  |     5.273 |                       0 |     7 |
| bucket-affinity | unbucketed-control |     3 | on  |     5.431 |                       0 |     7 |
| bucket-affinity | unbucketed-control |     4 | off |     5.340 |                       0 |     7 |
| bucket-affinity | divisor            |     1 | off |    27.805 |                 7056872 |    19 |
| bucket-affinity | divisor            |     2 | on  |     7.815 |                       0 |     6 |
| bucket-affinity | divisor            |     3 | on  |     7.395 |                       0 |     6 |
| bucket-affinity | divisor            |     4 | off |    25.849 |                 7056680 |    19 |

## Conclusion and limits

The implementation preserves results while removing input shuffle for the
supported equal/divisor layouts. Affinity provides stable preferences and
eligible-capacity fallback, and the combined divisor/affinity path recovered
from an actual executor process loss. Affinity intentionally binds one complete
group per task; it therefore used more tasks than bias scheduling, which may
bundle groups up to available vcores.

These short warm runs do not establish cold object-store performance,
production-scale speedups, or improved cache-hit rates. No OS cache eviction,
TPC-H SF10/SF100 run, or cache-hit instrumentation was performed. The peak RSS
samples cannot attribute memory savings to one arm. Both SPJ settings remain
off by default; selecting the affinity policy is explicit.

The end-to-end fixture also caught a downstream sort-merge correctness bug:
hash-shuffle task files concatenate several input partitions, so input sorting
cannot be advertised as output sorting. The implementation clears that
ordering claim and sorts downstream where required. The regression remains in
the distributed fixture, including dynamic-filter enabled/disabled contexts.

## Reproduce

Follow [the SPJ build instructions](../../user-guide/storage-partition-joins.md)
and build both binaries and the `storage_partition_join` test with
`--profile release-nonlto`. Then run:

```bash
uv run dev/test_storage_partition_join.py --profile release-nonlto
uv run dev/test_storage_partition_join.py --profile release-nonlto --kill-executor
uv run dev/test_storage_partition_join.py --profile release-nonlto --benchmark --policy bias
uv run dev/test_storage_partition_join.py --profile release-nonlto --benchmark --policy bucket-affinity
```

`BALLISTA_SPJ_BENCH_ROWS` changes the generated key count. The harness retains
scheduler/executor logs, analyzed plans, test output, and shuffle files under
`.local/spj-cluster-*`; all processes it creates are reaped on completion.
