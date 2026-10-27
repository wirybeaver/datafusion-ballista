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

# Storage partition joins with verified cold and warm page caches

## Scope and method

Measured on 2026-10-27 at Ballista
`7d19106fc420f0300c635079a0633afd33f338a9`, with DataFusion
`5c977d74d7188e8445a3dd6f4a76c4f533fe6b0d`. The host is an Apple M5 with
10 CPU cores and 24 GiB RAM, running macOS 26.6.2. Each run starts one
scheduler and two native executors, each with two vcores, a 1 GB memory pool,
and session-runtime cache capacity 16. Builds use `release-nonlto`. Benchmark
runs are serial; compilation finishes before the timed queries start.

The SF10 input files, Q12 SQL, and independent DuckDB oracle are unchanged from
the [earlier HTTP experiment](storage-partition-join-sf10.md). The added 32/2
case reassigns the existing orders-16 files to `bucket % 2`, which preserves
the declared modulo bucketing semantics and all rows. Only two nonempty join
groups can execute concurrently, compared with four available executor slots.

Each layout executes **off → on → on → off** on the same files. Every result
must match the independent oracle. Before **each** measured query, outside its
timer, `dev/spj_page_cache.py` prepares and verifies the storage server's OS
page cache:

- **Cold:** map each input file read-only and shared, call
  `msync(MS_SYNC | MS_INVALIDATE)`, and require zero resident pages.
- **Warm:** read each input file, then require all pages to be resident.
- Verify residency with `mincore`, without touching mapped bytes. Abort the
  run if the requested state is not observed. SF10 has 41,608 pages across
  80 Parquet files: every cold observation was 0/41,608; every warm observation
  was 41,608/41,608.

These use the documented macOS [cache invalidation](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/msync.2.html)
and [page-residency](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/mincore.2.html)
interfaces. Only the benchmark inputs are affected; there is no global cache
purge or new cache service. The HTTP server continues to use ordinary reads.
This controls its OS page cache, **not** disk-controller caches, executor
metadata caches, or a remote S3 service. It does not establish an affinity
cache-hit improvement.

## SF10 Q12 results

Times include submission, execution, collection, and oracle comparison. Each
mean has two observations; these are bounded local measurements, not an
official TPC result or a production performance guarantee.

| Server cache | Policy | Buckets | Mean off (s) | Mean on (s) |
| --- | --- | --- | ---: | ---: |
| cold | bias | 32/32 | 1.157774 | 0.679185 |
| cold | bias | 32/16 | 1.094194 | 0.776851 |
| cold | bias | 32/2 | 1.091890 | 1.103885 |
| cold | bucket-affinity | 32/32 | 1.134267 | 0.686760 |
| cold | bucket-affinity | 32/16 | 1.097876 | 0.666968 |
| cold | bucket-affinity | 32/2 | 1.104927 | 1.094705 |
| warm | bias | 32/32 | 1.027588 | 0.641558 |
| warm | bias | 32/16 | 0.989681 | 0.733513 |
| warm | bias | 32/2 | 0.986896 | 1.068322 |
| warm | bucket-affinity | 32/32 | 1.047995 | 0.657582 |
| warm | bucket-affinity | 32/16 | 0.973173 | 0.643558 |
| warm | bucket-affinity | 32/2 | 0.985126 | 1.086350 |

The 32/2 layout removes the join shuffle but loses its elapsed-time advantage:
the warm runs are about 8–10% slower than the ordinary-shuffle baseline. This
is evidence for retaining the configurable minimum-bucket policy; divisible
counts alone are not evidence that SPJ is cheaper.

All 48 measured Q12 visits matched the oracle. The executed SPJ plans had no
order-key shuffle. Total hash-shuffle data dropped from about 177–178 MB to
kilobytes of aggregation output, while each query still transferred about
578–581 MB of HTTP response payload. Cold and warm page-cache preparation
changes the backing read cost, not the requirement to transfer those bytes.

Task durations and input-row distributions come from the existing scheduler
API and are preserved in each run's `job-stages.json`. For example, the first
warm affinity SPJ visit has these join-stage measurements:

| Buckets | Tasks, including empty group | Task duration min / median / max (ms) | Input rows median / max |
| --- | ---: | --- | --- |
| 32/32 | 33 | 0 / 76 / 86 | 2,343,654 / 2,349,379 |
| 32/16 | 17 | 0 / 156 / 164 | 4,685,711 / 4,693,198 |
| 32/2 | 3 | 0 / 1,078 / 1,086 | 37,479,059 / 37,506,988 |

The minimum includes an empty null group. The two nonempty 32/2 tasks have
similar row counts, so their long duration reflects merged work and reduced
parallelism rather than a single hot key. Bias can bundle multiple partitions
in one task; affinity binds one complete storage group per task. Task counts
therefore differ between policies even for the same layout.

Sampled peak combined executor RSS was 1,158,304 KiB (cold bias), 1,186,048 KiB
(warm bias), 1,201,392 KiB (cold affinity), and 1,223,888 KiB (warm affinity).
Sampling is every 100 ms, covers the entire run including both settings, and
can miss short peaks; it does not establish a per-query memory improvement.

## Synthetic and unbucketed control

The same four cache/policy combinations also ran the 500,000-key synthetic
benchmark: 8/8, 8/4, and 8/2 joins over HTTP, plus an ordinary unbucketed
ListingTable scan. All 64 measured results matched the forced-shuffle baseline.
The join paths eliminated hash-shuffle data while retaining HTTP reads; raw
timings and byte counts are included in the CSV. The unrelated scan control
uses local file access, with the same per-file cache preparation, and retains
seven tasks and zero hash-shuffle bytes with either feature setting:

| Server cache | Policy | Control mean off (ms) | Control mean on (ms) |
| --- | --- | ---: | ---: |
| cold | bias | 7.4960 | 7.6235 |
| cold | bucket-affinity | 7.8375 | 7.7755 |
| warm | bias | 6.2985 | 6.3800 |
| warm | bucket-affinity | 6.5495 | 7.2815 |

Two short observations per arm are insufficient to establish a small control
regression or a universal performance win. Synthetic-run peak combined RSS
was 409,600 / 401,296 KiB for cold/warm bias and 401,104 / 388,688 KiB for
cold/warm affinity, subject to the same whole-run sampling limits.

## Reproduction and evidence

Prepare SF10 as described in the earlier report, configure the local
DataFusion override, then run each cache/policy combination separately:

```bash
uv run dev/test_storage_partition_join.py --profile release-nonlto \
  --tpch-data .local/spj-tpch-sf10/bucketed --http \
  --storage-cache cold --policy bias
```

Replace `cold` with `warm` and `bias` with `bucket-affinity` for the other
three runs. The controlled cache mode currently requires macOS. The ordinary
harness remains usable without it on other supported platforms.
Replace `--tpch-data .local/spj-tpch-sf10/bucketed` with `--benchmark` for the
synthetic workload and unbucketed control.

Raw per-visit results, correlated with join-stage task distributions, are in
[the CSV](storage-partition-join-cache.csv). Full plans, HTTP counters,
residency observations, scheduler task metrics, and process logs are retained
under these local run directories:

- `.local/spj-cluster-1793117176821182000`: cold bias.
- `.local/spj-cluster-1793117286646627000`: warm bias.
- `.local/spj-cluster-1793117305399858000`: cold affinity.
- `.local/spj-cluster-1793117324277935000`: warm affinity.
- `.local/spj-cluster-1793117352816414000`: synthetic cold bias.
- `.local/spj-cluster-1793117358628469000`: synthetic warm bias.
- `.local/spj-cluster-1793117364126654000`: synthetic cold affinity.
- `.local/spj-cluster-1793117369304295000`: synthetic warm affinity.

This report closes the controlled local cold/warm and reduced-parallelism
measurement gaps. It does not claim completion of the entire roadmap, a full
TPC-H suite, cold S3 behavior, or production cache-hit benefits.
