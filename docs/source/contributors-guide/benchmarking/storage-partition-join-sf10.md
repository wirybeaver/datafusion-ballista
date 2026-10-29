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

# TPC-H Q12 SF10 over an HTTP store

The subsequent [controlled cache experiment](storage-partition-join-cache.md)
adds verified cold/warm OS page caches, task distributions, and a 32/2 layout.
The results below retain their original source revision and methodology.

## Experiment

Validated on 2026-10-26 with Ballista `7f945c58f3ad2e5b416fb2292c1ed4307ee30066` and DataFusion
`5c977d74d`. This is a single TPC-H query validation, not the full TPC-H suite
or an official TPC performance result. The environment is the same Apple M5
host as the [initial measurements](storage-partition-join.md), with two native
executor processes, two vcores and a 1 GB pool per executor, and an optimized
`release-nonlto` build.

`tpchgen-cli 4.0.0` generated the original SF10 Parquet tables: **15,000,000
orders** and **59,986,052 lineitems**. The preparation script preserved all rows
and projected Q12's columns into immutable Iceberg-integer-bucket layouts:
orders at 16 and 32 buckets; lineitem at 32 buckets. The 32/32 layout exercises
equal-count SPJ; 32/16 exercises divisor alignment. PyArrow 26.0.0 and NumPy
2.5.4 performed the bounded-memory rewrite. DuckDB 2.0.0 evaluated the repository's
unchanged Q12 SQL against the **original** files, producing this independent oracle:

| Ship mode | High line count | Low line count |
| --------- | --------------: | -------------: |
| MAIL      |           62071 |          93045 |
| SHIP      |           62426 |          93261 |

The executors read projected files through a local read-only HTTP range server,
not through local file URLs. Server counters measure GET requests and response
payload bytes. Each policy run used fresh processes; each shape used separate
SPJ-off/on sessions. Visits were **off → on → on → off** with no query warm-up.
The first visits and repeats are kept individually below. Broadcast was disabled
and the hash-build budget was zero to hold the join algorithm constant.

An additional affinity run disabled session-runtime reuse. This isolates that
cache lifetime but does **not** evict the server's OS page cache or emulate a
cold remote S3 backend. HTTP counters are read before and after each measured
query, outside its timer. Both settings must match the independent oracle on
every visit. Analyzed plans assert that no order-key hash shuffle remains with SPJ;
the aggregation's ship-mode shuffle is still required.

## Key results

| Policy / runtime reuse | Layout  | Mean off (s) | Mean on (s) | Latency change | On: hash data bytes | On: HTTP response bytes |
| ---------------------- | ------- | -----------: | ----------: | -------------: | ------------------: | ----------------------: |
| bias / reuse 16        | equal   |     1.050898 |    0.611257 |         -41.8% |              101640 |               581005647 |
| bias / reuse 16        | divisor |     0.990198 |    0.715748 |         -27.7% |               51144 |               578201181 |
| affinity / reuse 16    | equal   |     1.020917 |    0.650373 |         -36.3% |              112008 |               581005647 |
| affinity / reuse 16    | divisor |     0.930539 |    0.638323 |         -31.4% |               56328 |               578201181 |
| affinity / reuse 0     | equal   |     0.978834 |    0.631306 |         -35.5% |              112008 |               581005647 |
| affinity / reuse 0     | divisor |     0.952157 |    0.620592 |         -34.8% |               56328 |               578201181 |

Latency change is `mean(on) / mean(off) - 1`; with two observations per arm,
mean and median coincide. First-visit effects and host variation are visible,
so these samples do not establish a production speedup or cache-hit advantage.

Order-key join shuffle was eliminated. Total hash-shuffle data fell from about
178 MB to 51–112 KB, which belongs to aggregation. Every query still read about
578–581 MB of HTTP payload: avoiding join shuffle did not eliminate storage
traffic. Disabling runtime reuse did not materially change these payload totals.
The affinity policy uses more tasks than bias because it binds a complete
storage group separately rather than bundling groups up to available vcores.

- Sampled peak combined executor RSS, bias / reuse 16: 1043.5 MiB (whole run, both SPJ settings).
- Sampled peak combined executor RSS, affinity / reuse 16: 1060.5 MiB (whole run, both SPJ settings).
- Sampled peak combined executor RSS, affinity / reuse 0: 1018.7 MiB (whole run, both SPJ settings).

## Raw visits

| Policy / runtime reuse | Layout  | Visit | SPJ |  Seconds | Hash data bytes | Tasks | HTTP GETs | HTTP payload bytes |
| ---------------------- | ------- | ----: | --- | -------: | --------------: | ----: | --------: | -----------------: |
| bias / reuse 16        | equal   |     1 | off | 1.180126 |       178240768 |    65 |      1353 |          581303263 |
| bias / reuse 16        | equal   |     2 | on  | 0.612834 |          101640 |    34 |      1279 |          581005647 |
| bias / reuse 16        | equal   |     3 | on  | 0.609680 |          101640 |    34 |      1279 |          581005647 |
| bias / reuse 16        | equal   |     4 | off | 0.921669 |       178239680 |    65 |      1347 |          581296736 |
| bias / reuse 16        | divisor |     1 | off | 1.017711 |       177471168 |    65 |      1355 |          578574461 |
| bias / reuse 16        | divisor |     2 | on  | 0.710558 |           51144 |    26 |      1263 |          578201181 |
| bias / reuse 16        | divisor |     3 | on  | 0.720938 |           51144 |    26 |      1263 |          578201181 |
| bias / reuse 16        | divisor |     4 | off | 0.962684 |       177452800 |    65 |      1351 |          578566371 |
| affinity / reuse 16    | equal   |     1 | off | 1.075390 |       178241600 |    65 |      1349 |          581298911 |
| affinity / reuse 16    | equal   |     2 | on  | 0.633201 |          112008 |    50 |      1279 |          581005647 |
| affinity / reuse 16    | equal   |     3 | on  | 0.667545 |          112008 |    50 |      1279 |          581005647 |
| affinity / reuse 16    | equal   |     4 | off | 0.966443 |       178239680 |    65 |      1349 |          581298913 |
| affinity / reuse 16    | divisor |     1 | off | 0.925153 |       177452928 |    65 |      1353 |          578570416 |
| affinity / reuse 16    | divisor |     2 | on  | 0.630897 |           56328 |    34 |      1263 |          578201181 |
| affinity / reuse 16    | divisor |     3 | on  | 0.645748 |           56328 |    34 |      1263 |          578201181 |
| affinity / reuse 16    | divisor |     4 | off | 0.935925 |       177472256 |    65 |      1349 |          578562326 |
| affinity / reuse 0     | equal   |     1 | off | 1.033616 |       178239872 |    65 |      1349 |          581298911 |
| affinity / reuse 0     | equal   |     2 | on  | 0.624503 |          112008 |    50 |      1279 |          581005647 |
| affinity / reuse 0     | equal   |     3 | on  | 0.638110 |          112008 |    50 |      1279 |          581005647 |
| affinity / reuse 0     | equal   |     4 | off | 0.924051 |       178239168 |    65 |      1347 |          581296736 |
| affinity / reuse 0     | divisor |     1 | off | 0.954217 |       177471808 |    65 |      1351 |          578566371 |
| affinity / reuse 0     | divisor |     2 | on  | 0.623397 |           56328 |    34 |      1263 |          578201181 |
| affinity / reuse 0     | divisor |     3 | on  | 0.617788 |           56328 |    34 |      1263 |          578201181 |
| affinity / reuse 0     | divisor |     4 | off | 0.950098 |       177471552 |    65 |      1351 |          578566371 |

## Standards audit

The review identified two concrete reproducibility defects: choosing the latest
executable by modification time could run another branch/feature build, and
JSON surrogate escapes could create invalid TOML for non-BMP Unicode paths.
The harness now resolves exact executables from Cargo JSON, including Fresh
artifacts; configuration writes UTF-8 without surrogate escapes. Both are fixed.

A possible hot-path cost was also identified: recomputing source affinity while
holding scheduling locks. Exhausted capacity now returns before traversing any
plan. Further namespace caching remains a profiling opportunity for large
manifests; this change does not claim to have eliminated all such overhead.

Standards: **two concrete defects fixed; one performance concern partly mitigated**.
The most consequential original issue was potentially validating the wrong build.

## Spec audit

The review found four partial requirements. Three implementation/coverage gaps
are addressed: source groups retain the existing broadcast choice, divisor
selection has a configurable minimum-bucket rejection policy, and result-level
coverage now includes incompatible schemes/orderings, non-divisor counts,
coalescing-enabled execution, and actual dynamic-filter routing decisions.

Two regressions first failed and then passed: a tiny build no longer reduces an
eight-bucket probe before broadcast; coalescing no longer shrinks only an
exchange side while leaving an inline storage scan unchanged. Result testing
also caught ordered partitions being concatenated by coalescing; that path now
preserves the original partitions. Native unordered coalescing remains enabled.

The performance requirement is strengthened by SF10 Q12, real HTTP payload
counters, first/repeated visits, and disabled-runtime-reuse control. **Cold S3,
SF100, the full TPC-H suite, and measured cache-hit improvements remain unverified.**

Spec: **three implementation/coverage gaps addressed; broader storage/performance
acceptance remains partial**. The most consequential original implementation
issue was suppressing broadcast for compatible storage inputs.

## Reproduce

See [the preparation and HTTP commands](../../user-guide/storage-partition-joins.md#remote-reads-and-tpc-h-q12).
The preparation refuses to replace existing output and writes the independent
oracle alongside each layout's manifest. The process harness records Cargo
artifact messages, scheduler/executor logs, HTTP counters, query results and
analyzed plans under `.local/spj-cluster-*`, and reaps all processes it starts.

The final run passed 227 AQE tests and three client regression tests, including
an HTTP two-executor failure/retry run. `cargo check`, formatting and Clippy
passed. The independent DataFusion grouped-domain checks remain in the prerequisite
branch; the added local DataFusion test also proves that grouped joins reject
partition-routed filters while a legitimately native downstream join actually
installs them and produces the same rows with filtering disabled.
