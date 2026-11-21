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

# Storage partition join acceptance record

The feature roadmap is maintained in [upstream issue #1677](https://github.com/apache/datafusion-ballista/issues/1677).
The Ballista feature PRs are stacked as `B1 -> B2 -> B3 -> B4`:

- [B1: provider layouts and executable tasks](https://github.com/apache/datafusion-ballista/pull/2694)
- [B2: AQE integration](https://github.com/wirybeaver/datafusion-ballista/pull/6)
- [B3: divisor alignment](https://github.com/wirybeaver/datafusion-ballista/pull/14)
- [B4: bucket affinity](https://github.com/wirybeaver/datafusion-ballista/pull/9)

This [personal validation PR](https://github.com/wirybeaver/datafusion-ballista/pull/7)
builds on B4; no feature PR depends on it. The DataFusion prerequisite is
[the grouping contract](https://github.com/apache/datafusion/pull/26875).
The Ballista integration uses its DataFusion 56.0.0-rc1 backport,
`spj/distribution-contract-56` at `11fcc3cb72e338673e4e2958ccf68ca3d6d47ffd`.

The evidence below records validation and review before the PR restack. The
measured stack used Ballista `b230f244` and DataFusion `5c977d74d`, based on
Ballista upstream `98d724b3bd9b603211f444d992b0bcca13c75bca`. Those timings are
not measurements of later refactoring or of the PR restack. The historical
checkpoints below are validation records, not additional
roadmap tasks.

The [user guide](../../user-guide/storage-partition-joins.md) describes the build
override, manifest and configuration contracts. The provider owns row-to-bucket
membership and supplies immutable/version-addressable paths. Ballista validates
descriptor structure and preserves each input's read view; it does not provide
cross-table snapshot isolation. Arbitrary providers must participate in scan
restriction and serialization to expose this capability.

## Correctness evidence

The [client scenarios](../../../../ballista/client/tests/storage_partition_join.rs)
use independently bucketed real Parquet with multiple files per bucket,
asymmetric missing groups, duplicates and nulls. Complete result multisets are
compared with ordinary-shuffle execution. They cover both join algorithms,
both divisor orientations, empty scans, filters, projection without the key,
downstream native joins and aggregation, and coalescing enabled.

Fallback coverage includes different schemes and group orderings, non-divisor
counts, a normal provider/native input, left outer joins, disabled adaptive
planning, and disabled adaptive join selection. Unsupported optimization
preserves query results. Direct DataFusion execution verifies that grouped
joins do not produce native partition-routed filters, while the downstream
native join actually exercises them; results agree with filtering disabled.

The [task-builder scenario](../../../../ballista/scheduler/src/state/task_builder.rs)
executes a manually assembled grouped join after real physical-plan encoding
and decoding. It checks all selected files, empty groups, duplicates, nulls,
global-to-local identities. Retry is covered by the graph/process scenarios.

The [AQE placement/recovery scenario](../../../../ballista/scheduler/src/state/aqe/test/bucket_affinity.rs)
uses production AQE and cluster binding for an 8/4 join. It verifies stable
preferences across job IDs and membership iteration order, saturation,
eligibility restrictions, member addition and loss. Retried task plans retain
both inputs' complete normalized read descriptors through the physical codec.
A stale successful protobuf message cannot replace the current task statuses.
These control-flow checks complement the real-process row-parity failure run.

The HTTP process harness targets the worker assigned a non-null matching group
in the first divisor join, observes lost-task recovery, and verifies the rows.
Affinity-off bias and round-robin runs also pass. Placement and eligibility are
advisory and cannot change group membership. Diagnostics distinguish exhausted
capacity from an ineligible preferred executor. Pull scheduling explicitly
uses capacity fallback.

The [storage join rule](../../../../ballista/scheduler/src/state/aqe/optimizer_rule/storage_join.rs)
is tested through the actual AQE planner and EXPLAIN path. Tests cover selected
group counts, incompatible layouts, disabled divisor support, the minimum-bucket
policy, and preservation of broadcast/probe parallelism. The integration with
upstream #2663 retains its protection against broadcasting an unmeasured join
output, including source-partitioned inputs. Deferred child join selection is
reported explicitly rather than presented as an already selected SPJ.

## Historical checks and performance evidence

Each of the six Ballista branch tips passed, independently:

```bash
cargo check --locked --config .local/spj-cargo.toml \
  -p ballista-core -p ballista-scheduler -p ballista --all-targets
```

The final integration also passed strict clippy, 234 AQE tests, 20 cluster
tests, 10 task-builder tests, four real-client correctness tests, and the
targeted two-executor failure scenario. One pre-existing native coalesced-reader
metadata test remains ignored; SPJ preserves its grouped domains by preventing
one-sided coalescing, and the relevant mixed/downstream cases have result-level
coverage. D0 passed cargo check, all-target/all-feature clippy and its grouped
wire-identity test; the file-backed scenarios exercise its property machinery
and codecs end to end.

The [equal-layout checkpoint](storage-partition-join-equal.md)
records the independent equal-layout baseline. The [final controlled-cache report](storage-partition-join-cache.md)
and [112 raw measurements](storage-partition-join-cache.csv) cover
equal counts, divisor counts, affinity off/on, reduced parallelism, and an
unbucketed control. They record hash-shuffle bytes, HTTP payload bytes, elapsed
time, task count and duration/input-row distributions, and sampled executor RSS.
Cold/warm server page residency is verified before each timed query.

SF10 Q12 qualifies on its order-key join and matches an independent DuckDB
oracle over the original files. Its aggregation still shuffles. The 32/2 case
demonstrates that reduced parallelism can outweigh avoided join shuffle; the
minimum-bucket policy remains available. The measurements establish neither
cold S3 behavior nor a cache-hit benefit from affinity, and do not promote any
default setting.

Real Iceberg/Hive catalogs and deletes/evolution, outer/semi/anti SPJ eligibility,
non-hash transforms, skew splitting/replication, grouping-aware aggregation,
and hard locality remain outside the roadmap's declared scope.

## Branch review

The whole stack was read before reviewing each delivery branch. The review
kept the provider/alignment/placement separation and made these focused changes:

- D0 shares the immutable scheme/group-ID layout across projections and key
  remapping, avoiding repeated copies and uniqueness checks.
- The manifest adapter keeps its documented manifest validation; no independent
  issue justified
  changing its input contract.
- The task-codec test executes its codec scenario once. Repeating identical work
  did not test
  retry; the existing graph/process scenarios cover that lifecycle.
- AQE storage-join planning compares normalized join expressions with storage
  keys directly. A
  singleton satisfies any distribution requirement, but that is not evidence
  that its storage key is the join key. The old shortcut caused unnecessary
  regrouping and a false selected-SPJ explanation; downstream AQE could still
  restore ordinary repartitioning. Per-group build-size estimation now folds
  directly instead of allocating an intermediate vector.
- The validation harness shares remote/standalone context setup. Warm-cache
  preparation finishes
  loading all files before checking their residency, so later preloads cannot
  invalidate earlier observations unnoticed.
- Divisor alignment adds one planning regression for two singleton scans joined
  on a different
  column. It verifies preserved group identities and the rejection reason,
  rather than claiming every valid fallback must introduce an exchange.
- Bucket affinity prepares immutable affinity keys once per running stage.
  Membership,
  eligibility and capacity remain live decisions on each bind. Duplicate codec
  assertions in the capacity scenarios were removed; retry still checks the
  serialized input descriptors.

Validation after review: grouped wire-identity test, task codec execution,
AQE tests, client result parity, actual two-executor HTTP failure recovery,
strict clippy, and per-branch cargo check. A real warm/cold preparation of all
80 SF10 files observed 41,608/41,608 and 0/41,608 resident pages respectively.
No new benchmark speedup is claimed from these refactorings.
