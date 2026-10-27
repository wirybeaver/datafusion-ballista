# Storage partition joins

Storage Partition Join (SPJ) uses a provider's existing file grouping to avoid
reshuffling compatible join inputs. Ballista does not own the table's storage
layout. The provider guarantees that the files assigned to a logical group
contain the rows for that group; the scheduler preserves that identity through
planning, task specialization, and retry.

This implementation requires the DataFusion `spj/distribution-contract` branch,
which adds `Partitioning::Grouped`. It is based on DataFusion 55.2.0. Build the
client, scheduler, and executors with the same dependency override; released
DataFusion 55.2.0 alone does not contain this property.

## Build the local stack

With the DataFusion prerequisite checked out beside this repository:

```bash
uv run dev/configure_storage_partition_join.py ../datafusion-spj
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

## Supply a bucket manifest

`ballista_core::storage_partitioning::BucketedParquetTable` is a reference
provider over an explicit immutable Parquet inventory. It supports one `Int32`
or `Int64` key with Iceberg integer bucketing:

- Hash the promoted eight-byte little-endian integer using Murmur3 x86-32,
  seed zero.
- Select `(hash & 0x7fffffff) % buckets`.
- Keep NULL keys in a separate group (`BucketFile::bucket = None`).

Each file belongs to exactly one declared bucket. Multiple files per bucket
are supported. The provider retains empty bucket positions and a separate null
position, so a four-bucket manifest has five execution groups even when some
are empty. Projecting away the key drops the partition guarantee and still
allows the query to execute.

The manifest supplies the registered object-store URL, key name, bucket count,
and each file's object path, exact byte length, optional exact row count, and
bucket label. A minimal construction is:

```rust
use std::sync::Arc;
use ballista_core::storage_partitioning::{
    BucketFile, BucketedParquetManifest, BucketedParquetTable,
};

// `schema` describes the files. The source, not Ballista, establishes that
// this immutable file contains only rows assigned to bucket zero.
let table = BucketedParquetTable::try_new(schema, BucketedParquetManifest {
    object_store_url: "s3://warehouse".into(),
    key: "customer_id".into(),
    buckets: 8,
    files: vec![BucketFile {
        path: "snapshot-42/customers/bucket-0.parquet".into(),
        size: 8192,
        bucket: Some(0),
        num_rows: None,
    }],
})?;
ctx.register_table("customers", Arc::new(table))?;
```

Use immutable/version-addressable paths and keep them accessible for the query
and its retries. The adapter does not implement Iceberg catalogs, delete files,
partition evolution, or cross-table snapshot isolation. It does not infer
bucketing from filenames. Register the required object store on the scheduler
and executors as for ordinary Ballista file scans.

## Enable planning and divisor grouping

Both optimization settings default to `false`. Set them in the session
configuration used to create the Ballista context:

```rust
let config = SessionConfig::new_with_ballista()
    .set_bool("ballista.optimizer.storage_partition_join", true)
    .set_bool("ballista.optimizer.storage_partition_join_divisor", true);
```

SPJ requires both adaptive planning and adaptive join selection. Disabling
either retains ordinary scan partitioning and reports the reason in `EXPLAIN`.

The production AQE planner aligns ordinary inner equi-joins on the supported
bucketing key before it creates exchanges. It uses the existing Hash Join or
Sort-Merge Join selection; source-provided per-group row counts allow the
existing hash-build-size budget to select Sort-Merge Join when appropriate.
Unknown source-group size also selects Sort-Merge Join when that budget is
enabled; setting the budget to zero explicitly opts into Hash Join.

Equal layouts match their semantic scheme and ordered group IDs. With divisor
grouping enabled, compatible 8/4 layouts combine the larger side's buckets
`{i, i+4}` with the smaller side's bucket `i`. NULL groups stay separate. File
groups are rebuilt before sorting; no global bucket-index arithmetic is sent
to an executor. Unsupported grouping rewrites, incompatible layouts, and
non-divisor counts retain the normal repartitioning path.

Divisor grouping reduces parallelism. The deployment can reject a merge below
`ballista.optimizer.storage_partition_join_min_buckets` non-null buckets
(default `1`, allowing all compatible counts). For example, a minimum of `8`
rejects an 8→4 merge and keeps the normal repartitioned join. Policy rejection
is reported separately from incompatible transforms. This is an explicit
parallelism constraint, not a general cost model or a promise that every
divisible layout is faster. Use the workload
benchmark before enabling it broadly. Parent operators can still need their
own exchange even when the join inputs do not.

The provider reports whole-scan as well as per-group statistics. A known-small
build remains eligible for the existing broadcast policy; that decision is
checked before regrouping the probe, preserving its original parallelism.
Source grouping is distinct from a join that already requested a shuffle.

`EXPLAIN` uses the initial AQE plan when adaptive planning is enabled. Its
`distributed_plan` row includes each join's storage-group decision, scheme,
effective execution-group count (including the null group), and effective
non-null bucket count. Rejections distinguish unsupported semantics/layouts
from disabled divisor alignment, the minimum-bucket policy, and an existing
broadcast choice. For example, an 8/4 alignment reports
`selected: aligned 8/4 buckets`, `effective_groups=5`, and
`effective_buckets=4`. Runtime statistics can change subsequent AQE decisions;
`EXPLAIN ANALYZE` shows the executed stages and their metrics.

AQE coalescing conservatively preserves stages containing storage groups,
including mixtures of an inline scan and a passthrough exchange. It also
preserves ordered exchanges because concatenating their sorted partitions
would lose the advertised ordering. Native unordered downstream shuffles
remain eligible for ordinary coalescing.

## Bucket affinity

For push scheduling, choose:

```text
ballista-scheduler --scheduler-policy push-staged --task-distribution bucket-affinity
ballista-executor --task-scheduling-policy push-staged ...
```

Affinity uses a deterministic rendezvous preference derived from the actual
source inventory, normalized group identity, and executor ID. The entire
aligned group is one scheduling unit. A saturated preferred executor causes an
immediate eligible-capacity fallback; losing an executor changes placement,
not the group's input files or retry identity. The existing task-attempt and
resource-accounting machinery remains responsible for retries.

Affinity is advisory. `bias` and `round-robin` remain valid baselines. Pull
scheduling only exposes the requesting executor to task binding, so this policy
explicitly falls back to ordinary capacity scheduling there. It does not claim
cross-executor affinity in pull mode. No new cache service or persistent
bucket-to-executor assignment is introduced.

## Inspect and validate

`EXPLAIN` shows `Grouped(scheme, group_ids, count)` on eligible scans. In a
compatible join, its input exchange is absent; with incompatible layouts the
usual exchange remains. Grouped properties are distinct from native hash
routing, including after projection and downstream joins. Partition-dependent
dynamic filtering is conservatively disabled for multi-group source joins.

Enable `ballista_scheduler::cluster=DEBUG` to see preferred/actual affinity
placement and capacity fallback reasons. Enable the storage-join optimizer's
debug log to see divisor alignment and fallback decisions. `EXPLAIN ANALYZE`
provides ordinary scan, join, and shuffle metrics. Storage reads and shuffle
traffic are separate costs.

Affinity diagnostics distinguish `preferred-capacity-exhausted` from
`preferred-ineligible`, while keeping the complete normalized group intact.

The integration fixture uses real Parquet data, duplicate keys, nulls,
asymmetric empty buckets, several files per group, both join algorithms,
downstream native joins, and projection without the bucket key. It compares
rows with an ordinary-shuffle oracle. Planner coverage separately verifies that
exchanges are actually removed and that disabling divisor support restores
them. The process harness additionally exercises executor loss.

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
correlation. See the [controlled cache results](../contributors-guide/benchmarking/storage-partition-join-cache.md).
