# Storage partition joins

Storage Partition Join (SPJ) uses a provider's existing file grouping to avoid
reshuffling compatible join inputs. Ballista does not own the table's storage
layout. The provider guarantees that the files assigned to a logical group
contain the rows for that group; the scheduler preserves that identity through
planning, task specialization, and retry.

This implementation requires the DataFusion `spj/distribution-contract-56` branch,
which adds `Partitioning::Grouped`. It is based on DataFusion 56.0.0-rc1. Build the
client, scheduler, and executors with the same dependency override; upstream
DataFusion 56.0.0-rc1 alone does not contain this property.

## Build and test

With the DataFusion prerequisite checked out beside this repository:

```bash
uv run dev/configure_storage_partition_join.py ../datafusion-spj-56
cargo check --config .local/spj-cargo.toml -p ballista-core -p ballista-scheduler
cargo test --config .local/spj-cargo.toml --profile ci \
  -p ballista --test storage_partition_join
```

The generated Cargo override contains local paths and stays in `.local`.
Build the client, scheduler, and executors with the same dependency override.

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
divisible layout is faster. Measure the workload before enabling it broadly.
Parent operators can still need their own exchange even when the join inputs do not.

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

## Inspect and validate

`EXPLAIN` shows `Grouped(scheme, group_ids, count)` on eligible scans. In a
compatible join, its input exchange is absent; with incompatible layouts the
usual exchange remains. Grouped properties are distinct from native hash
routing, including after projection and downstream joins. Partition-dependent
dynamic filtering is conservatively disabled for multi-group source joins.

Enable the storage-join optimizer's debug log to see divisor alignment and
fallback decisions. `EXPLAIN ANALYZE`
provides ordinary scan, join, and shuffle metrics. Storage reads and shuffle
traffic are separate costs.

The integration fixture uses real Parquet data, duplicate keys, nulls,
asymmetric empty buckets, several files per group, both join algorithms,
downstream native joins, and projection without the bucket key. It compares
rows with an ordinary-shuffle oracle. Planner coverage separately verifies that
exchanges are actually removed and that disabling divisor support restores
them.
