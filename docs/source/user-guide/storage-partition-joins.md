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

## Build and test

With the DataFusion prerequisite checked out beside this repository:

```bash
uv run dev/configure_storage_partition_join.py ../datafusion-spj
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

## Enable equal-layout storage joins

The session setting `ballista.optimizer.storage_partition_join` defaults to
`false`. Opt in with `SessionConfig::new_with_ballista().set_bool(
"ballista.optimizer.storage_partition_join", true)`.

The production AQE planner recognizes ordinary inner equi-joins whose inputs
have the same fully specified scheme and ordered group identities. Both Hash
Join and Sort-Merge Join are supported. Existing broadcast decisions remain
available. Different layouts, unsupported keys, and unsupported join semantics
retain ordinary planning. AQE preserves aligned source groups during replan;
projection without a key drops the distribution guarantee.

`EXPLAIN` reports the initial adaptive plan and each storage-group decision,
including the effective group count. Runtime statistics may change subsequent
AQE choices. `EXPLAIN ANALYZE` shows executed stages and their ordinary scan,
join, and exchange metrics. Partition-dependent dynamic filtering does not
assume native hash routing for externally grouped inputs.
