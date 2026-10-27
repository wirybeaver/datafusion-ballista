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

## Distributed validation and measurements

The process harness uses two executors and supports the existing `bias` and
`round-robin` policies. It exercises actual provider/task transport, both join
algorithms, asymmetric empty groups, multiple files, duplicate/null keys,
projection without the partition key, downstream joins, and executor loss.

```bash
uv run dev/test_storage_partition_join.py --policy bias --http --kill-executor
uv run dev/test_storage_partition_join.py --policy round-robin --http
uv run dev/test_storage_partition_join.py --profile release-nonlto \
  --benchmark --http --storage-cache cold --policy bias
```

The synthetic benchmark alternates feature off/on on identical files and
includes an unbucketed scan control. It reports elapsed time, task count,
actual hash-shuffle file bytes, and HTTP payload bytes. `job-stages.json`
retains task-duration and input-row distributions. Combined executor RSS is
sampled every 100 ms over the whole run.

On macOS, `--storage-cache cold` invalidates only the input files' OS pages
and verifies zero residency with `mincore`; `--storage-cache warm` preloads
them and verifies full residency. Preparation happens outside query timing.
This controls the storage server's OS page cache, not disk-controller or
executor caches, and does not emulate cold S3. The reference HTTP store does
not cache response data. Fewer exchanges do not mean zero network reads.

`dev/prepare_spj_tpch.py` prepares immutable bucketed Q12 inputs and an
independent DuckDB oracle from original TPC-H Parquet. Pass the prepared
folder using `--tpch-data PATH --http --profile release-nonlto` for the
32/32 Q12 checkpoint. This branch is the equal-count acceptance milestone;
divisor grouping and bucket affinity remain required later in the stack.
