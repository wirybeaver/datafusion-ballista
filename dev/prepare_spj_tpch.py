#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyarrow==26.0.0", "numpy==2.5.4", "duckdb==2.0.0"]
# ///
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


"""Prepare immutable, projected TPC-H Q12 layouts from tpchgen Parquet.

Generate orders and lineitem with tpchgen-cli 4.0.0 first. This script preserves
all rows and just projects Q12's columns and groups files by Iceberg integer
buckets. It refuses to replace an existing output directory.
"""
import argparse
import json
from pathlib import Path
import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def iceberg_hash(values):
    values = values.astype(np.uint64, copy=False)
    h = np.zeros(len(values), dtype=np.uint32)
    for shift in (0, 32):
        k = (values >> np.uint64(shift)).astype(np.uint32)
        k *= np.uint32(0xcc9e2d51)
        k = (k << np.uint32(15)) | (k >> np.uint32(17))
        k *= np.uint32(0x1b873593)
        h ^= k
        h = (h << np.uint32(13)) | (h >> np.uint32(19))
        h = h * np.uint32(5) + np.uint32(0xe6546b64)
    h ^= np.uint32(8)
    h ^= h >> np.uint32(16)
    h *= np.uint32(0x85ebca6b)
    h ^= h >> np.uint32(13)
    h *= np.uint32(0xc2b2ae35)
    h ^= h >> np.uint32(16)
    return h & np.uint32(0x7fffffff)


def prepare(raw, output, table, key, columns, counts):
    files = sorted((raw / table).glob("*.parquet"))
    if not files:
        raise ValueError(f"No {table} Parquet files in {raw}")
    expected = sum(pq.ParquetFile(file).metadata.num_rows for file in files)
    layouts = {}
    for count in counts:
        directory = output / f"{table}-{count}"
        directory.mkdir()
        layouts[count] = {"dir": directory, "writers": {}, "buffers": [[] for _ in range(count)],
                          "pending": [0] * count, "rows": [0] * count}
    schema = None
    seen = 0

    def flush(layout, bucket):
        batches = layout["buffers"][bucket]
        if not batches:
            return
        table_data = pa.Table.from_batches(batches, schema=schema)
        if bucket not in layout["writers"]:
            layout["writers"][bucket] = pq.ParquetWriter(
                layout["dir"] / f"bucket-{bucket}.parquet", schema, compression="snappy")
        layout["writers"][bucket].write_table(table_data, row_group_size=131072)
        batches.clear()
        layout["pending"][bucket] = 0

    try:
        for file in files:
            for batch in pq.ParquetFile(file).iter_batches(batch_size=65536, columns=columns):
                # Use ordinary UTF-8 on the wire, independent of generator view types.
                schema = pa.schema([pa.field(f.name, pa.string() if pa.types.is_string_view(f.type) else f.type,
                                             nullable=f.nullable, metadata=f.metadata) for f in batch.schema])
                batch = pa.Table.from_batches([batch]).cast(schema).to_batches()[0]
                keys = batch.column(schema.get_field_index(key))
                if keys.null_count:
                    raise ValueError("TPC-H primary/foreign order keys must be non-null")
                hashes = iceberg_hash(keys.to_numpy())
                for count, layout in layouts.items():
                    labels = hashes % np.uint32(count)
                    for bucket in range(count):
                        indices = np.flatnonzero(labels == bucket)
                        if not len(indices):
                            continue
                        piece = batch.take(pa.array(indices))
                        layout["buffers"][bucket].append(piece)
                        layout["pending"][bucket] += len(indices)
                        layout["rows"][bucket] += len(indices)
                        if layout["pending"][bucket] >= 131072:
                            flush(layout, bucket)
                seen += batch.num_rows
        assert seen == expected
        for layout in layouts.values():
            for bucket in range(len(layout["rows"])):
                flush(layout, bucket)
    finally:
        for layout in layouts.values():
            for writer in layout["writers"].values():
                writer.close()
    for count, layout in layouts.items():
        assert sum(layout["rows"]) == expected
        entries = []
        for bucket in sorted(layout["writers"]):
            file = (layout["dir"] / f"bucket-{bucket}.parquet").resolve()
            assert pq.ParquetFile(file).metadata.num_rows == layout["rows"][bucket]
            entries.append({"path": str(file), "size": file.stat().st_size,
                            "bucket": bucket, "num_rows": layout["rows"][bucket]})
        manifest = {"object_store_url": "file://", "key": key, "buckets": count, "files": entries}
        (output / f"{table}-{count}.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(f"{table}: buckets={count}, rows={expected}, bytes={sum(f['size'] for f in entries)}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    assert int(iceberg_hash(np.array([34], dtype=np.int64))[0]) == 2017239379
    args.output.mkdir(parents=True, exist_ok=False)
    prepare(args.raw, args.output, "orders", "o_orderkey", ["o_orderkey", "o_orderpriority"], [16, 32])
    prepare(args.raw, args.output, "lineitem", "l_orderkey",
            ["l_orderkey", "l_shipmode", "l_shipdate", "l_commitdate", "l_receiptdate"], [32])
    # Independent engine over the original files, not the rebucketed output.
    with duckdb.connect(config={"threads": 4, "memory_limit": "4GB",
                               "temp_directory": str(args.output / "oracle-temp")}) as oracle:
        for table in ["orders", "lineitem"]:
            oracle.read_parquet(str(args.raw / table / "*.parquet")).create_view(table)
        query = (Path(__file__).resolve().parent.parent / "benchmarks/queries/q12.sql").read_text()
        result = oracle.execute(query).fetchall()
    (args.output / "oracle-q12.json").write_text(json.dumps(result) + "\n")
    print(f"DuckDB {duckdb.__version__} Q12 oracle: {result}", flush=True)



if __name__ == "__main__":
    main()
