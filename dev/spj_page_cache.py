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

"""Verified per-file macOS page-cache preparation for local SPJ benchmarks.

Only the harness's Parquet inputs are affected. No global purge, file mutation,
new data cache, or assertion about the disk controller's cache is involved.
"""
import ctypes
import mmap
import os
import sys


def prepare(root, mode):
    if sys.platform != "darwin":
        raise RuntimeError("Verified page-cache preparation currently requires macOS")
    libc = ctypes.CDLL(None, use_errno=True)
    libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int,
                          ctypes.c_int, ctypes.c_int, ctypes.c_longlong]
    libc.mmap.restype = ctypes.c_void_p
    libc.msync.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int]
    libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
    libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]

    def checked(result):
        if result == -1:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))

    files = sorted(root.rglob("*.parquet"))
    # Finish preloading before checking residency: a later file may evict an
    # earlier one. Check the resulting state, not the state during preparation.
    if mode == "warm":
        for path in files:
            with path.open("rb", buffering=0) as data:
                while data.read(1024 * 1024):
                    pass
    total_pages = resident_pages = 0
    for path in files:
        with path.open("rb", buffering=0) as data:
            size = os.fstat(data.fileno()).st_size
            # MAP_SHARED matters: invalidating a private mapping does not
            # reliably evict the underlying file's cached pages on macOS.
            address = libc.mmap(None, size, mmap.PROT_READ, mmap.MAP_SHARED,
                                data.fileno(), 0)
            if address == ctypes.c_void_p(-1).value:
                checked(-1)
            try:
                if mode == "cold":
                    # Darwin <sys/mman.h>; Python does not expose MS_* flags.
                    checked(libc.msync(address, size, 0x10 | 0x02))
                pages = (size + mmap.PAGESIZE - 1) // mmap.PAGESIZE
                residency = (ctypes.c_ubyte * pages)()
                checked(libc.mincore(address, size, residency))
                resident = sum(value & 1 for value in residency)
                expected = 0 if mode == "cold" else pages
                if resident != expected:
                    raise RuntimeError(f"{path}: {resident}/{pages} resident pages after {mode} preparation")
                total_pages += pages
                resident_pages += resident
            finally:
                checked(libc.munmap(address, size))
    if not total_pages:
        raise RuntimeError("No Parquet pages found for cache preparation")
    return {"mode": mode, "files": len(files), "pages": total_pages,
            "resident_pages": resident_pages}


if __name__ == "__main__":
    import argparse
    import json
    from pathlib import Path
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("mode", choices=["cold", "warm"])
    args = parser.parse_args()
    print(json.dumps(prepare(args.root.resolve(strict=True), args.mode)))
