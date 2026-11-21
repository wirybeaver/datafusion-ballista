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


"""Read-only HTTP range store for the SPJ process harness (loopback only)."""
import http.server
import json
import re
import threading
import urllib.parse
from pathlib import Path


class RangeStore(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, root):
        self.root = Path(root).resolve()
        self.metrics_lock = threading.Lock()
        self.metrics = {"get_requests": 0, "head_requests": 0, "response_bytes": 0}
        super().__init__(("127.0.0.1", 0), Handler)

    def snapshot(self):
        with self.metrics_lock:
            return dict(self.metrics)

    def add(self, key, value):
        with self.metrics_lock:
            self.metrics[key] += value


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def do_HEAD(self):
        self.serve(head=True)

    def do_GET(self):
        self.serve(head=False)

    def serve(self, head):
        if self.path == "/__spj_metrics":
            payload = json.dumps(self.server.snapshot()).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if not head:
                self.wfile.write(payload)
            return
        path = (self.server.root / urllib.parse.unquote(urllib.parse.urlsplit(self.path).path).lstrip("/")).resolve()
        if not path.is_relative_to(self.server.root) or not path.is_file():
            self.send_error(404)
            return
        stat = path.stat()
        start, end = 0, stat.st_size - 1
        requested = self.headers.get("Range")
        if requested:
            match = re.fullmatch(r"bytes=(\d+)-(\d*)", requested)
            if not match:
                self.send_error(416)
                return
            start = int(match[1])
            end = min(int(match[2]) if match[2] else end, end)
            if start > end:
                self.send_error(416)
                return
        self.send_response(206 if requested else 200)
        self.send_header("Content-Length", str(end - start + 1))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("ETag", f'"{stat.st_size}-{stat.st_mtime_ns}"')
        self.send_header("Last-Modified", self.date_time_string(stat.st_mtime))
        self.send_header("Cache-Control", "no-store")
        if requested:
            self.send_header("Content-Range", f"bytes {start}-{end}/{stat.st_size}")
        self.end_headers()
        self.server.add("head_requests" if head else "get_requests", 1)
        if head:
            return
        try:
            with path.open("rb") as data:
                data.seek(start)
                remaining = end - start + 1
                while remaining:
                    chunk = data.read(min(remaining, 256 * 1024))
                    if not chunk:
                        raise OSError("Immutable test object changed during read")
                    self.wfile.write(chunk)
                    self.server.add("response_bytes", len(chunk))
                    remaining -= len(chunk)
        except (BrokenPipeError, ConnectionResetError):
            # Expected if an executor is killed during the failure test.
            pass


def start_store(root):
    server = RangeStore(root)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
