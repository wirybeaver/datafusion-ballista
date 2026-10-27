#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = []
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


"""Run the SPJ integration test against two real executor processes.

Build and resolve the scheduler, executors, and integration test through Cargo
for the requested profile and local DataFusion override. Logs are kept in .local.
Only the processes created by this command are stopped during cleanup.
"""
import argparse
import json
import os
import re
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
import urllib.request


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def build_artifacts(root, profile, logs):
    """Resolve binaries from this Cargo invocation, including Fresh artifacts."""
    common = ["cargo", "--config", str(root / ".local/spj-cargo.toml")]
    commands = [
        [*common, "build", "--profile", profile, "-p", "ballista-scheduler", "-p", "ballista-executor", "--bins", "--message-format=json"],
        [*common, "test", "--profile", profile, "-p", "ballista", "--test", "storage_partition_join", "--no-run", "--message-format=json"],
    ]
    artifacts = {}
    for i, command in enumerate(commands):
        with (logs / f"build-{i}.stderr").open("w") as errors:
            result = subprocess.run(command, cwd=root, stdout=subprocess.PIPE, stderr=errors, text=True, check=True)
        (logs / f"build-{i}.jsonl").write_text(result.stdout)
        for line in result.stdout.splitlines():
            event = json.loads(line)
            if event.get("reason") == "compiler-artifact" and event.get("executable"):
                artifacts[event["target"]["name"]] = Path(event["executable"])
    for name in ["ballista-scheduler", "ballista-executor", "storage_partition_join"]:
        if name not in artifacts:
            raise RuntimeError(f"Cargo did not produce {name}; see {logs}")
    return artifacts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="ci")
    parser.add_argument("--policy", choices=["bias", "round-robin", "bucket-affinity"], default="bucket-affinity")
    parser.add_argument("--kill-executor", action="store_true")
    parser.add_argument("--benchmark", action="store_true")
    parser.add_argument("--tpch-data", type=Path)
    parser.add_argument("--http", action="store_true", help="Serve inputs through a local HTTP range store")
    parser.add_argument("--executor-cache-capacity", type=int, default=16)
    parser.add_argument("--storage-cache", choices=["cold", "warm"],
                        help="Verify server OS page-cache state before each measured query (macOS)")
    args = parser.parse_args()
    if args.benchmark and args.tpch_data:
        parser.error("Choose either the synthetic benchmark or TPC-H")
    if (args.benchmark or args.tpch_data) and args.profile != "release-nonlto":
        parser.error("Benchmark measurements require --profile release-nonlto")
    if args.storage_cache and (not args.http or not (args.benchmark or args.tpch_data)):
        parser.error("--storage-cache requires --http and a benchmark workload")
    if args.storage_cache and sys.platform != "darwin":
        parser.error("Verified --storage-cache preparation currently requires macOS")
    root = Path(__file__).resolve().parent.parent
    logs = root / ".local" / f"spj-cluster-{time.time_ns()}"
    logs.mkdir(parents=True)
    artifacts = build_artifacts(root, args.profile, logs)
    test = artifacts["storage_partition_join"]
    http_store = None
    data_root = args.tpch_data.resolve(strict=True) if args.tpch_data else logs / "inputs"
    data_root.mkdir(exist_ok=True)
    scheduler_port = free_port()
    children = []
    handles = []
    stop = threading.Event()
    killed = threading.Event()
    lost_executor = []
    watcher = None
    sampler = None
    peak_rss_kib = [0]

    def launch(binary, arguments, name):
        output = (logs / f"{name}.log").open("w")
        handles.append(output)
        proc = subprocess.Popen([str(artifacts[binary]), *arguments], cwd=root, stdout=output, stderr=subprocess.STDOUT)
        children.append(proc)
        return proc

    try:
        if args.http:
            from spj_http_store import start_store
            http_store = start_store(data_root)
        scheduler = launch("ballista-scheduler", [
            "--bind-host", "127.0.0.1", "--external-host", "127.0.0.1",
            "--bind-port", str(scheduler_port), "--task-distribution", args.policy,
            "--scheduler-policy", "push-staged", "--min-ready-executors", "2",
            "--executor-timeout-seconds", "5", "--expire-dead-executor-interval-seconds", "1",
            "--log-level-setting", "INFO,ballista_scheduler::cluster=DEBUG",
        ], "scheduler")
        deadline = time.monotonic() + 60
        while True:
            if scheduler.poll() is not None:
                raise RuntimeError(f"Scheduler exited; see {logs}")
            try:
                with socket.create_connection(("127.0.0.1", scheduler_port), timeout=0.2):
                    break
            except OSError:
                if time.monotonic() > deadline:
                    raise TimeoutError("Scheduler startup timed out")
                time.sleep(0.05)
        executors = []
        for i in range(2):
            work = logs / f"work-{i}"
            work.mkdir()
            executors.append(launch("ballista-executor", [
                "--id", f"spj-executor-{i}", "--scheduler-host", "127.0.0.1",
                "--scheduler-port", str(scheduler_port), "--scheduler-connect-timeout-seconds", "20",
                "--bind-host", "127.0.0.1", "--external-host", "127.0.0.1",
                "--bind-port", str(free_port()), "--bind-grpc-port", str(free_port()),
                "--bind-health-port", str(free_port()), "--vcores", "2",
                "--task-scheduling-policy", "push-staged", "--executor-heartbeat-interval-seconds", "1",
                "--work-dir", str(work), "--memory-pool-size", "1GB",
                "--session-runtime-cache-capacity", str(args.executor_cache_capacity),
            ], f"executor-{i}"))
        while True:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{scheduler_port}/readyz", timeout=0.5) as response:
                    if response.status == 200:
                        break
            except OSError:
                pass
            if any(p.poll() is not None for p in children) or time.monotonic() > deadline:
                raise RuntimeError(f"Cluster did not become ready; see {logs}")
            time.sleep(0.05)

        def sample_memory():
            while not stop.wait(0.1):
                live = [str(proc.pid) for proc in executors if proc.poll() is None]
                if not live:
                    return
                sample = subprocess.run(["ps", "-o", "rss=", "-p", ",".join(live)],
                                        capture_output=True, text=True)
                values = [int(value) for value in sample.stdout.split() if value.isdigit()]
                peak_rss_kib[0] = max(peak_rss_kib[0], sum(values))
        sampler = threading.Thread(target=sample_memory, daemon=True)
        sampler.start()

        if args.kill_executor:
            def fail_after_binding():
                # The fixture's first join has matching non-null keys in
                # partition 1. Fail its actual worker, not an arbitrary worker
                # that might only have received an empty/resultless group.
                if args.policy == "bucket-affinity":
                    pattern = r'bucket-affinity job=\S+ stage=0 group=1 preferred=Some\("[^"]+"\) executor=(spj-executor-[01])\b'
                else:
                    pattern = r'bind_one: job=\S+ stage=0 exec=(spj-executor-[01]) [^\n]*partitions=\[[^\]]*\b1\b'
                while not stop.wait(0.005):
                    text = (logs / "scheduler.log").read_text()
                    match = re.search(pattern, text)
                    if match:
                        worker = match[1]
                        lost_executor.append(worker)
                        executors[int(worker.rsplit("-", 1)[1])].kill()
                        killed.set()
                        return
            watcher = threading.Thread(target=fail_after_binding, daemon=True)
            watcher.start()
        env = dict(os.environ, BALLISTA_SPJ_SCHEDULER=f"df://127.0.0.1:{scheduler_port}",
                   BALLISTA_SPJ_WORK_DIRS=os.pathsep.join(str(logs / f"work-{i}") for i in range(2)),
                   BALLISTA_SPJ_SCHEDULER_LOG=str(logs / "scheduler.log"))
        env["BALLISTA_SPJ_DATA_ROOT"] = str(data_root)
        if http_store:
            env["BALLISTA_SPJ_HTTP_URL"] = f"http://127.0.0.1:{http_store.server_port}"
        if args.storage_cache:
            env["BALLISTA_SPJ_STORAGE_CACHE"] = args.storage_cache
        if args.tpch_data:
            env["BALLISTA_SPJ_TPCH_DATA"] = str(data_root)
        start = time.monotonic()
        with (logs / "test.log").open("w") as output:
            command = [str(test), "--nocapture"]
            if args.benchmark:
                command.extend(["--ignored", "storage_partition_join_benchmark"])
            elif args.tpch_data:
                command.extend(["--ignored", "storage_partition_join_tpch_q12"])
            elif args.kill_executor:
                # Other tests may submit unrelated jobs concurrently. Restrict
                # failure injection to the file-backed SPJ lifecycle scenario.
                command.append("storage_partition_join_matches_shuffle")
            result = subprocess.run(command, cwd=root, env=env, stdout=output, stderr=subprocess.STDOUT, timeout=900 if args.tpch_data else (300 if args.benchmark else 120))
        print((logs / "test.log").read_text(), end="")
        print(f"policy={args.policy}, killed={killed.is_set()}, elapsed={time.monotonic()-start:.3f}s, peak_executor_rss_kib={peak_rss_kib[0]}, logs={logs}")
        if result.returncode:
            raise RuntimeError("Distributed SPJ test failed")
        if args.benchmark or args.tpch_data:
            # Preserve existing scheduler metrics, including task duration and
            # input-row percentiles, before the ephemeral cluster shuts down.
            job_ids = sorted(set(re.findall(r"bind_one: job=(\S+)",
                                           (logs / "scheduler.log").read_text())))
            stages = {}
            for job_id in job_ids:
                with urllib.request.urlopen(f"http://127.0.0.1:{scheduler_port}/api/job/{job_id}/stages?plan_format=metrics", timeout=10) as response:
                    stages[job_id] = json.load(response)
            (logs / "job-stages.json").write_text(json.dumps(stages, indent=2) + "\n")
        if args.policy == "bucket-affinity" and not args.benchmark and not args.tpch_data and not args.kill_executor:
            # The first two queries reverse the join inputs over the same
            # inventories. Job/task IDs must not change group preferences.
            assignments = {}
            for job, group, preferred in re.findall(
                r'bucket-affinity job=(\S+) stage=0 group=(\d+) preferred=Some\("([^"]+)"\)',
                (logs / "scheduler.log").read_text(),
            ):
                assignments.setdefault(job, {})[group] = preferred
            first = list(assignments.values())[:2]
            if len(first) != 2 or first[0] != first[1]:
                raise RuntimeError("Equivalent storage groups did not retain placement preferences")
        if args.kill_executor:
            if not killed.is_set():
                raise RuntimeError("Failure injection did not fire")
            scheduler_log = (logs / "scheduler.log").read_text()
            if f"on lost Executor {lost_executor[0]}" not in scheduler_log:
                raise RuntimeError("Executor was killed but task recovery was not observed")
    finally:
        if http_store:
            (logs / "http-metrics.json").write_text(json.dumps(http_store.snapshot(), indent=2) + "\n")
            http_store.shutdown()
            http_store.server_close()
        stop.set()
        if watcher:
            watcher.join(timeout=1)
        if sampler:
            sampler.join(timeout=1)
        for proc in reversed(children):
            if proc.poll() is None:
                proc.terminate()
        for proc in reversed(children):
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        for handle in handles:
            handle.close()


if __name__ == "__main__":
    main()
