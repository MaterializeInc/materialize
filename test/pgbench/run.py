# Copyright Materialize, Inc. and contributors. All rights reserved.
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.

"""Spike: run upstream pgbench per cluster and merge a common measurement window.

Each script iteration is one SELECT, with no SQL transaction wrapper. Sessions
stay connected and prepared mode retains statements across the warmup boundary.
Full transaction logs provide exact, pooled percentiles. A separate unlogged run
qualifies their overhead, but cannot provide warmup-filtered percentiles.
"""

import json
import math
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path


def allocation(clients: int, clusters: int) -> list[int]:
    if clients < 1 or clusters < 1:
        raise ValueError("clients and clusters must be positive")
    active = min(clients, clusters)
    return [clients // active + (i < clients % active) for i in range(active)]


def records(path: Path):
    for line in path.open():
        fields = line.split()
        if len(fields) != 6:
            raise ValueError("unexpected pgbench transaction log format")
        client, sequence, latency, script, seconds, micros = fields
        if script != "0" or not latency.isdigit():
            raise ValueError("failed or unexpected pgbench transaction")
        yield int(client), int(latency), int(seconds) * 1_000_000 + int(micros)


def merge_logs(paths: list[Path], clients: int, warmup: float, duration: float) -> dict:
    first: dict[tuple[int, int], int] = {}
    last: dict[tuple[int, int], int] = {}
    for i, path in enumerate(paths):
        for client, latency, completed in records(path):
            key = (i, client)
            first.setdefault(key, completed)
            last[key] = completed
    if len(first) != clients:
        raise ValueError("not every client produced transactions")
    start = max(first.values()) + round(warmup * 1_000_000)
    end = start + round(duration * 1_000_000)
    if min(last.values()) < end:
        raise ValueError("pgbench logs do not cover a common full measurement window")
    histogram: Counter[int] = Counter()
    for path in paths:
        for _, latency, completed in records(path):
            if start <= completed < end:
                histogram[latency] += 1
    count = sum(histogram.values())
    if not count:
        raise ValueError("empty measurement window")

    def percentile(p: float) -> float:
        rank = math.ceil(count * p)
        seen = 0
        for latency, n in sorted(histogram.items()):
            seen += n
            if seen >= rank:
                return latency / 1000
        raise AssertionError("empty histogram")

    return {
        "started_at": datetime.fromtimestamp(start / 1e6, UTC).isoformat(),
        "finished_at": datetime.fromtimestamp(end / 1e6, UTC).isoformat(),
        "queries": count,
        "elapsed_seconds": duration,
        "qps": count / duration,
        "mean_latency_ms": sum(k * n for k, n in histogram.items()) / count / 1000,
        "p50_latency_ms": percentile(0.50),
        "p95_latency_ms": percentile(0.95),
        "p99_latency_ms": percentile(0.99),
    }


def summary(text: str) -> dict:
    if not re.search(r"number of failed transactions: 0 \(", text):
        raise ValueError("pgbench did not report zero failed transactions")
    result = {}
    for key, pattern in {
        "queries": r"number of transactions actually processed: (\d+)",
        "qps": r"^tps = ([\d.]+)",
        "mean_latency_ms": r"^latency average = ([\d.]+) ms",
    }.items():
        match = re.search(pattern, text, re.MULTILINE)
        if not match:
            raise ValueError(f"missing pgbench summary field: {key}")
        result[key] = float(match[1])
    if any(not math.isfinite(x) or x <= 0 for x in result.values()):
        raise ValueError("invalid pgbench summary")
    return result


def cpu_stat() -> dict[str, int]:
    try:
        return {
            k: int(v)
            for k, v in (
                line.split()
                for line in Path("/sys/fs/cgroup/cpu.stat").read_text().splitlines()
            )
        }
    except FileNotFoundError:
        return {}


def thread_ticks(processes) -> dict[str, int]:
    ticks = {}
    for process, _ in processes:
        for path in Path(f"/proc/{process.pid}/task").glob("*/stat"):
            try:
                fields = path.read_text().rsplit(")", 1)[1].split()
                ticks[str(path)] = int(fields[11]) + int(fields[12])
            except FileNotFoundError:
                pass
    return ticks


def run(config: dict) -> dict:
    clients = config["concurrency"]
    assignments = allocation(clients, len(config["libpq_envs"]))
    logged = config.get("transaction_logging", True)
    warmup, duration = config["warmup_seconds"], config["duration_seconds"]
    # Leave five seconds for staggered connections and the log-window boundary.
    seconds = math.ceil(warmup + duration + 5)
    results = []
    processes = []
    with tempfile.TemporaryDirectory(prefix="qps-pgbench-") as tmp:
        root = Path(tmp)
        script = root / "query.sql"
        script.write_text(config["query"].rstrip("; \n") + ";\n")
        for i, n in enumerate(assignments):
            env = os.environ | config["libpq_envs"][i]
            preflight = subprocess.run(
                [
                    "psql",
                    "-XAt",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-c",
                    f"SELECT current_setting('cluster'), current_setting('statement_logging_sample_rate')::float, count(*) FROM ({config['query']}) q",
                ],
                env=env,
                capture_output=True,
                text=True,
                timeout=config["query_timeout_seconds"] + 10,
            )
            fields = preflight.stdout.strip().split("|")
            if preflight.returncode or len(fields) != 3:
                print(preflight.stdout, preflight.stderr, file=sys.stderr)
                raise ValueError(f"cluster {i}: logging/row-count preflight failed")
            if (
                fields[0] != config["cluster_names"][i]
                or float(fields[1]) != 0
                or int(fields[2]) != config["expected_rows"]
            ):
                raise ValueError(
                    f"cluster {i}: unexpected cluster, logging rate or row count: {fields}"
                )
        before = cpu_stat()
        wall_start = time.monotonic()
        peak_thread_percent = 0.0
        try:
            for i, n in enumerate(assignments):
                output = (root / f"summary-{i}").open("w+")
                processes.append(
                    (
                        subprocess.Popen(
                            [
                                "pgbench",
                                "-n",
                                "-M",
                                config["protocol"],
                                "-c",
                                str(n),
                                "-j",
                                str(min(n, config.get("threads_per_cluster", 1))),
                                "-T",
                                str(seconds),
                                "-f",
                                str(script),
                            ]
                            + (
                                ["-l", f"--log-prefix={root / f'transactions-{i}'}"]
                                if logged
                                else []
                            ),
                            env=os.environ | config["libpq_envs"][i],
                            stdout=output,
                            stderr=output,
                            text=True,
                        ),
                        output,
                    )
                )
            deadline = wall_start + seconds + config["query_timeout_seconds"] + 30
            previous = thread_ticks(processes)
            sample_time = time.monotonic()
            while any(process.poll() is None for process, _ in processes):
                if time.monotonic() >= deadline:
                    raise TimeoutError("pgbench exceeded its query/run deadline")
                time.sleep(0.5)
                now = time.monotonic()
                current = thread_ticks(processes)
                if current:
                    peak_thread_percent = max(
                        peak_thread_percent,
                        (
                            max(
                                100
                                * (value - previous[key])
                                / os.sysconf("SC_CLK_TCK")
                                / (now - sample_time)
                                for key, value in current.items()
                                if key in previous
                            )
                            if current.keys() & previous.keys()
                            else 0
                        ),
                    )
                previous, sample_time = current, now
            for i, (process, output) in enumerate(processes):
                status = process.wait(timeout=max(0.1, deadline - time.monotonic()))
                output.seek(0)
                text = output.read()
                print(f"--- cluster {i}\n{text}", file=sys.stderr)
                if status:
                    raise ValueError(
                        f"cluster {i}: pgbench exited with status {status}"
                    )
                results.append(summary(text))
        finally:
            for process, output in processes:
                if process.poll() is None:
                    process.kill()
                process.wait()
                output.close()
        elapsed = time.monotonic() - wall_start
        after = cpu_stat()
        if logged:
            result = merge_logs(
                sorted(root.glob("transactions-*.*")), clients, warmup, duration
            )
        else:
            count = sum(r["queries"] for r in results)
            result = {
                "queries": count,
                "qps": sum(r["qps"] for r in results),
                "mean_latency_ms": sum(
                    r["queries"] * r["mean_latency_ms"] for r in results
                )
                / count,
            }
        capacity = (
            len(getattr(os, "sched_getaffinity")(0))
            if hasattr(os, "sched_getaffinity")
            else os.cpu_count() or 1
        )
        try:
            quota, period = Path("/sys/fs/cgroup/cpu.max").read_text().split()
            if quota != "max":
                capacity = min(capacity, int(quota) / int(period))
        except FileNotFoundError:
            pass
        cpu = (after.get("usage_usec", 0) - before.get("usage_usec", 0)) / 1e6 / elapsed
        periods = after.get("nr_periods", 0) - before.get("nr_periods", 0)
        throttled = (
            100
            * (after.get("nr_throttled", 0) - before.get("nr_throttled", 0))
            / periods
            if periods
            else 0
        )
        warnings = []
        if not before or not after:
            warnings.append("load driver cgroup telemetry unavailable")
        if 100 * cpu / capacity >= 80 or throttled >= 5:
            warnings.append(
                "load driver CPU usage or throttling indicates possible saturation"
            )
        if peak_thread_percent >= 90:
            warnings.append(
                "a pgbench thread reached 90% CPU; qualify with more driver threads"
            )
        return result | {
            "concurrency": clients,
            "clusters": len(assignments),
            "protocol": config["protocol"],
            "errors": 0,
            "statement_logging_sample_rate": 0,
            "transaction_logging": logged,
            "driver_name": "pgbench",
            "threads_per_cluster": config.get("threads_per_cluster", 1),
            "pgbench_version": subprocess.check_output(
                ["pgbench", "--version"], text=True
            ).strip(),
            "native_summaries": results,
            "driver": {
                "cpu_percent": 100 * cpu / capacity,
                "cpu_cores": cpu,
                "capacity_cores": capacity,
                "throttled_period_percent": throttled,
                "peak_thread_cpu_percent": peak_thread_percent,
                "scheduling_lag_p99_ms": None,
                "warnings": warnings,
            },
        }


if __name__ == "__main__":
    json.dump(run(json.load(sys.stdin)), sys.stdout)
    print()
