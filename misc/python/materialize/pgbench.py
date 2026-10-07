# Copyright Materialize, Inc. and contributors. All rights reserved.
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.

"""Run upstream pgbench per cluster and merge a common measurement window.

Each script iteration is one SELECT, with no SQL transaction wrapper. Sessions
stay connected and prepared mode retains statements across the warmup boundary.
This minimal wrapper is tailored to the spec-sheet sweep. Transaction logs provide
pooled percentiles and maximum latency after warmup.
"""

import math
import re
import shlex
import subprocess
import sys
import tempfile
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from materialize import MZ_ROOT
from materialize.mzcompose.service import Service

if TYPE_CHECKING:
    from materialize.mzcompose.composition import Composition


def allocation(clients: int, clusters: int) -> list[int]:
    if clients < 1 or clusters < 1:
        raise ValueError("clients and clusters must be positive")
    active = min(clients, clusters)
    return [clients // active + (i < clients % active) for i in range(active)]


def records(path: Path):
    with path.open() as log:
        for line in log:
            fields = line.split()
            if len(fields) != 6:
                raise ValueError("unexpected pgbench transaction log format")
            client, sequence, latency, script, seconds, micros = fields
            if script != "0" or not latency.isdigit():
                raise ValueError("failed or unexpected pgbench transaction")
            yield int(client), int(latency), int(seconds) * 1_000_000 + int(micros)


def observe_startup(root: Path, assignments: list[int], first: dict) -> float | None:
    """Return the latest first completion once every client has executed a query."""
    for i, clients in enumerate(assignments):
        if sum(cluster == i for cluster, _ in first) == clients:
            continue
        for path in root.glob(f"transactions-{i}.*"):
            with path.open() as log:
                for line in log:
                    # A concurrently written final line may not be complete yet.
                    if not line.endswith("\n"):
                        break
                    fields = line.split()
                    if len(fields) != 6 or not fields[2].isdigit() or fields[3] != "0":
                        raise ValueError("failed or unexpected pgbench transaction")
                    first.setdefault(
                        (i, int(fields[0])), int(fields[4]) + int(fields[5]) / 1e6
                    )
                    if sum(cluster == i for cluster, _ in first) == clients:
                        break
    if len(first) == sum(assignments):
        return max(first.values())
    return None


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
        "max_latency_ms": max(histogram) / 1000,
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


def container_output(container: str, *args: str) -> str:
    return subprocess.check_output(["docker", "exec", container, *args], text=True)


def cpu_stat(container: str) -> dict[str, int]:
    text = container_output(
        container, "sh", "-c", "cat /sys/fs/cgroup/cpu.stat 2>/dev/null || true"
    )
    return {k: int(v) for k, v in (line.split() for line in text.splitlines())}


def thread_ticks(container: str) -> dict[str, int]:
    text = container_output(
        container,
        "sh",
        "-c",
        'for file in /work/pid-*; do [ -f "$file" ] || continue; '
        'read -r pid < "$file"; for stat in /proc/$pid/task/*/stat; do '
        'cat "$stat" 2>/dev/null || true; done; done',
    )
    ticks = {}
    for line in text.splitlines():
        fields = line.rsplit(")", 1)[1].split()
        ticks[line.split()[0]] = int(fields[11]) + int(fields[12])
    return ticks


def signal_clients(container: str, signal: str) -> None:
    container_output(
        container,
        "sh",
        "-c",
        'for file in /work/pid-*; do [ -f "$file" ] || continue; '
        f'read -r pid < "$file"; kill -{signal} "$pid" 2>/dev/null || true; done',
    )


def run(config: dict, composition: "Composition") -> dict:
    """Run pgbench in the existing Postgres image, supervising it from Python."""
    # Bind mounts must live in the checkout shared with the CI Docker host.
    temporary_root = MZ_ROOT / "temp"
    temporary_root.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="qps-pgbench-", dir=temporary_root) as tmp:
        root = Path(tmp)
        service = Service(
            "qps-pgbench",
            {
                "mzbuild": "postgres",
                "entrypoint": ["sleep", "infinity"],
                "volumes": [f"{root}:/work"],
            },
        )
        with composition.override(service):
            try:
                composition.up("qps-pgbench")
                container = composition.invoke(
                    "ps", "-q", "qps-pgbench", capture=True
                ).stdout.strip()
                if not container:
                    raise ValueError("pgbench container did not start")
                return measure(config, root, container)
            finally:
                composition.rm("qps-pgbench")


def measure(config: dict, root: Path, container: str) -> dict:
    clients = config["concurrency"]
    assignments = allocation(clients, len(config["libpq_envs"]))
    warmup, duration = config["warmup_seconds"], config["duration_seconds"]
    # pgbench's -T includes serial connection startup. Use it only as a safety
    # deadline, then send its normal expiry signal after the measured window.
    seconds = math.ceil(
        max(assignments) * config["query_timeout_seconds"] + warmup + duration + 30
    )
    results = []
    logging_rates = []
    processes = []
    script = root / "query.sql"
    script.write_text(config["query"].rstrip("; \n") + ";\n")

    def command(i: int, *args: str, track_pid: bool = False) -> list[str]:
        return [
            "docker",
            "exec",
            container,
            "sh",
            "-c",
            f". /work/env-{i}; "
            + (f"echo $$ > /work/pid-{i}; " if track_pid else "")
            + 'exec "$@"',
            "sh",
            *args,
        ]

    for i, n in enumerate(assignments):
        env_file = root / f"env-{i}"
        env_file.touch(mode=0o600)
        env_file.write_text(
            "".join(
                f"export {key}={shlex.quote(value)}\n"
                for key, value in config["libpq_envs"][i].items()
            )
        )
        preflight = subprocess.run(
            command(
                i,
                "psql",
                "-XAt",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                f"SELECT current_setting('cluster'), current_setting('statement_logging_sample_rate')::float, count(*) FROM ({config['query']}) q",
            ),
            capture_output=True,
            text=True,
            timeout=config["query_timeout_seconds"] + 10,
        )
        fields = preflight.stdout.strip().split("|")
        if preflight.returncode or len(fields) != 3:
            print(preflight.stdout, preflight.stderr, file=sys.stderr)
            raise ValueError(f"cluster {i}: routing/row-count preflight failed")
        if (
            fields[0] != config["cluster_names"][i]
            or int(fields[2]) != config["expected_rows"]
        ):
            raise ValueError(f"cluster {i}: unexpected cluster or row count: {fields}")
        logging_rates.append(float(fields[1]))
    capacity_text = container_output(
        container,
        "sh",
        "-c",
        "nproc; getconf CLK_TCK; cat /sys/fs/cgroup/cpu.max 2>/dev/null || true",
    ).splitlines()
    capacity = float(capacity_text[0])
    ticks_per_second = int(capacity_text[1])
    if len(capacity_text) > 2:
        quota, period = capacity_text[2].split()
        if quota != "max":
            capacity = min(capacity, int(quota) / int(period))
    before = cpu_stat(container)
    wall_start = time.monotonic()
    peak_thread_percent = 0.0
    try:
        for i, n in enumerate(assignments):
            output = (root / f"summary-{i}").open("w+")
            processes.append(
                (
                    subprocess.Popen(
                        command(
                            i,
                            "pgbench",
                            "-n",
                            "-M",
                            config["protocol"],
                            "-c",
                            str(n),
                            "-j",
                            "1",
                            "-T",
                            str(seconds),
                            "-f",
                            "/work/query.sql",
                            "-l",
                            f"--log-prefix=/work/transactions-{i}",
                            track_pid=True,
                        ),
                        stdout=output,
                        stderr=output,
                        text=True,
                    ),
                    output,
                )
            )
        deadline = wall_start + seconds + config["query_timeout_seconds"] + 30
        previous = thread_ticks(container)
        sample_time = time.monotonic()
        first: dict[tuple[int, int], float] = {}
        stop_at = None
        stopped = False
        while any(process.poll() is None for process, _ in processes):
            if time.monotonic() >= deadline:
                raise TimeoutError("pgbench exceeded its query/run deadline")
            if not stopped:
                for i, (process, output) in enumerate(processes):
                    if process.poll() is not None:
                        output.seek(0)
                        print(output.read(), file=sys.stderr)
                        raise ValueError(
                            f"cluster {i}: pgbench exited before the measurement window completed"
                        )
            time.sleep(0.5)
            if stop_at is None:
                started = observe_startup(root, assignments, first)
                if started is not None:
                    stop_at = started + warmup + duration + 1
            if stop_at is not None and not stopped and time.time() >= stop_at:
                signal_clients(container, "ALRM")
                stopped = True
            now = time.monotonic()
            current = thread_ticks(container)
            if current:
                peak_thread_percent = max(
                    peak_thread_percent,
                    (
                        max(
                            100
                            * (value - previous[key])
                            / ticks_per_second
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
                raise ValueError(f"cluster {i}: pgbench exited with status {status}")
            results.append(summary(text))
    finally:
        signal_clients(container, "KILL")
        for process, output in processes:
            if process.poll() is None:
                process.kill()
            process.wait()
            output.close()
    elapsed = time.monotonic() - wall_start
    after = cpu_stat(container)
    result = merge_logs(
        sorted(root.glob("transactions-*.*")), clients, warmup, duration
    )
    cpu = (after.get("usage_usec", 0) - before.get("usage_usec", 0)) / 1e6 / elapsed
    periods = after.get("nr_periods", 0) - before.get("nr_periods", 0)
    throttled = (
        100 * (after.get("nr_throttled", 0) - before.get("nr_throttled", 0)) / periods
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
        "statement_logging_sample_rates": logging_rates,
        "driver_name": "pgbench",
        "threads_per_cluster": 1,
        "pgbench_version": container_output(container, "pgbench", "--version").strip(),
        "native_summaries": results,
        "driver": {
            "cpu_percent": 100 * cpu / capacity,
            "cpu_cores": cpu,
            "capacity_cores": capacity,
            "throttled_period_percent": throttled,
            "peak_thread_cpu_percent": peak_thread_percent,
            "warnings": warnings,
        },
    }
