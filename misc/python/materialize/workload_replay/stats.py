# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Statistics collection, plotting, and comparison for workload replay.
"""

from __future__ import annotations

import json
import re
import subprocess
import threading
import time
from pathlib import Path
from typing import Any
from typing import Literal as TypeLiteral

import matplotlib.pyplot as plt
import numpy

from materialize import MZ_ROOT, buildkite
from materialize.mzcompose.test_result import TestFailureDetails

# Byte parsing utilities

_SI_UNITS = {
    "b": 1,
    "kb": 10**3,
    "mb": 10**6,
    "gb": 10**9,
    "tb": 10**12,
    "pb": 10**15,
}

_IEC_UNITS = {
    "kib": 2**10,
    "mib": 2**20,
    "gib": 2**30,
    "tib": 2**40,
    "pib": 2**50,
}


def parse_bytes(s: str) -> int:
    """
    Parse byte strings like:
      "76.5MB" -> 76500000
      "3.202GiB" -> 3438115842
      "1e+03kB" -> 1000000
      "0B" -> 0
    """
    s = s.strip()
    # number: decimal or scientific notation, e.g. 1e+03, 3.2E-1, .5
    m = re.fullmatch(
        r"([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*([A-Za-z]+)", s
    )
    if not m:
        raise ValueError(f"Cannot parse bytes: {s!r}")

    val = float(m.group(1))
    unit_raw = m.group(2)

    unit = unit_raw.lower()

    if unit in _SI_UNITS:
        mult = _SI_UNITS[unit]
    elif unit in _IEC_UNITS:
        mult = _IEC_UNITS[unit]
    else:
        raise ValueError(f"Unknown unit {unit_raw!r} in {s!r}")

    return int(val * mult)


def parse_two_bytes(s: str) -> tuple[int, int]:
    """Parse a string like '100MB / 200MB' into two integers."""
    a, b = (x.strip() for x in s.split("/", 1))
    return parse_bytes(a), parse_bytes(b)


def parse_percent(s: str) -> float:
    """Parse a percentage string like '45.2%' into a float."""
    return float(s.strip().rstrip("%"))


# Docker stats collection


def docker_stats(
    stats: list[tuple[int, dict[str, dict[str, Any]]]], stop_event: threading.Event
) -> None:
    """Continuously collect Docker container stats until stop_event is set."""
    while not stop_event.is_set():
        result = {}
        timestamp = int(time.time())
        for line in subprocess.check_output(
            ["docker", "stats", "--format", "json", "--no-trunc", "--no-stream"],
            text=True,
        ).splitlines():
            obj = json.loads(line)
            if not obj["Name"].startswith("workload-replay-"):
                continue
            result[obj["Name"].removeprefix("workload-replay-").removesuffix("-1")] = {
                "cpu_percent": parse_percent(obj["CPUPerc"]),
                "mem_percent": parse_percent(obj["MemPerc"]),
            }
        stats.append((timestamp, result))


def print_replay_stats(stats: dict[str, Any]) -> None:
    """Print a summary of replay statistics."""
    print("Queries:")
    print(f"   Total: {stats['queries']['total']}")
    failed = (
        100.0 * stats["queries"]["failed"] / stats["queries"]["total"]
        if stats["queries"]["total"]
        else 0
    )
    print(f"  Failed: {stats['queries']['failed']} ({failed:.0f}%)")
    slow = (
        100.0 * stats["queries"]["slow"] / stats["queries"]["total"]
        if stats["queries"]["total"]
        else 0
    )
    print(f"    Slow: {stats['queries']['slow']} ({slow:.0f}%)")
    print("Ingestions:")
    print(f"   Total: {stats['ingestions']['total']}")
    failed = (
        100.0 * stats["ingestions"]["failed"] / stats["ingestions"]["total"]
        if stats["ingestions"]["total"]
        else 0
    )
    print(f"  Failed: {stats['ingestions']['failed']} ({failed:.0f}%)")
    slow = (
        100.0 * stats["ingestions"]["slow"] / stats["ingestions"]["total"]
        if stats["ingestions"]["total"]
        else 0
    )
    print(f"    Slow: {stats['ingestions']['slow']} ({slow:.0f}%)")


# Plotting utilities


def upload_plots(
    plot_paths: list[str],
    file: str,
):
    """Upload plots to Buildkite or save locally."""
    if not plot_paths:
        return
    if buildkite.is_in_buildkite():
        for plot_path in plot_paths:
            buildkite.upload_artifact(plot_path, cwd=MZ_ROOT, quiet=True)
        print(f"+++ Plots for {file}")
        for plot_path in plot_paths:
            print(
                buildkite.inline_image(f"artifact://{plot_path}", f"Plots for {file}")
            )
    else:
        print(f"Saving plots to {plot_paths}")


class DockerSeries:
    """Container for time series data from Docker stats."""

    def __init__(
        self,
        *,
        t: list[int],
        cpu_percent: dict[str, list[float]],
        mem_percent: dict[str, list[float]],
    ) -> None:
        self.t = t
        self.cpu_percent = cpu_percent
        self.mem_percent = mem_percent


def extract_docker_series(
    docker_stats: list[tuple[int, dict[str, dict[str, Any]]]]
) -> DockerSeries:
    """Extract time series from Docker stats snapshots."""
    t0 = docker_stats[0][0]
    times: list[int] = [ts - t0 for (ts, _snapshot) in docker_stats]

    containers: set[str] = set()
    for _ts, snapshot in docker_stats:
        containers |= set(snapshot.keys())

    def init_float() -> dict[str, list[float]]:
        return {c: [] for c in sorted(containers)}

    cpu_percent = init_float()
    mem_percent = init_float()

    for _ts, snapshot in docker_stats:
        for c in sorted(containers):
            m = snapshot.get(c)

            def last_or(lst: list[Any], default: Any):
                return default if not lst else lst[-1]

            if m is None:
                cpu_percent[c].append(last_or(cpu_percent[c], 0.0))
                mem_percent[c].append(last_or(mem_percent[c], 0.0))
                continue

            cpu_percent[c].append(float(m["cpu_percent"]))
            mem_percent[c].append(float(m["mem_percent"]))

    return DockerSeries(
        t=times,
        cpu_percent=cpu_percent,
        mem_percent=mem_percent,
    )


YScale = TypeLiteral["linear", "log", "symlog", "logit"]


def plot_timeseries_compare(
    *,
    t_old: list[int],
    ys_old: dict[str, list[float]],
    t_new: list[int],
    ys_new: dict[str, list[float]],
    title: str,
    ylabel: str,
    out_path: Path,
    yscale: YScale | None = None,
):
    """Plot a comparison of two time series."""
    plt.figure(figsize=(10, 6))

    containers = sorted(set(ys_old.keys()) | set(ys_new.keys()))

    for c in containers:
        if c in ys_old:
            plt.plot(t_old, ys_old[c], linestyle="--", label=f"{c} (old)")
        if c in ys_new:
            plt.plot(t_new, ys_new[c], linestyle="-", label=f"{c} (new)")

    plt.xlabel("time [s]")
    plt.ylabel(ylabel)
    if yscale is not None:
        plt.yscale(yscale)

    plt.title(title)
    plt.legend(loc="best")  # type: ignore
    plt.grid(True)
    plt.ylim(bottom=0)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=300)
    plt.close()


def plot_docker_stats_compare(
    *,
    stats_old: dict[str, Any],
    stats_new: dict[str, Any],
    file: str,
    old_version: str,
    new_version: str,
):
    """Generate comparison plots for Docker stats."""
    plot_paths = []

    if "initial_data" in stats_old:
        old = extract_docker_series(stats_old["initial_data"]["docker"])
        new = extract_docker_series(stats_new["initial_data"]["docker"])

        plot_path = Path("plots") / f"{file}_initial_cpu.png"
        plot_timeseries_compare(
            t_old=old.t,
            ys_old=old.cpu_percent,
            t_new=new.t,
            ys_new=new.cpu_percent,
            title=f"{file} - Initial Data Phase CPU\n{old_version} [old] vs {new_version} [new]",
            ylabel="CPU [%]",
            out_path=plot_path,
        )
        plot_paths.append(plot_path)

        plot_path = Path("plots") / f"{file}_initial_mem.png"
        plot_timeseries_compare(
            t_old=old.t,
            ys_old=old.mem_percent,
            t_new=new.t,
            ys_new=new.mem_percent,
            title=f"{file} - Initial Data Phase Memory\n{old_version} [old] vs {new_version} [new]",
            ylabel="Memory [%]",
            out_path=plot_path,
        )
        plot_paths.append(plot_path)

    if "docker" in stats_old:
        old = extract_docker_series(stats_old["docker"])
        new = extract_docker_series(stats_new["docker"])

        plot_path = Path("plots") / f"{file}_continuous_cpu.png"
        plot_timeseries_compare(
            t_old=old.t,
            ys_old=old.cpu_percent,
            t_new=new.t,
            ys_new=new.cpu_percent,
            title=f"{file} - Continuous Phase CPU\n{old_version} [old] vs {new_version} [new]",
            ylabel="CPU [%]",
            out_path=plot_path,
        )
        plot_paths.append(plot_path)

        plot_path = Path("plots") / f"{file}_continuous_mem.png"
        plot_timeseries_compare(
            t_old=old.t,
            ys_old=old.mem_percent,
            t_new=new.t,
            ys_new=new.mem_percent,
            title=f"{file} - Continuous Phase Memory\n{old_version} [old] vs {new_version} [new]",
            ylabel="Memory [%]",
            out_path=plot_path,
        )
        plot_paths.append(plot_path)

    upload_plots(plot_paths, file)


def average_cpu_mem_for_container(
    docker_stats: list[tuple[int, dict[str, dict[str, Any]]]],
    container: str,
) -> tuple[float, float]:
    """Calculate average CPU and memory usage for a container."""
    cpu_sum = 0.0
    mem_sum = 0.0
    n = 0

    for _ts, snapshot in docker_stats:
        m = snapshot.get(container)
        if m is None:
            continue
        cpu_sum += float(m["cpu_percent"])
        mem_sum += float(m["mem_percent"])
        n += 1

    if n == 0:
        raise ValueError(f"container {container!r} not found in docker_stats")

    return (cpu_sum / n, mem_sum / n)


def query_timing_stats(stats: dict[str, Any]) -> dict[str, float]:
    """Calculate timing statistics for queries."""
    durations = sorted(float(t) for (_sql, t) in stats["queries"]["timings"])
    return {
        "avg": float(numpy.mean(durations)),
        "min": min(durations),
        "max": max(durations),
        "p50": float(numpy.median(durations)),
        "p95": float(numpy.percentile(durations, 95)),
        "p99": float(numpy.percentile(durations, 99)),
        "std": float(numpy.std(durations, ddof=1)),
    }


# Comparison utilities


def pct_change(old: float, new: float) -> float:
    """Calculate percentage change from old to new."""
    if old == 0:
        return float("inf") if new != 0 else 0.0
    return (new - old) / old * 100.0


def fmt_pct(delta: float) -> str:
    """Format a percentage change value."""
    if delta == float("inf"):
        return "inf"
    return f"{delta:+.1f}%"


def compare_table(
    filename: str, stats_old: dict[str, Any], stats_new: dict[str, Any]
) -> list[TestFailureDetails]:
    """Generate a comparison table and check for regressions."""
    rows = []
    if "object_creation" in stats_old:
        rows.append(
            (
                "Object creation (s)",
                stats_old["object_creation"],
                stats_new["object_creation"],
                1.2,
            )
        )

    if "initial_data" in stats_old:
        old_avg_cpu_initial_data, old_avg_mem_initial_data = (
            average_cpu_mem_for_container(
                stats_old["initial_data"]["docker"], "materialized"
            )
        )
        new_avg_cpu_initial_data, new_avg_mem_initial_data = (
            average_cpu_mem_for_container(
                stats_new["initial_data"]["docker"], "materialized"
            )
        )
        rows.append(
            (
                "Data ingestion time (s)",
                stats_old["initial_data"]["time"],
                stats_new["initial_data"]["time"],
                1.2,
            )
        )
        rows.extend(
            [
                (
                    "Data ingestion CPU (sum)",
                    old_avg_cpu_initial_data * stats_old["initial_data"]["time"],
                    new_avg_cpu_initial_data * stats_new["initial_data"]["time"],
                    1.2,
                ),
                (
                    "Data ingestion Mem (sum)",
                    old_avg_mem_initial_data * stats_old["initial_data"]["time"],
                    new_avg_mem_initial_data * stats_new["initial_data"]["time"],
                    1.2,
                ),
            ]
        )

    if "docker" in stats_old:
        old_avg_cpu, old_avg_mem = average_cpu_mem_for_container(
            stats_old["docker"], "materialized"
        )
        new_avg_cpu, new_avg_mem = average_cpu_mem_for_container(
            stats_new["docker"], "materialized"
        )
        rows.extend(
            [
                ("CPU avg (%)", old_avg_cpu, new_avg_cpu, 1.2),
                ("Mem avg (%)", old_avg_mem, new_avg_mem, 1.2),
            ]
        )

    if "timings" in stats_old["queries"]:
        old_q = query_timing_stats(stats_old)
        new_q = query_timing_stats(stats_new)
        rows.extend(
            [
                ("Query max (ms)", old_q["max"] * 1000, new_q["max"] * 1000, None),
                ("Query min (ms)", old_q["min"] * 1000, new_q["min"] * 1000, None),
                (
                    "Query avg (ms)",
                    old_q["avg"] * 1000,
                    new_q["avg"] * 1000,
                    None,
                ),
                (
                    "Query p50 (ms)",
                    old_q["p50"] * 1000,
                    new_q["p50"] * 1000,
                    None,
                ),
                ("Query p95 (ms)", old_q["p95"] * 1000, new_q["p95"] * 1000, None),
                ("Query p99 (ms)", old_q["p99"] * 1000, new_q["p99"] * 1000, None),
                ("Query std (ms)", old_q["std"] * 1000, new_q["std"] * 1000, None),
            ]
        )

    failures: list[TestFailureDetails] = []

    output_lines = [
        f"{'METRIC':<24} | {'OLD':^12} | {'NEW':^12} | {'CHANGE':^9} | {'THRESHOLD':^9} | {'REGRESSION?':^12}",
        "-" * 92,
    ]

    regressed = False
    for name, old, new, threshold in rows:
        delta = pct_change(old, new)

        if threshold is None:
            flag = ""
        elif new > old * threshold:
            regressed = True
            flag = "!!YES!!"
        else:
            flag = "no"
        threshold_field = (
            f"{(threshold - 1) * 100:>8.0f}%" if threshold is not None else ""
        )
        output_lines.append(
            f"{name:<24} | "
            f"{old:>12.3f} | "
            f"{new:>12.3f} | "
            f"{fmt_pct(delta):>9} | "
            f"{threshold_field:>9} | "
            f"{flag:^12}"
        )
    if regressed:
        failures.append(
            TestFailureDetails(
                message=f"Workload {filename} regressed",
                details="\n".join(output_lines),
                test_class_name_override=filename,
            )
        )

    print("\n".join(output_lines))
    return failures
