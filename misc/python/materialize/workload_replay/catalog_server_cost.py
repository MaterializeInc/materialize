# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Measure what console load costs mz_catalog_server during a workload replay.

Process CPU is utime + stime from /proc inside the materialized container, for
the mz_catalog_server replica's clusterd and for environmentd. Observed
locally: `mz_cluster_replica_metrics_history` reads 2.7-3.2x higher than
/proc, because the process orchestrator samples CPU through sysinfo.

Scheduled time per dataflow comes from `mz_scheduling_elapsed`, attributed to
each dataflow's outermost operator. Summing every operator double counts,
because subgraph operators include the time spent scheduling their children.
Scheduled time misses work outside operators (peeks served from indexes,
session handling, networking) and dataflows that were dropped before the
phase ended, so it reads below process CPU.
"""

from __future__ import annotations

import json
import pathlib
import re
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from psycopg import Connection

from materialize.mzcompose.composition import Composition

_PROC_SCRIPT = r"""
echo "uptime $(cut -d' ' -f1 /proc/uptime) $(getconf CLK_TCK 2>/dev/null || echo 100)"
for p in /proc/[0-9]*; do
  cmd=$(tr '\0' ' ' < "$p/cmdline" 2>/dev/null) || continue
  case "$cmd" in
    *clusterd*|*environmentd*) ;;
    *) continue ;;
  esac
  ticks=$(awk '{print $14 + $15}' "$p/stat" 2>/dev/null) || continue
  echo "proc ${p#/proc/} $ticks $cmd"
done
"""

_TOP_DATAFLOWS = 10


@dataclass
class _Snapshot:
    uptime: float
    hz: int
    ticks: dict[str, int]
    pids: dict[str, frozenset[int]]
    scheduled_ns: dict[str, int]
    operators: dict[str, int]
    subscriptions: int


def _category(dataflow: str) -> str:
    if dataflow.startswith("Dataflow: subscribe-"):
        return "subscribe"
    if dataflow.startswith("Dataflow: introspection-subscribe-"):
        return "introspection_subscribe"
    if dataflow.endswith("_ind"):
        return "index"
    return "other"


class CatalogServerCost:
    """Records catalog_server and environmentd CPU over named phases.

    Holds one SQL session for its lifetime. Each new session costs
    catalog_server CPU of its own, so a connection per sample would show up in
    the numbers being measured.
    """

    def __init__(self, c: Composition, workload: str) -> None:
        self.c = c
        self.workload = workload
        self.phases: list[dict[str, Any]] = []
        self.viewer_fit: dict[str, Any] | None = None
        self.conn: Connection = c.sql_connection()
        self._query("SET cluster = mz_catalog_server")
        # Introspection reads under strict serializable were observed to hang
        # on long-lived environments, past statement_timeout.
        self._query("SET transaction_isolation = serializable")
        ((cluster_id, replica_id, size),) = self._query("""
            SELECT c.id, r.id, r.size
            FROM mz_clusters c JOIN mz_cluster_replicas r ON r.cluster_id = c.id
            WHERE c.name = 'mz_catalog_server'
            """)
        self.replica = {
            "cluster_id": cluster_id,
            "replica_id": replica_id,
            "size": size,
        }
        self._replica_pattern = re.compile(
            rf"cluster-{cluster_id}-replica-{replica_id}(?![0-9a-z])"
        )

    def _query(self, sql: str) -> list[tuple[Any, ...]]:
        cursor = self.conn.execute(sql.encode())
        return cursor.fetchall() if cursor.description else []

    def subscriptions(self) -> int:
        return int(
            self._query("SELECT count(*) FROM mz_internal.mz_subscriptions")[0][0]
        )

    def _snapshot(self) -> _Snapshot:
        output = self.c.exec(
            "materialized", "sh", "-c", _PROC_SCRIPT, capture=True, silent=True
        ).stdout
        uptime, hz = 0.0, 100
        ticks: dict[str, int] = {}
        pids: dict[str, set[int]] = {}
        for line in output.splitlines():
            if line.startswith("uptime "):
                _, up, clk = line.split()
                uptime, hz = float(up), int(clk)
                continue
            if not line.startswith("proc "):
                continue
            _, pid, tick, cmd = line.split(" ", 3)
            executable = cmd.split(" ", 1)[0].rsplit("/", 1)[-1]
            if executable == "environmentd":
                process = "environmentd"
            elif executable == "clusterd" and self._replica_pattern.search(cmd):
                process = "catalog_server"
            else:
                continue
            ticks[process] = ticks.get(process, 0) + int(tick)
            pids.setdefault(process, set()).add(int(pid))
        scheduled_ns = {name: int(ns) for name, ns in self._query("""
                SELECT dod.dataflow_name, sum(se.elapsed_ns)::text
                FROM mz_introspection.mz_scheduling_elapsed se
                JOIN mz_introspection.mz_dataflow_operator_dataflows dod
                  ON dod.id = se.id
                WHERE dod.name = dod.dataflow_name
                GROUP BY dod.dataflow_name
                """)}
        operators = {name: int(n) for name, n in self._query("""
                SELECT dataflow_name, count(*)::text
                FROM mz_introspection.mz_dataflow_operator_dataflows
                GROUP BY dataflow_name
                """)}
        return _Snapshot(
            uptime=uptime,
            hz=hz,
            ticks=ticks,
            pids={k: frozenset(v) for k, v in pids.items()},
            scheduled_ns=scheduled_ns,
            operators=operators,
            subscriptions=self.subscriptions(),
        )

    def _record(
        self, name: str, start: _Snapshot, end: _Snapshot, extra: dict[str, Any]
    ) -> None:
        seconds = end.uptime - start.uptime
        cores: dict[str, float | None] = {}
        for process in ("catalog_server", "environmentd"):
            # A restarted process resets its counters, so the delta means nothing.
            if (
                process not in start.ticks
                or start.pids.get(process) != end.pids.get(process)
                or seconds <= 0
            ):
                cores[process] = None
            else:
                cores[process] = (
                    (end.ticks[process] - start.ticks[process]) / end.hz / seconds
                )

        by_dataflow: dict[str, float] = {}
        for dataflow, ns in end.scheduled_ns.items() if seconds > 0 else []:
            before = start.scheduled_ns.get(dataflow, 0)
            # A counter below its start value belongs to a dataflow recreated
            # under the same name during the phase.
            by_dataflow[dataflow] = (
                (ns - before if ns >= before else ns) / 1e9 / seconds
            )
        by_category: dict[str, float] = {}
        subscribe_dataflows = 0
        subscribe_operators = 0
        for dataflow, dataflow_cores in by_dataflow.items():
            category = _category(dataflow)
            by_category[category] = by_category.get(category, 0.0) + dataflow_cores
            if category == "subscribe":
                subscribe_dataflows += 1
                subscribe_operators += end.operators.get(dataflow, 0)
        top = sorted(
            (
                (dataflow, dataflow_cores)
                for dataflow, dataflow_cores in by_dataflow.items()
                if _category(dataflow) != "subscribe"
            ),
            key=lambda item: item[1],
            reverse=True,
        )[:_TOP_DATAFLOWS]

        phase = {
            "name": name,
            "seconds": round(seconds, 1),
            "cores": cores,
            "scheduled_cores": {
                "total": sum(by_dataflow.values()),
                **dict(sorted(by_category.items())),
            },
            "subscribes": {
                "open_at_end": end.subscriptions,
                "dataflows": subscribe_dataflows,
                "avg_operators": (
                    round(subscribe_operators / subscribe_dataflows)
                    if subscribe_dataflows
                    else 0
                ),
            },
            "top_dataflows": [
                {
                    "name": dataflow,
                    "cores": dataflow_cores,
                    "operators": end.operators.get(dataflow, 0),
                }
                for dataflow, dataflow_cores in top
            ],
            **extra,
        }
        self.phases.append(phase)
        catalog_server = cores["catalog_server"]
        print(
            f"-- catalog_server cost, {name}: "
            + (
                f"{catalog_server:.3f} cores"
                if catalog_server is not None
                else "process restarted, no reading"
            )
            + f" over {seconds:.0f}s"
        )

    @contextmanager
    def phase(self, name: str, **extra: Any) -> Iterator[dict[str, Any]]:
        """Measure the enclosed block. `extra`, which the block may update, is
        stored with the phase."""
        start = self._snapshot()
        yield extra
        self._record(name, start, self._snapshot(), extra)

    def window(self, name: str, seconds: float, **extra: Any) -> None:
        """Measure whatever runs during the next `seconds`."""
        with self.phase(name, **extra):
            time.sleep(seconds)

    def fit_viewers(self, baseline_phases: list[str]) -> None:
        """Fit the cost per viewer over the phases that carry `viewers`.

        The slope is a least-squares fit through the origin of cores above
        the mean of `baseline_phases`, so it assumes cost grows linearly with
        viewers. Check the per-step points before trusting it.
        """
        fit: dict[str, Any] = {"baseline_phases": baseline_phases}
        for process in ("catalog_server", "environmentd"):
            baselines = [
                p["cores"][process]
                for p in self.phases
                if p["name"] in baseline_phases and p["cores"][process] is not None
            ]
            points = [
                (p["viewers"], p["cores"][process])
                for p in self.phases
                if "viewers" in p and p["cores"][process] is not None
            ]
            if not baselines or not points:
                fit[process] = None
                continue
            baseline = sum(baselines) / len(baselines)
            slope = sum(n * (cores - baseline) for n, cores in points) / sum(
                n * n for n, _ in points
            )
            fit[process] = {
                "baseline_cores": baseline,
                "mcpu_per_viewer": slope * 1000,
                "above_baseline": [
                    {"viewers": n, "cores": cores - baseline} for n, cores in points
                ],
            }
        self.viewer_fit = fit

    def report(self, path: pathlib.Path) -> None:
        """Write the phases as JSON and print a summary table."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "workload": self.workload,
                    "catalog_server_replica": self.replica,
                    "phases": self.phases,
                    "viewer_fit": self.viewer_fit,
                },
                indent=2,
            )
            + "\n"
        )

        def fmt(value: float | None) -> str:
            return f"{value:.3f}" if value is not None else "-"

        print(f"+++ catalog_server cost ({self.workload}), written to {path}")
        print(
            f"{'phase':<28} {'seconds':>8} {'catalog_server':>15} "
            f"{'environmentd':>13} {'scheduled':>10} {'subscribes':>11}"
        )
        for p in self.phases:
            print(
                f"{p['name']:<28} {p['seconds']:>8.0f} "
                f"{fmt(p['cores']['catalog_server']):>15} "
                f"{fmt(p['cores']['environmentd']):>13} "
                f"{fmt(p['scheduled_cores']['total']):>10} "
                f"{p['subscribes']['open_at_end']:>11}"
            )
        if self.viewer_fit:
            for process in ("catalog_server", "environmentd"):
                result = self.viewer_fit.get(process)
                if result:
                    print(
                        f"{process}: {result['mcpu_per_viewer']:.2f} mCPU per "
                        f"viewer above a {result['baseline_cores']:.3f}-core baseline"
                    )

    def close(self) -> None:
        self.conn.close()
