# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Recovery checks that must pass once faults stop, before the components' own.

Each check polls until it passes or its deadline does, then asserts once. The
messages are those of the legacy recovery check.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
)

from materialize.antithesis import crash_loops
from materialize.antithesis_driver.client import Target, connect
from materialize.antithesis_driver.context import Context, log


def poll(
    check: Callable[[], tuple[bool, Any]], deadline: float, name: str
) -> tuple[bool, Any]:
    details: Any = None
    while True:
        try:
            passed, details = check()
        except (psycopg.Error, OSError) as e:
            passed, details = False, f"{type(e).__name__}: {e}"
        if passed or time.monotonic() >= deadline:
            log(f"{name}: {'passed' if passed else 'failed'}: {details}")
            return passed, details
        time.sleep(5)


def query(target: Target, sql: str) -> list[tuple[Any, ...]]:
    with connect(target, "driver-health") as conn:
        conn.execute(b"SET statement_timeout = '30s'")
        return conn.execute(sql.encode()).fetchall()


def eventually(ctx: Context, deadline: float) -> bool:
    """Returns whether the environment serves SQL, so later checks are meaningful."""
    user = Target(ctx.config.host)
    system = Target(ctx.config.host, internal=True)

    serving, details = poll(
        lambda: (query(user, "SELECT 1") == [(1,)], None), deadline, "canary"
    )
    always(serving, "canary queries succeed after faults stop", {"error": details})
    if not serving:
        return False

    def replicas_online() -> tuple[bool, Any]:
        offline = query(
            system,
            "SELECT c.name, r.name, s.process_id, s.status, s.reason"
            " FROM mz_cluster_replicas r"
            " JOIN mz_clusters c ON r.cluster_id = c.id"
            " JOIN mz_internal.mz_cluster_replica_statuses s ON s.replica_id = r.id"
            " WHERE r.id LIKE 'u%' AND s.status <> 'online'",
        )
        return not offline, {"offline": [list(map(str, r)) for r in offline]}

    online, details = poll(replicas_online, deadline, "replicas online")
    always(online, "every cluster replica is online after faults stop", details)
    return True


CRASHLOOP_WINDOW_S = 120.0
CRASHLOOP_SAMPLE_S = 5.0
# `src/persist-client/src/read.rs` reserves this code for a live reader that
# lost its persist lease.
READER_LEASE_EXPIRED_EXIT_CODE = 168


def exit_class(exit_code: int, reason: str | None) -> str:
    """The class of a container exit, as the legacy recovery check names it.

    `halt!` exits 166 and the panic hook aborts (134), 101 is an unwinding
    panic, 137 and 143 are SIGKILL and SIGTERM from outside.
    """
    if reason == "OOMKilled":
        return "oom"
    if exit_code == 0:
        return "clean"
    if exit_code == 166:
        return "halt"
    if exit_code == READER_LEASE_EXPIRED_EXIT_CODE:
        return "reader_lease_expired"
    if exit_code in (137, 143):
        return "killed"
    if exit_code in (101, 134):
        return "panic"
    return "unknown"


def samples(kube: Any) -> list[crash_loops.Sample]:
    result = []
    for target in ("environmentd", "clusterd", "orchestratord"):
        for pod in kube.pods(target):
            for cs in pod.status.container_statuses or []:
                current = cs.state.terminated if cs.state else None
                last = cs.last_state.terminated if cs.last_state else None
                termination = current or last
                waiting = cs.state.waiting if cs.state else None
                result.append(
                    crash_loops.Sample(
                        key=(pod.metadata.uid, cs.name),
                        name=f"{target}/{pod.metadata.name}/{cs.name}",
                        # A container that is down now counts as restarted,
                        # not only once its backoff ends.
                        restart_count=cs.restart_count + (1 if current else 0),
                        exit_class=(
                            exit_class(termination.exit_code, termination.reason)
                            if termination
                            else None
                        ),
                        exit_code=termination.exit_code if termination else None,
                        finished_at=(
                            str(termination.finished_at) if termination else None
                        ),
                        waiting_reason=waiting.reason if waiting else None,
                        deleting=pod.metadata.deletion_timestamp is not None,
                    )
                )
    return result


def no_crash_loops(kube: Any, deadline: float) -> None:
    """No Materialize container crashes repeatedly once faults stop.

    Samples every pod for two crash-loop windows. One crash is not a loop: a
    fenced generation, or a process that lost its lease, halts once.
    """
    tracker = crash_loops.Tracker()
    end = min(deadline, time.monotonic() + 2 * CRASHLOOP_WINDOW_S + CRASHLOOP_SAMPLE_S)
    while time.monotonic() < end:
        try:
            tracker.observe(time.monotonic(), samples(kube))
        except Exception as e:
            log(f"crash loops: cannot sample pods: {type(e).__name__}: {e}")
        time.sleep(CRASHLOOP_SAMPLE_S)
    loops = tracker.loops(CRASHLOOP_WINDOW_S, span_s=CRASHLOOP_WINDOW_S)
    covered = tracker.covered(2 * CRASHLOOP_WINDOW_S)
    log(f"crash loops: covered {covered}, loops {loops}")
    always(
        covered and not loops,
        "No Materialize pod crash-loops after faults stop",
        {"covered": covered, "crash_loops": loops, "window_s": CRASHLOOP_WINDOW_S},
    )
