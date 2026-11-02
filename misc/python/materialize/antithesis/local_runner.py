# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Schedule measured harness commands locally and retain their actual evidence."""

from __future__ import annotations

import argparse
import fcntl
import fnmatch
import json
import os
import random
import runpy
import signal
import sys
import time
from pathlib import Path

from materialize.antithesis.local_evidence import (
    counter_delta,
    evidence,
    snapshot_valid,
    telemetry_snapshot,
)
from materialize.antithesis.local_scheduler import (
    Command,
    Scheduler,
    discover,
    nonnegative_seconds,
)


def seed_local_random(seed: int) -> None:
    from materialize.antithesis import rng

    draw = random.Random(seed)
    rng.get_random = lambda: draw.getrandbits(64)


def _select_commands(
    args: argparse.Namespace,
) -> tuple[list[Command], list[Command], list[Command]]:
    commands = discover(args.templates / args.profile)
    matches = lambda command, patterns: any(
        fnmatch.fnmatch(command.path.name, pattern) for pattern in patterns
    )
    runnable = [
        command
        for command in commands
        if command.mode in ("parallel", "serial", "singleton", "anytime")
    ]
    checkable = [
        command
        for command in commands
        if command.mode in ("anytime", "eventually", "finally")
    ]
    for option, patterns, eligible in (
        ("include", args.include, runnable),
        ("check", args.check, checkable),
    ):
        unmatched = [
            pattern
            for pattern in patterns
            if not any(
                fnmatch.fnmatch(command.path.name, pattern) for command in eligible
            )
        ]
        if unmatched:
            raise ValueError(
                f"profile {args.profile!r}: unmatched --{option} patterns: {unmatched!r}"
            )
    selected = [
        command
        for command in runnable
        if not args.include or matches(command, args.include)
    ]
    if not selected:
        raise ValueError(
            "no runnable commands matched the selected profile/include patterns"
        )
    if any(command.mode == "singleton" for command in selected):
        raise ValueError(
            "singleton branches are not supported by the bounded local runner; select parallel/serial/anytime commands explicitly"
        )
    checks = [command for command in checkable if matches(command, args.check)]
    unmatched_comparisons = [
        pattern
        for pattern in getattr(args, "require_comparison", [])
        if not any(
            fnmatch.fnmatch(command.path.name, pattern)
            for command in [*selected, *checks]
        )
    ]
    if unmatched_comparisons:
        raise ValueError(
            f"profile {args.profile!r}: unmatched --require-comparison patterns among selected commands: {unmatched_comparisons!r}"
        )
    if sum(command.mode in ("eventually", "finally") for command in checks) > 1:
        raise ValueError(
            "a local experiment can request at most one eventually/finally terminal hook"
        )
    return commands, selected, checks


def _run(
    args: argparse.Namespace,
    scheduler: Scheduler,
    commands: list[Command],
    selected: list[Command],
    checks: list[Command],
) -> int:
    if not args.skip_first:
        scheduler.timeout = args.setup_timeout
        scheduler.start(
            next(command for command in commands if command.mode == "first")
        )
        until = time.monotonic() + args.setup_timeout
        while scheduler.running and time.monotonic() < until:
            scheduler.reap()
            time.sleep(0.02)
        scheduler.reap(phase_end=True)
        setup_evidence = evidence(args.output)
        if (
            any(
                row["returncode"] or row["incomplete"] or not row["sdk_present"]
                for row in scheduler.completed
            )
            or setup_evidence["false_hits"]
            or setup_evidence["harness_errors"]
        ):
            (args.output / "summary.json").write_text(
                json.dumps(
                    {
                        "status": "failed",
                        "stage": "initialization",
                        "schedule": scheduler.completed,
                        **setup_evidence,
                    },
                    indent=2,
                )
            )
            return 1
        scheduler.timeout = args.command_timeout
    before = telemetry_snapshot(args.output, "before")
    if not snapshot_valid(before) or before.get("active_commands", 0):
        (args.output / "summary.json").write_text(
            json.dumps(
                {
                    "status": "failed",
                    "stage": "baseline",
                    "telemetry": before,
                    "reason": "baseline telemetry must be healthy with no active commands",
                },
                indent=2,
            )
        )
        return 1
    phase_started = time.monotonic()
    scheduler.phase(selected, args.duration)
    skipped_checks = []
    for command in checks:
        drivers = [
            row for row in scheduler.completed if row["mode"] in ("parallel", "serial")
        ]
        if (
            command.mode in ("eventually", "finally")
            and not drivers
            or command.mode == "finally"
            and any(row["incomplete"] for row in drivers)
        ):
            skipped_checks.append(command.path.name)
            continue
        scheduler.start(command)
        until = time.monotonic() + args.command_timeout
        while scheduler.running and time.monotonic() < until:
            scheduler.reap()
            time.sleep(0.02)
        scheduler.reap(phase_end=True)
    after = telemetry_snapshot(args.output, "after")
    result = evidence(args.output)
    if not snapshot_valid(after):
        result["harness_errors"].append(
            {"error": "final telemetry is unavailable or invalid", "telemetry": after}
        )
    result.update(
        profile=args.profile,
        scheduler_seed=args.seed,
        requested_concurrency=args.concurrency,
        seed_semantics="Per-command invocation seeds replace only the local child SDK random draw source. Antithesis entropy is unchanged.",
        include_patterns=args.include,
        requested_checks=args.check,
        requested_comparisons=getattr(args, "require_comparison", []),
        maximum_active_commands=scheduler.max_running,
        schedule=scheduler.completed,
        command_interval_overlaps=sorted(scheduler.overlaps),
        phase_started=phase_started,
        requested_phase_seconds=args.duration,
        admission_end=scheduler.admission_end,
        drain_elapsed_seconds=scheduler.drain_elapsed,
        requested_drain_seconds=scheduler.drain_timeout,
        ordinary_fault_interval_seconds=scheduler.fault_interval,
        first_skipped=args.skip_first,
        skipped_requested_checks=skipped_checks,
        unrun_checks=[
            command.path.name
            for command in commands
            if command.mode in ("eventually", "finally") and command not in checks
        ],
        telemetry_commands=counter_delta(
            before, after, "commands", ("profile", "command")
        ),
        witness_evaluations=counter_delta(
            before, after, "observations", ("profile", "command", "message", "observed")
        ),
        coverage=counter_delta(
            before,
            after,
            "coverage",
            ("profile", "phase", "role", "family", "path", "outcome"),
        ),
        comparisons=counter_delta(
            before,
            after,
            "comparisons",
            ("profile", "command", "family", "path", "outcome"),
        ),
    )
    result["command_failures"] = [
        row
        for row in scheduler.completed
        if row["returncode"] and not row["incomplete"]
    ]
    missing_sdk = [
        row["token"]
        for row in scheduler.completed
        if not row["sdk_present"] and not row["incomplete"]
    ]
    if missing_sdk:
        result["harness_errors"].append(
            {"error": "commands produced no SDK output", "commands": missing_sdk}
        )
    admitted = {row["command"] for row in scheduler.completed}
    result["comparison_commands"] = sorted(
        {
            row["command"]
            for row in result["comparisons"]
            if row.get("profile") == args.profile
            and row["command"] in admitted
            and row.get("outcome") == "passed"
            and row.get("units", 0) > 0
        }
    )
    result["has_evaluated_oracle"] = bool(result["comparison_commands"])
    result["required_comparisons_missing"] = sorted(
        {
            command.path.name
            for command in [*selected, *checks]
            if any(
                fnmatch.fnmatch(command.path.name, pattern)
                for pattern in getattr(args, "require_comparison", [])
            )
            and command.path.name not in result["comparison_commands"]
        }
    )
    result["required_witnesses_missing"] = [
        pattern
        for pattern in args.require_witness
        if not any(
            fnmatch.fnmatch(message, pattern)
            for message in result["positive_witnesses"]
        )
    ]
    unexpected_timeout = any(
        row["incomplete"] == "command_timeout"
        or row["incomplete"]
        and row["command"] in {command.path.name for command in checks}
        for row in scheduler.completed
    )
    result["status"] = (
        "failed"
        if result["false_hits"]
        or result["harness_errors"]
        or result["command_failures"]
        else (
            "incomplete"
            if not result["has_evaluated_oracle"]
            or result["required_comparisons_missing"]
            or result["required_witnesses_missing"]
            or skipped_checks
            or unexpected_timeout
            else "completed"
        )
    )
    (args.output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: result[key]
                for key in (
                    "status",
                    "profile",
                    "requested_concurrency",
                    "maximum_active_commands",
                    "has_evaluated_oracle",
                    "required_witnesses_missing",
                    "required_comparisons_missing",
                )
            }
        )
    )
    return (
        1
        if result["status"] == "failed"
        else 2 if result["status"] == "incomplete" else 0
    )


def run(args: argparse.Namespace) -> int:
    commands, selected, checks = _select_commands(args)
    os.umask(0o077)
    args.output.mkdir(parents=True, exist_ok=False)
    state_dir = Path(
        os.environ.get("MZ_ANTITHESIS_STATE_DIR", "/var/lib/antithesis-workload")
    )
    state_dir.mkdir(parents=True, exist_ok=True)
    scheduler = Scheduler(
        args.output,
        args.concurrency,
        args.seed,
        args.command_timeout,
        args.shutdown_grace,
        args.check_interval,
        getattr(args, "drain_timeout", 0),
        fault_interval=getattr(args, "fault_interval", 30),
    )
    with (state_dir / "local-scheduler.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(
                "another local scheduler owns this workload state"
            ) from error

        def interrupted(signum, frame):
            raise KeyboardInterrupt("local scheduler terminated")

        previous = signal.signal(signal.SIGTERM, interrupted)
        try:
            return _run(args, scheduler, commands, selected, checks)
        finally:
            scheduler.reap(phase_end=True)
            signal.signal(signal.SIGTERM, previous)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", type=Path)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--profile", default="transactions")
    parser.add_argument(
        "--templates", type=Path, default=Path("/opt/antithesis/test/v1")
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--duration", type=float, default=120)
    parser.add_argument("--drain-timeout", type=float, default=60)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--command-timeout", type=float, default=180)
    parser.add_argument("--setup-timeout", type=float, default=600)
    parser.add_argument("--shutdown-grace", type=float, default=2)
    parser.add_argument("--check-interval", type=float, default=5)
    parser.add_argument(
        "--fault-interval",
        type=nonnegative_seconds,
        default=30,
        help="seconds after an ordinary pod-fault command finishes before admitting the next (0 disables pacing)",
    )
    parser.add_argument("--include", action="append", default=[])
    parser.add_argument(
        "--check",
        action="append",
        default=[],
        help="run matching checks after the concurrent phase drains",
    )
    parser.add_argument("--require-witness", action="append", default=[])
    parser.add_argument(
        "--require-comparison",
        action="append",
        default=[],
        help="require each matching selected command to record a passing comparison in this run",
    )
    parser.add_argument("--skip-first", action="store_true")
    args = parser.parse_args()
    if args.worker:
        seed_local_random(args.seed)
        runpy.run_path(str(args.worker), run_name="__main__")
        return 0
    if (
        args.output is None
        or args.concurrency < 1
        or args.drain_timeout < 0
        or min(
            args.duration, args.command_timeout, args.setup_timeout, args.shutdown_grace
        )
        <= 0
    ):
        parser.error(
            "output, positive duration/timeouts and concurrency >= 1 are required"
        )
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
