# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Schedule bounded local workload processes with deterministic invocation seeds."""

from __future__ import annotations

import ast
import hashlib
import json
import os
import random
import signal
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

EXCLUSIVE = {
    "atomic_commits": 1,
    "bounded_subscribe": 1,
    "index_handover": 1,
    "aggregate_retractions": 2,
}


@dataclass(frozen=True)
class Command:
    path: Path
    family: str
    mode: str


def discover(directory: Path) -> list[Command]:
    commands = []
    for path in sorted(directory.glob("*.py")):
        name = path.name
        if not name.startswith(
            (
                "first_",
                "parallel_driver_",
                "serial_driver_",
                "singleton_driver_",
                "anytime_",
                "eventually_",
                "finally_",
            )
        ):
            continue
        tree = ast.parse(path.read_text())
        sources = [
            node.args[1].value
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "run_command"
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
        ]
        if len(sources) != 1 or not isinstance(sources[0], str):
            raise ValueError(f"expected a generated measured wrapper: {path}")
        source = Path(sources[0])
        families = set()
        for node in ast.walk(ast.parse(source.read_text())):
            if isinstance(node, ast.ImportFrom) and node.module:
                prefix = "materialize.antithesis.drivers."
                if node.module.startswith(prefix):
                    families.add(node.module.removeprefix(prefix).split(".")[0])
                elif node.module == "materialize.antithesis.drivers":
                    families.update(alias.name for alias in node.names)
        if len(families) > 1:
            raise ValueError(f"ambiguous driver family: {source}")
        mode = (
            "serial"
            if name.startswith("serial_driver_")
            else (
                "singleton"
                if name.startswith("singleton_driver_")
                else name.split("_", 1)[0]
            )
        )
        commands.append(Command(path, next(iter(families), "telemetry"), mode))
    if sum(command.mode == "first" for command in commands) != 1:
        raise ValueError("profile must contain exactly one first command")
    return commands


def child_seed(seed: int, command: str, ordinal: int) -> int:
    return int.from_bytes(
        hashlib.sha256(f"{seed}:{command}:{ordinal}".encode()).digest()[:8], "big"
    )


def compatible(command: Command, running: list[Command], concurrency: int) -> bool:
    if len(running) >= concurrency:
        return False
    if command.mode != "anytime" and any(item.mode == "serial" for item in running):
        return False
    if command.mode == "serial":
        return not any(item.mode != "anytime" for item in running)
    if command.mode in ("singleton", "anytime") and command in running:
        return False
    limit = EXCLUSIVE.get(command.family)
    return (
        limit is None or sum(item.family == command.family for item in running) < limit
    )


def stop_group(process: subprocess.Popen, grace: float) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        pass
    # The leader may exit on TERM while one of its descendants ignores it.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


class Scheduler:
    def __init__(
        self,
        output: Path,
        concurrency: int,
        seed: int,
        timeout: float,
        grace: float,
        check_interval: float,
        drain_timeout: float = 0,
    ):
        self.output, self.concurrency, self.seed = output, concurrency, seed
        self.timeout, self.grace, self.check_interval = timeout, grace, check_interval
        self.drain_timeout = drain_timeout
        self.admission_end: float | None = None
        self.drain_elapsed = 0.0
        self.running: list[dict] = []
        self.completed: list[dict] = []
        self.dispatched: Counter[str] = Counter()
        self.ready: dict[str, float] = {}
        self.random = random.Random(seed)
        self.max_running = 0
        self.overlaps = set()

    def event(self, value: dict) -> None:
        with (self.output / "timeline.jsonl").open("a") as stream:
            stream.write(json.dumps(value) + "\n")

    def start(self, command: Command) -> None:
        self.dispatched[command.path.name] += 1
        ordinal = self.dispatched[command.path.name]
        token = f"{len(self.completed) + len(self.running):05d}-{command.path.stem}-{ordinal}"
        seed = child_seed(self.seed, command.path.name, ordinal)
        sdk = self.output / f"{token}.sdk.jsonl"
        log = (self.output / f"{token}.log").open("wb")
        process = subprocess.Popen(
            [
                sys.executable,
                "-u",
                "-m",
                "materialize.antithesis.local_runner",
                "--worker",
                str(command.path),
                "--seed",
                str(seed),
            ],
            env={**os.environ, "ANTITHESIS_SDK_LOCAL_OUTPUT": str(sdk)},
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        for item in self.running:
            self.overlaps.add(
                tuple(sorted((command.path.name, item["command"].path.name)))
            )
        self.running.append(
            {
                "command": command,
                "process": process,
                "log": log,
                "started": time.monotonic(),
                "seed": seed,
                "token": token,
            }
        )
        self.max_running = max(self.max_running, len(self.running))
        self.event(
            {
                "stage": "start",
                "command": command.path.name,
                "pid": process.pid,
                "seed": seed,
                "token": token,
                "at": self.running[-1]["started"],
                "active": len(self.running),
            }
        )

    def reap(self, phase_end: bool = False) -> None:
        now = time.monotonic()
        for item in list(self.running):
            process = item["process"]
            reason = (
                "phase_deadline"
                if phase_end
                else (
                    "command_timeout" if now - item["started"] >= self.timeout else None
                )
            )
            if process.poll() is None and reason is not None:
                stop_group(process, self.grace)
            elif process.poll() is None:
                continue
            else:
                reason = None
                # Reap descendants even if the direct command already exited.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            item["log"].close()
            command = item["command"]
            self.completed.append(
                {
                    "command": command.path.name,
                    "family": command.family,
                    "mode": command.mode,
                    "pid": process.pid,
                    "seed": item["seed"],
                    "token": item["token"],
                    "started": item["started"],
                    "finished": time.monotonic(),
                    "returncode": process.returncode,
                    "incomplete": reason,
                    "sdk_present": (
                        self.output / f"{item['token']}.sdk.jsonl"
                    ).is_file(),
                }
            )
            self.event({"stage": "end", **self.completed[-1]})
            self.ready[command.path.name] = time.monotonic() + (
                self.check_interval if command.mode == "anytime" else 0
            )
            self.running.remove(item)

    def phase(self, commands: list[Command], duration: float) -> None:
        deadline = time.monotonic() + duration
        ranks = {command.path.name: self.random.random() for command in commands}
        try:
            while time.monotonic() < deadline:
                self.reap()
                while (
                    len(self.running) < self.concurrency and time.monotonic() < deadline
                ):
                    candidates = sorted(
                        (
                            command
                            for command in commands
                            if self.ready.get(command.path.name, 0) <= time.monotonic()
                        ),
                        key=lambda command: (
                            self.dispatched[command.path.name],
                            ranks[command.path.name],
                        ),
                    )
                    if not candidates:
                        break
                    # Let an overdue serial command drain the running set.
                    if candidates[0].mode == "serial" and any(
                        row["command"].mode != "anytime" for row in self.running
                    ):
                        break
                    available = [
                        command
                        for command in candidates
                        if compatible(
                            command,
                            [row["command"] for row in self.running],
                            self.concurrency,
                        )
                    ]
                    if not available:
                        break
                    self.start(available[0])
                time.sleep(0.02)
            self.admission_end = time.monotonic()
            self.event(
                {
                    "stage": "admission_end",
                    "at": self.admission_end,
                    "active": len(self.running),
                }
            )
            drain_started = time.monotonic()
            try:
                while (
                    self.running
                    and time.monotonic() - drain_started < self.drain_timeout
                ):
                    self.reap()
                    if self.running:
                        time.sleep(0.02)
            finally:
                self.drain_elapsed = time.monotonic() - drain_started
        finally:
            self.reap(phase_end=True)
