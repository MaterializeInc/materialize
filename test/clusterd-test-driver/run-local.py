# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Run programmable compute tests in an isolated native runtime on the host.

Run with bin/pyactivate test/clusterd-test-driver/run-local.py. SCRIPT selects a
.spec file, PROFILE selects the Cargo profile, and WRAPPER optionally profiles
the headless-driver process that now owns compute (e.g. heaptrack or perf).
DRIVER_WORKERS and DRIVER_UNIFIED configure the native runtime. The driver uses
the system allocator, so heaptrack needs no clusterd allocator feature override.
"""

import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent.parent


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


PUBSUB_PORT = env("PUBSUB_PORT", "6879")
COCKROACH_PORT = env("COCKROACH_PORT", "26257")
CONSENSUS_URL = env(
    "PERSIST_CONSENSUS_URL",
    f"postgres://root@127.0.0.1:{COCKROACH_PORT}?options=--search_path=consensus",
)
BLOB_DIR = env("BLOB_DIR", "/tmp/clusterd-test-driver-blob")
SCRIPT = env("SCRIPT", "test/clusterd-test-driver/scripts/index.spec")
WRAPPER = env("WRAPPER", "")
PROFILE = env("PROFILE", "optimized")
PROFILE_DIR = "debug" if PROFILE == "dev" else PROFILE


def run(cmd: list[str], **kwargs: Any) -> None:
    subprocess.run(cmd, check=True, cwd=ROOT, **kwargs)


def ensure_cockroach() -> None:
    up = subprocess.run(
        ["docker", "exec", "cockroach", "true"],
        capture_output=True,
    )
    if up.returncode != 0:
        started = subprocess.run(["docker", "start", "cockroach"], capture_output=True)
        if started.returncode != 0:
            run(
                [
                    "docker",
                    "run",
                    "--name=cockroach",
                    "-d",
                    "-p",
                    f"{COCKROACH_PORT}:26257",
                    "-p",
                    "26258:8080",
                    "cockroachdb/cockroach:latest",
                    "start-single-node",
                    "--insecure",
                    "--store=type=mem,size=2G",
                ]
            )
        time.sleep(3)
    run(
        [
            "docker",
            "exec",
            "cockroach",
            "cockroach",
            "sql",
            "--insecure",
            "-e",
            "CREATE SCHEMA IF NOT EXISTS consensus",
        ]
    )


def terminate_group(child: "subprocess.Popen[bytes]", grace: float = 10) -> None:
    """Reap this invocation's group, even if its wrapper has already exited."""
    previous = {
        sig: signal.signal(sig, signal.SIG_IGN)
        for sig in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline:
            child.poll()
            try:
                os.killpg(child.pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.05)
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    finally:
        child.wait()
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def main() -> int:
    def interrupted(signum: int, _frame: object) -> None:
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, interrupted)
    Path(BLOB_DIR).mkdir(parents=True, exist_ok=True)
    if "PERSIST_CONSENSUS_URL" not in os.environ:
        ensure_cockroach()
    run(
        [
            "cargo",
            "build",
            "--profile",
            PROFILE,
            "-p",
            "mz-clusterd-test-driver",
            "--bin",
            "headless-driver",
        ]
    )
    script_path = Path(SCRIPT)
    if not script_path.is_absolute():
        script_path = ROOT / script_path
    driver_env = dict(
        os.environ,
        PERSIST_BLOB_URL=env("PERSIST_BLOB_URL", f"file://{BLOB_DIR}"),
        PERSIST_CONSENSUS_URL=CONSENSUS_URL,
        DRIVER_PUBSUB_BIND=f"127.0.0.1:{PUBSUB_PORT}",
        DRIVER_SCRIPT=str(script_path),
    )
    command = [
        *shlex.split(WRAPPER),
        str(ROOT / "target" / PROFILE_DIR / "headless-driver"),
    ]
    print(f"Running native driver: {shlex.join(command)}", flush=True)
    with subprocess.Popen(
        command, env=driver_env, cwd=ROOT, start_new_session=True
    ) as child:
        try:
            return child.wait()
        finally:
            # On interruption, reap only this invocation and its profiler. Native
            # workers otherwise run until process exit, independently of clients.
            terminate_group(child)


if __name__ == "__main__":
    sys.exit(main())
