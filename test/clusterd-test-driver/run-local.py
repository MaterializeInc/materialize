# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Run the clusterd-test-driver entirely on the host, without docker images.

Brings up (or reuses) CockroachDB for persist consensus, launches a local
clusterd, and runs the headless-driver against it. Everything is on localhost,
so a single PubSub address and a single persist location work for both the
driver and clusterd.

Run via the repo virtualenv so the `materialize` helpers are importable:

    bin/pyactivate test/clusterd-test-driver/run-local.py

Configuration is via environment variables (see CONFIG below), matching the
former shell script. To profile clusterd, set WRAPPER (e.g. "heaptrack" or
"perf record -g --"); the inner clusterd is terminated on exit so the wrapper
flushes its output.
"""

import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

# Reuse the exact timely configuration the mzcompose `Clusterd` service uses, so
# the local runner stays in sync with CI/prod defaults (notably the arrangement
# merge effort, `arrangement_exert_proportionality`).
from materialize.mzcompose.services.clusterd import (
    DEFAULT_COMPUTE_EXERT_PROPORTIONALITY,
    DEFAULT_STORAGE_EXERT_PROPORTIONALITY,
    timely_config,
)

ROOT = Path(__file__).resolve().parent.parent.parent


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


COMPUTE_ADDR = env("CLUSTERD_COMPUTE_ADDR", "127.0.0.1:2101")
STORAGE_ADDR = env("CLUSTERD_STORAGE_ADDR", "127.0.0.1:2100")
PUBSUB_PORT = env("PUBSUB_PORT", "6879")
COCKROACH_PORT = env("COCKROACH_PORT", "26257")
CONSENSUS_URL = env(
    "PERSIST_CONSENSUS_URL",
    f"postgres://root@127.0.0.1:{COCKROACH_PORT}?options=--search_path=consensus",
)
BLOB_DIR = env("BLOB_DIR", "/tmp/clusterd-test-driver-blob")
SCRATCH_DIR = env("SCRATCH_DIR", "/tmp/clusterd-test-driver-scratch")
SECRETS_DIR = env("SECRETS_DIR", "/tmp/clusterd-test-driver-secrets")
TARGET_BYTES = env("TARGET_BYTES", str(2 * 1024 * 1024 * 1024))  # 2 GiB
SCENARIO = env("SCENARIO", "index")
N_TIMESTAMPS = env("N_TIMESTAMPS", "64")
RUN_CLUSTERD = env("RUN_CLUSTERD", "1") == "1"
# Command prepended to clusterd, e.g. "heaptrack" or "perf record -g --".
WRAPPER = env("WRAPPER", "")
# Cargo profile; `optimized` is release-like with debug symbols.
PROFILE = env("PROFILE", "optimized")
PROFILE_DIR = "debug" if PROFILE == "dev" else PROFILE
ENVIRONMENT_ID = "mzcompose-us-east-1-00000000-0000-0000-0000-000000000000-0"

# heaptrack hooks the system allocator, which jemalloc bypasses, so build
# clusterd without default features (drops jemalloc) when profiling with it.
NO_DEFAULT_FEATURES = "heaptrack" in WRAPPER or bool(
    os.environ.get("CLUSTERD_NO_DEFAULT_FEATURES")
)


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
                    "docker", "run", "--name=cockroach", "-d",
                    "-p", f"{COCKROACH_PORT}:26257", "-p", "26258:8080",
                    "cockroachdb/cockroach:latest", "start-single-node",
                    "--insecure", "--store=type=mem,size=2G",
                ]
            )
        time.sleep(3)
    run(
        ["docker", "exec", "cockroach", "cockroach", "sql", "--insecure", "-e",
         "CREATE SCHEMA IF NOT EXISTS consensus"]
    )


def cargo_build() -> None:
    feature_args = ["--no-default-features"] if NO_DEFAULT_FEATURES else []
    if RUN_CLUSTERD:
        print(f"Building clusterd + headless-driver (profile: {PROFILE}"
              f"{' --no-default-features' if NO_DEFAULT_FEATURES else ''})...")
        # Single cargo invocation builds both bins. --no-default-features applies
        # to both packages, but the driver crate has no default features.
        run(
            ["cargo", "build", "--profile", PROFILE, *feature_args,
             "-p", "mz-clusterd", "--bin", "clusterd",
             "-p", "mz-clusterd-test-driver", "--bin", "headless-driver"]
        )
    else:
        print(f"Building headless-driver (profile: {PROFILE})...")
        run(["cargo", "build", "--profile", PROFILE,
             "-p", "mz-clusterd-test-driver", "--bin", "headless-driver"])


def clusterd_command() -> list[str]:
    compute_tc = timely_config(
        ["127.0.0.1"], 2102, 1, DEFAULT_COMPUTE_EXERT_PROPORTIONALITY
    )
    storage_tc = timely_config(
        ["127.0.0.1"], 2103, 1, DEFAULT_STORAGE_EXERT_PROPORTIONALITY
    )
    return [
        *shlex.split(WRAPPER),
        str(ROOT / "target" / PROFILE_DIR / "clusterd"),
        "--compute-controller-listen-addr", COMPUTE_ADDR,
        "--storage-controller-listen-addr", STORAGE_ADDR,
        "--compute-timely-config", compute_tc,
        "--storage-timely-config", storage_tc,
        "--process", "0",
        "--environment-id", ENVIRONMENT_ID,
        "--secrets-reader", "local-file",
        "--secrets-reader-local-file-dir", SECRETS_DIR,
        "--scratch-directory", SCRATCH_DIR,
    ]


def inner_clusterd_pid(launched_pid: int) -> int | None:
    """Resolve the clusterd PID. With no wrapper it is `launched_pid`; with a
    wrapper, the wrapper's argv also contains the clusterd path, so exclude it."""
    if not WRAPPER:
        return launched_pid
    pattern = (
        f"target/{PROFILE_DIR}/clusterd --compute-controller-listen-addr {COMPUTE_ADDR}"
    )
    out = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True)
    pids = [int(p) for p in out.stdout.split() if int(p) != launched_pid]
    return pids[0] if pids else launched_pid


def wait_for_port(addr: str, timeout: float = 30.0) -> None:
    import socket

    host, port = addr.rsplit(":", 1)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, int(port)), timeout=0.2):
                return
        except OSError:
            time.sleep(0.5)


def terminate(launched: "subprocess.Popen[bytes]", clusterd_pid: int | None) -> None:
    # Terminate the inner clusterd first so a wrapper (heaptrack/perf) sees its
    # child exit and flushes its own output. Escalate to SIGKILL if needed.
    if clusterd_pid is not None:
        try:
            os.kill(clusterd_pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        for _ in range(20):
            try:
                os.kill(clusterd_pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.3)
        else:
            try:
                os.kill(clusterd_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    # Give the wrapper time to finish flushing, then force it.
    if launched.pid != clusterd_pid:
        try:
            launched.wait(timeout=10)
        except subprocess.TimeoutExpired:
            launched.terminate()


def main() -> int:
    for d in (BLOB_DIR, SCRATCH_DIR, SECRETS_DIR):
        Path(d).mkdir(parents=True, exist_ok=True)
    ensure_cockroach()
    cargo_build()

    launched: "subprocess.Popen[bytes] | None" = None
    clusterd_pid: int | None = None
    try:
        if RUN_CLUSTERD:
            cmd = clusterd_command()
            print("clusterd command:\n  PERSIST_PUBSUB_URL=http://127.0.0.1:"
                  f"{PUBSUB_PORT} \\\n  " + " ".join(shlex.quote(a) for a in cmd))
            log = open("/tmp/clusterd-test-driver-clusterd.log", "w")
            clusterd_env = dict(os.environ, PERSIST_PUBSUB_URL=f"http://127.0.0.1:{PUBSUB_PORT}")
            launched = subprocess.Popen(cmd, stdout=log, stderr=log, env=clusterd_env, cwd=ROOT)
            # Wait for the wrapper to fork clusterd, then resolve its pid.
            clusterd_pid = None
            for _ in range(60):
                clusterd_pid = inner_clusterd_pid(launched.pid)
                if clusterd_pid is not None:
                    break
                time.sleep(0.5)
            wait_for_port(COMPUTE_ADDR)

        print("Running driver...")
        driver_env = dict(
            os.environ,
            CLUSTERD_COMPUTE_ADDR=COMPUTE_ADDR,
            PERSIST_BLOB_URL=f"file://{BLOB_DIR}",
            PERSIST_CONSENSUS_URL=CONSENSUS_URL,
            DRIVER_PUBSUB_BIND=f"0.0.0.0:{PUBSUB_PORT}",
            TARGET_BYTES=TARGET_BYTES,
            SCENARIO=SCENARIO,
            N_TIMESTAMPS=N_TIMESTAMPS,
        )
        result = subprocess.run(
            [str(ROOT / "target" / PROFILE_DIR / "headless-driver")],
            env=driver_env,
            cwd=ROOT,
        )
        return result.returncode
    finally:
        if launched is not None:
            terminate(launched, clusterd_pid)


if __name__ == "__main__":
    sys.exit(main())
