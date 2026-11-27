# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Audit the live persist state transitions retained after a scenario."""

import json
import math
import os
import re
import selectors
import subprocess
import time

from materialize.invariants.framework import EventLog, InvariantViolation
from materialize.mzcompose.composition import Composition, Service
from materialize.mzcompose.services.minio import minio_blob_uri

AUDIT_BUDGET_S = 240
AUDIT_SHARD_TIMEOUT_S = 20
AUDIT_MAX_BYTES = 50_000_000
AUDIT_MAX_PARTS = 500
BLOB_KEY = re.compile(r'"((?:n[0-9]+|w[0-9a-f-]{36}|v[0-9]+)/[a-z][0-9a-f-]{36})"')


def _inspect(container: str, deadline: float, *args: str) -> tuple[bytearray, bool]:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("persist inspection deadline expired")
    # Killing docker exec only stops the client. The image's timeout also
    # bounds the process inside the sidecar if we stop reading its output.
    cmd = [
        "docker",
        "exec",
        container,
        "/busybox/timeout",
        "-s",
        "KILL",
        str(math.ceil(remaining)),
        "persistcli",
        "--startup-log-filter=warn",
        "inspect",
        *args,
        f"--blob-uri={minio_blob_uri()}",
    ]
    with subprocess.Popen(cmd, stdout=subprocess.PIPE) as proc:
        assert proc.stdout is not None
        output = bytearray()
        try:
            with selectors.DefaultSelector() as readable:
                readable.register(proc.stdout, selectors.EVENT_READ)
                while len(output) <= AUDIT_MAX_BYTES:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not readable.select(remaining):
                        raise TimeoutError("persist inspection deadline expired")
                    chunk = os.read(
                        proc.stdout.fileno(),
                        min(65536, AUDIT_MAX_BYTES + 1 - len(output)),
                    )
                    if not chunk:
                        status = proc.wait(timeout=max(0, deadline - time.monotonic()))
                        if status:
                            raise subprocess.CalledProcessError(status, cmd)
                        return output, False
                    output.extend(chunk)
            # Only complete JSON records before the cap are usable.
            del output[output.rfind(b"\n", 0, AUDIT_MAX_BYTES) + 1 :]
            return output, True
        finally:
            if proc.poll() is None:
                proc.kill()


def _check_history(shard: str, output: bytearray) -> bool:
    previous_keys: set[str] | None = None
    removed: set[str] = set()
    for line in output.splitlines():
        transition = json.loads(line)
        if previous_keys is None:
            previous_keys = set(BLOB_KEY.findall(json.dumps(transition["previous"])))
        state = transition["new"]
        keys = set(BLOB_KEY.findall(json.dumps(state)))
        if resurrected := removed & keys:
            raise InvariantViolation(
                f"shard {shard} resurrects {min(resurrected)} at seqno {state['seqno']}"
            )
        removed.update(previous_keys - keys)
        previous_keys = keys
    return previous_keys is not None


def audit_persist_history(c: Composition, log: EventLog) -> None:
    """Check that each blob key appears over one contiguous run of states."""
    c.up(Service("persistcli", idle=True))
    container = c.container_id("persistcli")
    if container is None:
        raise InvariantViolation("persist audit sidecar is unavailable")
    deadline = time.monotonic() + AUDIT_BUDGET_S
    output, capped = _inspect(container, deadline, "blob-count")
    if capped:
        raise InvariantViolation("persist audit shard listing exceeded the byte limit")
    counts = json.loads(output)
    # The CLI emits two full states per transition. Skip large shards to
    # leave time for histories from the tables the workload created and dropped.
    shards = sorted(
        (
            shard
            for shard, count in counts.items()
            if count["batch_part_count"] <= AUDIT_MAX_PARTS
        ),
        key=lambda shard: -counts[shard]["batch_part_count"],
    )
    audited = truncated = 0
    for shard in shards:
        if time.monotonic() >= deadline:
            break
        try:
            output, capped = _inspect(
                container,
                min(deadline, time.monotonic() + AUDIT_SHARD_TIMEOUT_S),
                "state-diff",
                f"--shard-id={shard}",
                "--consensus-uri=postgres://root@postgres-metadata:26257?options=--search_path=consensus",
            )
        except (TimeoutError, subprocess.SubprocessError) as e:
            log.log("audit", f"{shard}: inspect failed ({e})", echo=False)
            continue
        if _check_history(shard, output):
            audited += 1
        truncated += int(capped)
    if not audited:
        raise InvariantViolation(
            f"persist audit read no state transitions across {len(shards)} eligible shards"
        )
    log.log(
        "audit",
        f"checked histories of {audited}/{len(shards)} eligible shards ({truncated} truncated)",
    )
