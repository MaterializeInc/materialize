# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
E2E tests for mz-debug
"""

import urllib.request

from materialize import spawn
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz_debug import MzDebug

# The internal HTTP port of `materialized`, which serves the unauthenticated
# `/prof` profiling endpoints (heap, cpu, mode).
INTERNAL_HTTP_PORT = 6878

SERVICES = [
    Materialized(
        ports=[
            "6875:6875",
            "6877:6877",
            f"{INTERNAL_HTTP_PORT}:{INTERNAL_HTTP_PORT}",
        ]
    ),
    MzDebug(),
]


def _heap_profile_inuse(port: int) -> tuple[int, int]:
    """Dumps the jemalloc heap profile and returns
    `(total in-use sampled bytes, number of sampled stacks)`.

    The `dump_jeheap` action returns the raw jemalloc profile in `jeprof` text
    format. Each sampled stack is a line starting with `@` followed by a
    `t*: <objs>: <bytes> [...]` line whose `<bytes>` is the in-use bytes charged
    to that stack. The leading global `t*:` summary line has no preceding `@`
    and is therefore ignored, so stacks are not double counted.
    """
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/prof/",
        data=b"action=dump_jeheap",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            # The internal listener runs with no authenticator, but the
            # profiling route group still requires an authenticated identity.
            # `mz_system` is accepted on the internal listener without a
            # password.
            "x-materialize-user": "mz_system",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        text = response.read().decode("utf-8", "replace")

    total_bytes = 0
    num_stacks = 0
    pending_stack = False
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("@"):
            pending_stack = True
            num_stacks += 1
        elif pending_stack and line.startswith("t*:"):
            # Format: `t*: <objs>: <bytes> [<cum objs>: <cum bytes>]`.
            total_bytes += int(line.split()[2])
            pending_stack = False
    return total_bytes, num_stacks


def _assert_cpu_capture_preserves_heap_profile(
    c: Composition, container_id: str
) -> None:
    """
    Asserts that a CPU profile capture reset heap profiling.
    """
    # Plant long-lived allocations in environmentd's heap. Each view definition
    # embeds a ~512 KiB literal that the catalog holds verbatim. We stay under
    # the 1 MiB statement-batch limit and well above jemalloc's 512 KiB average
    # sampling interval so that the allocations are reliably sampled.
    ballast = "x" * (512 * 1024)
    for i in range(16):
        c.sql(
            f"CREATE VIEW heap_ballast_{i} AS SELECT '{ballast}'::text AS c",
            print_statement=False,
        )

    before_bytes, before_stacks = _heap_profile_inuse(INTERNAL_HTTP_PORT)
    print(
        f"heap profile before CPU capture: {before_bytes} bytes across {before_stacks} stacks"
    )
    # Sanity check: profiling must have captured the planted ballast, otherwise
    # the comparison below is meaningless and could pass spuriously.
    assert before_bytes >= 1_000_000, (
        f"expected the planted allocations to show up in the heap profile, "
        f"only saw {before_bytes} bytes across {before_stacks} stacks"
    )

    # Capture a CPU profile
    spawn.runv(
        [
            "./mz-debug",
            "emulator",
            "--docker-container-id",
            container_id,
            "--dump-cpu-profiles=true",
            "--dump-heap-profiles=false",
            "--dump-prometheus-metrics=false",
            "--dump-system-catalog=false",
            "--dump-docker=false",
            "--cpu-profile-duration-seconds=1",
        ]
    )

    # Assert that the new heap profile has at least as many stacks as the previous one.
    after_bytes, after_stacks = _heap_profile_inuse(INTERNAL_HTTP_PORT)
    print(
        f"heap profile after CPU capture: {after_bytes} bytes across {after_stacks} stacks"
    )
    assert after_bytes >= before_bytes // 2, (
        "CPU profile capture reset the accumulated heap profile: "
        f"{before_bytes} bytes / {before_stacks} stacks before, "
        f"{after_bytes} bytes / {after_stacks} stacks after. "
        "The capture must suspend memory profiling without calling prof.reset."
    )


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.up("materialized", Service("mz-debug", idle=True))
    c.invoke("cp", "mz-debug:/usr/local/bin/mz-debug", ".")
    container_id = c.container_id("materialized")
    if container_id is None:
        raise ValueError("Failed to get materialized container ID")

    # Assert that the CPU capture doesn't reset heap profiling.
    _assert_cpu_capture_preserves_heap_profile(c, container_id)

    # Smoke test: a full `mz-debug` run against the emulator completes without
    # error.
    spawn.runv(
        [
            "./mz-debug",
            "emulator",
            "--docker-container-id",
            container_id,
            "--mz-connection-url",
            "postgres://mz_system@localhost:6877/materialize",
        ]
    )
