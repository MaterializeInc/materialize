# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import os
import subprocess
import time
from collections.abc import Callable
from typing import Any

import psycopg
from antithesis.assertions import always, reachable  # pyright: ignore
from antithesis.random import AntithesisRandom  # pyright: ignore

LIVE = """
SELECT s.shard_id, o.id, o.name
FROM mz_internal.mz_storage_shards s
JOIN mz_internal.mz_object_global_ids g ON g.global_id = s.object_id
JOIN mz_objects o ON o.id = g.id
"""
PAIRS = """
SELECT src.id, ss.shard_id, t.id, ts.shard_id
FROM mz_tables t
JOIN mz_sources src ON src.id = t.source_id
JOIN mz_internal.mz_object_global_ids tg ON tg.id = t.id
JOIN mz_internal.mz_storage_shards ts ON ts.object_id = tg.global_id
JOIN mz_internal.mz_object_global_ids sg ON sg.id = src.id
JOIN mz_internal.mz_storage_shards ss ON ss.object_id = sg.global_id
WHERE t.id LIKE 'u%'
"""


def audit(
    mz: Callable[[str], list[Any]],
    consensus: str,
    blob: str,
    rnd: AntithesisRandom,
    stopped: Callable[[], bool],
    deadline: float | None = None,
) -> str | None:
    recovered = deadline is not None
    end = deadline if deadline is not None else time.monotonic() + 180
    env = {**os.environ, "CONSENSUS_URI": consensus, "BLOB_URI": blob}

    def inspect(command: str, shard: str) -> dict[str, Any] | None:
        timeout = min(end - time.monotonic(), 60 if recovered else 20)
        if timeout <= 0 or stopped():
            return None
        try:
            proc = subprocess.run(
                ["persistcli", "inspect", command, "--shard-id", shard],
                env=env,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            return None
        if proc.returncode:
            print(
                f"{command} {shard} exited {proc.returncode}: {proc.stderr[-1000:]}",
                flush=True,
            )
            return None
        return json.loads(proc.stdout.splitlines()[-1])

    def pick[T](items: list[T], count: int) -> list[T]:
        return rnd.sample(items, min(count, len(items)))

    live = {s: (obj, name) for s, obj, name in mz(LIVE)}
    with psycopg.connect(consensus, connect_timeout=10, autocommit=True) as conn:
        conn.execute("SET statement_timeout = '10s'")
        known = {s for (s,) in conn.execute("SELECT DISTINCT shard FROM consensus")}
    user = {s for s, (obj, _) in live.items() if obj.startswith("u")}
    n = 25 if recovered else rnd.randint(1, 3)
    shards = pick(sorted(user), n) + pick(
        sorted(live.keys() - user), 5 if recovered else 3 - n
    )
    # Consensus also contains system and dropped shards that have no live catalog entry.
    shards += pick(sorted(known - live.keys()), 10 if recovered else 1)
    checked_shards = checked_pairs = 0
    for shard in shards:
        blobs = inspect("audit-blobs", shard)
        if blobs is None:
            continue
        details = {
            "shard": shard,
            "object": live.get(shard),
            "missing": blobs["missing"][:20],
        }
        always(
            not blobs["missing"],
            "every blob referenced by live persist state exists",
            details,
        )
        if shard in live and blobs["since"] is not None:
            # A concurrent DROP releases the hold and permits finalization.
            if blobs["since"] or (shard, *live[shard]) in mz(LIVE):
                always(
                    bool(blobs["since"]),
                    "a live collection retains a readable persist shard",
                    details,
                )
                checked_shards += 1

    for pair in pick(sorted(mz(PAIRS)), 20 if recovered else 2):
        source, remap, table, export = pair
        # An advancing remap since can establish a violation only against an
        # export upper read later, while the same dependency still exists.
        left = inspect("audit-frontiers", remap)
        right = inspect("audit-frontiers", export)
        if not left or not right or right["upper"] in ([], [0]):
            continue
        since, upper = left["since"], right["upper"][0]
        valid = bool(since) and since[0] < upper
        # A new export may append empty batches up to the remap since before
        # its snapshot finishes. Bootstrap exempts that case (#40353).
        if since and not valid and right["updates"] == 0:
            continue
        if not valid and pair not in mz(PAIRS):
            continue
        always(
            valid,
            "a writable source export holds its remap since below its upper",
            {
                "source": source,
                "remap": remap,
                "table": table,
                "export": export,
                "since": since,
                "upper": upper,
            },
        )
        checked_pairs += 1
    if checked_shards and checked_pairs:
        reachable("persist audited live shards and a writable source export", {})
        return None
    return f"audited {checked_shards} live shards and {checked_pairs} writable source exports"
