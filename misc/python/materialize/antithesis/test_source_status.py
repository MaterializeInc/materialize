# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import sqlite3
from unittest.mock import Mock

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis.drivers import (
    kafka_sources,
    postgres_sources,
    recovery,
)
from materialize.antithesis.drivers import (
    source_status as s,
)
from materialize.antithesis.drivers.sources_common import Deadline, Heartbeat


@pytest.mark.parametrize(
    "advance_after, expected_elapsed, expected_ok",
    [(0, 0, True), (20, 20, True), (None, 600, False)],
    ids=["healthy", "temporary_stall", "permanent_wedge"],
)
def test_progress_skips_terminal_exports_but_detects_wedges(
    monkeypatch: pytest.MonkeyPatch,
    advance_after: float | None,
    expected_elapsed: float,
    expected_ok: bool,
) -> None:
    now = [0.0]
    monkeypatch.setattr(s.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(
        s.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds)
    )
    monkeypatch.setattr(
        s,
        "_data_exports",
        lambda conn: [("u1", "u3", "kafka"), ("u2", "u4", "postgres")],
    )
    statuses = {"u1": {"r1": ("running", "t0")}, "u2": {"r1": ("stalled", "t0")}}
    monkeypatch.setattr(s, "latest_statuses", lambda conn, ids: statuses)
    monkeypatch.setattr(
        s,
        "statistics",
        lambda conn, ids: {
            ("u1", "r1"): {
                "snapshot_committed": True,
                "rehydrated": True,
                "offset_known": 0,
                "offset_committed": 0,
            }
        },
    )
    monkeypatch.setattr(
        s,
        "frontiers",
        lambda conn, ids: {
            "u1": (
                0,
                101 if advance_after is not None and now[0] >= advance_after else 100,
            ),
            "u2": (0, None),
        },
    )
    monkeypatch.setattr(s, "mz_now_ms", lambda conn: 100)
    monkeypatch.setattr(
        s, "lookup_ids", lambda conn, names: {name: "u2" for name in names}
    )
    monkeypatch.setattr(kafka_sources, "send_heartbeats", lambda host: [])
    monkeypatch.setattr(
        postgres_sources,
        "send_heartbeats",
        lambda host: [Heartbeat("terminal_export", "SELECT terminal_heartbeat()", ())],
    )
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE expected_unhealthy (object_name TEXT)")
    db.execute("INSERT INTO expected_unhealthy VALUES ('terminal_export')")
    monkeypatch.setattr(recovery, "open_state", lambda: db)
    assertions: list[tuple[bool, str]] = []
    monkeypatch.setattr(
        s,
        "always",
        lambda condition, name, details: assertions.append((condition, name)),
    )
    monkeypatch.setattr(s, "sometimes", lambda *args: None)

    conn = Mock()
    conn.execute.side_effect = [None, psycopg.OperationalError("terminal export")]
    assert s._eventually("host", conn, Deadline(1020)) == 0
    progress = [(ok, name) for ok, name in assertions if "write frontier" in name]
    assert len(progress) == 1
    assert now[0] == expected_elapsed
    assert progress[0][0] == expected_ok
    conn.execute.assert_called_once_with("SET transaction_isolation = 'serializable'")
