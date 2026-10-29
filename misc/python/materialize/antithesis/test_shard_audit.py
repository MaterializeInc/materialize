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

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis.drivers import shard_audit as s


@pytest.mark.parametrize("unavailable", [False, True])
def test_audit_has_one_budget_and_reports_incomplete_work(
    monkeypatch: pytest.MonkeyPatch, unavailable: bool
) -> None:
    now = [0.0]
    monkeypatch.setattr(s.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(s, "FINALLY_BUDGET_S", 5, raising=False)
    monkeypatch.setattr(s, "Environment", Mock())
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(s.state, "open_db", lambda name: db)
    monkeypatch.setattr(s, "txns_shard", lambda host: None)
    monkeypatch.setattr(s.Auditor, "signature", lambda self: None)
    monkeypatch.setattr(
        s,
        "live_shards",
        lambda host: {
            "s1": s.LiveShard("s1", "u1", "table", "t1"),
            "s2": s.LiveShard("s2", "u2", "table", "t2"),
        },
    )
    remaps = Mock(return_value=[])
    monkeypatch.setattr(s, "remap_pairs", remaps)
    monkeypatch.setattr(s, "consensus_shards", lambda endpoints: set())
    timeouts = []

    def inspect(command, shard, endpoints, timeout, extra):
        timeouts.append(timeout)
        now[0] += timeout if unavailable else 1
        return (
            None
            if unavailable
            else {
                "shard_id": shard,
                "initialized": False,
                "missing": [],
                "negative_count": 0,
                "checked_updates": 0,
            }
        )

    monkeypatch.setattr(s, "run_persistcli", inspect)
    assertions: list[tuple[bool, str]] = []
    monkeypatch.setattr(
        s, "always", lambda ok, name, details: assertions.append((ok, name))
    )
    monkeypatch.setattr(s, "sometimes", lambda *args: None)
    auditor = s.Auditor("finally", 3)
    assert auditor.run(2, 0, 0, 0) == 0
    assert now[0] <= 5
    assert timeouts == ([3, 2] if unavailable else [3, 3, 3, 2])
    completeness = [ok for ok, name in assertions if "selected audits complete" in name]
    assert completeness == [not unavailable]
    assert db.execute("SELECT count(*) FROM audits").fetchone()[0] == 2
    assert remaps.called is not unavailable


def test_final_audit_reports_environment_setup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(s, "Auditor", Mock(side_effect=RuntimeError("unavailable")))
    assertions: list[bool] = []
    monkeypatch.setattr(s, "always", lambda ok, *args: assertions.append(ok))
    assert s.finally_main() == 0
    assert assertions == [False]
