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


@pytest.mark.parametrize("identity", ["s-txns", "s-other", None])
def test_negative_counts_require_known_txns_shard_identity(
    monkeypatch: pytest.MonkeyPatch, identity: str | None
) -> None:
    auditor = s.Auditor.__new__(s.Auditor)
    auditor.host = "sql-host"
    auditor.txns_shard = None
    auditor.txns_shard_retried = False
    auditor.deadline = float("inf")
    auditor.incomplete = 0
    lookup = Mock(return_value=identity)
    monkeypatch.setattr(s, "txns_shard", lookup)
    assertions: list[bool] = []
    monkeypatch.setattr(s, "always", lambda ok, *args: assertions.append(ok))
    mult = {
        "negative_count": 1,
        "checked_updates": 1,
        "schema_ids": ["h0"],
        "negative": [{"key": "{{s-data,42,},}", "val": "null", "diff": -1}],
    }

    complete = auditor.check_multiplicities(mult, False, {"shard_id": "s-txns"})

    assert assertions == ([False] if identity == "s-other" else [])
    assert complete is (identity is not None)
    assert auditor.incomplete == int(identity is None)
    lookup.assert_called_once_with("sql-host", timeout_s=5)
    if identity is None:
        auditor.check_multiplicities(mult, False, {"shard_id": "s-txns"})
        lookup.assert_called_once()
        assert auditor.incomplete == 2


def test_negative_counts_do_not_retry_identity_after_audit_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    auditor = s.Auditor.__new__(s.Auditor)
    auditor.host = "sql-host"
    auditor.txns_shard = None
    auditor.txns_shard_retried = False
    auditor.deadline = 0
    auditor.incomplete = 0
    lookup = Mock(return_value="s-txns")
    monkeypatch.setattr(s, "txns_shard", lookup)
    assertion = Mock()
    monkeypatch.setattr(s, "always", assertion)

    assert not auditor.check_multiplicities(
        {"negative_count": 1, "checked_updates": 1, "negative": []},
        False,
        {"shard_id": "s-txns"},
    )

    assert auditor.incomplete == 1
    lookup.assert_not_called()
    assertion.assert_not_called()


def test_missing_txns_identity_marks_audit_partial_and_keeps_blob_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    auditor = s.Auditor.__new__(s.Auditor)
    auditor.host = "sql-host"
    auditor.origin = "anytime"
    auditor.txns_shard = None
    auditor.txns_shard_retried = False
    auditor.deadline = float("inf")
    auditor.incomplete = 0
    auditor.db = Mock()
    auditor.db.execute.return_value.fetchone.return_value = None
    blobs = {"missing": []}
    mult = {"initialized": True, "negative_count": 1, "checked_updates": 1}
    auditor.inspect = Mock(side_effect=[blobs, mult])
    auditor.record = Mock()
    auditor.terminal_tag = Mock(return_value=False)
    auditor.report_unfinalized = Mock()
    monkeypatch.setattr(s, "txns_shard", lambda host, **kwargs: None)
    monkeypatch.setattr(s, "reachable", lambda *args: None)
    assertions: list[tuple[bool, str]] = []
    monkeypatch.setattr(
        s, "always", lambda ok, name, details: assertions.append((ok, name))
    )

    auditor.audit_unmapped("s-txns")

    auditor.record.assert_called_once_with("s-txns", "partial", blobs, mult)
    assert auditor.incomplete == 1
    assert assertions == [
        (
            True,
            "shard audit: every blob referenced by an unmapped shard's persist state exists",
        )
    ]
