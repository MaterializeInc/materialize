# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sqlite3
from unittest.mock import Mock

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis.drivers import atomic_commits as a


@pytest.fixture
def ledger(monkeypatch):
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(a.state, "open_db", lambda *args: db)
    a.open_state()
    with db:
        db.execute(
            "INSERT INTO plans (shards,value,outcome) VALUES ('[0,1]',7,'indeterminate')"
        )
    monkeypatch.setattr(a.telemetry, "comparison", Mock())
    monkeypatch.setattr(a, "sometimes", Mock())
    monkeypatch.setattr(a, "always", lambda ok, *args: ok or pytest.fail(str(args)))
    yield db
    db.close()


def test_acknowledgement_after_read_start_is_not_a_snapshot_obligation(
    ledger, monkeypatch
):
    def execute(*args):
        with ledger:
            ledger.execute("UPDATE plans SET outcome='ok'")
        return Mock(fetchall=lambda: [])

    monkeypatch.setattr(a, "_execute", execute)
    assert a.check(ledger, Mock(), 100)


def test_snapshot_overlapping_the_held_writer_deadline_is_not_forced_empty(
    ledger, monkeypatch
):
    held = [True]

    def execute(*args):
        held[0] = False
        return Mock(fetchall=lambda: [(0, 1, 7), (1, 1, 7)])

    monkeypatch.setattr(a, "_execute", execute)
    assert a.check(ledger, Mock(), 100, uncommitted=1, still_held=lambda: held[0])
    assert ledger.execute("SELECT outcome FROM plans").fetchone() == ("observed",)


def test_still_held_transaction_cannot_be_visible(ledger, monkeypatch):
    monkeypatch.setattr(
        a, "_execute", Mock(return_value=Mock(fetchall=lambda: [(0, 1, 7), (1, 1, 7)]))
    )
    with pytest.raises(pytest.fail.Exception, match="uncommitted"):
        a.check(ledger, Mock(), 100, uncommitted=1, still_held=lambda: True)


def test_cleanup_keeps_unknown_commit_evidence_and_applies_backpressure(
    ledger, monkeypatch
):
    monkeypatch.setattr(a, "MAX_PLANS", 1)
    execute = Mock()
    monkeypatch.setattr(a, "_execute", execute)
    assert not a._prune(ledger, Mock(), 100)
    execute.assert_not_called()
    assert ledger.execute("SELECT retired FROM plans").fetchone() == (0,)


def test_interrupted_cleanup_cannot_erase_future_markers(ledger, monkeypatch):
    monkeypatch.setattr(a, "MAX_PLANS", 1)
    with ledger:
        ledger.execute("UPDATE plans SET outcome='ok'")
    calls = []

    def execute(conn, deadline, query, params):
        calls.append((query, params))
        assert "WHERE id <= %s" in query and params == (1,)
        raise OSError("lost cleanup response")

    monkeypatch.setattr(a, "_execute", execute)
    with pytest.raises(OSError):
        a._prune(ledger, Mock(), 100)
    assert len(calls) == 1
    assert ledger.execute("SELECT retired FROM plans").fetchone() == (1,)
    with ledger:
        ledger.execute(
            "INSERT INTO plans (shards,value,outcome) VALUES ('[0,2]',9,'ok')"
        )
    assert ledger.execute("SELECT max(id) FROM plans").fetchone() == (2,)


def test_replica_discovery_failure_does_not_invent_an_acknowledged_fault(monkeypatch):
    monkeypatch.setattr(a.telemetry, "resolve_cluster_id", lambda *args: "u2")
    monkeypatch.setattr(a.rollouts, "Kube", Mock())
    monkeypatch.setattr(a.quiet, "active_quiet_period", lambda: False)
    monkeypatch.setattr(
        a.pod_restarts,
        "pick_victim",
        Mock(side_effect=a.pod_restarts.ApiException(status=503)),
    )
    acknowledged = [False]
    action = a._fault(Mock(), "host", 100, acknowledged)
    assert action is not None
    action.run(lambda: True, Mock())
    assert acknowledged == [False]
