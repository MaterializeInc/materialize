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
import threading
from collections import Counter
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

pytest.importorskip("antithesis")

from kubernetes import client

from materialize.antithesis import ivm_model as model
from materialize.antithesis import quiet
from materialize.antithesis.drivers import ivm, pod_restarts, read_paths


def test_read_path_kills_wait_for_quiet_periods(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(quiet, "active_quiet_period", lambda: True)
    replicas = Mock(side_effect=AssertionError("must not select a victim during quiet"))
    monkeypatch.setattr(read_paths, "replicas", replicas)
    read_paths._kill_replica_pod(Mock(), Mock())
    replicas.assert_not_called()


def test_read_path_kill_records_pod_incarnation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(quiet, "active_quiet_period", lambda: False)
    monkeypatch.setattr(read_paths, "replicas", lambda _: [("u1", "u2", "r1")])
    victim = SimpleNamespace(
        metadata=SimpleNamespace(
            name="mz-cluster-u1-replica-u2-gen-1-0",
            uid="incarnation",
            deletion_timestamp=None,
        )
    )
    kube = Mock(namespace="ns")
    kube.core.list_namespaced_pod.return_value.items = [victim]
    monkeypatch.setattr(read_paths, "Kube", lambda _: kube, raising=False)
    monkeypatch.setattr(client, "CoreV1Api", lambda: kube.core)
    delete = Mock(return_value=True)
    monkeypatch.setattr(pod_restarts, "delete_recorded", delete)
    read_paths._kill_replica_pod(
        Mock(endpoints=SimpleNamespace(namespace="ns")), Mock()
    )
    assert delete.call_args.kwargs["uid"] == "incarnation"
    assert delete.call_args.kwargs["origin"] == "read_paths_load"
    assert kube.core.list_namespaced_pod.call_args.kwargs["_request_timeout"]


def test_read_path_round_stops_sending_reads_when_budget_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [0.0]
    monkeypatch.setattr(read_paths.time, "monotonic", lambda: now[0])
    conn = Mock(closed=False)

    def execute(query: str | bytes) -> Mock:
        statement = query.decode() if isinstance(query, bytes) else query
        if statement.startswith("SELECT"):
            now[0] += 1.0
        return Mock(fetchall=lambda: [(1, 2, 3)])

    conn.execute.side_effect = execute
    connect = Mock(return_value=conn)
    monkeypatch.setattr(read_paths, "_connect", connect)
    round = read_paths.Round("host", 5, [])
    round.deadline = 1.0
    assert round.select(
        read_paths.RELATIONS[0], "index", read_paths.CLUSTER, None
    ).rows == Counter({("1", "2", "3"): 1})
    assert (
        round.select(
            read_paths.RELATIONS[0], "persist", read_paths.OTHER_CLUSTER, None
        ).rows
        is None
    )
    assert connect.call_args.kwargs["statement_timeout_ms"] == 1000
    selects = [
        call
        for call in conn.execute.call_args_list
        if isinstance(call.args[0], bytes) and call.args[0].startswith(b"SELECT")
    ]
    assert len(selects) == 1


def test_read_path_subscribe_waits_for_progress_before_closing_a_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        (6, False, 1, 99, 99, 99),
        (5, False, 1, 1, 2, 3),
        (6, True, None, None, None, None),
    ]
    round = read_paths.Round("host", 5, [])
    monkeypatch.setattr(round, "target", lambda *args: conn)
    result = round.subscribe(read_paths.RELATIONS[0], None)
    assert result.rows == Counter({("1", "2", "3"): 1})


def _snapshot_rows() -> list[tuple]:
    base = {name: Counter() for name in model.TABLE_NAMES}
    base["t"][(1, 0, 5, 1)] += 1
    rows = [(10, "t", *model.pad((1, 0, 5, 1)))]
    for view in model.VIEWS:
        for variant in model.VARIANTS:
            for row, n in model.reference(view.kind, base, 10).items():
                rows.extend([(10, model.tag(view.kind, variant), *row)] * n)
    return rows


def _write_ledger(outcome: str | None = "ok") -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.execute(
        "CREATE TABLE writes (op_id INTEGER, outcome TEXT, invoke_rt REAL, complete_rt REAL, commit_ts INTEGER)"
    )
    if outcome is not None:
        db.execute("INSERT INTO writes VALUES (1, ?, 0, 1, NULL)", (outcome,))
    return db


@pytest.mark.parametrize("corrupt", [False, True])
@pytest.mark.parametrize("outcome", ["ok", "pending", "indeterminate"])
def test_final_ivm_checks_current_view_contents(
    monkeypatch: pytest.MonkeyPatch, corrupt: bool, outcome: str
) -> None:
    rows = _snapshot_rows()
    if corrupt:
        rows = [row for row in rows if row[1] != model.tag("reduce", "mv")]
    conn = Mock()
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    conn.execute.return_value.fetchall.return_value = rows
    monkeypatch.setattr(ivm, "_connect", lambda *args, **kwargs: conn)
    monkeypatch.setattr(ivm, "open_state", lambda: _write_ledger(outcome))
    checks = []
    monkeypatch.setattr(
        ivm,
        "always",
        lambda condition, message, details: checks.append((condition, message)),
    )
    assert ivm._final_snapshot("host")
    failed = [message for passed, message in checks if not passed]
    assert failed == ([ivm.KIND_EQUAL["reduce"]] if corrupt else [])
    assert any(
        "strict serializable" in str(call.args[0])
        for call in conn.execute.call_args_list
    )


@pytest.mark.parametrize("outcome", [None, "rejected"])
def test_final_ivm_rejects_unattempted_or_rejected_base_rows(
    monkeypatch: pytest.MonkeyPatch, outcome: str | None
) -> None:
    conn = Mock()
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    conn.execute.return_value.fetchall.return_value = _snapshot_rows()
    monkeypatch.setattr(ivm, "_connect", lambda *args, **kwargs: conn)
    monkeypatch.setattr(ivm, "open_state", lambda: _write_ledger(outcome))
    checks = []
    monkeypatch.setattr(
        ivm,
        "always",
        lambda condition, message, details: checks.append((condition, message)),
    )
    assert ivm._final_snapshot("host")
    assert [message for passed, message in checks if not passed] == [
        "ivm: every base row carries the id of a write that was attempted and not definitely rejected"
    ]


def test_final_ivm_reports_a_stuck_read(monkeypatch: pytest.MonkeyPatch) -> None:
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE writes (op_id INTEGER)")
    db.execute("INSERT INTO writes VALUES (1)")
    monkeypatch.setattr(ivm, "open_state", lambda: db)
    monkeypatch.setattr(
        ivm, "Environment", lambda: SimpleNamespace(sql_host=lambda: "host")
    )
    release = threading.Event()
    monkeypatch.setattr(ivm, "_final_snapshot", lambda _: release.wait(5.0))
    monkeypatch.setattr(ivm, "FINAL_CHECK_BUDGET_S", 0.02)
    checks = []
    monkeypatch.setattr(
        ivm,
        "always",
        lambda condition, message, details: checks.append((condition, message)),
    )
    try:
        assert ivm.final_main() == 0
        assert checks == [
            (
                False,
                "ivm: every maintained view can be compared with a current reference after faults stop",
            )
        ]
    finally:
        release.set()


def test_final_ivm_does_not_accept_an_empty_union(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = Mock()
    conn.__enter__ = Mock(return_value=conn)
    conn.__exit__ = Mock(return_value=False)
    conn.execute.return_value.fetchall.return_value = []
    monkeypatch.setattr(ivm, "_connect", lambda *args, **kwargs: conn)
    assert not ivm._final_snapshot("host")
