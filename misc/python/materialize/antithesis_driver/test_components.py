# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
from collections import Counter
from pathlib import Path
from typing import Any

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import ivm_model as model
from materialize.antithesis import serializability as sz
from materialize.antithesis import sql
from materialize.antithesis_driver import (
    client,
    counters,
    history,
    local,
    views,
)


class Recorder:
    """Stands in for an SDK assertion function, keeping every evaluation."""

    def __init__(self) -> None:
        self.calls: list[tuple[bool, str, Any]] = []

    def __call__(self, condition: bool, message: str, details: Any = None) -> None:
        self.calls.append((bool(condition), message, details))

    def failed(self) -> list[str]:
        return sorted({m for c, m, _ in self.calls if not c})


def test_counter_bounds_count_only_what_the_read_window_allows() -> None:
    writes = [
        counters.Write(0, invoke=1.0, complete=2.0, outcome="ok"),
        counters.Write(0, invoke=3.0, complete=9.0, outcome="ok"),
        counters.Write(0, invoke=4.0, complete=5.0, outcome="rejected"),
        counters.Write(0, invoke=6.0, complete=None, outcome=None),
        counters.Write(1, invoke=1.0, complete=2.0, outcome="ok"),
    ]
    # Read from 2.5 to 7: only the first increment was acknowledged before it
    # began. Three were attempted before it ended, one of them rejected.
    assert counters.bounds(writes, 0, 2.5, 7.0) == (1, 3, 1)
    assert counters.bounds(writes, 0, 10.0, 11.0) == (2, 3, 1)


def test_parse_observation_reads_values_and_counts() -> None:
    path = history.paths(3, 1, False)[0]
    observation = history.parse_observation(path, "5,7,9", "7:2,9:0")
    assert observation == sz.Observation(1, "table", (5, 7, 9), ((7, 2), (9, 0)))
    assert history.parse_observation(path, None, None).values == ()


def test_paths_route_the_index_to_its_cluster() -> None:
    assert [p.name for p in history.paths(0, 0, True)] == ["index"]
    assert [p.name for p in history.paths(0, 0, False)] == ["table"]
    assert [p.name for p in history.paths(0, 2, False)] == ["table", "join_mv"]


def test_a_pending_operation_is_checked_as_indeterminate() -> None:
    record = history.Record(
        op_id=1, kind=sz.Kind.APPEND, invoke=1.0, isolation=sz.STRICT, write_key=0
    )
    assert record.freeze().outcome is sz.Outcome.INFO


def test_history_reports_a_stale_strict_read(monkeypatch: pytest.MonkeyPatch) -> None:
    recorder = Recorder()
    monkeypatch.setattr(history, "always", recorder)
    monkeypatch.setattr(history, "sometimes", Recorder())
    ops = [
        sz.Op(1, sz.Kind.APPEND, sz.Outcome.OK, invoke=1.0, complete=2.0, write_key=0),
        sz.Op(
            2,
            sz.Kind.READ,
            sz.Outcome.OK,
            invoke=3.0,
            complete=4.0,
            observations=(sz.Observation(0, "table", ()),),
        ),
    ]
    history.assert_report(sz.check(ops), 0, "test")
    assert (
        "client history: strict serializable reads observe every write acknowledged before they began"
        in recorder.failed()
    )
    # Every assertion is evaluated on every check, so each one is reached.
    assert len({m for _, m, _ in recorder.calls}) == 20


def _snapshot(base: dict[str, Counter], ts: int) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for name, bag in base.items():
        for row, n in bag.items():
            rows += [(str(ts), name, *model.pad(row))] * n
    for view in model.VIEWS:
        expected = model.reference(view.kind, base, ts)
        for variant in model.VARIANTS:
            for row, n in expected.items():
                rows += [(str(ts), model.tag(view.kind, variant), *row)] * n
    return rows


def test_views_detect_a_wrong_maintained_row(monkeypatch: pytest.MonkeyPatch) -> None:
    base = {
        "t": Counter({(1, 0, 5, 1): 1, (2, 0, -3, 2): 1, (3, 1, 7, 3): 1}),
        "d": Counter({(0, 4): 1}),
        "ev": Counter({(1, 1000): 1}),
        "e": Counter({(0, 1): 1, (1, 2): 1}),
    }
    ts = 990
    component = views.Views.__new__(views.Views)
    component._lock = __import__("threading").Lock()
    component.writes = {1: "ok", 2: "ok", 3: "unknown", 4: "ok"}
    component.counts = Counter()
    component.now_ms = 0

    recorder = Recorder()
    monkeypatch.setattr(views, "always", recorder)
    monkeypatch.setattr(views, "sometimes", Recorder())
    rows = _snapshot(base, ts)
    component.compare(rows, "strict serializable")
    assert recorder.failed() == []

    recorder.calls.clear()
    corrupt = [r for r in rows if r[1] != model.tag("reduce", "mv")]
    corrupt.append((str(ts), model.tag("reduce", "mv"), 0, 99, 9, 0, 0))
    component.compare(corrupt, "strict serializable")
    assert recorder.failed() == [
        "ivm: the reduce view equals the reference at a read's timestamp"
    ]


def test_views_reject_rows_of_rejected_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    base = {
        "t": Counter({(1, 0, 5, 7): 1}),
        "d": Counter(),
        "ev": Counter(),
        "e": Counter(),
    }
    component = views.Views.__new__(views.Views)
    component._lock = __import__("threading").Lock()
    component.writes = {7: "rejected"}
    component.counts = Counter()
    component.now_ms = 0
    recorder = Recorder()
    monkeypatch.setattr(views, "always", recorder)
    monkeypatch.setattr(views, "sometimes", Recorder())
    component.compare(_snapshot(base, 5), "strict serializable")
    assert recorder.failed() == [
        "ivm: every base row carries the id of a write that was attempted and not definitely rejected"
    ]


class _Error(psycopg.errors.InternalError_):
    def __init__(self, message: str) -> None:
        super().__init__(message)

    @property
    def sqlstate(self) -> str | None:  # type: ignore[override]
        return "XX000"


def test_classify_outcome_unknown_internal_errors() -> None:
    unknown = _Error(
        "statement execution abandoned, outcome unknown (server shutting down)"
    )
    assert client.classify(unknown).outcome is sql.Outcome.INDETERMINATE
    assert sql.classify(unknown).outcome is sql.Outcome.VIOLATION
    other = _Error("something else broke")
    assert client.classify(other).outcome is sql.Outcome.VIOLATION
    rolled_back = client.classify(client.RolledBack("ROLLBACK"))
    assert rolled_back.outcome is sql.Outcome.REJECTED


def test_would_fail_lists_unmet_and_false_assertions(tmp_path: Path) -> None:
    events = [
        {
            "antithesis_assert": {
                "message": "a",
                "assert_type": "always",
                "display_type": "Always",
                "hit": True,
                "condition": True,
                "must_hit": True,
            }
        },
        {
            "antithesis_assert": {
                "message": "b",
                "assert_type": "always",
                "display_type": "Always",
                "hit": True,
                "condition": False,
                "must_hit": True,
            }
        },
        {
            "antithesis_assert": {
                "message": "c",
                "assert_type": "sometimes",
                "display_type": "Sometimes",
                "hit": True,
                "condition": False,
                "must_hit": True,
            }
        },
        {
            "antithesis_assert": {
                "message": "d",
                "assert_type": "always",
                "display_type": "Always",
                "hit": False,
                "condition": False,
                "must_hit": True,
            }
        },
        {"antithesis_setup": {"status": "complete"}},
    ]
    sdk = tmp_path / "sdk.jsonl"
    sdk.write_text("".join(json.dumps(e) + "\n" for e in events))
    assert local.would_fail(sdk) == ["Always\tb", "Sometimes\tc", "Always\td"]


def test_an_operation_on_a_busy_session_completes_as_not_applied() -> None:
    pool = client.Pool("t", client.Target("unused.invalid"), 1)
    pool.sessions[0].busy_since = 1.0
    outcomes: list[client.Outcome] = []
    op = client.Op("probe", lambda conn: None, outcomes.append)
    assert pool.call(0, op, 0) is client.Status.NOT_SENT
    assert len(outcomes) == 1 and outcomes[0].rejected


def test_a_session_resets_the_settings_of_earlier_operations() -> None:
    class Conn:
        def __init__(self) -> None:
            self.sent: list[bytes] = []

        def execute(self, query: bytes) -> None:
            self.sent.append(query)

    session = client.Session.__new__(client.Session)
    session._settings = {}
    conn: Any = Conn()
    session._apply(conn, {"cluster": "quickstart", "real_time_recency": "true"})
    session._apply(conn, {"cluster": "quickstart"})
    assert conn.sent == [
        b"SET cluster = quickstart",
        b"SET real_time_recency = true",
        b"RESET real_time_recency",
    ]


def test_atomic_requires_only_what_was_settled_before_the_read() -> None:
    from materialize.antithesis import atomic_commits_model
    from materialize.antithesis_driver import atomic

    component = atomic.Atomic.__new__(atomic.Atomic)
    early = atomic.Txn((0, 1), 5, False, outcome="ok", acked_at=1.0)
    late = atomic.Txn((0, 2), 6, False, outcome="ok", acked_at=9.0)
    plans = {1: component._plan(early, 2.0), 2: component._plan(late, 2.0)}
    # The late commit may or may not be visible to a read that began at 2.
    assert atomic_commits_model.violations(plans, [(0, 1, 5), (1, 1, 5)]) == []
    # The early commit must be visible, and whole.
    assert atomic_commits_model.violations(plans, [(0, 1, 5)]) != []
    assert atomic_commits_model.violations(
        plans, [(0, 1, 5), (1, 1, 5), (0, 2, 6)]
    ) == ["torn transaction 2"]


def test_atomic_read_older_than_a_prune_may_see_its_rows() -> None:
    from materialize.antithesis import atomic_commits_model
    from materialize.antithesis_driver import atomic

    component = atomic.Atomic.__new__(atomic.Atomic)
    component.txns = {
        1: atomic.Txn(
            (0, 1), 5, False, outcome="ok", acked_at=1.0, retired=True, pruned_at=4.0
        )
    }
    rows = [(0, 1, 5), (1, 1, 5)]
    assert atomic_commits_model.violations(component._plans(3.0), rows) == []
    assert atomic_commits_model.violations(component._plans(3.0), []) == []
    assert atomic_commits_model.violations(component._plans(5.0), rows) == [
        "unattempted marker 1",
        "unattempted marker 1",
    ]


def test_subscriber_starts_its_stream_at_the_first_progress_row() -> None:
    base = {
        "t": Counter({(1, 0, 5, 1): 1}),
        "d": Counter(),
        "ev": Counter(),
        "e": Counter(),
    }
    snapshot = [
        (ts, False, 1, tag, *payload) for ts, tag, *payload in _snapshot(base, 100)
    ]
    rows = [(100, True, None, None, None, None, None, None, None)]
    rows += [
        (int(ts), progressed, diff, tag, *p)
        for ts, progressed, diff, tag, *p in snapshot
    ]
    rows += [(101, True, None, None, None, None, None, None, None)]
    subscriber = views.Subscriber.__new__(views.Subscriber)
    subscriber.last_progress = None
    streams: list[model.Stream] = []
    checker = model.ViewChecker()
    assert subscriber._feed(streams, checker, rows)
    assert streams[0].as_of == 100 and not streams[0].late
    assert {t: s.mismatch for t, s in checker.stats.items() if s.mismatch} == {}


def _pg_model() -> Any:
    from materialize.antithesis_driver import cdc

    model = cdc.Model()
    plans = [
        (cdc.Plan(0, "upsert", 1, "a"), cdc.Plan(1, "upsert", 1, "b")),
        (cdc.Plan(0, "update", 1, "c"),),
        (cdc.Plan(1, "delete", 1),),
    ]
    for i, p in enumerate(plans, start=1):
        model.commit(cdc.Txn(i, p, begun=100 * i, acked_at=None))
        model.positions[i] = 100 * i + 50
    return model


def test_cdc_judge_accepts_a_consistent_prefix() -> None:
    from materialize.antithesis_driver import cdc

    model = _pg_model()
    snap = cdc.Snapshot(
        ts=1,
        position=260,
        position_rows=1,
        ledger=[(1, 0), (1, 1), (2, 0)],
        exports=[[(1, "c")], [(1, "b")], []],
    )
    v = cdc.judge(model, snap)
    assert v.prefix == 2 and not v.mismatches and not v.committed_below_missing


def test_cdc_judge_reports_torn_gaps_and_stale_exports() -> None:
    from materialize.antithesis_driver import cdc

    model = _pg_model()
    torn = cdc.judge(model, cdc.Snapshot(1, None, 0, [(1, 0)], [[], [], []]))
    assert torn.torn == [1]
    gap = cdc.judge(model, cdc.Snapshot(1, None, 0, [(2, 0)], [[], [], []]))
    assert gap.gaps == [2]
    # The ledger says two transactions, table 0 still shows the first.
    stale = cdc.judge(
        model,
        cdc.Snapshot(1, 260, 1, [(1, 0), (1, 1), (2, 0)], [[(1, "a")], [(1, "b")], []]),
    )
    assert stale.mismatches[0]["changed"] == [1]
    # The mapped LSN is past the second commit, but the ledger misses it.
    behind = cdc.judge(
        model, cdc.Snapshot(1, 260, 1, [(1, 0), (1, 1)], [[(1, "a")], [(1, "b")], []])
    )
    assert behind.committed_below_missing == [2]


def test_partition_frontier_needs_exactly_one_range_per_partition() -> None:
    from materialize.antithesis_driver import kafka

    rows = [
        ("0", "0", True, True, "5"),
        ("1", "1", True, True, "7"),
        ("1", None, False, False, "0"),
    ]
    assert kafka.partition_frontier(rows, 2) == ({0: 5, 1: 7}, [])
    overlapping = rows + [("0", "1", True, True, "3")]
    assert kafka.partition_frontier(overlapping, 2)[1] == [0, 1]


def test_cdc_judge_places_a_visible_inflight_commit_last() -> None:
    from materialize.antithesis_driver import cdc

    model = _pg_model()
    pending = cdc.Txn(4, (cdc.Plan(2, "upsert", 5, "d"),), 400, None)
    ledger = [(1, 0), (1, 1), (2, 0), (3, 1), (4, 2)]
    exports: list[list[tuple[int, str | None]]] = [[(1, "c")], [], [(5, "d")]]
    v = cdc.judge(model, cdc.Snapshot(1, None, 0, ledger, exports), pending)
    assert not v.unattempted and v.prefix == 4 and not v.mismatches
    # Without the in-flight transaction, the same read shows an unknown one.
    assert cdc.judge(model, cdc.Snapshot(1, None, 0, ledger, exports)).unattempted == [
        4
    ]
    # Not visible yet: the read is a prefix of the known transactions.
    v = cdc.judge(
        model, cdc.Snapshot(1, None, 0, ledger[:-1], exports[:2] + [[]]), pending
    )
    assert v.prefix == 3 and not v.mismatches


def test_mysql_gtid_max_reads_only_the_servers_uuid() -> None:
    from materialize.antithesis_driver import mysql

    own = "3E11FA47-71CA-11E1-9E33-C80AA9429562"
    other = "a0000000-0000-0000-0000-000000000000"
    gtids = f"{other}:1-900,\n{own.lower()}:1-5:7-12"
    assert mysql.gtid_max(gtids, own) == 12
    assert mysql.gtid_max(f"{other}:1-3", own) == 0


def test_webhook_judge_requires_rows_acknowledged_before_a_visible_send() -> None:
    from materialize.antithesis_driver import webhook

    requests_ = {
        1: webhook.Request(1, "src", "a", sent_at=1.0, acked_at=2.0),
        2: webhook.Request(2, "src", "b", sent_at=3.0, acked_at=4.0),
        3: webhook.Request(3, "src", "c", sent_at=3.5, acked_at=None),
        4: webhook.Request(4, "tbl", "d", sent_at=5.0, acked_at=6.0),
    }
    ok = webhook.judge(requests_, [("src", "1", "a"), ("src", "2", "b")])
    assert not ok.out_of_order and ok.ordered_pairs == 1
    # Row 2 was sent after row 1 was acknowledged: showing 2 without 1 is wrong.
    gap = webhook.judge(requests_, [("src", "2", "b")])
    assert gap.out_of_order == [1]
    # Row 3 has no acknowledgement, so a read may show or omit it.
    assert not webhook.judge(requests_, [("src", "1", "a")]).out_of_order
    odd = webhook.judge(
        requests_,
        [("src", "1", "a"), ("src", "1", "a"), ("tbl", "1", "a"), ("src", "3", "x")],
    )
    assert odd.duplicates == [("src", "1")]
    assert odd.phantoms == [("tbl", "1")]
    assert odd.altered == [("src", 3)]
    assert odd.unknown_visible == 1
