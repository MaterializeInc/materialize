# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import runpy
import sqlite3
import threading
import time
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest

pytest.importorskip("antithesis")

from kubernetes.client.rest import ApiException  # type: ignore

from materialize.antithesis import ivm_model, telemetry, telemetry_coverage, templates
from materialize.antithesis import serializability as sz
from materialize.antithesis.drivers import history, ivm, pod_restarts


@pytest.mark.parametrize(
    "checked,correct", [(True, True), (False, True), (True, False)]
)
def test_only_completed_correct_history_comparisons_credit_interleavings(
    monkeypatch: pytest.MonkeyPatch, checked: bool, correct: bool
) -> None:
    operations = [
        sz.Op(1, sz.Kind.APPEND, sz.Outcome.OK, 1, 4, write_key=0),
        sz.Op(
            2,
            sz.Kind.READ,
            sz.Outcome.OK,
            5,
            6,
            observations=(sz.Observation(0, "table", (1,)),),
        ),
    ]
    monkeypatch.setattr(telemetry, "enabled", lambda: True)
    monkeypatch.setattr(
        telemetry,
        "events_between",
        lambda *args: [
            SimpleNamespace(
                kind="phase", started=2, completed=2, phase="Promoting", role=""
            )
        ],
    )
    comparison = Mock()
    record = Mock()
    monkeypatch.setattr(telemetry, "comparison", comparison)
    monkeypatch.setattr(telemetry, "record_coverage", record)
    report = sz.Report() if correct else sz.Report(lost_writes=[{"write": 1}])
    telemetry_coverage.history_checked(operations, report, {2} if checked else set())
    assert comparison.call_args.args[2] is (correct if checked else None)
    if checked and correct:
        assert record.call_args.args[0] == {
            ("Promoting", "", "append", "table", "acked")
        }
    else:
        record.assert_not_called()


@pytest.mark.parametrize(
    "error",
    [
        None,
        ApiException(status=404),
        ApiException(status=409),
        ApiException(status=500),
    ],
)
def test_only_acknowledged_pod_deletes_emit_exposure_evidence(
    monkeypatch: pytest.MonkeyPatch, error: ApiException | None
) -> None:
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(pod_restarts, "open_db", lambda *args: db)
    clock = Mock(side_effect=[0.0, 1.0, 2.0])
    monkeypatch.setattr(pod_restarts.time, "monotonic", clock)
    observed = Mock()
    monkeypatch.setattr(telemetry, "fault_observed", observed)
    monkeypatch.setattr(pod_restarts.recovery, "preserve_pod_exit", Mock())
    kube = Mock()
    kube.core.delete_namespaced_pod.side_effect = error
    acked = pod_restarts.delete_recorded(
        kube,
        origin="test",
        role="environmentd_active",
        namespace="n",
        pod="p",
        uid="u",
        grace=1,
        reason="test",
    )
    assert acked is (error is None)
    if acked:
        observed.assert_called_once_with(
            "environmentd", 1.0, 2.0, key="pod_restarts:1", cluster_id=""
        )
    else:
        observed.assert_not_called()


@pytest.mark.parametrize(
    "before_active,after_active,victim_generation,uid,expected",
    [
        (1, 1, 1, "uid", "environmentd_active"),
        (1, 1, 2, "uid", "environmentd_candidate"),
        (1, 2, 2, "uid", "environmentd"),
        (1, 1, None, "uid", "environmentd"),
        (1, 1, 1, None, "environmentd"),
    ],
)
def test_environmentd_generation_witness_requires_stable_role_and_uid(
    monkeypatch: pytest.MonkeyPatch,
    before_active,
    after_active,
    victim_generation,
    uid,
    expected,
) -> None:
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(pod_restarts, "open_db", lambda *args: db)
    monkeypatch.setattr(pod_restarts.recovery, "preserve_pod_exit", Mock())
    observed = Mock()
    monkeypatch.setattr(telemetry, "fault_observed", observed)
    kube = Mock()
    kube.try_snapshot.side_effect = [
        pod_restarts.Snapshot(
            {"status": {"activeGeneration": active, "resourceId": "environment"}}
        )
        for active in [before_active, after_active]
    ]
    arguments = (
        {} if victim_generation is None else {"victim_generation": victim_generation}
    )
    assert pod_restarts.delete_recorded(
        kube,
        origin="test",
        role="environmentd_active",
        namespace="mz",
        pod="pod",
        uid=uid,
        grace=0,
        reason="test",
        **arguments,
    )
    assert observed.call_args.args[0] == expected


def test_autocommit_write_is_not_held_before_its_actual_sql_future() -> None:
    dispatched, finished = threading.Event(), threading.Event()

    def write(on_invoke) -> None:
        on_invoke()
        dispatched.set()
        assert finished.wait(1)

    def disrupt() -> None:
        assert dispatched.wait(1)
        finished.set()

    assert history._overlap_write(
        write,
        disrupt,
        race_commit=False,
        hold_transaction=False,
        deadline=time.monotonic() + 2,
    )


def test_history_disruption_runs_inside_the_unresolved_real_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(history.state, "open_db", lambda *args: db)
    history.open_state()
    calls = []
    conn = Mock()
    conn.execute.side_effect = lambda query: calls.append(query.decode())

    @contextmanager
    def transaction():
        calls.append("BEGIN")
        yield
        calls.append("COMMIT")

    conn.transaction.side_effect = transaction
    session = Mock(spec=history.Session)
    session.ensure.return_value = conn
    session.name = "s"
    session.isolation = history.STRICT
    session.target = history.Target("host", "service")

    def disrupt() -> None:
        assert calls[0] == "BEGIN"
        assert calls[-1].startswith("INSERT INTO")
        assert db.execute("SELECT outcome, complete_rt FROM ops").fetchone() == (
            None,
            None,
        )
        calls.append("DELETE ACK")

    assert not history._write(db, session, 1, 0, None, before_commit=disrupt)
    assert calls[-2:] == ["DELETE ACK", "COMMIT"]
    assert db.execute("SELECT outcome, complete_rt FROM ops").fetchone()[0] == "ok"
    db.close()


def test_rmw_interleaving_keeps_one_atomic_autocommit_statement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(history.state, "open_db", lambda *args: db)
    history.open_state()
    conn, session, invoked = Mock(), Mock(spec=history.Session), Mock()
    session.ensure.return_value = conn
    session.name, session.isolation = "s", history.STRICT
    session.target = history.Target("host", "service")
    assert not history._write(db, session, 1, 0, 1, on_invoke=invoked)
    invoked.assert_called_once_with()
    conn.transaction.assert_not_called()
    assert conn.execute.call_count == 1
    assert b"INSERT INTO" in conn.execute.call_args.args[0]
    assert b"SELECT" in conn.execute.call_args.args[0]
    assert db.execute("SELECT outcome FROM ops").fetchone() == ("ok",)
    with pytest.raises(ValueError, match="must be an append"):
        history._write(db, session, 1, 0, 1, before_commit=Mock())
    assert conn.execute.call_count == 1
    db.close()


@pytest.mark.parametrize("measured", [False, True])
def test_history_epoch_checks_remain_offline(
    monkeypatch: pytest.MonkeyPatch,
    measured: bool,
) -> None:
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(history.state, "open_db", lambda *args: db)
    history.open_state()
    environment = Mock(
        side_effect=AssertionError("offline checker consulted environment")
    )
    monkeypatch.setattr(history, "Environment", environment)
    monkeypatch.setattr(telemetry, "enabled", lambda: measured)
    assert history._check_epoch(db, 1, "offline") == []
    environment.assert_not_called()
    db.close()


@pytest.mark.parametrize("race_commit", [False, True])
def test_staged_write_can_hold_or_race_its_commit_with_a_disruption(
    race_commit: bool,
) -> None:
    committed = threading.Event()
    calls = []

    def write(before_commit) -> None:
        calls.append("INSERT")
        before_commit()
        committed.set()

    def disrupt() -> None:
        assert calls == ["INSERT"]
        if race_commit:
            assert committed.wait(1)
        else:
            assert not committed.is_set()
        calls.append("DELETE ACK")

    assert history._overlap_write(
        write,
        disrupt,
        race_commit=race_commit,
        deadline=time.monotonic() + 2,
    )
    assert committed.is_set()
    assert calls == ["INSERT", "DELETE ACK"]


def test_staged_write_is_released_when_disruption_fails() -> None:
    committed = threading.Event()

    def write(before_commit) -> None:
        before_commit()
        committed.set()

    def disrupt() -> None:
        raise RuntimeError("disruption failure")

    with pytest.raises(RuntimeError, match="disruption failure"):
        history._overlap_write(
            write,
            disrupt,
            race_commit=False,
            deadline=time.monotonic() + 2,
        )
    assert committed.is_set()


def test_no_write_is_started_after_the_interleaving_deadline() -> None:
    write, disrupt = Mock(), Mock()
    assert not history._overlap_write(
        write,
        disrupt,
        race_commit=False,
        deadline=time.monotonic() - 1,
    )
    write.assert_not_called()
    disrupt.assert_not_called()


def test_replica_interleavings_select_only_the_oracle_cluster(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def pod(name, cluster_id):
        return SimpleNamespace(
            metadata=SimpleNamespace(
                name=name,
                uid=name,
                deletion_timestamp=None,
                labels={"test/cluster-id": cluster_id},
            )
        )

    kube = Mock()
    kube.namespace, kube.operator_namespace = "mz", "ops"
    kube.core.list_namespaced_pod.return_value.items = [
        pod("unrelated", "u9"),
        pod("owned", "u3"),
    ]
    monkeypatch.setattr(pod_restarts.rng, "choice", lambda values: values[0])
    victim = pod_restarts.pick_victim(kube, "clusterd", cluster_id="u3")
    assert victim is not None
    assert (victim.pod, victim.cluster_id) == ("owned", "u3")


def test_interleaving_promotion_revalidates_the_observed_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pod_restarts.templates, "enabled_driver", lambda *args: True)
    monkeypatch.setattr(pod_restarts, "quiet_period_active", lambda *args: False)
    monkeypatch.setattr(pod_restarts.rng, "random", lambda: 0.0)
    initial = pod_restarts.Snapshot(
        {
            "metadata": {"generation": 1},
            "spec": {"requestRollout": "one"},
            "status": {
                "conditions": [
                    {
                        "type": "UpToDate",
                        "reason": "ReadyToPromote",
                        "observedGeneration": 1,
                    }
                ]
            },
        }
    )
    later = pod_restarts.Snapshot(
        {
            "metadata": {"generation": 2},
            "spec": {"requestRollout": "two"},
            "status": {
                "conditions": [
                    {
                        "type": "UpToDate",
                        "reason": "ReadyToPromote",
                        "observedGeneration": 2,
                    }
                ]
            },
        }
    )
    kube = Mock()
    kube.try_snapshot.return_value = initial
    kube.snapshot.return_value = later
    action = pod_restarts.interleaving_disruption(
        kube,
        cluster_id="u3",
        deadline=time.monotonic() + 2,
    )
    assert action is not None
    action()
    kube.patch_spec.assert_not_called()


@pytest.mark.parametrize(
    "profile,triggers,candidate",
    [
        ("main", set(pod_restarts.TRIGGERS), True),
        ("transactions", {"now"}, False),
        ("cdc", {"now", "postgres_terminal", "source_created"}, False),
        (
            "rollouts",
            {
                "now",
                "postgres_terminal",
                "source_created",
                "source_created_applying",
                "rollout_applying",
                "rollout_promoting",
            },
            True,
        ),
    ],
)
def test_focused_faults_do_not_wait_for_unscheduled_workloads(
    monkeypatch: pytest.MonkeyPatch, profile: str, triggers: set[str], candidate: bool
) -> None:
    monkeypatch.setenv(templates.PROFILE_ENV, profile)
    monkeypatch.setattr(pod_restarts.rng, "choice", lambda values: 1)
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE params (key TEXT PRIMARY KEY, value TEXT)")
    params = pod_restarts.timeline_params(db)
    assert set(params.trigger_weights) == triggers
    assert ("environmentd_candidate" in params.target_weights) is candidate


def test_generated_command_records_real_measurements(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = {
        "ENVIRONMENT": "test",
        "KAFKA_BROKER": "unused:9092",
        "SCHEMA_REGISTRY_URL": "http://unused:8081",
        "UPSTREAM_POSTGRES_URL": "postgres://unused/db",
        "METADATA_POSTGRES_URL": "postgres://unused/db",
        "STATE_DIR": str(tmp_path / "state"),
    }
    for key, value in settings.items():
        monkeypatch.setenv(f"MZ_ANTITHESIS_{key}", value)
    db = telemetry.state.open_db(telemetry.exit_audit.STATE_DB)
    try:
        telemetry.exit_audit.initialize(db)
        with db:
            db.execute(
                "INSERT INTO exit_observer VALUES (1, ?, '{}')",
                (telemetry.time.monotonic(),),
            )
    finally:
        db.close()
    source = tmp_path / "sources"
    source.mkdir()
    first = source / "first_configure.py"
    first.write_text("pass\n")
    first.chmod(0o755)
    check = source / "anytime_comparison.py"
    check.write_text(
        "from materialize.antithesis import telemetry\n"
        "telemetry.comparison('history', 'table', True)\n"
    )
    check.chmod(0o755)
    output = tmp_path / "generated"
    templates.generate(source, output)
    with pytest.raises(SystemExit) as exited:
        runpy.run_path(str(output / "transactions" / check.name), run_name="__main__")
    assert exited.value.code == 0
    result = telemetry.summary()
    assert result["active_commands"] == 0
    assert result["commands"][0]["profile"] == "transactions"
    assert result["commands"][0]["completed"] == 1
    assert result["commands"][0]["no_comparison"] == 0
    assert result["comparisons"][0]["outcome"] == "passed"
    assert result["comparisons"][0]["units"] == 1


def test_subscribe_coverage_waits_for_closed_correct_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(telemetry, "enabled", lambda: True)
    monkeypatch.setattr(ivm.time, "monotonic", lambda: 6.0)
    window = ivm.Window(Mock(), "host", Mock(), 1, 0, None)
    window.stream = ivm_model.Stream(10)
    window.fetch_started = 5.0
    base = {name: Counter() for name in ivm_model.TABLE_NAMES}
    base["t"][(1, 0, 5, 1)] = 1
    rows = [(10, False, 1, "t", *ivm_model.pad((1, 0, 5, 1)))]
    for view in ivm_model.VIEWS:
        for variant in ivm_model.VARIANTS:
            for row, n in ivm_model.reference(view.kind, base, 10).items():
                rows.append((10, False, n, ivm_model.tag(view.kind, variant), *row))
    window.feed(rows)
    assert not window.closed_ids
    window.feed([(11, True, None, None, None, None, None, None, None)])
    assert window.checker.first_mismatch_time is None
    assert window.closed_ids == {1: (5.0, 6.0)}
    window.feed([(12, False, 1, "t", *ivm_model.pad((2, 0, 5, 2)))])
    assert 2 in window.seen_ids
    assert 2 not in window.closed_ids


@pytest.fixture
def ivm_write(monkeypatch: pytest.MonkeyPatch):
    db = sqlite3.connect(":memory:")
    db.execute(
        "CREATE TABLE writes (op_id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT, "
        "invoke_rt REAL, complete_rt REAL, outcome TEXT, commit_ts INTEGER)"
    )
    clock = SimpleNamespace(now=1.0, commit_error=False)
    events = []
    conn = Mock(closed=False)

    @contextmanager
    def transaction():
        events.append("begin")
        try:
            yield
        except Exception:
            clock.now = 4.0
            events.append("rollback")
            raise
        else:
            assert db.execute("SELECT outcome FROM writes").fetchone() == ("pending",)
            events.append("commit")
            clock.now = 4.0
            if clock.commit_error:
                raise psycopg.OperationalError("lost commit response")

    def execute(*args, **kwargs):
        assert db.execute("SELECT complete_rt FROM writes").fetchone() == (None,)
        clock.now = 2.0
        events.append("insert")

    conn.transaction.side_effect = transaction
    conn.execute.side_effect = execute
    writer = ivm.Writer(db, conn, {"t": 0, "d": 0}, 0)
    monkeypatch.setattr(ivm.time, "monotonic", lambda: clock.now)
    monkeypatch.setattr(writer, "pick", lambda: "t_insert")

    def disrupt():
        assert events == ["begin", "insert"]
        assert db.execute("SELECT outcome, complete_rt FROM writes").fetchone() == (
            "pending",
            None,
        )
        clock.now = 3.0
        events.append("disrupt")

    yield SimpleNamespace(
        db=db, conn=conn, clock=clock, events=events, writer=writer, disrupt=disrupt
    )
    db.close()


def test_ivm_disruption_occurs_before_commit_resolves_write(ivm_write):
    assert ivm_write.writer.write(before_commit=ivm_write.disrupt, kind="t_insert")
    assert ivm_write.events == ["begin", "insert", "disrupt", "commit"]
    assert ivm_write.db.execute(
        "SELECT outcome, invoke_rt, complete_rt FROM writes"
    ).fetchone() == ("ok", 1.0, 4.0)


def test_ivm_lost_commit_after_disruption_remains_indeterminate(ivm_write):
    ivm_write.clock.commit_error = True
    assert not ivm_write.writer.write(before_commit=ivm_write.disrupt, kind="t_insert")
    assert ivm_write.events == ["begin", "insert", "disrupt", "commit"]
    assert ivm_write.db.execute(
        "SELECT outcome, complete_rt FROM writes"
    ).fetchone() == ("indeterminate", 4.0)


@pytest.mark.parametrize("lost_commit", [False, True])
def test_ivm_protocol_diagnostics_report_the_actual_commit_outcome(
    ivm_write,
    monkeypatch: pytest.MonkeyPatch,
    lost_commit,
):
    from materialize.antithesis import observations

    monkeypatch.setattr(
        telemetry, "_recorder", telemetry.Recorder("main", "ivm_interleavings")
    )
    emitted = Mock()
    monkeypatch.setattr(observations, "send_event", emitted)
    ivm_write.clock.commit_error = lost_commit
    ivm_write.writer.write(before_commit=ivm_write.disrupt, kind="t_insert")
    events = [
        call.args[1]
        for call in emitted.call_args_list
        if call.args[0] == "antithesis_interleaving"
    ]
    assert [event["stage"] for event in events] == ["start", "write", "end"]
    assert len({event["round"] for event in events}) == 1
    assert events[0]["mode"] == "held_ivm"
    assert events[1]["write_id"] == 1
    assert events[1]["outcome"] == ("indeterminate" if lost_commit else "ok")


def test_ivm_regular_writes_keep_the_autocommit_path(ivm_write):
    assert ivm_write.writer.write()
    ivm_write.conn.transaction.assert_not_called()
    assert ivm_write.events == ["insert"]


def test_ivm_disruption_callback_failure_is_harness_failure(
    ivm_write, monkeypatch: pytest.MonkeyPatch
):
    from materialize.antithesis import observations

    health = Mock()
    violation = Mock()
    monkeypatch.setattr(observations, "harness_error", health)
    monkeypatch.setattr(ivm, "unreachable", violation)

    def broken_callback():
        raise ValueError("broken harness callback")

    assert not ivm_write.writer.write(before_commit=broken_callback, kind="t_insert")
    assert ivm_write.events == ["begin", "insert", "rollback"]
    assert ivm_write.db.execute("SELECT outcome FROM writes").fetchone() == (
        "rejected",
    )
    assert health.call_count == 1
    violation.assert_not_called()


@pytest.mark.parametrize("lost_commit", [False, True])
def test_ivm_interleaving_checks_the_same_write_after_disruption(
    ivm_write, monkeypatch: pytest.MonkeyPatch, lost_commit
):
    ivm_write.clock.commit_error = lost_commit
    env = Mock()
    env.sql_host.return_value = "host"
    monkeypatch.setattr(ivm, "Environment", lambda: env)
    monkeypatch.setattr(ivm, "open_state", lambda: ivm_write.db)
    monkeypatch.setattr(ivm, "ensure_objects", lambda *args: True)
    monkeypatch.setattr(ivm.history, "start_watchdog", lambda *args: None)
    monkeypatch.setattr(ivm, "_cluster_id", lambda *args: "u-ivm")

    @contextmanager
    def connect(*args, **kwargs):
        yield ivm_write.conn

    monkeypatch.setattr(ivm, "_connect", connect)
    monkeypatch.setattr(
        ivm.Writer, "refresh", lambda self: self.counts.update(t=0, d=0)
    )
    selection = Mock(return_value=ivm_write.disrupt)
    monkeypatch.setattr(pod_restarts, "interleaving_disruption", selection)
    observed = []

    def check(env_arg, host, db):
        assert env_arg is env and host == "host" and db is ivm_write.db
        assert ivm_write.events == ["begin", "insert", "disrupt", "commit"]
        observed.extend(db.execute("SELECT op_id, outcome FROM writes"))

    monkeypatch.setattr(ivm, "_check_windows", check, raising=False)

    assert ivm.interleaving_main() == 0

    assert observed == [(1, "indeterminate" if lost_commit else "ok")]
    assert selection.call_args.kwargs["cluster_id"] == "u-ivm"


def test_ivm_interleaving_skips_writes_when_no_disruption_is_available(
    ivm_write, monkeypatch: pytest.MonkeyPatch
):
    env = Mock()
    env.sql_host.return_value = "host"
    monkeypatch.setattr(ivm, "Environment", lambda: env)
    monkeypatch.setattr(ivm, "open_state", lambda: ivm_write.db)
    monkeypatch.setattr(ivm, "ensure_objects", lambda *args: True)
    monkeypatch.setattr(ivm.history, "start_watchdog", lambda *args: None)
    monkeypatch.setattr(ivm, "_cluster_id", lambda *args: "u-ivm")

    @contextmanager
    def connect(*args, **kwargs):
        yield ivm_write.conn

    monkeypatch.setattr(ivm, "_connect", connect)
    monkeypatch.setattr(
        pod_restarts, "interleaving_disruption", Mock(return_value=None)
    )
    checker = Mock()
    monkeypatch.setattr(ivm, "_check_windows", checker, raising=False)

    assert ivm.interleaving_main() == 0
    assert not ivm_write.events
    checker.assert_not_called()
