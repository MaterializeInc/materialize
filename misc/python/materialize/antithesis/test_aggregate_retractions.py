# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import random
import sqlite3
import threading
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import aggregate_retractions_model as model
from materialize.antithesis.drivers import aggregate_retractions as driver
from materialize.antithesis.drivers import recovery


def assert_product(condition, message, details):
    assert condition, (message, details)


def test_exact_components_do_not_carry_days_or_months_and_preserve_null_counts():
    expected = model.evaluate(
        [(0, (1, 40, 1)), (0, (1, 40, 1)), (0, (-1, -20, -1)), (0, None), (1, None)]
    )
    assert expected.rows == ((0, 1, 60, 1, 3, 4), (1, None, None, None, 0, 1))
    assert not expected.overflows


@pytest.mark.parametrize("axis,bits", list(enumerate(model.BITS)))
@pytest.mark.parametrize("sign", [-1, 1])
def test_overflow_can_be_repaired_by_exact_retraction(axis, bits, sign):
    components = [0, 0, 0]
    components[axis] = sign * ((1 << (bits - 1)) - 1)
    value = (components[0], components[1], components[2])
    assert model.evaluate([(0, value), (0, value)]).overflows == (
        (0, model.AXES[axis]),
    )
    assert model.evaluate([(0, value)]).rows == ((0, *components, 1, 1),)
    assert not model.evaluate(
        [(0, value), (0, (-value[0], -value[1], -value[2]))]
    ).overflows


def test_generated_plans_cover_all_components_with_bounded_representable_inputs():
    axes = set()
    for seed in range(100):
        plan = model.generate(random.Random(seed))
        axes.add(plan.axis)
        assert len(plan.baseline) <= 33
        assert not model.evaluate(plan.baseline).overflows
        assert model.evaluate((*plan.baseline, *plan.poison)).overflows == (
            (0, plan.axis),
        )
        assert not model.evaluate((*plan.baseline, plan.extra)).overflows
        for _, value in (*plan.baseline, *plan.poison, plan.extra):
            if value is not None:
                assert all(
                    -(1 << (bits - 1)) <= v < (1 << (bits - 1))
                    for bits, v in zip(model.BITS, value)
                )
    assert axes == set(model.AXES)


@pytest.mark.parametrize(
    "actual",
    [
        [],
        [(0, 0, 0, 0, 2, 1)],
        [(0, -1, 0, 0, 1, 2)],
        [(0, 0, 0, 0, 1, 2), (1, None, None, None, 0, 1)],
    ],
)
def test_independent_oracle_rejects_wrong_count_sum_and_group_mutants(
    monkeypatch, actual
):
    monkeypatch.setattr(driver, "always", assert_product)
    expected = model.evaluate([(0, (0, 0, 0)), (0, None)])
    with pytest.raises(AssertionError):
        driver.check_rows(actual, expected, "mv")


def test_grouped_overflow_must_not_silently_wrap_or_return_null(monkeypatch):
    monkeypatch.setattr(driver, "always", assert_product)
    expected = model.evaluate([(0, (2_000_000_000, 0, 0))] * 2)
    conn = Mock()
    conn.execute.return_value.fetchall.return_value = [(0, None, None, None, 2, 2)]
    with pytest.raises(AssertionError):
        driver._peek(conn, "query", expected, "mv")


def test_recorded_wrong_result_stops_the_round_when_sdk_assertion_returns(monkeypatch):
    verdict = Mock()
    monkeypatch.setattr(driver, "always", verdict)
    monkeypatch.setattr(driver.telemetry, "comparison", Mock())
    with pytest.raises(driver.Interrupted):
        driver.check_rows([], model.evaluate([(0, (1, 0, 0))]), "mv")
    assert verdict.call_args.args[0] is False


def test_overflow_is_authorized_only_by_model_and_exact_sql_context():
    healthy = model.evaluate([(0, (1, 0, 0))])
    dirty = model.evaluate([(0, (2_000_000_000, 0, 0))] * 2)
    overflow = psycopg.errors.IntervalFieldOverflow(
        "Evaluation error: interval out of range"
    )
    internal = psycopg.errors.InternalError_("Evaluation error: interval out of range")
    unrelated = psycopg.errors.InternalError_("unexpected worker failure")
    assert driver.expected_overflow(overflow, dirty, subscribe=False)
    assert driver.expected_overflow(internal, dirty, subscribe=True)
    assert not driver.expected_overflow(overflow, healthy, subscribe=False)
    assert not driver.expected_overflow(internal, dirty, subscribe=False)
    assert not driver.expected_overflow(unrelated, dirty, subscribe=True)


def test_fresh_healthy_read_cannot_ignore_a_stale_overflow_error(monkeypatch):
    violation = Mock(side_effect=AssertionError("unexpected stale error"))
    monkeypatch.setattr(driver, "unreachable", violation)
    conn = Mock()
    conn.execute.side_effect = psycopg.errors.IntervalFieldOverflow(
        "interval out of range"
    )
    with pytest.raises(AssertionError, match="unexpected stale error"):
        driver._peek(conn, "query", model.evaluate([(0, (1, 0, 0))]), "mv")
    assert violation.call_count == 1


def test_a_lagging_stream_cannot_skip_the_repaired_poison(monkeypatch):
    monkeypatch.setattr(driver, "always", assert_product)
    plan = model.generate(random.Random(3))
    dirty = model.evaluate((*plan.baseline, *plan.poison))
    monkeypatch.setattr(driver, "unreachable", Mock())
    progress = (10, True, None)
    conn = Mock()
    conn.execute.return_value.fetchall.side_effect = lambda: [progress]
    with pytest.raises(driver.Interrupted):
        driver._lagging_stream_reports_poison(conn, dirty, time.monotonic() + 0.05)
    conn.execute.return_value.fetchall.side_effect = [
        [progress],
        [(11, False, 1, 0, 1, 2, 3, 1, 1)],
    ]
    with pytest.raises(AssertionError):
        driver._lagging_stream_reports_poison(conn, dirty, time.monotonic() + 30)
    overflow = psycopg.errors.InternalError_("interval out of range")
    conn.execute.return_value.fetchall.side_effect = [[progress], overflow]
    driver._lagging_stream_reports_poison(conn, dirty, time.monotonic() + 30)
    aborted = psycopg.errors.InFailedSqlTransaction("transaction aborted")
    conn.execute.return_value.fetchall.side_effect = [aborted]
    with pytest.raises(driver.Interrupted):
        driver._lagging_stream_reports_poison(conn, dirty, time.monotonic() + 30)


@pytest.mark.parametrize("diff", [0, -1, 1_000_000_000_000])
def test_closed_snapshot_rejects_missing_negative_or_excess_multiplicity_without_expanding(
    monkeypatch, diff
):
    monkeypatch.setattr(driver, "always", assert_product)
    conn = Mock()
    conn.execute.return_value.fetchall.return_value = [
        (10, False, diff, 0, 1, 0, 0, 1, 1),
        (11, True, None),
    ]
    with pytest.raises(AssertionError):
        driver._snapshot(
            conn, model.evaluate([(0, (1, 0, 0))]), time.monotonic() + 1, 10
        )


def test_empty_snapshot_closes_at_progress_and_is_checked(monkeypatch):
    monkeypatch.setattr(driver, "always", assert_product)
    conn = Mock()
    conn.execute.return_value.fetchall.side_effect = lambda: [
        (10 if conn.execute.call_count == 1 else 11, True, None)
    ]
    with pytest.raises(AssertionError):
        driver._snapshot(
            conn, model.evaluate([(0, (1, 0, 0))]), time.monotonic() + 1, 10
        )
    assert conn.execute.call_count == 2


def test_only_matching_uid_acknowledged_replica_delete_can_guide_repair(monkeypatch):
    monkeypatch.setattr(driver.quiet, "active_quiet_period", lambda: False)
    monkeypatch.setattr(driver.rng, "random", lambda: 0)
    monkeypatch.setattr(driver.telemetry, "resolve_cluster_id", lambda *args: "u2")
    monkeypatch.setattr(driver, "Kube", lambda env: Mock())
    victim = SimpleNamespace(
        role="clusterd", namespace="ns", pod="p", uid="uid", cluster_id="foreign"
    )
    monkeypatch.setattr(
        driver.pod_restarts, "pick_victim", lambda *args, **kwargs: victim
    )
    delete = Mock(return_value=True)
    monkeypatch.setattr(driver.pod_restarts, "delete_recorded", delete)
    assert driver._fault(Mock(), "host", time.monotonic() + 1) is None
    delete.assert_not_called()
    victim.cluster_id = "u2"
    evidence = driver._fault(Mock(), "host", time.monotonic() + 1)
    assert evidence == {"acked": True, "cluster_id": "u2", "pod_uid": "uid"}
    assert delete.call_args.kwargs["uid"] == "uid"
    delete.return_value = False
    evidence = driver._fault(Mock(), "host", time.monotonic() + 1)
    assert evidence is not None and evidence["acked"] is False


def test_slot_reclamation_is_bounded_and_old_owner_cannot_release_a_new_epoch(
    monkeypatch,
):
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(driver.lifecycle, "pid_alive", lambda pid: True)
    first, second = driver.claim(db), driver.claim(db)
    assert first is not None and second is not None and first.number != second.number
    assert driver.claim(db) is None
    driver.release(db, first)
    third = driver.claim(db)
    assert (
        third is not None
        and third.number == first.number
        and third.epoch > second.epoch
    )
    driver.release(db, first)
    assert db.execute(
        "SELECT epoch FROM slots WHERE slot = ?", (third.number,)
    ).fetchone() == (third.epoch,)
    db.close()


@pytest.fixture
def poisoned_fixture(monkeypatch):
    db = sqlite3.connect(":memory:")
    slot = driver.Slot(0, 42)
    name = "v0_e42"
    objects = {name: "u42", "unrelated_error": "u99"}
    dropped = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, query, params: Any = None):
            query = query.decode() if isinstance(query, bytes) else query
            if query.startswith("SELECT v.id"):
                identity = objects.get(params[1])
                return Mock(fetchone=lambda: (identity,) if identity else None)
            if query.startswith("DROP MATERIALIZED VIEW"):
                target = query.split()[-1].split(".")[-1]
                dropped.append(target)
                objects.pop(target, None)
            return Mock()

    conn: Any = Connection()
    monkeypatch.setattr(driver, "_connect", lambda host: conn)
    monkeypatch.setattr(driver.ddl_hold, "active", lambda: False)
    monkeypatch.setattr(driver, "_process_marker", lambda pid: "original-owner")
    driver._arm_poison(db, conn, slot)
    monkeypatch.setattr(driver, "_process_marker", lambda pid: None)
    yield db, slot, conn, objects, dropped
    db.close()


def test_interrupted_poison_cleanup_removes_only_its_exact_fixture_before_health(
    monkeypatch, poisoned_fixture
):
    db, slot, conn, objects, dropped = poisoned_fixture
    check = recovery.Recovery.__new__(recovery.Recovery)
    check.host = "host"
    check.exempt = set()

    def rows(host, query, *args, **kwargs):
        if query.startswith("SELECT d.name"):
            return [
                ("materialize", driver.SCHEMA, name, "materialized-view", 1)
                for name in objects
            ]
        if "unrelated_error" in query:
            raise psycopg.errors.InternalError_("real unrelated product error")
        raise psycopg.errors.IntervalFieldOverflow("interval out of range")

    monkeypatch.setattr(recovery, "rows", rows)
    before, info = check.collections_readable()
    assert not before and len(info["unreadable"]) == 2
    complete, _ = driver.cleanup_abandoned("host", db)
    assert complete
    assert dropped == ["v0_e42"]
    after, info = check.collections_readable()
    assert not after
    assert [failure[0] for failure in info["unreadable"]] == [
        "materialize.aggregate_retractions.unrelated_error"
    ]
    assert "unrelated_error" in objects


def test_a_live_poison_owner_is_not_cleaned(monkeypatch, poisoned_fixture):
    db, _, _, objects, dropped = poisoned_fixture
    monkeypatch.setattr(driver, "_process_marker", lambda pid: "original-owner")
    complete, _ = driver.cleanup_abandoned("host", db)
    assert not complete
    assert not dropped
    assert "v0_e42" in objects
    assert db.execute("SELECT count(*) FROM poison_intents").fetchone() == (1,)


def test_same_name_replacement_is_left_to_ordinary_product_oracles(poisoned_fixture):
    db, _, _, objects, dropped = poisoned_fixture
    objects["v0_e42"] = "new-incarnation"
    complete, details = driver.cleanup_abandoned("host", db)
    assert complete and details["replaced"] == 1
    assert objects["v0_e42"] == "new-incarnation"
    assert not dropped


def test_pid_reuse_does_not_keep_an_abandoned_fixture_live(
    monkeypatch, poisoned_fixture
):
    db, _, _, _, dropped = poisoned_fixture
    monkeypatch.setattr(driver, "_process_marker", lambda pid: "new-process-start")
    assert driver.cleanup_abandoned("host", db)[0]
    assert dropped == ["v0_e42"]


def test_unknown_drop_response_retains_poison_journal_for_retry(
    monkeypatch, poisoned_fixture
):
    db, _, conn, _, dropped = poisoned_fixture
    execute = conn.execute

    def lose_drop_response(query, params=None):
        text = query.decode() if isinstance(query, bytes) else query
        if text.startswith("DROP MATERIALIZED VIEW"):
            raise psycopg.OperationalError("DROP response lost")
        return execute(query, params)

    monkeypatch.setattr(conn, "execute", lose_drop_response)
    assert not driver.cleanup_abandoned("host", db)[0]
    assert db.execute("SELECT count(*) FROM poison_intents").fetchone() == (1,)
    monkeypatch.setattr(conn, "execute", execute)
    assert driver.cleanup_abandoned("host", db)[0]
    assert dropped == ["v0_e42"]


def test_lost_poison_insert_response_keeps_durable_intent(
    monkeypatch, poisoned_fixture
):
    db, slot, _, _, _ = poisoned_fixture
    db.execute("DELETE FROM poison_intents")
    db.commit()
    monkeypatch.setattr(driver, "_process_marker", lambda pid: "current-owner")
    monkeypatch.setattr(driver, "_prepare", Mock())
    monkeypatch.setattr(driver, "_peek", Mock())
    monkeypatch.setattr(driver, "_begin_stream", lambda *args: 10)
    monkeypatch.setattr(driver, "_snapshot", Mock())

    def lose_poison_response(conn, current_slot, batch, values):
        if batch == 1:
            assert db.execute(
                "SELECT view_id, phase FROM poison_intents WHERE epoch=?", (slot.epoch,)
            ).fetchone() == ("u42", "armed")
            raise psycopg.OperationalError("poison may have committed")

    monkeypatch.setattr(driver, "_insert", lose_poison_response)
    with pytest.raises(psycopg.OperationalError):
        driver.run_round(
            "host", slot, model.generate(random.Random(3)), time.monotonic() + 30, db=db
        )
    assert db.execute("SELECT count(*) FROM poison_intents").fetchone() == (1,)


def test_verified_repair_retires_intent_only_after_fresh_stream_check(
    monkeypatch, poisoned_fixture
):
    db, slot, _, _, _ = poisoned_fixture
    db.execute("DELETE FROM poison_intents")
    db.commit()
    monkeypatch.setattr(driver, "_process_marker", lambda pid: "current-owner")
    monkeypatch.setattr(driver, "_prepare", Mock())
    monkeypatch.setattr(driver, "_peek", Mock(return_value=True))
    monkeypatch.setattr(driver, "_begin_stream", lambda *args: 10)
    monkeypatch.setattr(driver, "_insert", Mock())
    monkeypatch.setattr(driver, "_poison", Mock(return_value="expected overflow"))
    monkeypatch.setattr(driver, "_lagging_stream_reports_poison", Mock())
    snapshots = 0

    def snapshot(*args):
        nonlocal snapshots
        snapshots += 1
        if snapshots == 3:
            assert db.execute("SELECT count(*) FROM poison_intents").fetchone() == (1,)

    monkeypatch.setattr(driver, "_snapshot", snapshot)
    driver.run_round(
        "host", slot, model.generate(random.Random(3)), time.monotonic() + 30, db=db
    )
    assert snapshots == 3
    assert db.execute("SELECT count(*) FROM poison_intents").fetchone() == (0,)


def test_unexpected_product_error_in_cleanup_is_reported_not_allowlisted(
    monkeypatch, poisoned_fixture
):
    db, _, conn, _, _ = poisoned_fixture
    violation = Mock()
    monkeypatch.setattr(driver, "unreachable", violation)
    monkeypatch.setattr(
        conn,
        "execute",
        Mock(
            side_effect=psycopg.errors.InternalError_("unexpected controller failure")
        ),
    )
    assert not driver.cleanup_abandoned("host", db)[0]
    violation.assert_called_once()
    assert violation.call_args.args[1]["phase"] == "abandoned poison cleanup"
    assert db.execute("SELECT count(*) FROM poison_intents").fetchone() == (1,)


def _recovery_with_passing_checks(monkeypatch) -> tuple[recovery.Recovery, list[str]]:
    check = recovery.Recovery.__new__(recovery.Recovery)
    check.host = "host"
    check.env = Mock()
    check.db = Mock()
    check.mode = "test"
    check.timings = {}
    check.restarts_lock = threading.Lock()
    check.restarts = Mock(started=time.monotonic() - 1000)
    check.sampling_done = threading.Event()
    observed: list[str] = []

    def good(name):
        def execute():
            observed.append(name)
            return True, {}

        return execute

    for name in (
        "canary",
        "cr_up_to_date",
        "replicas_online",
        "reconfigurations_settled",
        "hung_alters",
        "no_crash_loop",
        "cleanup_aggregate_fixtures",
        "cleanup_upsert_fixtures",
        "hydrated",
        "frontiers_advance",
        "collections_readable",
    ):
        setattr(check, name, good(name))
    check.restart_and_compare = Mock(
        side_effect=lambda: observed.append("restart_and_compare")
    )
    check.drain_replacements = Mock()
    check.retry_ledger = Mock()

    def self_check(route):
        observed.append(route)
        return True, ""

    check.self_check = self_check
    monkeypatch.setattr(
        recovery, "poll", lambda budget, callback, label: (*callback(), 0)
    )
    monkeypatch.setattr(recovery, "always", assert_product)
    monkeypatch.setattr(recovery, "sometimes", Mock())
    monkeypatch.setattr(recovery, "observe_pods", lambda env: [])
    monkeypatch.setattr(recovery, "record_terminations", Mock())
    monkeypatch.setattr(recovery.lifecycle, "Session", Mock())
    monkeypatch.setattr(recovery.lifecycle, "open_state", Mock())
    monkeypatch.setattr(recovery.lifecycle, "check_readable", Mock())
    monkeypatch.setattr(recovery.rng, "random", lambda: 0)
    return check, observed


@pytest.mark.parametrize("blocked_fixture", ("aggregate", "upsert"))
def test_incomplete_fixture_cleanup_preserves_independent_recovery_checks(
    monkeypatch, blocked_fixture
):
    check, observed = _recovery_with_passing_checks(monkeypatch)
    for fixture in ("aggregate", "upsert"):

        def cleanup(fixture=fixture):
            observed.append(f"cleanup_{fixture}")
            return fixture != blocked_fixture, {"owner_still_active": True}

        setattr(check, f"cleanup_{fixture}_fixtures", cleanup)
    check.hydrated = Mock(side_effect=AssertionError("poison hydration checked"))
    check.frontiers_advance = Mock(
        side_effect=AssertionError("poison frontier checked")
    )
    check.collections_readable = Mock(
        side_effect=AssertionError("poison readability checked")
    )
    check.restart_and_compare = Mock(
        side_effect=AssertionError("poison restart snapshot checked")
    )
    assert check.run() == 0
    assert "reconfigurations_settled" in observed
    assert "hung_alters" in observed
    assert "/api/catalog/check" in observed
    assert "/api/coordinator/check" in observed
    assert "no_crash_loop" in observed
    assert "cleanup_aggregate" in observed
    assert "cleanup_upsert" in observed
    check.retry_ledger.assert_called_once()
    check.drain_replacements.assert_called_once()
    check.hydrated.assert_not_called()
    check.frontiers_advance.assert_not_called()
    check.collections_readable.assert_not_called()
    check.restart_and_compare.assert_not_called()
    assert check.sampling_done.is_set()


def test_recovery_restarts_pods_before_the_crash_loop_window(monkeypatch):
    check, observed = _recovery_with_passing_checks(monkeypatch)
    assert check.run() == 0
    assert observed.index("restart_and_compare") < observed.index("no_crash_loop")
