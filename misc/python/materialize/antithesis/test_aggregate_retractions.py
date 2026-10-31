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
import time
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import aggregate_retractions_model as model
from materialize.antithesis.drivers import aggregate_retractions as driver


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


def test_a_poisoned_cursor_cannot_silently_recover(monkeypatch):
    monkeypatch.setattr(driver, "always", assert_product)
    conn = Mock()
    conn.execute.return_value.fetchall.return_value = []
    with pytest.raises(AssertionError):
        driver._old_stream_stays_failed(conn, "interval out of range")
    conn.execute.side_effect = psycopg.errors.InFailedSqlTransaction(
        "transaction aborted"
    )
    driver._old_stream_stays_failed(conn, "interval out of range")


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
