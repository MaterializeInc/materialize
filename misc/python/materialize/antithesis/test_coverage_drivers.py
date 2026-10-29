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
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip("antithesis")

from kubernetes.client.rest import ApiException  # type: ignore

from materialize.antithesis import ivm_model, telemetry, telemetry_coverage, templates
from materialize.antithesis import serializability as sz
from materialize.antithesis.drivers import ivm, pod_restarts


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
    "error", [None, ApiException(status=404), ApiException(status=500)]
)
def test_only_acknowledged_pod_deletes_emit_exposure_evidence(
    monkeypatch: pytest.MonkeyPatch, error: ApiException | None
) -> None:
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(pod_restarts, "open_db", lambda *args: db)
    clock = Mock(side_effect=[1.0, 2.0])
    monkeypatch.setattr(pod_restarts.time, "monotonic", clock)
    observed = Mock()
    monkeypatch.setattr(telemetry, "fault_observed", observed)
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
            "environmentd_active", 1.0, 2.0, key="pod_restarts:1"
        )
    else:
        observed.assert_not_called()


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
