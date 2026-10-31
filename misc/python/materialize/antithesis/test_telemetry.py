# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import observations, quiet, state, telemetry


@pytest.fixture(autouse=True)
def local_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(telemetry.exit_audit, "main", lambda: 0)
    for key, value in {
        "ENVIRONMENT": "telemetry",
        "KAFKA_BROKER": "unused",
        "SCHEMA_REGISTRY_URL": "http://unused",
        "UPSTREAM_POSTGRES_URL": "postgres://unused/local",
        "METADATA_POSTGRES_URL": "postgres://unused/local",
        "STATE_DIR": str(tmp_path / "state"),
    }.items():
        monkeypatch.setenv(f"MZ_ANTITHESIS_{key}", value)
    monkeypatch.delenv("ANTITHESIS_STOP_FAULTS", raising=False)


def command(tmp_path: Path, name: str, body: str) -> Path:
    path = tmp_path / f"{name}.py"
    path.write_text(body)
    return path


def test_hooks_do_nothing_outside_a_measured_command(tmp_path: Path) -> None:
    assert not telemetry.enabled()
    telemetry.comparison("ivm", "subscribe", True)
    telemetry.phase_observed("Promoting", 1, "request")
    telemetry.fault_observed("clusterd", 1, 2, "kill")
    telemetry.record_coverage([("Promoting", "clusterd", "append", "table", "acked")])
    assert telemetry.events_between(0, 10) == []
    assert not (tmp_path / "state").exists()


def test_replica_fault_events_keep_the_actual_cluster_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(telemetry, "_recorder", telemetry.Recorder("main", "driver"))
    monkeypatch.setattr(telemetry.time, "monotonic", lambda: 100.0)
    telemetry.fault_observed("clusterd", 95, 96, "delete", cluster_id="u3")
    assert telemetry.events_between(90, 100) == [
        telemetry.Event("fault", 95, 96, role="clusterd", cluster_id="u3")
    ]


def test_existing_event_ledgers_migrate_without_inventing_cluster_identity() -> None:
    with state.open_db(telemetry.STATE_DB) as db:
        db.execute(
            "CREATE TABLE events (id INTEGER PRIMARY KEY, profile TEXT, kind TEXT,"
            " started REAL, completed REAL, phase TEXT, role TEXT, event_key TEXT,"
            " UNIQUE (profile, kind, event_key))"
        )
        db.execute(
            "INSERT INTO events VALUES (1, 'main', 'fault', 1, 2, '', 'clusterd', 'old')"
        )
    for _ in range(2):
        db = telemetry._open()
        try:
            assert db.execute("SELECT cluster_id FROM events").fetchone() == ("",)
        finally:
            db.close()


def test_short_transaction_can_request_a_fresh_sample_of_an_observed_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(telemetry, "_recorder", telemetry.Recorder("main", "driver"))
    clock = [100.0]
    monkeypatch.setattr(telemetry.time, "monotonic", lambda: clock[0])
    telemetry.phase_observed("Promoting", 2, "request")
    clock[0] = 101.0
    telemetry.phase_observed("Promoting", 2, "request")
    assert telemetry.events_between(100.5, 101.5) == []
    telemetry.phase_observed("Promoting", 2, "request", started=100.9, force=True)
    observed = telemetry.events_between(100.5, 101.5)
    assert len(observed) == 1
    assert (observed[0].started, observed[0].completed, observed[0].phase) == (
        100.9,
        101.0,
        "Promoting",
    )


def test_cluster_identity_cache_retries_unknown_identity_and_keeps_successes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(telemetry, "_recorder", telemetry.Recorder("main", "driver"))
    connection = Mock()
    connection.execute.return_value.fetchone.side_effect = [None, ("u3",)]

    @contextmanager
    def connect(*args, **kwargs):
        yield connection

    monkeypatch.setattr(telemetry.sql, "connection", connect)
    assert telemetry.resolve_cluster_id("host", "owned") is None
    assert telemetry.resolve_cluster_id("host", "owned") == "u3"
    assert telemetry.resolve_cluster_id("host", "owned") == "u3"
    assert connection.execute.call_count == 2


def test_protocol_diagnostics_correlate_rounds_without_completing_interrupted_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = telemetry.Recorder(
        "transactions", "parallel_driver_history_interleavings.py"
    )
    monkeypatch.setattr(telemetry, "_recorder", recorder)
    emitted = Mock()
    monkeypatch.setattr(observations, "send_event", emitted)
    with pytest.raises(RuntimeError, match="interrupted"):
        with telemetry.interleaving("held_append", epoch=1) as interrupted:
            telemetry.interleaving_progress(
                "disruption", action="delete", outcome="acked"
            )
            raise RuntimeError("interrupted")
    with telemetry.interleaving("race_rmw", epoch=1) as current:
        telemetry.interleaving_progress(
            "write", expected_round=interrupted, outcome="ok"
        )
        telemetry.interleaving_progress("end", outcome="completed")
    events = [call.args[1] for call in emitted.call_args_list]
    assert [event["stage"] for event in events] == [
        "start",
        "disruption",
        "start",
        "end",
    ]
    assert [event["round"] for event in events] == [
        interrupted,
        interrupted,
        current,
        current,
    ]
    assert interrupted != current
    assert all(event["profile"] == "transactions" for event in events)
    assert all(event["command"] == recorder.command for event in events)
    assert recorder.interleaving_round is None
    assert [event["mode"] for event in events] == [
        "held_append",
        "held_append",
        "race_rmw",
        "race_rmw",
    ]


def test_protocol_outcomes_keep_copied_metadata_and_restore_the_outer_round(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = telemetry.Recorder("main", "driver")
    monkeypatch.setattr(telemetry, "_recorder", recorder)
    emitted = Mock()
    monkeypatch.setattr(observations, "send_event", emitted)
    metadata = {"key": 1}
    with telemetry.interleaving("outer", subject=metadata) as outer:
        metadata["key"] = 2
        with telemetry.interleaving("inner", epoch=2):
            telemetry.interleaving_progress("end", outcome="completed")
        telemetry.interleaving_progress("end", outcome="unresolved")
    events = [call.args[1] for call in emitted.call_args_list]
    assert events[-2]["mode"] == "inner" and events[-2]["epoch"] == 2
    assert events[-1]["round"] == outer and events[-1]["mode"] == "outer"
    assert events[-1]["subject"] == {"key": 1}
    assert recorder.interleaving_details == {}


def test_concurrent_comparisons_are_counted_and_normalized(tmp_path: Path) -> None:
    source = command(
        tmp_path,
        "anytime_concurrent",
        """
from concurrent.futures import ThreadPoolExecutor
from materialize.antithesis.telemetry import comparison
def compare(i):
    for j in range(100):
        comparison('read_paths', f'replica:{i}', True)
with ThreadPoolExecutor(max_workers=8) as pool:
    list(pool.map(compare, range(8)))
comparison('read_paths', 'subscribe', None, 5)
""",
    )
    assert telemetry.run_command("transactions", source) == 0
    assert not telemetry.enabled()
    report = telemetry.summary()
    rows = {(r["path"], r["outcome"]): r["units"] for r in report["comparisons"]}
    assert rows == {("replica", "passed"): 800, ("subscribe", "skipped"): 5}
    assert report["commands"][0]["completed"] == 1
    assert report["commands"][0]["no_comparison"] == 0


def test_skipped_checks_and_uninstrumented_checks_are_distinct(tmp_path: Path) -> None:
    skipped = command(
        tmp_path,
        "anytime_skipped",
        "from materialize.antithesis.telemetry import comparison\ncomparison('ivm', 'subscribe', None)\n",
    )
    empty = command(tmp_path, "anytime_empty", "pass\n")
    driver = command(tmp_path, "parallel_driver_empty", "pass\n")
    for source in (skipped, empty, driver):
        assert telemetry.run_command("main", source) == 0
    rows = {r["command"]: r for r in telemetry.summary()["commands"]}
    assert rows[skipped.name]["no_comparison"] == 1
    assert rows[skipped.name]["uninstrumented"] == 0
    assert rows[empty.name]["no_comparison"] == 1
    assert rows[empty.name]["uninstrumented"] == 1
    assert rows[driver.name]["no_comparison"] == 0
    assert rows[driver.name]["uninstrumented"] == 0


@pytest.mark.parametrize("value,expected", [(None, 0), (0, 0), (7, 7), ("message", 1)])
def test_exit_status_and_profile_environment_are_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, value, expected: int
) -> None:
    monkeypatch.setenv(telemetry.PROFILE_ENV, "outer")
    source = command(
        tmp_path,
        "parallel_driver_exit",
        f"""
import os
assert os.environ[{telemetry.PROFILE_ENV!r}] == 'cdc'
raise SystemExit({value!r})
""",
    )
    assert telemetry.run_command("cdc", source) == expected
    assert os.environ[telemetry.PROFILE_ENV] == "outer"
    row = telemetry.summary()["commands"][0]
    assert row["started"] == 1
    assert row["completed"] == int(expected == 0)
    assert row["errors"] == int(expected != 0)
    assert telemetry.summary()["active_commands"] == 0


def test_command_exceptions_are_not_hidden(tmp_path: Path) -> None:
    source = command(
        tmp_path, "anytime_error", "raise RuntimeError('underlying failure')\n"
    )
    with pytest.raises(RuntimeError, match="underlying failure"):
        telemetry.run_command("main", source)
    assert not telemetry.enabled()
    row = telemetry.summary()["commands"][0]
    assert row["errors"] == 1 and row["completed"] == 0


def test_killed_command_is_reaped_without_crediting_its_completion(
    tmp_path: Path,
) -> None:
    db = state.open_db(telemetry.exit_audit.STATE_DB)
    try:
        telemetry.exit_audit.initialize(db)
        with db:
            db.execute(
                "INSERT INTO exit_observer VALUES (1, ?, '{}')",
                (telemetry.time.monotonic(),),
            )
    finally:
        db.close()
    source = command(
        tmp_path,
        "anytime_watchdog",
        """
import os
from materialize.antithesis.telemetry import comparison
comparison('ivm', 'subscribe', True)
os._exit(0)
""",
    )
    subprocess.run(
        [
            sys.executable,
            "-c",
            "from materialize.antithesis.telemetry import run_command; import sys; run_command('main', sys.argv[1])",
            str(source),
        ],
        check=True,
    )
    db = telemetry._open()
    try:
        assert db.execute("SELECT count(*) FROM active_commands").fetchone()[0] == 1
    finally:
        db.close()
    next_source = command(tmp_path, "parallel_driver_next", "pass\n")
    telemetry.run_command("main", next_source)
    report = telemetry.summary()
    row = next(r for r in report["commands"] if r["command"] == source.name)
    assert row["started"] == 1 and row["interrupted"] == 1
    assert row["completed"] == 0 and row["errors"] == 0
    assert report["active_commands"] == 0
    assert report["comparisons"] == []


def test_pid_reuse_does_not_keep_an_interrupted_command_alive() -> None:
    db = telemetry._open()
    try:
        db.execute(
            "INSERT INTO commands (profile, command, started) VALUES ('main', 'old', 1)"
        )
        db.execute(
            "INSERT INTO active_commands VALUES ('old', 'main', 'old', ?, 'old-token', 0)",
            (os.getpid(),),
        )
        db.commit()
    finally:
        db.close()
    row = telemetry.summary()["commands"][0]
    assert row["interrupted"] == 1


def test_measurements_do_not_claim_system_or_unobserved_interrupted_cpu(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [100.0]
    monkeypatch.setattr(telemetry.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(telemetry.time, "process_time", Mock(side_effect=[1.0, 4.0]))
    monkeypatch.setattr(telemetry, "_child_cpu", Mock(side_effect=[10.0, 15.0]))
    source = command(
        tmp_path,
        "parallel_driver_cost",
        "from materialize.antithesis import telemetry\ntelemetry.time.monotonic = lambda: 105.0\n",
    )
    telemetry.run_command("main", source)
    report = telemetry.summary()
    assert report["commands"][0]["wall_s"] == 5
    assert report["workload_self_cpu_s"] == 3
    assert report["workload_child_cpu_s"] == 5
    assert report["sut_cpu_measured"] is False
    assert report["interrupted_cpu_measured"] is False


def test_events_are_throttled_deduplicated_and_filtered_by_overlap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        telemetry, "_recorder", telemetry.Recorder("main", "anytime_test.py")
    )
    clock = [100.0]
    monkeypatch.setattr(telemetry.time, "monotonic", lambda: clock[0])
    telemetry.phase_observed("Applying", 1, "request")
    clock[0] = 101.0
    telemetry.phase_observed("Applying", 1, "request")
    clock[0] = 102.0
    telemetry.phase_observed("ReadyToPromote", 1, "request")
    clock[0] = 103.0
    telemetry.phase_observed("Applying", 1, "request")
    telemetry.fault_observed("environmentd_active", 101.5, 102.5, "delete")
    telemetry.fault_observed("environmentd_active", 101.5, 102.5, "delete")
    events = telemetry.events_between(0, 200)
    assert [(e.phase, e.started) for e in events if e.kind == "phase"] == [
        ("Applying", 100),
        ("ReadyToPromote", 102),
        ("Applying", 103),
    ]
    selected = telemetry.events_between(101.8, 102.2)
    assert [e.kind for e in selected] == ["fault", "phase"]
    assert selected[0].role == "environmentd_active"


def test_event_storage_is_bounded_by_count_and_age(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        telemetry, "_recorder", telemetry.Recorder("main", "anytime_test.py")
    )
    monkeypatch.setattr(telemetry, "EVENT_CAP", 3)
    monkeypatch.setattr(telemetry, "EVENT_RETENTION_S", 5)
    clock = [100.0]
    monkeypatch.setattr(telemetry.time, "monotonic", lambda: clock[0])
    for i in range(10):
        clock[0] = 100 + i
        telemetry.fault_observed("clusterd", clock[0], clock[0], str(i))
    assert [e.started for e in telemetry.events_between(0, 200)] == [107, 108, 109]
    report = telemetry.summary()
    assert report["pruned_events"] == 7
    assert report["event_capacity"] == 3
    assert report["event_retention_s"] == 5
    clock[0] = 120
    assert telemetry.events_between(0, 200) == []
    assert telemetry.summary()["pruned_events"] == 10


def test_phase_observation_preserves_the_entire_api_request_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        telemetry, "_recorder", telemetry.Recorder("main", "anytime_test.py")
    )
    monkeypatch.setattr(telemetry.time, "monotonic", lambda: 110.0)
    telemetry.phase_observed("Promoting", 1, "request", started=100.0)
    assert telemetry.events_between(105, 106) == [
        telemetry.Event("phase", 100.0, 110.0, "Promoting", "")
    ]


def test_coverage_preserves_evidence_and_bounds_label_cardinality(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        telemetry, "_recorder", telemetry.Recorder("main", "anytime_test.py")
    )
    telemetry.record_coverage(
        [
            ("Promoting", "environmentd_active", "append", "replica:r1", "acked"),
            ("Promoting", "environmentd_active", "append", "replica:r2", "acked"),
            ("", "environmentd", "rmw", "table", "indeterminate"),
            ("", "", "rmw", "join_mv", "acked"),
            ("random", "random", "random", "random", "random"),
        ]
    )
    rows = telemetry.summary()["coverage"]
    assert len(rows) == 4
    matched = next(r for r in rows if r["phase"] == "Promoting")
    assert matched["path"] == "replica" and matched["hits"] == 2
    assert any(r["phase"] == "" and r["role"] == "environmentd" for r in rows)
    assert any(r["phase"] == "" and r["role"] == "" for r in rows)


def test_quiet_fraction_counts_the_union_of_overlapping_requests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [100.0]
    monkeypatch.setattr(telemetry.time, "monotonic", lambda: clock[0])
    telemetry.run_command("main", command(tmp_path, "parallel_driver_start", "pass\n"))
    quiet.request_quiet_period(20)
    clock[0] = 110
    quiet.request_quiet_period(20)
    clock[0] = 140
    assert telemetry.summary()["quiet_fraction"] == 0.75


def test_measurement_failures_are_reported_without_masking_command_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed = Mock()
    monkeypatch.setattr(observations, "harness_error", failed)
    monkeypatch.setattr(
        telemetry.state, "open_db", Mock(side_effect=OSError("unavailable"))
    )
    source = command(
        tmp_path, "parallel_driver_error", "raise RuntimeError('underlying failure')\n"
    )
    with pytest.raises(RuntimeError, match="underlying failure"):
        telemetry.run_command("main", source)
    failed.assert_called_once()
    assert not telemetry.enabled()


def test_observations_keep_false_counts_and_unseen_inventory_targets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog = tmp_path / "coverage.json"
    catalog.write_text(
        '{"version":1,"targets":[{"message":"seen"},{"message":"never evaluated"}],'
        '"unresolved":[{"kind":"sometimes","file":"driver.py","line":4,"reason":"dynamic"}]}'
    )
    monkeypatch.setenv(observations.CATALOG_ENV, str(catalog))
    monkeypatch.setattr(observations, "send_event", lambda *args: None)
    source = command(
        tmp_path,
        "parallel_driver_observations",
        "from materialize.antithesis.observations import sometimes\n"
        "for _ in range(10):\n    sometimes(False, 'seen')\n"
        "sometimes(True, 'seen')\n",
    )
    telemetry.run_command("main", source)
    report = telemetry.summary()
    targets = {row["message"]: row for row in report["coverage_targets"]}
    assert targets["seen"]["observed"] == 1
    assert targets["seen"]["not_observed"] == 10
    assert targets["never evaluated"]["observed"] == 0
    assert targets["never evaluated"]["not_observed"] == 0
    assert report["coverage_catalog_available"]
    assert len(report["coverage_catalog_unresolved"]) == 1


def test_unreadable_required_inventory_marks_harness_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(observations.CATALOG_ENV, str(tmp_path / "missing.json"))
    invalid = Mock()
    monkeypatch.setattr(observations, "harness_error", invalid)
    report = telemetry.summary()
    assert report["telemetry_available"] is False
    assert report["harness_health"]["valid"] is False
    invalid.assert_called_once_with(
        "telemetry", "summarize", {"error": "FileNotFoundError"}
    )


def test_harness_health_is_counted_persisted_and_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(observations, "send_event", lambda *args: None)
    monkeypatch.setattr(telemetry, "HEALTH_DIAGNOSTIC_CAPACITY", 2)
    source = command(
        tmp_path,
        "parallel_driver_health",
        "from materialize.antithesis.observations import harness_error\n"
        "for i in range(5):\n    harness_error('test', 'diagnostic', {'value': i})\n",
    )
    assert telemetry.run_command("main", source) == 0
    report = telemetry.summary()
    health = report["harness_health"]
    assert not health["valid"]
    assert health["errors"] == report["commands"][0]["harness_errors"] == 5
    assert len(health["diagnostics"]) == 2
    assert health["diagnostics_omitted"] == 3
    assert health["first_error"]["details"] == {"value": 0}


def test_telemetry_storage_failure_leaves_a_sticky_invalidity_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = command(tmp_path, "parallel_driver_lost_metrics", "pass\n")
    original = telemetry.state.open_db
    with monkeypatch.context() as unavailable:
        unavailable.setattr(
            telemetry.state, "open_db", Mock(side_effect=OSError("unavailable"))
        )
        assert telemetry.run_command("main", source) == 0
    assert telemetry.state.open_db is original
    health = telemetry.summary()["harness_health"]
    assert not health["valid"]
    assert health["first_error"]["component"] == "telemetry"
    assert health["first_error"]["message"] == "start command"


def test_unavailable_telemetry_still_returns_explicit_harness_health(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        telemetry.state, "open_db", Mock(side_effect=OSError("unavailable"))
    )
    report = telemetry.summary()
    assert report["telemetry_available"] is False
    health = report["harness_health"]
    assert health["valid"] is False
    assert health["errors"] is None
    assert health["first_error"]["component"] == "telemetry"
    assert health["first_error"]["message"] == "summarize"


def test_unavailable_storage_and_marker_cannot_omit_invalidity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        telemetry.state, "open_db", Mock(side_effect=OSError("unavailable"))
    )
    monkeypatch.setattr(
        observations, "durable_health", Mock(side_effect=OSError("unreadable"))
    )
    report = telemetry.summary()
    assert report["harness_health"]["valid"] is False
    assert report["harness_health"]["first_error"]["details"] == {"error": "OSError"}


def test_late_summary_metric_failure_updates_returned_health(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        quiet, "recent_quiet_fraction", Mock(side_effect=OSError("unavailable"))
    )
    report = telemetry.summary()
    assert report["quiet_fraction"] is None
    assert report["harness_health"]["valid"] is False


@pytest.mark.parametrize("reset", ["changed", "missing"])
def test_state_reset_cannot_regenerate_a_model_and_make_vacuous_claims(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reset: str
) -> None:
    directory = tmp_path / "state"
    directory.mkdir()
    epoch = directory / state.EPOCH_FILE
    epoch.write_text("first")
    (directory / state.SETUP_MARKER).touch()
    db = state.open_db("model")
    with db:
        db.execute("CREATE TABLE acknowledged (value INTEGER)")
        db.execute("INSERT INTO acknowledged VALUES (7)")
    db.close()
    if reset == "changed":
        epoch.write_text("second")
    else:
        epoch.unlink()
    connect = sqlite3.connect
    connection = connect(directory / "model.sqlite")
    monkeypatch.setattr(state.sqlite3, "connect", lambda *args, **kwargs: connection)
    with pytest.raises(RuntimeError, match="state.*reset"):
        with state.open_db("model") as db:
            db.execute("DELETE FROM acknowledged")
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connection.execute("SELECT 1")
    with connect(directory / "model.sqlite") as intact:
        assert intact.execute("SELECT value FROM acknowledged").fetchall() == [(7,)]
    invalid = observations.durable_health()
    assert invalid is not None and not invalid["valid"]
    assert invalid["component"] == "state"


def test_new_model_database_can_register_the_current_setup_epoch(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "state"
    directory.mkdir()
    (directory / state.EPOCH_FILE).write_text("first")
    (directory / state.SETUP_MARKER).touch()
    with state.open_db("new-model") as db:
        assert db.execute("SELECT id FROM workload_state_epoch").fetchall() == [
            ("first",)
        ]


@pytest.mark.parametrize("replacement", ["missing", "empty", "another-model"])
def test_previously_opened_model_cannot_be_recreated_or_replaced(
    tmp_path: Path, replacement: str
) -> None:
    directory = tmp_path / "state"
    state.open_db("model").close()
    database = directory / "model.sqlite"
    database.unlink()
    if replacement == "empty":
        sqlite3.connect(database).close()
    elif replacement == "another-model":
        state.open_db("another-model").close()
        shutil.copyfile(directory / "another-model.sqlite", database)
    with pytest.raises(state.StateResetError):
        state.open_db("model")
    invalid = observations.durable_health()
    assert invalid is not None and invalid["component"] == "state"


def test_first_open_retry_reuses_committed_identity_before_marker_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "state"
    link = state.os.link
    identities = []

    def interrupted(source: Path, destination: Path) -> None:
        record = json.loads(source.read_text())
        assert not destination.exists()
        connection = sqlite3.connect(directory / "model.sqlite")
        try:
            identity = connection.execute(
                "SELECT id FROM workload_database_identity"
            ).fetchone()[0]
            assert identity == record["identity"]
            identities.append(identity)
        finally:
            connection.close()
        raise KeyboardInterrupt

    with monkeypatch.context() as interrupted_open:
        interrupted_open.setattr(state.os, "link", interrupted)
        with pytest.raises(KeyboardInterrupt):
            state.open_db("model")
    assert state.os.link is link
    assert not (directory / "model.identity").exists()
    connection = state.open_db("model")
    try:
        assert connection.execute(
            "SELECT id FROM workload_database_identity"
        ).fetchone() == (identities[0],)
    finally:
        connection.close()
    assert (
        json.loads((directory / "model.identity").read_text())["identity"]
        == identities[0]
    )
    assert observations.durable_health() is None


def test_interrupted_identity_initialization_can_retry_without_a_false_reset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with monkeypatch.context() as interrupted:
        interrupted.setattr(state.uuid, "uuid4", Mock(side_effect=KeyboardInterrupt))
        with pytest.raises(KeyboardInterrupt):
            state.open_db("model")
    assert not (tmp_path / "state" / "model.identity").exists()
    state.open_db("model").close()
    assert observations.durable_health() is None


def test_concurrent_first_opens_publish_one_complete_identity(tmp_path: Path) -> None:
    directory = tmp_path / "state"
    directory.mkdir()
    connection = sqlite3.connect(directory / "model.sqlite")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.close()

    def opened(_: int) -> str:
        connection = state.open_db("model")
        try:
            return connection.execute(
                "SELECT id FROM workload_database_identity"
            ).fetchone()[0]
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=8) as threads:
        identities = set(threads.map(opened, range(16)))
    assert len(identities) == 1
    assert (
        json.loads((directory / "model.identity").read_text())["identity"] in identities
    )
    assert observations.durable_health() is None


@pytest.mark.parametrize(
    "contents",
    ["{", "[]", "{}", '{"version":1,"database":"model","identity":"invalid"}'],
)
def test_malformed_model_identity_cannot_reach_an_oracle(
    tmp_path: Path, contents: str
) -> None:
    directory = tmp_path / "state"
    state.open_db("model").close()
    (directory / "model.identity").write_text(contents)
    with pytest.raises(state.StateResetError, match="malformed"):
        state.open_db("model")
