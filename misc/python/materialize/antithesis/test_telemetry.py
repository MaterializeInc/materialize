# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import quiet, telemetry


@pytest.fixture(autouse=True)
def local_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
    monkeypatch.setattr(telemetry, "unreachable", failed)
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
