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
import sqlite3
import subprocess
import sys
from collections import Counter
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import exit_audit, observations, telemetry, templates


@pytest.mark.parametrize("body", ["pass", "raise RuntimeError('command failed')"])
@pytest.mark.parametrize("profile", templates.PROFILES)
def test_measured_commands_audit_on_entry_and_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, body: str, profile: str
) -> None:
    audit = Mock()
    monkeypatch.setattr(telemetry, "_audit_exit_observer", audit, raising=False)
    monkeypatch.setattr(telemetry, "_begin", lambda *args: None)
    monkeypatch.setattr(telemetry, "_finish", lambda *args: None)
    source = tmp_path / "driver.py"
    source.write_text(body)
    if body.startswith("raise"):
        with pytest.raises(RuntimeError, match="command failed"):
            telemetry.run_command(profile, source)
    else:
        assert telemetry.run_command(profile, source) == 0
    assert audit.call_count == 2


def test_fresh_heartbeat_audit_does_not_load_product_clients(tmp_path: Path) -> None:
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, time; from materialize.antithesis import exit_audit; "
            "db = exit_audit.state.open_db(exit_audit.STATE_DB); "
            "exit_audit.initialize(db); "
            "db.execute('INSERT INTO exit_observer VALUES (1, ?, ?)', "
            "(time.monotonic(), '{}')); db.commit(); db.close(); "
            "assert exit_audit.main() == 0; "
            "assert not any(name in sys.modules for name in "
            "['psycopg', 'kubernetes', 'requests', "
            "'materialize.antithesis.telemetry', "
            "'materialize.antithesis.drivers.recovery'])",
        ],
        env={
            **os.environ,
            "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
            "MZ_ANTITHESIS_STATE_DIR": str(tmp_path),
            **{
                f"MZ_ANTITHESIS_{name}": "unused"
                for name in [
                    "ENVIRONMENT",
                    "KAFKA_BROKER",
                    "SCHEMA_REGISTRY_URL",
                    "UPSTREAM_POSTGRES_URL",
                    "METADATA_POSTGRES_URL",
                ]
            },
        },
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.fixture
def audit_db() -> Iterator[sqlite3.Connection]:
    db = sqlite3.connect(":memory:")
    exit_audit.initialize(db)
    yield db
    db.close()


def test_fresh_heartbeat_has_one_audit_per_shared_cadence(
    audit_db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [100.0]
    monkeypatch.setattr(exit_audit.time, "monotonic", lambda: clock[0])
    with audit_db:
        audit_db.execute(
            "INSERT INTO exit_observer VALUES (1, 99, ?)",
            (json.dumps({"samples": 7, "counts": {}}),),
        )
    emit, fallback = Mock(), Mock()
    for now in [100.0, 100.0, 104.9, 105.0]:
        clock[0] = now
        assert exit_audit.audit(audit_db, emit=emit, fallback=fallback) == 0
    assert [call.args[0]["sample_age_s"] for call in emit.call_args_list] == [1, 6]
    assert all(call.args[0]["samples"] == 7 for call in emit.call_args_list)
    fallback.assert_not_called()


@pytest.mark.parametrize("sample", [None, 89.0, 101.0])
def test_missing_stale_or_future_heartbeat_claims_before_fallback(
    audit_db: sqlite3.Connection,
    monkeypatch: pytest.MonkeyPatch,
    sample: float | None,
) -> None:
    monkeypatch.setattr(exit_audit.time, "monotonic", lambda: 100.0)
    if sample is not None:
        with audit_db:
            audit_db.execute("INSERT INTO exit_observer VALUES (1, ?, '{}')", (sample,))

    def fallback(age: float | None) -> int:
        assert age == (100 - sample if sample is not None else None)
        assert audit_db.execute(
            "SELECT retry_at FROM exit_observer_fallback"
        ).fetchone() == (130,)
        return 12

    assert exit_audit.audit(audit_db, fallback=fallback) == 12


def test_concurrent_stale_audits_share_one_fallback_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(exit_audit.time, "monotonic", lambda: 100.0)
    path = tmp_path / "recovery.sqlite"
    with sqlite3.connect(path) as db:
        exit_audit.initialize(db)
    fallback = Mock(return_value=0)

    def audit(_: int) -> int:
        with sqlite3.connect(path) as db:
            return exit_audit.audit(db, force=True, fallback=fallback)

    with ThreadPoolExecutor(max_workers=8) as pool:
        assert list(pool.map(audit, range(24))) == [0] * 24
    fallback.assert_called_once_with(None)


def test_failed_fallback_keeps_its_retry_budget(
    audit_db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [100.0]
    monkeypatch.setattr(exit_audit.time, "monotonic", lambda: clock[0])
    fallback = Mock(side_effect=RuntimeError("fallback unavailable"))
    with pytest.raises(RuntimeError, match="fallback unavailable"):
        exit_audit.audit(audit_db, force=True, fallback=fallback)
    clock[0] = 129.9
    assert exit_audit.audit(audit_db, force=True, fallback=fallback) == 0
    clock[0] = 130.0
    with pytest.raises(RuntimeError, match="fallback unavailable"):
        exit_audit.audit(audit_db, force=True, fallback=fallback)
    assert fallback.call_count == 2


def test_audit_error_persists_invalid_harness_health(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MZ_ANTITHESIS_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(
        exit_audit, "main", Mock(side_effect=sqlite3.OperationalError("unavailable"))
    )
    emit = Mock()
    monkeypatch.setattr(observations, "send_event", emit)
    recorder = telemetry.Recorder("transactions", "driver.py")
    monkeypatch.setattr(telemetry, "_recorder", recorder)
    telemetry._audit_exit_observer()
    assert recorder.harness_errors == 1
    assert exit_audit.health(Counter())["valid"] is False
    durable = observations.durable_health()
    assert durable is not None
    assert durable["message"] == "exit observer audit"
    assert any(
        call.args[0] == "harness_health" and call.args[1]["valid"] is False
        for call in emit.call_args_list
    )


def test_heartbeat_health_preserves_passive_observer_denial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(observations, "durable_health", lambda: None)
    assert exit_audit.health(Counter(forbidden=1))["valid"] is False
    assert exit_audit.health(Counter())["valid"] is True


def test_unreadable_health_marker_cannot_mask_the_driver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MZ_ANTITHESIS_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(
        exit_audit, "main", Mock(side_effect=PermissionError("unavailable"))
    )
    monkeypatch.setattr(
        observations,
        "durable_health",
        Mock(side_effect=PermissionError("unavailable")),
    )
    emit = Mock()
    monkeypatch.setattr(observations, "send_event", emit)
    telemetry._audit_exit_observer()
    statuses = [
        call.args[1] for call in emit.call_args_list if call.args[0] == "harness_health"
    ]
    assert len(statuses) == 1 and statuses[0]["valid"] is False
    assert (
        statuses[0]["first_error"]["message"] == "durable harness health is unavailable"
    )
