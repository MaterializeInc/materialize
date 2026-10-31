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
import sqlite3
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis.drivers import configure

FRONTEND = "enable_frontend_peek_sequencing"


@pytest.fixture
def config_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    path = tmp_path / "config.sqlite"
    monkeypatch.setattr(configure.state, "open_db", lambda _: sqlite3.connect(path))
    monkeypatch.setattr(configure, "always", lambda *args: None)
    monkeypatch.setattr(configure, "reachable", lambda *args: None)
    monkeypatch.setattr(configure, "sometimes", lambda *args: None)
    return path


def flag_value(conn: object, name: str) -> str:
    if name == FRONTEND:
        raise psycopg.errors.UndefinedObject(
            f'unrecognized configuration parameter "{name}"'
        )
    return "false"


@pytest.mark.parametrize("frontend_supported", [False, True])
def test_startup_requests_and_credits_only_supported_flags(
    monkeypatch: pytest.MonkeyPatch, config_state: Path, frontend_supported: bool
) -> None:
    env = Mock(sql_host=lambda: "host", endpoints=SimpleNamespace(namespace="ns"))
    monkeypatch.setattr(configure, "Environment", lambda: env)
    monkeypatch.setattr(
        configure.sql, "connect_with_retry", lambda *args, **kwargs: nullcontext(Mock())
    )
    monkeypatch.setattr(configure, "mz_version", lambda _: "v26.48.2")
    monkeypatch.setattr(
        configure,
        "show",
        (lambda conn, name: "false") if frontend_supported else flag_value,
    )
    monkeypatch.setattr(
        configure,
        "rng",
        SimpleNamespace(choice=lambda values: values[0], random=lambda: 0.9),
    )
    written = []
    checked = []
    credits = []

    original_update = configure.update_system_params
    config_map = SimpleNamespace(
        data={configure.CONFIGMAP_KEY: json.dumps({FRONTEND: "false"})}
    )
    api = Mock()
    api.read_namespaced_config_map.return_value = config_map
    monkeypatch.setattr(configure.client, "CoreV1Api", lambda: api)

    def update(namespace: str, params: dict[str, str], **kwargs) -> dict[str, str]:
        merged = original_update(namespace, params, **kwargs)
        written.append(merged)
        return merged

    def reflected(host: str, params: dict[str, str]) -> dict[str, str]:
        checked.append(params)
        return {}

    monkeypatch.setattr(configure, "update_system_params", update)
    monkeypatch.setattr(configure, "wait_until_reflected", reflected)
    monkeypatch.setattr(configure, "ensure_shared_cluster", lambda *args: None)
    monkeypatch.setattr(
        configure,
        "sometimes",
        lambda condition, message, details: credits.append((message, condition)),
    )

    assert configure.main() == 0
    assert (FRONTEND in written[0]) is frontend_supported
    assert (FRONTEND in checked[0]) is frontend_supported
    assert any("frontend peek sequencing" in name for name, _ in credits) is (
        frontend_supported
    )
    assert written[0]["enable_upsert_v2"] == "true"
    assert written[0][configure.LEASE_PARAM] == configure.LEASE_MENU[0]
    with sqlite3.connect(config_state) as db:
        assert (
            bool(
                db.execute(
                    "SELECT 1 FROM profile WHERE key = ?", (FRONTEND,)
                ).fetchone()
            )
            is frontend_supported
        )
        assert (
            bool(
                db.execute(
                    "SELECT 1 FROM flag_history WHERE name = ?", (FRONTEND,)
                ).fetchone()
            )
            is frontend_supported
        )


@pytest.mark.parametrize("frontend_supported", [False, True])
def test_mid_run_flip_rechecks_supported_flags(
    monkeypatch: pytest.MonkeyPatch, config_state: Path, frontend_supported: bool
) -> None:
    env = Mock(sql_host=lambda: "host", endpoints=SimpleNamespace(namespace="ns"))
    monkeypatch.setattr(
        configure.sql, "connection", lambda *args, **kwargs: nullcontext(Mock())
    )
    monkeypatch.setattr(
        configure,
        "show",
        (lambda conn, name: "false") if frontend_supported else flag_value,
    )
    monkeypatch.setattr(
        configure,
        "rng",
        SimpleNamespace(
            choice=lambda values: FRONTEND if FRONTEND in values else values[0]
        ),
    )
    update = Mock()
    monkeypatch.setattr(configure, "update_system_params", update)
    credits = Mock()
    monkeypatch.setattr(configure, "sometimes", credits)
    db = configure.open_state()
    try:
        configure.record_flag(db, "enable_upsert_v2", "true", "first")
        selected = FRONTEND if frontend_supported else "enable_upsert_v2"
        value = "true" if frontend_supported else "false"
        assert configure.roll_flag(env, db) == selected
        assert update.call_args.args[1] == {selected: value}
        assert configure.current_flag_value(db, FRONTEND) == (
            "true" if frontend_supported else None
        )
        expected_credits = {
            "mid-run roll flipped enable_upsert_v2": not frontend_supported,
            "mid-run roll flipped enable_upsert_chunked_stash": False,
            "mid-run roll flipped enable_frontend_subscribes": False,
            "mid-run roll flipped enable_cluster_reconfiguration_lag_gate": False,
        }
        if frontend_supported:
            expected_credits["mid-run roll flipped enable_frontend_peek_sequencing"] = (
                True
            )
        assert {call.args[1]: call.args[0] for call in credits.call_args_list} == (
            expected_credits
        )
    finally:
        db.close()


@pytest.mark.parametrize("update_fails", [False, True])
def test_mid_run_flip_closes_its_own_state_connection(
    monkeypatch: pytest.MonkeyPatch, config_state: Path, update_fails: bool
) -> None:
    monkeypatch.setattr(
        configure.sql, "connection", lambda *args, **kwargs: nullcontext(Mock())
    )
    monkeypatch.setattr(configure, "show", flag_value)
    monkeypatch.setattr(
        configure, "rng", SimpleNamespace(choice=lambda values: values[0])
    )
    monkeypatch.setattr(
        configure,
        "update_system_params",
        Mock(side_effect=RuntimeError("update failed") if update_fails else None),
    )
    db = configure.open_state()
    monkeypatch.setattr(configure, "open_state", lambda: db)
    env = Mock(sql_host=lambda: "host", endpoints=SimpleNamespace(namespace="ns"))
    try:
        if update_fails:
            with pytest.raises(RuntimeError, match="update failed"):
                configure.roll_flag(env)
        else:
            configure.roll_flag(env)
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            db.execute("SELECT 1")
    finally:
        db.close()


def test_mid_run_probe_failure_does_not_skip_or_credit_supported_flags(
    monkeypatch: pytest.MonkeyPatch, config_state: Path
) -> None:
    error = psycopg.OperationalError("connection reset while probing flags")
    monkeypatch.setattr(configure.sql, "connection", Mock(side_effect=error))
    update = Mock()
    monkeypatch.setattr(configure, "update_system_params", update)
    db = configure.open_state()
    try:
        with pytest.raises(psycopg.OperationalError, match="probing flags"):
            configure.roll_flag(Mock(sql_host=lambda: "host"), db)
        update.assert_not_called()
        assert db.execute("SELECT count(*) FROM flag_history").fetchone() == (0,)
    finally:
        db.close()


def test_mid_run_flip_skips_a_binary_without_optional_flags(
    monkeypatch: pytest.MonkeyPatch, config_state: Path
) -> None:
    monkeypatch.setattr(
        configure.sql, "connection", lambda *args, **kwargs: nullcontext(Mock())
    )
    monkeypatch.setattr(
        configure,
        "show",
        Mock(side_effect=psycopg.errors.UndefinedObject("unrecognized parameter")),
    )
    update = Mock()
    monkeypatch.setattr(configure, "update_system_params", update)
    db = configure.open_state()
    try:
        assert configure.roll_flag(Mock(sql_host=lambda: "host"), db) is None
        update.assert_not_called()
        assert db.execute("SELECT count(*) FROM flag_history").fetchone() == (0,)
    finally:
        db.close()


def test_unsupported_probe_does_not_abort_following_probes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Connection:
        def __init__(self, autocommit: bool):
            self.autocommit = autocommit
            self.aborted = False

        def execute(self, statement: bytes):
            if self.aborted:
                raise psycopg.errors.InFailedSqlTransaction("transaction aborted")
            name = statement.decode().removeprefix("SHOW ")
            if name == FRONTEND:
                self.aborted = not self.autocommit
                raise psycopg.errors.UndefinedObject("unrecognized parameter")
            return Mock(fetchone=lambda: ("false",))

    factory = Mock(side_effect=lambda **kwargs: Connection(kwargs["autocommit"]))
    monkeypatch.setattr(configure.sql.psycopg, "connect", factory)
    conn = configure.sql.connect("host", internal=True)
    values = configure.implementation_flags(conn)
    assert factory.call_args.kwargs["autocommit"] is True
    assert FRONTEND not in values
    assert values["enable_frontend_subscribes"] == "false"


@pytest.mark.parametrize(
    "error",
    [
        psycopg.OperationalError("connection reset"),
        psycopg.errors.QueryCanceled("read timed out"),
        psycopg.errors.InsufficientPrivilege("permission denied"),
        psycopg.errors.InternalError_("unexpected failure"),
    ],
)
def test_failed_flag_probe_is_not_an_unsupported_flag(
    monkeypatch: pytest.MonkeyPatch, error: psycopg.Error
) -> None:
    monkeypatch.setattr(configure, "show", Mock(side_effect=error))
    with pytest.raises(type(error)):
        configure.show_or_none(Mock(), "enable_upsert_v2")


@pytest.fixture
def readback_clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    now = [0.0]

    def sleep(seconds: float) -> None:
        now[0] += seconds

    monkeypatch.setattr(configure, "SYNC_TIMEOUT_S", 2)
    monkeypatch.setattr(
        configure, "time", SimpleNamespace(monotonic=lambda: now[0], sleep=sleep)
    )
    monkeypatch.setattr(
        configure.sql, "connection", lambda *args, **kwargs: nullcontext(Mock())
    )
    return now


def test_unknown_parameter_readback_fails_without_spending_timeout(
    monkeypatch: pytest.MonkeyPatch, readback_clock: list[float]
) -> None:
    monkeypatch.setattr(configure, "show", flag_value)
    result = configure.wait_until_reflected("host", {FRONTEND: "true"})
    assert result
    assert FRONTEND in result
    assert "unrecognized configuration parameter" in result["<readback>"]
    assert readback_clock == [0.0]


def test_failed_readback_preserves_last_mismatch_and_read_error(
    monkeypatch: pytest.MonkeyPatch, readback_clock: list[float]
) -> None:
    first = True

    def show(conn: object, name: str) -> str:
        nonlocal first
        if name == "known":
            value = "false" if first else "true"
            first = False
            return value
        raise psycopg.OperationalError("connection reset during readback")

    monkeypatch.setattr(configure, "show", show)
    result = configure.wait_until_reflected("host", {"known": "true", "later": "true"})
    assert "have false" in result["known"]
    assert "connection reset during readback" in result["<readback>"]


def test_read_error_preserves_the_same_parameters_previous_mismatch(
    monkeypatch: pytest.MonkeyPatch, readback_clock: list[float]
) -> None:
    values = iter(["false", psycopg.OperationalError("readback failed")])

    def show(conn: object, name: str) -> str:
        value = next(values)
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(configure, "show", show)
    result = configure.wait_until_reflected("host", {"known": "true"})
    assert "have false" in result["known"]
    assert "readback failed" in result["<readback>"]


def test_readback_can_recover_after_a_failed_round(
    monkeypatch: pytest.MonkeyPatch, readback_clock: list[float]
) -> None:
    values = iter([psycopg.OperationalError("connection reset"), "true"])

    def show(conn: object, name: str) -> str:
        value = next(values)
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(configure, "show", show)
    assert configure.wait_until_reflected("host", {"known": "true"}) == {}
