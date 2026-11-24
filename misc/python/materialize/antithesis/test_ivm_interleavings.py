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
import time
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import telemetry
from materialize.antithesis.drivers import ivm, pod_restarts


@pytest.fixture
def writer(monkeypatch, tmp_path):
    path = tmp_path / "ivm.sqlite"
    with sqlite3.connect(path) as db:
        db.execute(
            "CREATE TABLE writes (op_id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT,"
            " invoke_rt REAL, complete_rt REAL, outcome TEXT, commit_ts INTEGER)"
        )
    events = []
    committed = threading.Event()
    closed = threading.Event()
    refresh_block = threading.Event()
    refresh_block.set()
    commit_block = threading.Event()
    commit_block.set()
    behavior = SimpleNamespace(commit_error=False, refresh_error=None, full=False)
    owners = []
    controller = threading.get_ident()

    class Database(sqlite3.Connection):
        def close(self):
            assert threading.get_ident() != controller
            super().close()
            events.append("sqlite_closed")
            closed.set()

    def open_state():
        owner = threading.get_ident()
        assert owner != controller
        owners.append(("sqlite", owner))
        return sqlite3.connect(path, factory=Database)

    @contextmanager
    def connect(*args, **kwargs):
        owner = threading.get_ident()
        assert owner != controller
        owners.append(("sql", owner))

        class Connection:
            closed = False

            def execute(self, *args, **kwargs):
                assert threading.get_ident() == owner
                events.append("insert")

            @contextmanager
            def transaction(self):
                assert threading.get_ident() == owner
                events.append("begin")
                yield
                events.append("commit")
                assert commit_block.wait(2)
                committed.set()
                if behavior.commit_error:
                    raise psycopg.OperationalError("lost commit response")

        try:
            yield Connection()
        finally:
            assert threading.get_ident() == owner
            events.append("sql_closed")

    def refresh(self):
        assert threading.get_ident() != controller
        assert refresh_block.wait(2)
        if behavior.refresh_error:
            raise behavior.refresh_error
        self.counts.update(
            (name, ivm.MAX_ROWS[name] if behavior.full else 0) for name in ("t", "d")
        )

    monkeypatch.setattr(ivm, "open_state", open_state)
    monkeypatch.setattr(ivm, "_connect", connect)
    monkeypatch.setattr(ivm.Writer, "refresh", refresh)
    progress = Mock()
    monkeypatch.setattr(telemetry, "interleaving_progress", progress)
    return SimpleNamespace(
        path=path,
        events=events,
        committed=committed,
        closed=closed,
        refresh_block=refresh_block,
        commit_block=commit_block,
        behavior=behavior,
        owners=owners,
        progress=progress,
    )


@pytest.mark.parametrize("race_commit", [False, True])
def test_ivm_selected_disruption_can_race_the_actual_commit(writer, race_commit):
    def effect(pending, release_commit):
        assert pending()
        assert not writer.committed.is_set()
        writer.events.append("selected")
        release_commit()
        if race_commit:
            assert writer.committed.wait(1)
            assert writer.closed.wait(1)
            assert not pending()
        else:
            assert not writer.committed.wait(0.02)
            with sqlite3.connect(writer.path) as db:
                assert db.execute("SELECT outcome FROM writes").fetchone() == (
                    "pending",
                )
            assert pending()
        writer.events.append("effect_finished")

    assert ivm._interleaved_write(
        "unused",
        pod_restarts.InterleavingDisruption(effect),
        race_commit=race_commit,
        deadline=time.monotonic() + 2,
    )
    before, after = (
        ("commit", "effect_finished") if race_commit else ("effect_finished", "commit")
    )
    assert writer.events.index(before) < writer.events.index(after)
    assert len({owner for _, owner in writer.owners}) == 1
    assert {kind for kind, _ in writer.owners} == {"sqlite", "sql"}
    with sqlite3.connect(writer.path) as db:
        assert db.execute("SELECT outcome FROM writes").fetchone() == ("ok",)


def test_ivm_lost_commit_response_still_needs_its_followup_oracle(writer):
    writer.behavior.commit_error = True
    assert ivm._interleaved_write(
        "unused",
        pod_restarts.InterleavingDisruption(lambda pending, release: release()),
        race_commit=True,
        deadline=time.monotonic() + 2,
    )
    with sqlite3.connect(writer.path) as db:
        assert db.execute("SELECT outcome FROM writes").fetchone() == ("indeterminate",)
    assert writer.closed.is_set()


def test_ivm_worker_exception_propagates_after_closing_owned_resources(writer):
    writer.behavior.refresh_error = RuntimeError("broken setup")
    disruption = Mock()
    with pytest.raises(RuntimeError, match="broken setup"):
        ivm._interleaved_write(
            "unused", disruption, race_commit=True, deadline=time.monotonic() + 2
        )
    disruption.assert_not_called()
    assert writer.closed.is_set()


def test_ivm_full_fixture_is_unstaged_and_does_not_credit_completion(writer):
    writer.behavior.full = True
    disruption = Mock()
    assert not ivm._interleaved_write(
        "unused", disruption, race_commit=True, deadline=time.monotonic() + 2
    )
    disruption.assert_not_called()
    assert writer.closed.is_set()
    assert writer.progress.call_args.kwargs["outcome"] == "indeterminate"


def test_ivm_worker_deadline_cannot_touch_the_controllers_connection(writer):
    writer.refresh_block.clear()
    disruption = Mock()
    with sqlite3.connect(writer.path) as controller_db:
        try:
            assert not ivm._interleaved_write(
                "unused", disruption, race_commit=True, deadline=time.monotonic() + 0.03
            )
            assert controller_db.execute("SELECT count(*) FROM writes").fetchone() == (
                0,
            )
            disruption.assert_not_called()
            assert not writer.closed.is_set()
            assert writer.progress.call_args.kwargs["outcome"] == "indeterminate"
        finally:
            writer.refresh_block.set()
            assert writer.closed.wait(1)
        assert controller_db.execute("SELECT count(*) FROM writes").fetchone() == (0,)


def test_ivm_unresolved_commit_at_deadline_never_credits_completion(writer):
    writer.commit_block.clear()

    def effect(pending, release_commit):
        assert pending()
        release_commit()

    try:
        assert not ivm._interleaved_write(
            "unused",
            pod_restarts.InterleavingDisruption(effect),
            race_commit=True,
            deadline=time.monotonic() + 0.1,
        )
        assert writer.progress.call_args.kwargs["outcome"] == "indeterminate"
        assert not writer.closed.is_set()
        with sqlite3.connect(writer.path) as db:
            assert db.execute("SELECT outcome FROM writes").fetchone() == ("pending",)
    finally:
        writer.commit_block.set()
        assert writer.closed.wait(1)
    with sqlite3.connect(writer.path) as db:
        assert db.execute("SELECT outcome FROM writes").fetchone() == ("ok",)


@pytest.mark.parametrize("race_commit", [False, True])
def test_ivm_controller_context_records_the_actual_commit_order(
    writer, monkeypatch, race_commit
):
    modes = []

    @contextmanager
    def context(mode):
        modes.append(mode)
        yield

    monkeypatch.setattr(telemetry, "interleaving", context)
    assert ivm._interleaved_write(
        "unused",
        pod_restarts.InterleavingDisruption(lambda pending, release: release()),
        race_commit=race_commit,
        deadline=time.monotonic() + 2,
    )
    assert modes == ["race_ivm" if race_commit else "held_ivm"]


@pytest.mark.parametrize("race_commit", [False, True])
def test_ivm_disruption_failure_releases_and_joins_the_staged_writer(
    writer, race_commit
):
    def effect(pending, release_commit):
        assert pending()
        raise ValueError("failed effect")

    with pytest.raises(ValueError, match="failed effect"):
        ivm._interleaved_write(
            "unused",
            pod_restarts.InterleavingDisruption(effect),
            race_commit=race_commit,
            deadline=time.monotonic() + 2,
        )
    assert writer.committed.is_set()
    assert writer.closed.is_set()


@pytest.mark.parametrize("race_commit", [False, True])
@pytest.mark.parametrize("completed", [False, True])
def test_ivm_main_draws_both_commit_orders_and_checks_only_finished_writes(
    monkeypatch, race_commit, completed
):
    env = Mock()
    env.sql_host.return_value = "unused"
    db = Mock()
    monkeypatch.setattr(ivm, "Environment", lambda: env)
    monkeypatch.setattr(ivm, "open_state", lambda: db)
    monkeypatch.setattr(ivm, "ensure_objects", lambda *args: True)
    monkeypatch.setattr(ivm.history, "start_watchdog", lambda *args: None)
    monkeypatch.setattr(ivm, "Kube", lambda value: None)
    monkeypatch.setattr(ivm, "_cluster_id", lambda conn: "u2")
    monkeypatch.setattr(ivm.rng, "choice", lambda items: race_commit)
    effect = pod_restarts.InterleavingDisruption(lambda pending, release: None)
    monkeypatch.setattr(
        pod_restarts, "interleaving_disruption", lambda *args, **kwargs: effect
    )

    @contextmanager
    def connect(*args, **kwargs):
        yield None

    monkeypatch.setattr(ivm, "_connect", connect)
    overlap = Mock(return_value=completed)
    monkeypatch.setattr(ivm, "_interleaved_write", overlap)
    checker = Mock()
    monkeypatch.setattr(ivm, "_check_windows", checker)
    assert ivm.interleaving_main() == 0
    assert overlap.call_args.args == ("unused", effect)
    assert overlap.call_args.kwargs["race_commit"] is race_commit
    assert checker.call_count == int(completed)
    if completed:
        checker.assert_called_once_with(env, "unused", db, allow_faults=False)
    db.close.assert_called_once()
