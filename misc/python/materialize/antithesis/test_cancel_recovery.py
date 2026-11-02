# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import cancel_recovery as c

ROWS = ((0, None, "g"), (1, 5, "g"), (1, 5, "g"))


class Cancelled(psycopg.errors.QueryCanceled):
    @property
    def diag(self):
        return SimpleNamespace(
            message_primary="canceling statement due to user request"
        )


class TimedOut(psycopg.errors.QueryCanceled):
    @property
    def diag(self):
        return SimpleNamespace(
            message_primary="canceling statement due to statement timeout"
        )


class Protocol:
    def __init__(self):
        self.cancel = threading.Event()
        self.entered = threading.Event()
        self.retired = threading.Event()
        self.worker = Mock()
        self.controller = Mock()
        self.observer = Mock()
        for conn in (self.worker, self.controller, self.observer):
            conn.autocommit = True
            conn.closed = False
            conn.info.transaction_status = psycopg.pq.TransactionStatus.IDLE
        self.statements = []
        self.peek_visible = True
        self.cancel_ack = (True,)
        self.current = ("original",)
        self.finish_during_identity = False
        self.pid = 42
        self.identity_after = ("original", 42)
        self.error = Cancelled()
        self.aborted = True
        self.rows = list(ROWS)
        self.read_rows = None
        self.early_success = False
        self.retirement_lag = 1
        self.retirement_id = "peek-uuid"
        self.controller_error = None
        self.allow_read_exit = None
        self.worker.cancel_safe.side_effect = lambda **kwargs: self.cancel.set()
        self.worker.close.side_effect = self.close

    def close(self):
        self.worker.closed = True
        self.cancel.set()
        if self.allow_read_exit is not None:
            self.allow_read_exit.set()

    def execute(self, conn, deadline, statement, params):
        if conn is self.worker and " AS OF " not in statement:
            assert not self.entered.is_set() or self.retired.is_set()
        self.statements.append((conn, statement, params, threading.get_ident()))
        if " AS OF " in statement:
            self.entered.set()
            try:
                if not self.early_success:
                    assert self.cancel.wait(3)
                if self.allow_read_exit is not None:
                    assert self.allow_read_exit.wait(3)
                if self.error is not None:
                    raise self.error
                return Mock(fetchall=lambda: self.read_rows)
            finally:
                self.retired.set()
        if statement.startswith("SELECT id::text,pg_backend_pid()"):
            return Mock(
                fetchone=lambda: (
                    self.identity_after
                    if self.retired.is_set()
                    else ("original", self.pid)
                )
            )
        if statement.startswith("SELECT id::text FROM mz_introspection"):
            rows = []
            if self.entered.is_set() and self.peek_visible:
                if not self.retired.is_set() or self.retirement_lag:
                    rows = [
                        (self.retirement_id if self.retired.is_set() else "peek-uuid",)
                    ]
                    if self.retired.is_set():
                        self.retirement_lag -= 1
            return Mock(fetchall=lambda: rows)
        if statement.startswith("SELECT id::text FROM mz_internal"):
            if self.finish_during_identity:
                self.cancel.set()
                assert self.retired.wait(3)
            return Mock(fetchone=lambda: self.current)
        if statement.startswith("SELECT pg_cancel_backend"):
            if self.controller_error is not None:
                raise self.controller_error
            self.cancel.set()
            return Mock(fetchone=lambda: self.cancel_ack)
        if statement == "SELECT 1" and self.aborted:
            raise psycopg.errors.InFailedSqlTransaction()
        if statement == "EXECUTE cancel_read":
            return Mock(fetchall=lambda: self.rows)
        return Mock()


@pytest.fixture
def protocol(monkeypatch):
    p = Protocol()
    monkeypatch.setattr(c, "always", Mock())
    monkeypatch.setattr(c, "sometimes", Mock())
    monkeypatch.setattr(c.telemetry, "comparison", Mock())
    monkeypatch.setattr(c, "POLL_SECONDS", 0.001)
    yield p
    p.cancel.set()
    if p.allow_read_exit is not None:
        p.allow_read_exit.set()
    if p.entered.is_set():
        assert p.retired.wait(3)


def run(p, seconds: float = 2):
    return c.run(
        p.worker,
        p.controller,
        p.observer,
        deadline=time.monotonic() + seconds,
        table="private_r1.t0",
        index_id="u100",
        expected_rows=ROWS,
        execute=p.execute,
    )


def test_cancelled_registered_peek_recovers_same_session_and_prepared_plan(protocol):
    original_thread = threading.get_ident()
    details = run(protocol)
    assert details == {
        "session": "original",
        "pid": 42,
        "index_id": "u100",
        "peek_ids": ["peek-uuid"],
    }
    assert all(call.args[0] for call in c.always.call_args_list)
    assert c.sometimes.call_count == 2
    protocol.worker.cancel_safe.assert_not_called()
    protocol.worker.close.assert_not_called()
    future = [row for row in protocol.statements if " AS OF " in row[1]]
    assert len(future) == 1 and future[0][3] != original_thread
    cancellation = next(
        row
        for row in protocol.statements
        if row[1].startswith("SELECT pg_cancel_backend")
    )
    assert cancellation[0] is protocol.controller and cancellation[2] == (42,)
    assert all(
        row[3] == original_thread for row in protocol.statements if row is not future[0]
    )
    assert [row[1] for row in protocol.statements][-1] == "DEALLOCATE cancel_read"


def test_negative_backend_pid_uses_unsigned_identity_and_signed_cancellation(protocol):
    protocol.pid = -7
    protocol.identity_after = ("original", -7)
    assert run(protocol)["pid"] == -7
    identity = next(
        row
        for row in protocol.statements
        if row[1].startswith("SELECT id::text FROM mz_internal")
    )
    assert identity[2] == (str((-7) & 0xFFFFFFFF),)
    cancellation = next(
        row
        for row in protocol.statements
        if row[1].startswith("SELECT pg_cancel_backend")
    )
    assert cancellation[2] == (-7,)
