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


@pytest.mark.parametrize("early", [False, True])
@pytest.mark.parametrize("wrong_rows", [False, True])
def test_successful_future_reads_check_immutable_rows_but_never_credit_cancellation(
    protocol, early, wrong_rows
):
    protocol.early_success = early
    protocol.error = None
    protocol.read_rows = list(ROWS[:-1] if wrong_rows else ROWS)
    with pytest.raises(c.Incomplete):
        run(protocol)
    assert c.always.call_count == 1
    assert c.always.call_args.args[0] is not wrong_rows
    c.sometimes.assert_not_called()
    assert not any(row[1] == "ROLLBACK" for row in protocol.statements)


def test_query_error_during_identity_recheck_is_not_hidden_as_an_incomplete_cancel(
    protocol,
):
    protocol.finish_during_identity = True
    protocol.error = psycopg.errors.InternalError_("unexpected execution failure")
    with pytest.raises(
        psycopg.errors.InternalError_, match="unexpected execution failure"
    ):
        run(protocol)
    c.always.assert_not_called()
    c.sometimes.assert_not_called()


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_duplicate",
        "extra_row",
        "wrong_value",
        "wrong_type",
        "wrong_session",
        "not_aborted",
    ],
)
def test_recovery_wrong_results_fail_without_completion_guidance(protocol, mutation):
    if mutation == "missing_duplicate":
        protocol.rows = list(ROWS[:-1])
    elif mutation == "extra_row":
        protocol.rows = [*ROWS, (2, 3, "g")]
    elif mutation == "wrong_value":
        protocol.rows = [(0, None, "g"), (1, 7, "g"), (1, 7, "g")]
    elif mutation == "wrong_type":
        protocol.rows = [(False, None, "g"), *ROWS[1:]]
    elif mutation == "wrong_session":
        protocol.identity_after = ("replacement", 42)
    else:
        protocol.aborted = False
    with pytest.raises(c.Incomplete, match="violated"):
        run(protocol)
    assert any(not call.args[0] for call in c.always.call_args_list)
    c.sometimes.assert_not_called()


@pytest.mark.parametrize(
    "case", ["unseen_peek", "reused_pid", "cancel_rejected", "statement_timeout"]
)
def test_unproven_cancellation_never_credits_recovery(protocol, case):
    if case == "unseen_peek":
        protocol.peek_visible = False
    elif case == "reused_pid":
        protocol.current = ("replacement",)
    elif case == "cancel_rejected":
        protocol.cancel_ack = (False,)
    else:
        protocol.error = TimedOut()
    with pytest.raises(c.Incomplete):
        run(protocol, seconds=0.03 if case == "unseen_peek" else 2)
    c.sometimes.assert_not_called()
    c.always.assert_not_called()
    assert not any(row[1] == "ROLLBACK" for row in protocol.statements)


def test_controller_error_joins_worker_and_preserves_original_failure(protocol):
    error = psycopg.OperationalError("connection lost")
    protocol.controller_error = error
    with pytest.raises(psycopg.OperationalError, match="connection lost"):
        run(protocol)
    assert protocol.retired.is_set()
    protocol.worker.cancel_safe.assert_called_once_with(timeout=1)
    c.sometimes.assert_not_called()


def test_unfinished_read_is_not_reused_or_credited(monkeypatch, protocol):
    protocol.allow_read_exit = threading.Event()
    monkeypatch.setattr(c, "JOIN_GRACE_SECONDS", 0.005)
    with pytest.raises(c.Incomplete, match="did not retire"):
        run(protocol, seconds=0.03)
    assert protocol.retired.is_set()
    protocol.worker.close.assert_called_once_with()
    assert protocol.worker.closed
    assert not any(
        row[1] in ("ROLLBACK", "EXECUTE cancel_read") for row in protocol.statements
    )
    c.sometimes.assert_not_called()


@pytest.mark.parametrize("replacement", [False, True])
def test_retirement_lag_expires_without_false_product_assertion(protocol, replacement):
    protocol.retirement_lag = 100000
    if replacement:
        protocol.retirement_id = "new-peek-uuid"
    with pytest.raises(c.Incomplete, match="retirement was not observed"):
        run(protocol, seconds=0.03)
    assert all(call.args[0] for call in c.always.call_args_list)
    c.sometimes.assert_not_called()


def test_controller_error_closes_reader_if_out_of_band_cancel_does_not_retire_it(
    monkeypatch, protocol
):
    protocol.allow_read_exit = threading.Event()
    protocol.worker.cancel_safe.side_effect = None
    protocol.controller_error = psycopg.OperationalError("controller disconnected")
    monkeypatch.setattr(c, "JOIN_GRACE_SECONDS", 0.005)
    with pytest.raises(
        psycopg.OperationalError, match="controller disconnected"
    ) as captured:
        run(protocol)
    assert captured.value is protocol.controller_error
    assert protocol.retired.is_set()
    protocol.worker.close.assert_called_once_with()
    assert not any(
        row[1] in ("ROLLBACK", "EXECUTE cancel_read") for row in protocol.statements
    )
    c.sometimes.assert_not_called()


@pytest.mark.parametrize("name", ["worker", "controller", "observer"])
@pytest.mark.parametrize("setting", ["autocommit", "transaction_status"])
def test_busy_or_transactional_callers_are_rejected_before_sql(protocol, name, setting):
    conn = getattr(protocol, name)
    if setting == "autocommit":
        conn.autocommit = False
    else:
        conn.info.transaction_status = psycopg.pq.TransactionStatus.INTRANS
    with pytest.raises(ValueError, match="idle in autocommit"):
        run(protocol)
    assert not protocol.statements


@pytest.mark.parametrize("closed", [True, False])
def test_closed_connection_is_an_incomplete_fault_context(protocol, closed):
    protocol.controller.closed = closed
    protocol.controller.info.transaction_status = psycopg.pq.TransactionStatus.UNKNOWN
    with pytest.raises(c.Incomplete, match="unavailable"):
        run(protocol)
    assert not protocol.statements


@pytest.mark.parametrize("rows", [[None], [[0]], [(0, None, ["not text"])]])
def test_malformed_recovery_rows_are_product_mismatches(rows):
    assert not c._rows_match(rows, ROWS)


def test_controller_cannot_share_the_busy_worker_connection(protocol):
    with pytest.raises(ValueError, match="separate controller"):
        c.run(
            protocol.worker,
            protocol.worker,
            protocol.observer,
            deadline=time.monotonic() + 1,
            table="s.t",
            index_id="u1",
            expected_rows=ROWS,
            execute=protocol.execute,
        )
    assert not protocol.statements
