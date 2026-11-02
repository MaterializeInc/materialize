# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from contextlib import AbstractContextManager
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import psycopg
import pytest
from psycopg.pq import TransactionStatus

pytest.importorskip("antithesis")

from materialize.antithesis import copy_recovery as h

ROWS = ((1, None, "seed"), (2, 3, "peer"))
BATCH = (
    (1, None, "seed"),
    (2, 3, "peer"),
    (1, None, "seed"),
    (1, None, None),
    (1, None, r"\N"),
    (1, -7, "tab\tline\nslash\\return\r雪"),
)


class Session:
    def __init__(self, engine):
        self.engine = engine
        self.autocommit = True
        self.info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)
        self.pgconn = Mock()
        self.pending = []
        self.read_only = False

    def cursor(self):
        return Cursor(self)


class Cursor(AbstractContextManager):
    def __init__(self, conn):
        self.connection = conn
        self.rowcount = -1

    def __exit__(self, *exc):
        return False

    def copy(self, statement, *, writer):
        return Copy(self, writer)


class Copy(AbstractContextManager):
    def __init__(self, cursor, writer):
        self.cursor = cursor
        self.writer = writer
        self.session = cursor.connection
        self.engine = self.session.engine

    def __enter__(self):
        self.engine.open_copy = True
        self.engine.copy_calls += 1
        self.engine.copy_tokens.append(self.writer.token)
        return self

    def write(self, data):
        self.writer.sent_bytes += len(data)
        self.writer.sent_chunks += 1

    def __exit__(self, exc_type, exc, traceback):
        self.engine.open_copy = False
        if isinstance(exc, h._AbortCopy):
            self.writer.confirmed_abort = self.engine.problem != "unconfirmed_abort"
            if self.engine.problem == "abort_leak":
                self.engine.committed.extend(BATCH[:1])
            self.session.info.transaction_status = (
                TransactionStatus.INERROR
                if self.session.info.transaction_status == TransactionStatus.INTRANS
                else TransactionStatus.IDLE
            )
            return False
        if exc is not None:
            return False
        if self.session.read_only:
            if self.engine.problem == "readonly_leak":
                self.engine.committed.extend(BATCH)
            self.session.info.transaction_status = TransactionStatus.INERROR
            raise psycopg.errors.ReadOnlySqlTransaction(
                "transaction in read-only mode",
                info={ord("M"): b"transaction in read-only mode"},
            )
        target = (
            self.session.pending
            if self.session.info.transaction_status == TransactionStatus.INTRANS
            else self.engine.committed
        )
        target.extend(BATCH)
        if self.engine.problem == "duplicates_collapsed":
            target[:] = list(dict.fromkeys(target))
        self.cursor.rowcount = len(BATCH) + int(self.engine.problem == "wrong_rowcount")
        if self.engine.problem == "lost_copy_done_ack":
            raise psycopg.OperationalError("response lost after commit")
        return False


class Engine:
    def __init__(self):
        self.committed = []
        self.open_copy = False
        self.copy_calls = 0
        self.copy_tokens = []
        self.problem = None
        self.statements = []

    def ddl(self, conn, deadline, text):
        self.statements.append(text)

    def execute(self, conn, deadline, text, params=None):
        self.statements.append(text)
        if text.startswith("INSERT"):
            assert params is not None
            self.committed.extend(
                tuple(params[i : i + 3]) for i in range(0, len(params), 3)
            )
        if text.startswith("BEGIN"):
            conn.info.transaction_status = TransactionStatus.INTRANS
            conn.read_only = text == "BEGIN READ ONLY"
        if text in ("COMMIT", "ROLLBACK"):
            if text == "COMMIT" or self.problem == "rollback_leak":
                self.committed.extend(conn.pending)
            conn.pending.clear()
            conn.read_only = False
            conn.info.transaction_status = TransactionStatus.IDLE
        rows = list(self.committed)
        if self.open_copy and self.problem == "premature_visibility":
            rows.extend(BATCH)
        if self.problem == "bad_type":
            rows = [(True if key == 1 else key, value, tag) for key, value, tag in rows]
        if self.problem == "wrong_null" and len(rows) > len(ROWS):
            rows = [
                (key, value, r"\N" if tag is None else tag) for key, value, tag in rows
            ]
        if self.problem == "malformed_row":
            rows = [None]
        if text.startswith('EXECUTE "Copy Recovery"'):
            rows = [(len(ROWS) if self.problem == "stale_prepared" else len(rows),)]
        return Mock(fetchall=lambda: rows)


@pytest.fixture
def protocol(monkeypatch):
    engine = Engine()
    monkeypatch.setattr(h, "always", Mock())
    monkeypatch.setattr(h, "sometimes", Mock())
    monkeypatch.setattr(h.telemetry, "comparison", Mock())
    return engine


def run(engine, **kwargs):
    sessions = [cast(psycopg.Connection, Session(engine)) for _ in range(3)]
    return h.run_round(
        *sessions,
        schema="owned_round",
        rows=ROWS,
        deadline=float("inf"),
        execute=engine.execute,
        ddl=engine.ddl,
        **kwargs,
    )


def test_copy_codec_distinguishes_null_literal_tokens_and_escaped_delimiters():
    assert h.encode_rows(
        ((1, None, None), (-7, 2, r"\N"), (3, 4, "tab\tline\nslash\\return\r雪"))
    ) == (
        b"1\t\\N\t\\N\n-7\t2\t\\\\N\n3\t4\ttab\\tline\\nslash\\\\return\\r\xe9\x9b\xaa\n"
    )


def test_copy_protocol_checks_aborts_before_crediting_successful_session_reuse(
    protocol,
):
    facts = run(protocol, chunk_bytes=1)
    assert facts == {
        "abort_bytes": 2 * h.DEFAULT_ABORT_BYTES,
        "abort_chunks": 2,
        "rows_per_copy": len(BATCH),
        "committed_copy_rows": 2 * len(BATCH),
    }
    assert protocol.committed == list(ROWS) + list(BATCH) * 2
    assert protocol.copy_calls == 6
    assert len(set(protocol.copy_tokens)) == protocol.copy_calls
    assert protocol.statements.count("ROLLBACK") == 3
    assert protocol.statements.count("COMMIT") == 1
    assert protocol.statements.count('EXECUTE "Copy Recovery"') == 6
    assert all(call.args[0] for call in h.always.call_args_list)
    assert [call.args[0] for call in h.sometimes.call_args_list] == [True] * 4 + [False]


@pytest.mark.parametrize(
    "problem",
    [
        "premature_visibility",
        "abort_leak",
        "rollback_leak",
        "readonly_leak",
        "duplicates_collapsed",
        "wrong_rowcount",
        "unconfirmed_abort",
        "bad_type",
        "malformed_row",
        "wrong_null",
        "stale_prepared",
    ],
)
def test_copy_wrong_result_mutants_cannot_complete(protocol, problem):
    protocol.problem = problem
    with pytest.raises(h.Incomplete, match="violated"):
        run(protocol)
    assert any(not call.args[0] for call in h.always.call_args_list)
    h.sometimes.assert_not_called()


def test_lost_copy_done_ack_never_drives_later_expected_results(protocol):
    protocol.problem = "lost_copy_done_ack"
    with pytest.raises(psycopg.OperationalError, match="response lost"):
        run(protocol)
    assert protocol.committed == list(ROWS) + list(BATCH)
    assert protocol.copy_calls == 3
    h.sometimes.assert_not_called()
    assert all(call.args[0] for call in h.always.call_args_list)


def test_large_abort_opportunity_is_bounded_and_does_not_expand_successful_bags(
    protocol,
):
    facts = run(protocol, abort_bytes=h.LARGE_ABORT_BYTES)
    assert facts["abort_bytes"] == h.LARGE_ABORT_BYTES + h.DEFAULT_ABORT_BYTES
    assert facts["abort_chunks"] < 300
    assert len(protocol.committed) == len(ROWS) + 2 * len(BATCH)
    assert all(call.args[0] for call in h.sometimes.call_args_list)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"abort_bytes": 0},
        {"abort_bytes": h.MAX_ABORT_BYTES + 1},
        {"chunk_bytes": 0},
        {"chunk_bytes": h.MAX_CHUNK_BYTES + 1},
    ],
)
def test_copy_plan_rejects_unbounded_or_empty_chunks_before_ddl(protocol, kwargs):
    with pytest.raises(ValueError):
        run(protocol, **kwargs)
    assert protocol.statements == []


@pytest.mark.parametrize(
    "problem",
    ["worker_not_autocommit", "peer_not_idle", "same_worker_peer", "same_admin_worker"],
)
def test_copy_preconditions_reject_bad_callers_without_sql_or_product_failures(
    protocol, problem
):
    sessions = [Session(protocol) for _ in range(3)]
    if problem == "worker_not_autocommit":
        sessions[1].autocommit = False
    elif problem == "peer_not_idle":
        sessions[2].info.transaction_status = TransactionStatus.INTRANS
    elif problem == "same_worker_peer":
        sessions[2] = sessions[1]
    elif problem == "same_admin_worker":
        sessions[1] = sessions[0]
    with pytest.raises(ValueError, match="distinct idle autocommit"):
        h.run_round(
            *[cast(psycopg.Connection, session) for session in sessions],
            schema="owned_round",
            rows=ROWS,
            deadline=float("inf"),
            execute=protocol.execute,
            ddl=protocol.ddl,
        )
    assert protocol.statements == []
    h.always.assert_not_called()
    h.sometimes.assert_not_called()
