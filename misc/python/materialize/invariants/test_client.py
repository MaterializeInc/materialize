# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from materialize.invariants import mz
from materialize.invariants.checkers import SubscribeChecker
from materialize.invariants.framework import (
    COMPLEXITIES,
    Endpoints,
    Outcome,
    ScenarioContext,
    TransientError,
)
from materialize.invariants.mz import ConnectFailed, MzClient, UnexpectedQueryError


@pytest.fixture
def ctx():
    return ScenarioContext(
        Endpoints("localhost", 6875, 6877),
        COMPLEXITIES["low"],
        random.Random(0),
        Mock(),
        "0",
    )


@pytest.fixture
def connection(ctx):
    reader, writer = socket.socketpair()
    with reader, writer:
        conn = Mock(closed=False)
        conn.fileno.return_value = reader.fileno()
        conn.execute.return_value.description = None
        client = MzClient(ctx, "test")
        try:
            with patch.object(mz.psycopg, "connect", return_value=conn) as connect:
                yield client, conn, connect, reader
        finally:
            client.close()


def test_disconnected_transaction_cannot_autocommit_its_suffix(connection):
    client, conn, connect, _ = connection
    bounded = client._bounded

    def interrupt_after_first_insert(sql, params, timeout, session):
        result = bounded(sql, params, timeout, session)
        if sql == "INSERT 1":
            client.hard_close()
        return result

    with patch.object(client, "_bounded", interrupt_after_first_insert):
        outcome = client.write_txn([("INSERT 1", None), ("INSERT 2", None)])
    assert outcome == Outcome.FAILED
    assert [call.args[0] for call in conn.execute.call_args_list] == [
        b"BEGIN",
        b"INSERT 1",
    ]
    connect.assert_called_once()


def test_next_query_observes_disconnect_in_open_transaction(connection):
    client, conn, connect, _ = connection
    client.query("BEGIN")
    client.hard_close()
    with pytest.raises(ConnectFailed):
        client.query("INSERT 1")
    connect.assert_called_once()
    conn.execute.assert_called_once_with(b"BEGIN", None)


def test_watchdog_interrupts_unacknowledged_cancellation(connection, monkeypatch):
    client, conn, _, reader = connection
    entered = threading.Event()
    now = 0.0
    monkeypatch.setattr(mz, "time", SimpleNamespace(monotonic=lambda: now))
    monkeypatch.setattr(mz.psycopg.capabilities, "has_cancel_safe", lambda: True)

    def execute(sql, params):
        entered.set()
        assert reader.recv(1) == b""
        return Mock(description=None)

    conn.execute.side_effect = execute
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(client.query, "SELECT 1", timeout=1)
        try:
            assert entered.wait(2)
            now = 2.0
            client.maybe_cancel()
            conn.cancel_safe.assert_called_once()
            assert not future.done()
            now = 8.0
            client.maybe_cancel()
            assert future.result(timeout=2) == []
        finally:
            client.hard_close()

    client.close()
    assert not client._watchdog.is_alive()
    with pytest.raises(ConnectFailed):
        client.query("SELECT 1")


class _Subscription(SubscribeChecker):
    def validate_state(self, state: dict[tuple, int], ts: int) -> None:
        pass


def test_expired_checkpoint_restarts_with_a_fresh_snapshot(ctx):
    with patch("materialize.invariants.checkers.MzClient") as client_type:
        checker = _Subscription(ctx.rng, ctx, "test", "SELECT 1", durable=True)
    client = client_type.return_value
    checker.last_validated_ts = 9
    checker._state[(17,)] = 1
    expired = UnexpectedQueryError("could not find a valid timestamp for the query")
    client.query.side_effect = [[], [], [], expired, [], [], [], []]
    with pytest.raises(TransientError, match="checkpoint expired"):
        checker.check_once()
    checker.check_once()

    statements = [call.args[0] for call in client.query.call_args_list]
    assert "SNAPSHOT = false" in statements[2] and "AS OF 8" in statements[2]
    assert "SNAPSHOT = false" not in statements[6] and "AS OF" not in statements[6]
    assert not checker._state
    assert checker.resumes == 0


@pytest.mark.parametrize("failure", ["declare", "first_fetch", "later_fetch"])
def test_resume_surfaces_unexpected_errors(ctx, failure):
    with patch("materialize.invariants.checkers.MzClient") as client_type:
        checker = _Subscription(ctx.rng, ctx, "test", "SELECT 1", durable=True)
    checker.last_validated_ts = 9
    expired = UnexpectedQueryError("could not find a valid timestamp for the query")
    responses = {
        "declare": [[], [], expired],
        "first_fetch": [[], [], [], UnexpectedQueryError("unexpected server error")],
        "later_fetch": [[], [], [], [], expired],
    }
    client_type.return_value.query.side_effect = responses[failure]
    if failure == "later_fetch":
        checker.check_once()
    with pytest.raises(UnexpectedQueryError):
        checker.check_once()
