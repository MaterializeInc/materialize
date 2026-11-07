# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import socket
import time
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest
from psycopg import waiting
from psycopg.generators import WAIT_R

pytest.importorskip("antithesis")

from materialize.antithesis import copy_recovery as c


class Cancelled(psycopg.errors.QueryCanceled):
    def __init__(self, message):
        super().__init__(message)
        self.message = message

    @property
    def diag(self):
        return SimpleNamespace(message_primary=self.message)


def completed(value=None, error=None):
    if False:
        yield
    if error is not None:
        raise error
    return value


@pytest.fixture
def writer(monkeypatch):
    connection = Mock()
    cursor = Mock(connection=connection)
    cursor._results = []

    def wait(operation, interval):
        with pytest.raises(StopIteration) as stopped:
            next(operation)
        return stopped.value.value

    connection.wait.side_effect = wait
    monkeypatch.setattr(c, "always", Mock())
    monkeypatch.setattr(c.telemetry, "comparison", Mock())
    return c._Writer(cursor, time.monotonic() + 10, "own-token")


@pytest.mark.parametrize(
    "message,confirmed",
    [
        ("COPY from stdin failed: own-token", True),
        ("COPY from stdin failed: previous-token", False),
        ("canceling statement due to statement timeout", False),
        ("canceling statement due to user request", False),
    ],
)
def test_only_exact_copyfail_acknowledgment_establishes_no_write(
    monkeypatch, writer, message, confirmed
):
    error = Cancelled(message)
    end = Mock(side_effect=lambda *args: completed(error=error))
    monkeypatch.setattr(c, "copy_end", end)
    if confirmed:
        writer.finish(c._AbortCopy())
    else:
        with pytest.raises(Cancelled) as raised:
            writer.finish(c._AbortCopy())
        assert raised.value is error
    end.assert_called_once_with(writer._pgconn, b"own-token")
    assert writer.confirmed_abort is confirmed
    c.always.assert_not_called()


@pytest.mark.parametrize("abort", [False, True])
def test_lost_copydone_or_copyfail_ack_never_confirms_completion(
    monkeypatch, writer, abort
):
    error = psycopg.OperationalError("connection lost after completion frame")
    monkeypatch.setattr(c, "copy_end", lambda *args: completed(error=error))
    with pytest.raises(psycopg.OperationalError) as raised:
        writer.finish(c._AbortCopy() if abort else None)
    assert raised.value is error
    assert not writer.confirmed_abort
    assert writer.cursor._results == []
    c.always.assert_not_called()


def test_success_response_to_copyfail_is_a_product_mismatch(monkeypatch, writer):
    monkeypatch.setattr(c, "copy_end", lambda *args: completed(Mock()))
    with pytest.raises(c.Incomplete, match="violated"):
        writer.finish(c._AbortCopy())
    assert not writer.confirmed_abort
    assert any(not call.args[0] for call in c.always.call_args_list)


def test_native_wait_deadline_closes_and_poisoned_finish_never_sends_late_frames(
    monkeypatch, writer
):
    left, right = socket.socketpair()
    calls = []

    def blocked():
        while True:
            assert time.monotonic() < writer.deadline, "protocol resumed after deadline"
            calls.append("resume")
            yield WAIT_R

    writer.connection.wait.side_effect = lambda operation, interval: waiting.wait(
        operation, left.fileno(), interval=interval
    )
    writer.deadline = time.monotonic() + 0.04
    try:
        with pytest.raises(c.Incomplete, match="deadline elapsed"):
            writer._wait(blocked())
        assert writer.poisoned
        writer.connection.close.assert_called_once()
        assert len(calls) <= 1
        end = Mock(side_effect=AssertionError("late completion frame"))
        monkeypatch.setattr(c, "copy_end", end)
        for error in (None, c._AbortCopy()):
            with pytest.raises(c.Incomplete, match="closed after its deadline"):
                writer.finish(error)
        end.assert_not_called()
        assert not writer.confirmed_abort
    finally:
        left.close()
        right.close()
