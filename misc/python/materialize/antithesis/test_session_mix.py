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
from unittest.mock import MagicMock, patch

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis.drivers import session_mix


def session() -> session_mix.Session:
    return session_mix.Session(
        "host",
        MagicMock(),
        0,
        session_mix.SessionConfig(session_mix.STRICT, True, "1s", "0"),
        [("c", None)],
        session_mix.RaceLog(),
        True,
    )


def test_deadline_waits_for_inflight_cancel_before_connection_reuse() -> None:
    cancelling = threading.Event()
    release = threading.Event()
    body_done = threading.Event()
    exited = threading.Event()

    def cancel(**kwargs: object) -> None:
        cancelling.set()
        release.wait(5)

    conn = MagicMock()
    conn.cancel_safe.side_effect = cancel

    def run() -> None:
        with session_mix.ClientDeadline(conn, 0):
            if not cancelling.wait(5):
                return
            body_done.set()
        exited.set()

    worker = threading.Thread(target=run)
    worker.start()
    try:
        assert body_done.wait(5), "the cancellation must start inside the operation"
        assert not exited.wait(0.1), "the next operation could receive the old cancel"
    finally:
        release.set()
        worker.join(5)
    assert exited.is_set()


def test_failed_session_setup_closes_its_connection() -> None:
    s = session()
    conn = MagicMock(closed=False)
    conn.execute.side_effect = psycopg.OperationalError("connection lost")
    with (
        patch.object(session_mix.sql, "connect", return_value=conn),
        patch.object(session_mix.time, "sleep"),
    ):
        s.run(1, float("inf"))
    conn.close.assert_called_once()


def test_retarget_reports_unexpected_server_errors() -> None:
    s = session()
    s.conn = MagicMock(closed=False)
    with (
        patch.object(session_mix.rng, "random", return_value=0),
        patch.object(
            s, "apply_target", side_effect=psycopg.errors.InternalError_("oops")
        ),
        patch.object(session_mix, "unreachable") as violation,
    ):
        s.run(1, float("inf"))
    violation.assert_called_once()


def test_failed_upstream_write_does_not_report_successful_recency_read() -> None:
    s = session()
    s.conn = MagicMock()
    with (
        patch.object(session_mix, "ClientDeadline"),
        patch.object(s, "rtr_read", return_value={"marker": None}),
        patch.object(s, "report_reached") as reached,
    ):
        s.run_op("rtr_read")
    assert reached.call_args.args[2] is False
