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
from collections import Counter
from contextlib import nullcontext
from typing import Any
from unittest.mock import Mock

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis.drivers import index_handover as h


@pytest.fixture
def db(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(h.state, "open_db", lambda _: sqlite3.connect(":memory:"))
    conn = h.open_state()
    yield conn
    conn.close()


def test_ledger_keeps_only_acknowledged_mutations(monkeypatch, db):
    with db:
        db.execute("INSERT INTO runs (id) VALUES (1)")
    execute = Mock()
    monkeypatch.setattr(h, "_execute", execute)
    h._write(Mock(), db, 1, h.Mutation(1, (1, 2, 7), "insert"), 100)
    h._write(Mock(), db, 1, h.Mutation(2, (2, 2, 7), "insert"), 100)
    h._write(Mock(), db, 1, h.Mutation(1, (1, 3, -8), "update"), 100)
    h._write(Mock(), db, 1, h.Mutation(2, None, "delete"), 100)
    execute.side_effect = psycopg.OperationalError(
        "response lost after possible commit"
    )
    with pytest.raises(psycopg.OperationalError):
        h._write(Mock(), db, 1, h.Mutation(1, (1, 9, 99), "update"), 100)
    assert h.expected_rows(db, 1) == Counter({(1, 3, -8): 1})
    assert db.execute(
        "SELECT count(*) FROM writes WHERE acknowledged=0"
    ).fetchone() == (1,)


def test_stream_applies_all_updates_only_at_a_closed_timestamp():
    stream = h.Stream()
    stream.feed([(10, True, None, None, None, None), (10, False, 1, 1, 0, 7)])
    assert not stream.snapshot_closed
    assert not stream.bag
    stream.feed([(11, True, None, None, None, None), (12, False, -1, 1, 0, 7)])
    assert stream.snapshot_closed
    assert stream.bag == Counter({(1, 0, 7): 1})
    stream.feed([(12, False, 1, 1, 0, -3), (13, True, None, None, None, None)])
    assert stream.bag == Counter({(1, 0, -3): 1})
    assert not stream.errors


@pytest.mark.parametrize(
    "batch, expected",
    [
        ([(9, True, None, None, None, None)], "progress regressed"),
        ([(9, False, 1, 1, 0, 3)], "below closed progress"),
        (
            [(10, False, -1, 1, 0, 3), (11, True, None, None, None, None)],
            "negative multiplicity",
        ),
    ],
)
def test_stream_rejects_invalid_frontiers_and_multiplicities(batch, expected):
    stream = h.Stream()
    stream.feed([(10, True, None, None, None, None)])
    stream.feed(batch)
    assert any(expected in error for error in stream.errors)


def test_stream_requires_initial_progress():
    stream = h.Stream()
    stream.feed([(10, False, 1, 1, 0, 7)])
    assert "subscription data precedes initial progress" in stream.errors


def test_progress_payload_fields_are_ignored():
    stream = h.Stream()
    stream.feed(
        [
            (10, True, "ignored", "ignored", None, object()),
            (11, True, None, None, None, None),
        ]
    )
    assert stream.snapshot_closed
    assert not stream.errors


@pytest.mark.parametrize(
    "row",
    [
        None,
        (None, True, None, None, None, None),
        (10, None, None, None, None, None),
        (10, False, None, 1, 2, 3),
        (10, False, 1, None, 2, 3),
        (10, False, 1, 1, 2, None),
        (10, False, 1),
    ],
)
def test_malformed_stream_rows_are_product_protocol_failures(monkeypatch, row):
    monkeypatch.setattr(h, "_execute", lambda *args: Mock(fetchall=lambda: [row]))
    report = Mock()
    monkeypatch.setattr(h, "always", report)
    with pytest.raises(h.Incomplete, match="violated its stream protocol"):
        h._drain(Mock(), h.Stream(), h.time.monotonic() + 5, None)
    report.assert_called_once()
    assert report.call_args.args[0] is False


