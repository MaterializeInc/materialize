# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from unittest.mock import Mock

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import session_rebinding_model as model

A = ((0, None, "a"), (1, 5, "a"), (1, 5, "a"), (2, -2, "a"))
B = ((0, None, "b"), (1, -7, "b"), (2, 3, "b"))
PARAMETERS = model.Parameters(-2, 3, False)
PA = ((0, None, "a"), (1, -7, "a"), (1, -7, "a"), (2, 7, "a"))
PB = ((0, None, "b"), (1, 17, "b"), (2, -3, "b"))


def test_model_freezes_begun_cursor_and_keeps_session_bindings_separate():
    expected = model.Model((A, B))
    expected.begin_cursor("first", PARAMETERS)
    assert expected.fetch("first", 2) == PA[:2]
    expected.replace(1)
    assert expected.rows("second", PARAMETERS) == PB
    expected.create_temp("first", 0)
    expected.create_temp("second", 1)
    expected.replace(0)
    assert expected.rows("first", PARAMETERS) == PA
    assert expected.rows("second", PARAMETERS) == PB
    assert expected.rows("second", PARAMETERS, qualified=True) == PA
    assert expected.fetch("first", None) == PA[2:]
    assert expected.fetch("first", None) == ()
    expected.commit("first")
    with pytest.raises(KeyError):
        expected.fetch("first", None)
    expected.drop_temp("second")
    assert expected.rows("second", PARAMETERS) == PA


@pytest.mark.parametrize("factor,offset", [(0, -5), (-7, 2), (1, 0), (5, 29)])
def test_parameter_projection_preserves_nulls_duplicates_and_order(factor, offset):
    parameters = model.Parameters(factor, offset, True)
    assert parameters.project(A) == (
        (2, -2 * factor + offset, "a"),
        (1, 5 * factor + offset, "a"),
        (1, 5 * factor + offset, "a"),
        (0, None, "a"),
    )


def test_cursor_rejects_negative_fetch_without_advancing():
    cursor = model.Cursor(PA)
    with pytest.raises(ValueError):
        cursor.fetch(-1)
    assert cursor.fetch(None) == PA


def stamped(rows, timestamp="100"):
    return [(*row, timestamp) for row in rows]


class Schedule:
    def __init__(self):
        self.responses = [
            stamped(PA),
            stamped(PA),
            stamped(PA[:2]),
            stamped(PB, "200"),
            stamped(PA[2:]),
            [],
            stamped(PB, "200"),
            stamped(PA, "200"),
            stamped(PB, "200"),
            stamped(PA, "200"),
            stamped(PB, "200"),
            stamped(PB, "200"),
            stamped(PA, "300"),
            stamped(PB, "300"),
            stamped(PA, "300"),
            stamped(PA, "300"),
        ]
        self.statements: list[str] = []
        self.post_commit = False
        self.return_dead_portal = False
        self.interrupt_at: str | None = None

    def execute(self, conn, deadline, statement, params=None):
        self.statements.append(statement)
        if self.interrupt_at and self.interrupt_at in statement:
            raise psycopg.OperationalError("connection lost")
        if statement.startswith("SELECT id::text FROM"):
            return Mock(fetchone=lambda: ("admin-session",))
        if statement == "COMMIT":
            self.post_commit = True
        if statement.startswith("FETCH") and self.post_commit:
            self.post_commit = False
            if not self.return_dead_portal:
                raise psycopg.errors.InvalidCursorName("cursor does not exist")
            return Mock(fetchall=list)
        if statement.startswith(("EXECUTE", "FETCH")):
            rows = self.responses.pop(0)
            return Mock(fetchall=lambda: rows)
        return Mock()


class CatalogSchedule:
    value = PARAMETERS.factor * 997 + PARAMETERS.offset

    def __init__(self):
        self.statements = []
        self.snapshots = []
        for ending in ("commit", "rollback"):
            prefix = f"txn_{ending}_"
            before = {prefix + "original": "u1", prefix + "dependent": "u2"}
            peer = {**before, prefix + "late": "u3", prefix + "unrelated": "u4"}
            after = dict(peer)
            if ending == "commit":
                after[prefix + "final"] = after.pop(prefix + "original")
            self.snapshots.extend([before, peer, after])
        conflict = {"txn_conflict_original": "u5", "txn_conflict_unrelated": "u6"}
        self.snapshots.extend([conflict, conflict])
        self.commit_count = 0
        self.conflict = True
        self.rows = None
        self.definition = None
        self.interrupt_at = None

    def execute(self, conn, deadline, statement, params=None):
        self.statements.append((conn, statement))
        if self.interrupt_at is not None and self.interrupt_at in statement:
            raise psycopg.OperationalError("connection lost")
        if statement.startswith("SELECT o.name,o.id"):
            snapshot = self.snapshots.pop(0)
            return Mock(fetchall=lambda: list(snapshot.items()))
        if statement.startswith("SELECT value FROM"):
            rows = self.rows
            if rows is None:
                rows = [(self.value + int(statement.endswith("unrelated")),)]
            return Mock(fetchall=lambda: rows)
        if statement.startswith("SELECT * FROM"):
            return Mock(fetchall=lambda: [(self.value, None)])
        if statement.startswith("SHOW CREATE VIEW"):
            target = "final" if "txn_commit_" in statement else "original"
            prefix = "txn_commit_" if "txn_commit_" in statement else "txn_rollback_"
            definition = (
                self.definition
                or f"CREATE VIEW s.{prefix}late AS SELECT value FROM s.{prefix}{target}"
            )
            return Mock(fetchone=lambda: ("late", definition))
        if statement.startswith("SHOW CREATE TABLE"):
            return Mock(
                fetchone=lambda: ("original", "CREATE TABLE original(value bigint)")
            )
        if statement == "COMMIT":
            self.commit_count += 1
            if self.commit_count == 2 and self.conflict:
                raise psycopg.errors.SerializationFailure("catalog changed")
        return Mock()
