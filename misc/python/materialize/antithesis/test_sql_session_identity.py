# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sqlite3

import pytest

psycopg = pytest.importorskip("psycopg")

from materialize.antithesis import sql


@pytest.mark.parametrize("connection_id", [0, 2**31 - 1, 2**31, 2**32 - 1])
def test_session_lookup_preserves_unsigned_connection_identity(connection_id):
    pid = connection_id if connection_id < 2**31 else connection_id - 2**32
    expression = sql.SESSION_CONNECTION_ID.replace("::bigint", "").replace(
        "::uint4", ""
    )
    with sqlite3.connect(":memory:") as db:
        db.create_function("pg_backend_pid", 0, lambda: pid)
        db.execute("CREATE TABLE sessions(connection_id INTEGER, id TEXT)")
        db.executemany(
            "INSERT INTO sessions VALUES (?,?)",
            [(connection_id, "original"), (connection_id ^ 2**31, "peer")],
        )
        assert db.execute(
            f"SELECT id FROM sessions WHERE connection_id={expression}"
        ).fetchall() == [("original",)]


@pytest.mark.parametrize(
    "message",
    [
        'cannot drop index "ix6_t1": still depended upon by materialized view "a",'
        ' materialized view "b"',
        'cannot drop indexes "i1", "i2": still depended upon by index "j"',
    ],
)
def test_drop_of_an_index_in_use_is_a_rejection(message):
    c = sql.classify(psycopg.errors.DependentObjectsStillExist(message))
    assert c.outcome is sql.Outcome.REJECTED
    assert c.race is sql.CatalogRace.DEPENDED_UPON


def test_other_dependent_objects_errors_stay_violations():
    c = sql.classify(
        psycopg.errors.DependentObjectsStillExist(
            'role "r" cannot be dropped because some objects depend on it'
        )
    )
    assert c.outcome is sql.Outcome.VIOLATION


@pytest.mark.parametrize(
    "message,rtr_outcome",
    [
        ("storage error: error communicating with the server", sql.Outcome.REJECTED),
        ("storage error: error connecting to server", sql.Outcome.REJECTED),
        ("storage error: db error", sql.Outcome.VIOLATION),
    ],
)
def test_upstream_transport_errors_are_rejections_only_for_rtr_reads(
    message, rtr_outcome
):
    error = psycopg.errors.InternalError_(message)
    assert sql.classify_rtr_read(error).outcome is rtr_outcome
    assert sql.classify(error).outcome is sql.Outcome.VIOLATION
