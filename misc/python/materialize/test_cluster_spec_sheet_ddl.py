# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import psycopg
import pytest

from materialize.cluster_spec_sheet_ddl import DdlMeasurement, ddl_statements


class Connection:
    def __init__(self, fail=False):
        self.statements = []
        self.fail = fail
        self.closed = False

    def execute(self, statement):
        text = statement if isinstance(statement, str) else statement.as_string()
        self.statements.append(text)
        if self.fail and text.startswith("CREATE"):
            raise psycopg.OperationalError("lost response")

    def close(self):
        self.closed = True


def test_batches_use_persistent_connections_and_record_every_statement():
    connections = []

    def connect():
        connection = Connection()
        connections.append(connection)
        return connection

    events = []
    driver = DdlMeasurement(connect, 4, "probe", "c", events.append)
    try:
        created = driver.run("table", "create", slots=3)
        dropped = driver.run("table", "drop", slots=3)
    finally:
        driver.close()
    assert len(connections) == 4
    for result in (created, dropped):
        assert result["successful_statements"] == 12
        assert result["errors"] == 0
        assert len(result["samples"]) == 12
        assert result["elapsed_seconds"] >= max(
            sample["start_offset_seconds"] + sample["latency_seconds"]
            for sample in result["samples"]
        )
    assert all(c.closed and len(c.statements) == 7 for c in connections)
    creates = [q for c in connections for q in c.statements if q.startswith("CREATE")]
    assert len(set(creates)) == 12


def test_failed_ddl_is_reported_without_retry():
    connection = Connection(fail=True)
    events = []
    driver = DdlMeasurement(lambda: connection, 1, "probe", "c", events.append)
    try:
        with pytest.raises(RuntimeError, match="1 errors"):
            driver.run("view", "cycle", duration=1)
    finally:
        driver.close()
    assert len(connection.statements) == 2  # Session setup and one failed CREATE.
    assert events[-1]["errors"] == 1
    assert events[-1]["successful_statements"] == 0
    assert events[-1]["samples"][0]["error"] == "OperationalError"


def test_index_name_is_unqualified_and_identifiers_are_quoted():
    create, drop = ddl_statements("index", 's"x', "cluster", 'i"x')
    assert create.as_string() == (
        'CREATE INDEX "i""x" IN CLUSTER "cluster" ON "s""x"."base" (a)'
    )
    assert drop.as_string() == 'DROP INDEX "s""x"."i""x"'
