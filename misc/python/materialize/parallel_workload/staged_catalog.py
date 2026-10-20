# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Deterministic catalog mutations between preparation and completion."""

import re
import time
from collections.abc import Iterator
from concurrent.futures import Future
from contextlib import contextmanager, nullcontext
from threading import Thread

from psycopg import Connection, Error

from materialize.mzcompose.composition import Composition
from materialize.parallel_workload.catalog_transactions import execute, outcome

PREPARATION_LOG_FILTER = "info,mz_adapter::catalog_preparation=debug"


@contextmanager
def paused_preparation(
    c: Composition, kind: str, statement: str, connection: Connection | None = None
) -> Iterator[Future[None]]:
    """Pause preparation, releasing and joining on exit without closing a caller's connection."""
    failpoint = f"catalog_{kind}_ready"
    with (
        c.sql_connection(user="mz_system", port=6877) as control,
        (
            nullcontext(connection)
            if connection is not None
            else c.sql_connection(startup_params={"statement_timeout": "60s"})
        ) as conn,
    ):
        result: Future[None] = Future()

        def run() -> None:
            try:
                execute(conn, statement)
            except BaseException as error:
                result.set_exception(error)
            else:
                result.set_result(None)

        worker = Thread(target=run, daemon=True)
        initial_logs = c.invoke(
            "logs",
            "--no-color",
            "materialized",
            capture=True,
            capture_stderr=True,
            silent=True,
        )
        initial_stdout_lines = len(initial_logs.stdout.splitlines())
        initial_stderr_lines = len(initial_logs.stderr.splitlines())
        execute(control, f"SET failpoints = '{failpoint}=return'")
        started = False
        try:
            worker.start()
            started = True
            deadline = time.monotonic() + 30
            while True:
                output = c.invoke(
                    "logs",
                    "--no-color",
                    "materialized",
                    capture=True,
                    capture_stderr=True,
                    silent=True,
                )
                logs = "\n".join(
                    output.stdout.splitlines()[initial_stdout_lines:]
                    + output.stderr.splitlines()[initial_stderr_lines:]
                )
                logs = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", logs)
                if any(
                    "catalog preparation complete" in line
                    and "mz_adapter::catalog_preparation" in line
                    and re.search(rf'\bkind="?{kind}"?(?:\s|$)', line)
                    and re.search(rf"\bconn_id={conn.info.backend_pid}\b", line)
                    for line in logs.splitlines()
                ):
                    break
                if result.done():
                    result.result()
                    raise AssertionError("statement completed without pausing")
                if time.monotonic() >= deadline:
                    raise AssertionError(
                        f"no {kind} preparation event for PID {conn.info.backend_pid}: "
                        f"{logs[-4000:]}"
                    )
                time.sleep(0.1)
            yield result
        finally:
            try:
                execute(control, f"SET failpoints = '{failpoint}=off'")
            finally:
                if started:
                    worker.join(timeout=90)
                    assert not worker.is_alive(), "statement exceeded its timeout"


def purification_connection_change(c: Composition) -> None:
    with c.sql_connection(
        service="postgres", user="postgres", password="postgres", database="postgres"
    ) as upstream:
        for database in ("a", "b"):
            execute(upstream, f"CREATE DATABASE {database}")
    for database, columns in (("a", "value_a integer"), ("b", "value_b text")):
        with c.sql_connection(
            service="postgres", user="postgres", password="postgres", database=database
        ) as upstream:
            execute(upstream, f"CREATE TABLE input_{database} ({columns})")
            execute(upstream, f"ALTER TABLE input_{database} REPLICA IDENTITY FULL")
            execute(
                upstream, f"CREATE PUBLICATION staged_pub FOR TABLE input_{database}"
            )

    for case in ("success", "stale_failure", "current_failure"):
        if case == "stale_failure":
            with c.sql_connection(
                service="postgres", user="postgres", password="postgres", database="a"
            ) as upstream:
                execute(upstream, "DROP PUBLICATION staged_pub")
        schema = f"staged_{case}"
        with c.sql_connection(startup_params={"statement_timeout": "60s"}) as conn:
            execute(conn, f"CREATE SCHEMA {schema}")
            execute(conn, f"CREATE SECRET {schema}.password AS 'postgres'")
            execute(
                conn,
                f"CREATE CONNECTION {schema}.pg TO POSTGRES (HOST 'postgres', DATABASE 'a', "
                f"USER 'postgres', PASSWORD SECRET {schema}.password)",
            )
            objects_query = (
                "SELECT o.id, o.name FROM mz_objects o JOIN mz_schemas s ON s.id = o.schema_id "
                f"WHERE s.name = '{schema}' ORDER BY o.id"
            )
            before = execute(conn, objects_query).fetchall()
            with paused_preparation(
                c,
                "purification",
                f"CREATE SOURCE {schema}.source FROM POSTGRES CONNECTION {schema}.pg "
                "(PUBLICATION 'staged_pub') FOR ALL TABLES",
            ) as result:
                if case != "current_failure":
                    execute(conn, f"ALTER CONNECTION {schema}.pg SET (DATABASE 'b')")
                assert execute(conn, "SELECT 7").fetchall() == [(7,)]
            if case == "current_failure":
                try:
                    result.result()
                except Error as error:
                    assert "publication staged_pub does not exist" in str(error), error
                    assert error.sqlstate != "40001", error
                else:
                    raise AssertionError("missing publication must fail preparation")
                assert execute(conn, objects_query).fetchall() == before
                continue
            result.result()
            exports = execute(
                conn,
                "SELECT p.schema_name, p.table_name, t.name, col.name, col.type "
                "FROM mz_internal.mz_postgres_source_tables p "
                "JOIN mz_relations t ON t.id = p.id "
                "JOIN mz_schemas s ON s.id = t.schema_id "
                "JOIN mz_columns col ON col.id = t.id "
                f"WHERE s.name = '{schema}' ORDER BY t.name, col.position",
            ).fetchall()
            assert exports == [
                ("public", "input_b", "input_b", "value_b", "text")
            ], exports

    with c.sql_connection(
        service="postgres", user="postgres", password="postgres", database="a"
    ) as upstream:
        execute(upstream, "CREATE PUBLICATION staged_pub FOR TABLE input_a")
    for collision in (False, True):
        final_commit_connection_change(c, collision)


def final_commit_connection_change(c: Composition, collision: bool) -> None:
    schema = f"staged_commit_{'collision' if collision else 'success'}"
    with (
        c.sql_connection(startup_params={"statement_timeout": "60s"}) as txn,
        c.sql_connection(startup_params={"statement_timeout": "60s"}) as other,
    ):
        execute(other, f"CREATE SCHEMA {schema}")
        execute(other, f"CREATE SECRET {schema}.password AS 'postgres'")
        execute(
            other,
            f"CREATE CONNECTION {schema}.pg TO POSTGRES (HOST 'postgres', DATABASE 'b', "
            f"USER 'postgres', PASSWORD SECRET {schema}.password)",
        )
        execute(other, f"CREATE VIEW {schema}.original AS SELECT 1")
        objects_query = (
            "SELECT o.id, o.name FROM mz_objects o JOIN mz_schemas s ON s.id = o.schema_id "
            f"WHERE s.name = '{schema}' ORDER BY o.id"
        )
        before = execute(other, objects_query).fetchall()
        batch = [
            f"ALTER VIEW {schema}.original RENAME TO first",
            f"ALTER VIEW {schema}.first RENAME TO second",
            f"CREATE SOURCE {schema}.source FROM POSTGRES CONNECTION {schema}.pg "
            "(PUBLICATION 'staged_pub') FOR ALL TABLES",
            f"ALTER VIEW {schema}.second RENAME TO final",
        ]
        execute(txn, "BEGIN")
        for statement in batch:
            execute(txn, statement)
        # Invalidate the source's preparation so COMMIT must prepare it again.
        execute(other, f"ALTER CONNECTION {schema}.pg SET (DATABASE 'a')")
        with paused_preparation(c, "purification", "COMMIT", txn) as result:
            assert execute(other, objects_query).fetchall() == before
            execute(other, f"ALTER CONNECTION {schema}.pg SET (DATABASE 'b')")
            execute(other, f"DROP VIEW {schema}.original")
            execute(other, f"CREATE VIEW {schema}.original AS SELECT 2")
            if collision:
                execute(other, f"CREATE VIEW {schema}.final AS SELECT 3")
            after_interference = execute(other, objects_query).fetchall()
            assert execute(other, "SELECT 7").fetchall() == [(7,)]
            assert not result.done(), "COMMIT completed while preparation was paused"

        if collision:
            try:
                result.result()
            except Error as error:
                assert "already exists" in str(error) and "final" in str(error), error
                concurrent = (error.sqlstate, error.diag.message_primary)
            else:
                raise AssertionError("suffix name collision must fail COMMIT")
            assert execute(other, objects_query).fetchall() == after_interference
            with c.sql_connection(
                startup_params={"statement_timeout": "60s"}
            ) as reference:
                serial = outcome(reference, ["BEGIN", *batch, "COMMIT"])
            assert concurrent == serial, (concurrent, serial)
            assert execute(other, objects_query).fetchall() == after_interference
        else:
            result.result()
            assert execute(other, f"SELECT * FROM {schema}.final").fetchall() == [(2,)]
            objects = execute(other, objects_query).fetchall()
            names = {name for _, name in objects}
            assert {"source", "input_b", "final"} <= names, objects
            assert not names & {"original", "first", "second"}, objects
            exports = execute(
                other,
                "SELECT p.schema_name, p.table_name, t.name, col.name, col.type "
                "FROM mz_internal.mz_postgres_source_tables p "
                "JOIN mz_relations t ON t.id = p.id "
                "JOIN mz_schemas s ON s.id = t.schema_id "
                "JOIN mz_columns col ON col.id = t.id "
                f"WHERE s.name = '{schema}' ORDER BY t.name, col.position",
            ).fetchall()
            assert exports == [
                ("public", "input_b", "input_b", "value_b", "text")
            ], exports
