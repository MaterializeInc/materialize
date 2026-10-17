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
from contextlib import contextmanager
from threading import Thread

from materialize.mzcompose.composition import Composition
from materialize.parallel_workload.catalog_transactions import execute

PREPARATION_LOG_FILTER = "info,mz_adapter::catalog_preparation=debug"


@contextmanager
def paused_preparation(
    c: Composition, kind: str, statement: str
) -> Iterator[Future[None]]:
    """Yield after this statement's preparation event, releasing and joining on exit."""
    failpoint = f"catalog_{kind}_ready"
    with (
        c.sql_connection(user="mz_system", port=6877) as control,
        c.sql_connection(startup_params={"statement_timeout": "60s"}) as conn,
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

    with c.sql_connection(startup_params={"statement_timeout": "60s"}) as conn:
        execute(conn, "CREATE SCHEMA staged")
        execute(conn, "CREATE SECRET staged.password AS 'postgres'")
        execute(
            conn,
            "CREATE CONNECTION staged.pg TO POSTGRES (HOST 'postgres', DATABASE 'a', "
            "USER 'postgres', PASSWORD SECRET staged.password)",
        )
        with paused_preparation(
            c,
            "purification",
            "CREATE SOURCE staged.source FROM POSTGRES CONNECTION staged.pg "
            "(PUBLICATION 'staged_pub') FOR ALL TABLES",
        ) as result:
            execute(conn, "ALTER CONNECTION staged.pg SET (DATABASE 'b')")
        result.result()

        exports = execute(
            conn,
            "SELECT p.schema_name, p.table_name, t.name, col.name, col.type "
            "FROM mz_internal.mz_postgres_source_tables p "
            "JOIN mz_relations t ON t.id = p.id "
            "JOIN mz_schemas s ON s.id = t.schema_id "
            "JOIN mz_columns col ON col.id = t.id "
            "WHERE s.name = 'staged' ORDER BY t.name, col.position",
        ).fetchall()
        assert exports == [("public", "input_b", "input_b", "value_b", "text")], exports
