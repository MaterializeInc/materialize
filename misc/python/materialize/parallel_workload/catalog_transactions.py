# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Check catalog transaction isolation without retrying SQL statements."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from psycopg import Connection, Cursor

from materialize.mzcompose.composition import Composition


def execute(conn: Connection, statement: str) -> Cursor:
    """Execute SQL containing only harness-generated identifiers and numerics."""
    print(f"{conn.info.backend_pid}: {statement}", flush=True)
    return conn.execute(statement.encode())


def independent_ddl(c: Composition, before_commit: bool, same_schema: bool) -> None:
    schema = f"ddl_independent_{int(before_commit)}_{int(same_schema)}"
    other_schema = schema if same_schema else f"{schema}_other"
    with c.sql_connection() as txn, c.sql_connection() as other:
        execute(other, f"CREATE SCHEMA {schema}")
        if not same_schema:
            execute(other, f"CREATE SCHEMA {other_schema}")
        execute(other, f"CREATE VIEW {schema}.original AS SELECT 42 AS value")
        execute(other, f"CREATE VIEW {schema}.dependent AS SELECT * FROM {schema}.original")
        original_id = execute(
            other,
            "SELECT id FROM mz_views WHERE schema_id = "
            f"(SELECT id FROM mz_schemas WHERE name = '{schema}') AND name = 'original'",
        ).fetchone()
        assert original_id is not None

        execute(txn, "BEGIN")
        execute(txn, f"ALTER VIEW {schema}.original RENAME TO intermediate")
        if before_commit:
            execute(txn, f"ALTER VIEW {schema}.intermediate RENAME TO final")
        execute(other, f"CREATE VIEW {other_schema}.unrelated AS SELECT 7")
        if not before_commit:
            execute(txn, f"ALTER VIEW {schema}.intermediate RENAME TO final")
        execute(txn, "COMMIT")

        assert execute(other, f"SELECT * FROM {schema}.final").fetchall() == [(42,)]
        assert execute(other, f"SELECT * FROM {schema}.dependent").fetchall() == [(42,)]
        unrelated = execute(other, f"SELECT * FROM {other_schema}.unrelated").fetchall()
        assert unrelated == [(7,)]
        final_id = execute(
            other,
            "SELECT id FROM mz_views WHERE schema_id = "
            f"(SELECT id FROM mz_schemas WHERE name = '{schema}') AND name = 'final'",
        ).fetchone()
        assert final_id == original_id
        names = execute(
            other,
            "SELECT name FROM mz_views WHERE schema_id = "
            f"(SELECT id FROM mz_schemas WHERE name = '{schema}') ORDER BY name",
        ).fetchall()
        expected = [("dependent",), ("final",)]
        if same_schema:
            expected.append(("unrelated",))
        assert names == expected, names


def hammer(c: Composition, workers: int, rounds: int) -> None:
    schema = "ddl_hammer"
    with c.sql_connection() as conn:
        execute(conn, f"CREATE SCHEMA {schema}")
        for worker in range(workers):
            execute(conn, f"CREATE VIEW {schema}.v{worker} AS SELECT {worker} AS value")
            execute(
                conn,
                f"CREATE VIEW {schema}.d{worker} AS SELECT * FROM {schema}.v{worker}",
            )

    # Each round forces an autocommit change into every transaction's open
    # interval. The final rendezvous prevents the next round from overtaking
    # workers still committing. A failed worker breaks all rendezvous.
    barrier = Barrier(workers + 1, timeout=60)

    def worker_loop(worker: int) -> None:
        try:
            with c.sql_connection(startup_params={"statement_timeout": "30s"}) as conn:
                for iteration in range(rounds):
                    if worker == workers:
                        barrier.wait()
                        execute(conn, f"CREATE VIEW {schema}.churn AS SELECT {iteration}")
                        execute(conn, f"DROP VIEW {schema}.churn")
                        barrier.wait()
                    else:
                        execute(conn, "BEGIN")
                        execute(
                            conn,
                            f"ALTER VIEW {schema}.v{worker} RENAME TO pending{worker}",
                        )
                        if iteration % 2:
                            execute(
                                conn,
                                f"ALTER VIEW {schema}.pending{worker} RENAME TO v{worker}",
                            )
                        barrier.wait()
                        barrier.wait()
                        if not iteration % 2:
                            execute(
                                conn,
                                f"ALTER VIEW {schema}.pending{worker} RENAME TO v{worker}",
                            )
                        execute(conn, "COMMIT")
                    barrier.wait()
        except BaseException:
            barrier.abort()
            raise

    with ThreadPoolExecutor(max_workers=workers + 1) as executor:
        futures = [executor.submit(worker_loop, worker) for worker in range(workers + 1)]
        failures = [
            error for future in futures if (error := future.exception()) is not None
        ]
    if failures:
        raise BaseExceptionGroup("Concurrent catalog transactions failed", failures)

    with c.sql_connection() as conn:
        for worker in range(workers):
            for name in (f"v{worker}", f"d{worker}"):
                rows = execute(conn, f"SELECT * FROM {schema}.{name}").fetchall()
                assert rows == [(worker,)]
        names = execute(
            conn,
            "SELECT name FROM mz_views WHERE schema_id = "
            f"(SELECT id FROM mz_schemas WHERE name = '{schema}') ORDER BY name",
        ).fetchall()
        expected = sorted(
            (f"{prefix}{worker}",) for prefix in ("v", "d") for worker in range(workers)
        )
        assert names == expected, names
    print(f"Committed {workers * rounds} DDL transactions without client retries")
