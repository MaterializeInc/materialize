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
from psycopg.errors import SerializationFailure

from materialize.mzcompose.composition import Composition


def execute(conn: Connection, statement: str) -> Cursor:
    """Execute SQL containing only harness-controlled values."""
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
        execute(
            other, f"CREATE VIEW {schema}.dependent AS SELECT * FROM {schema}.original"
        )
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
        execute(
            other,
            f"CREATE VIEW {schema}.late_dependent AS SELECT * FROM {schema}.original",
        )
        if not before_commit:
            execute(txn, f"ALTER VIEW {schema}.intermediate RENAME TO final")
        execute(txn, "COMMIT")

        assert execute(other, f"SELECT * FROM {schema}.final").fetchall() == [(42,)]
        assert execute(other, f"SELECT * FROM {schema}.dependent").fetchall() == [(42,)]
        assert execute(other, f"SELECT * FROM {schema}.late_dependent").fetchall() == [
            (42,)
        ]
        definition = execute(
            other, f"SHOW CREATE VIEW {schema}.late_dependent"
        ).fetchone()
        assert definition is not None
        assert f"{schema}.final" in definition[1].replace('"', ""), definition
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
        expected = [("dependent",), ("final",), ("late_dependent",)]
        if same_schema:
            expected.append(("unrelated",))
        assert names == expected, names


def conflicting_ddl(c: Composition, replace: bool) -> None:
    schema = f"ddl_conflict_{int(replace)}"
    with c.sql_connection() as txn, c.sql_connection() as other:
        execute(other, f"CREATE SCHEMA {schema}")
        execute(other, f"CREATE VIEW {schema}.sentinel AS SELECT 7")
        execute(other, f"CREATE VIEW {schema}.target AS SELECT 42")
        execute(txn, "BEGIN")
        execute(txn, f"ALTER VIEW {schema}.sentinel RENAME TO sentinel_pending")
        execute(txn, f"ALTER VIEW {schema}.target RENAME TO target_pending")
        if replace:
            execute(other, f"DROP VIEW {schema}.target")
            execute(other, f"CREATE VIEW {schema}.target AS SELECT 43")
            surviving_name = "target"
            expected = 43
        else:
            execute(other, f"ALTER VIEW {schema}.target RENAME TO renamed")
            surviving_name = "renamed"
            expected = 42
        try:
            execute(txn, "COMMIT")
        except SerializationFailure:
            execute(txn, "ROLLBACK")
        else:
            raise AssertionError("conflicting DDL committed")
        assert execute(other, f"SELECT * FROM {schema}.sentinel").fetchall() == [(7,)]
        assert execute(
            other, f"SELECT * FROM {schema}.{surviving_name}"
        ).fetchall() == [(expected,)]
        names = execute(
            other,
            "SELECT name FROM mz_views WHERE schema_id = "
            f"(SELECT id FROM mz_schemas WHERE name = '{schema}') ORDER BY name",
        ).fetchall()
        assert names == sorted([("sentinel",), (surviving_name,)]), names


def rename_precondition_conflicts(c: Composition) -> None:
    for conflict in ("destination", "authorization"):
        schema = f"ddl_precondition_{conflict}"
        with c.sql_connection() as other:
            execute(other, f"CREATE SCHEMA {schema}")
            execute(other, f"CREATE VIEW {schema}.sentinel AS SELECT 7")
            execute(other, f"CREATE VIEW {schema}.target AS SELECT 42")
            if conflict == "authorization":
                execute(other, "CREATE ROLE ddl_owner")
                execute(other, "CREATE ROLE ddl_actor")
                execute(other, "GRANT ddl_owner TO ddl_actor")
                execute(other, "GRANT ddl_owner TO materialize")
                with c.sql_connection(port=6877, user="mz_system") as system:
                    execute(system, "GRANT USAGE ON DATABASE materialize TO ddl_actor")
                    execute(system, "GRANT USAGE ON CLUSTER quickstart TO ddl_actor")
                execute(other, f"GRANT ALL PRIVILEGES ON SCHEMA {schema} TO ddl_owner")
                for name in ("sentinel", "target"):
                    execute(other, f"ALTER VIEW {schema}.{name} OWNER TO ddl_owner")
            user = "ddl_actor" if conflict == "authorization" else "materialize"
            with c.sql_connection(user=user) as txn:
                execute(txn, "BEGIN")
                execute(txn, f"ALTER VIEW {schema}.sentinel RENAME TO sentinel_pending")
                execute(txn, f"ALTER VIEW {schema}.target RENAME TO target_pending")
                if conflict == "destination":
                    execute(other, f"CREATE VIEW {schema}.target_pending AS SELECT 99")
                else:
                    execute(other, "REVOKE ddl_owner FROM ddl_actor")
                try:
                    execute(txn, "COMMIT")
                except SerializationFailure:
                    execute(txn, "ROLLBACK")
                else:
                    raise AssertionError(
                        f"changed {conflict} precondition was not validated"
                    )
                assert execute(
                    other, f"SELECT * FROM {schema}.sentinel"
                ).fetchall() == [(7,)]
                assert execute(other, f"SELECT * FROM {schema}.target").fetchall() == [
                    (42,)
                ]
                names = execute(
                    other,
                    "SELECT name FROM mz_views WHERE schema_id = "
                    f"(SELECT id FROM mz_schemas WHERE name = '{schema}') ORDER BY name",
                ).fetchall()
                expected = [("sentinel",), ("target",)]
                if conflict == "destination":
                    expected.append(("target_pending",))
                    assert execute(
                        other, f"SELECT * FROM {schema}.target_pending"
                    ).fetchall() == [(99,)]
                assert names == expected, names


def source_ddl(c: Composition) -> None:
    schema = "ddl_source"
    with c.sql_connection() as txn, c.sql_connection() as other:
        execute(other, f"CREATE SCHEMA {schema}")
        execute(txn, "BEGIN")
        execute(
            txn,
            f"CREATE SOURCE {schema}.source FROM LOAD GENERATOR COUNTER "
            "(AS OF 0, UP TO 1)",
        )
        execute(other, f"CREATE VIEW {schema}.unrelated AS SELECT 7")
        execute(txn, f"CREATE TABLE {schema}.export FROM SOURCE {schema}.source")
        execute(txn, f"ALTER TABLE {schema}.export RENAME TO final")
        execute(txn, "COMMIT")
        assert execute(other, f"SELECT count(*) FROM {schema}.final").fetchall() == [
            (1,)
        ]
        assert execute(other, f"SELECT * FROM {schema}.unrelated").fetchall() == [(7,)]


def source_noop_conflict(c: Composition) -> None:
    with c.sql_connection() as txn, c.sql_connection() as other:
        execute(other, "CREATE SOURCE ddl_existing FROM LOAD GENERATOR COUNTER")
        execute(other, "CREATE VIEW ddl_noop_sentinel AS SELECT 7")
        execute(txn, "BEGIN")
        execute(
            txn, "CREATE SOURCE IF NOT EXISTS ddl_existing FROM LOAD GENERATOR COUNTER"
        )
        execute(other, "DROP SOURCE ddl_existing")
        execute(txn, "ALTER VIEW ddl_noop_sentinel RENAME TO ddl_noop_pending")
        try:
            execute(txn, "COMMIT")
        except SerializationFailure:
            execute(txn, "ROLLBACK")
        else:
            raise AssertionError("source existence observation was not validated")
        assert execute(other, "SELECT * FROM ddl_noop_sentinel").fetchall() == [(7,)]


def source_notice_conflicts(c: Composition) -> None:
    c.up("postgres")
    with c.sql_connection(
        service="postgres", user="postgres", database="postgres", password="postgres"
    ) as upstream:
        execute(upstream, "CREATE TABLE ddl_input (value integer PRIMARY KEY)")
        execute(upstream, "ALTER TABLE ddl_input REPLICA IDENTITY FULL")
        execute(upstream, "CREATE PUBLICATION ddl_pub FOR TABLE ddl_input")
    with c.sql_connection() as txn, c.sql_connection() as other:
        execute(other, "CREATE SECRET ddl_password AS 'postgres'")
        execute(
            other,
            "CREATE CONNECTION ddl_pg TO POSTGRES (HOST 'postgres', DATABASE 'postgres', "
            "USER 'postgres', PASSWORD SECRET ddl_password)",
        )
        for dropped in ("cluster", "schema"):
            name = f"ddl_notice_{dropped}"
            execute(other, f"CREATE SCHEMA {name}")
            execute(
                other,
                f"CREATE CLUSTER {name} SIZE 'scale=1,workers=1', REPLICATION FACTOR 2",
            )
            execute(other, f"CREATE VIEW {name}_sentinel AS SELECT 7")
            execute(txn, "BEGIN")
            execute(txn, f"ALTER VIEW {name}_sentinel RENAME TO {name}_pending")
            execute(other, f"DROP {dropped.upper()} {name}")
            execute(
                txn,
                f"CREATE SOURCE {name}.source IN CLUSTER {name} "
                "FROM POSTGRES CONNECTION ddl_pg (PUBLICATION 'ddl_pub') FOR ALL TABLES",
            )
            try:
                execute(txn, "COMMIT")
            except SerializationFailure:
                execute(txn, "ROLLBACK")
            else:
                raise AssertionError(f"dropped {dropped} observation was not validated")
            assert execute(other, f"SELECT * FROM {name}_sentinel").fetchall() == [(7,)]


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
                        execute(
                            conn, f"CREATE VIEW {schema}.churn AS SELECT {iteration}"
                        )
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
        futures = [
            executor.submit(worker_loop, worker) for worker in range(workers + 1)
        ]
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
