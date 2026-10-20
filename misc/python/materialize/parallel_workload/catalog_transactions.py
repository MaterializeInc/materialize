# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Check catalog transaction isolation without retrying SQL statements."""

import random
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from psycopg import Connection, Cursor, Error
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


def outcome(conn: Connection, statements: list[str]) -> tuple[str, str] | None:
    """Return the SQLSTATE and primary error, or commit successfully, without retrying."""
    try:
        for statement in statements:
            execute(conn, statement)
    except Error as error:
        print(
            f"{conn.info.backend_pid}: SQLSTATE={error.sqlstate}: {error}", flush=True
        )
        assert error.sqlstate is not None, error
        assert error.diag.message_primary is not None, error
        return error.sqlstate, error.diag.message_primary
    finally:
        execute(conn, "ROLLBACK")
    print(f"{conn.info.backend_pid}: success", flush=True)
    return None


def view_ids(conn: Connection, schema: str) -> dict[str, str]:
    return dict(
        execute(
            conn,
            "SELECT name, id FROM mz_views WHERE schema_id = "
            f"(SELECT id FROM mz_schemas WHERE name = '{schema}') ORDER BY name",
        ).fetchall()
    )


def late_binding(c: Composition, interference: str, seed: int, steps: int) -> None:
    # Both executions use the same batch. Fresh schemas isolate the serial
    # reference from the concurrent outcome, including unexpected partial writes.
    rng = random.Random(seed)
    renames = [("sentinel", "sentinel_pending"), ("target", "target_pending")]
    names = ["sentinel_pending", "target_pending"]
    for step in range(steps):
        item = rng.randrange(2)
        destination = f"step_{step}"
        renames.append((names[item], destination))
        names[item] = destination

    results = []
    for concurrent in (False, True):
        schema = f"ddl_{interference}_{steps}_{int(concurrent)}"
        print(f"schedule={schema} seed={seed} renames={renames}", flush=True)
        with c.sql_connection() as other:
            execute(other, f"CREATE SCHEMA {schema}")
            execute(other, f"CREATE VIEW {schema}.sentinel AS SELECT 7")
            execute(other, f"CREATE VIEW {schema}.target AS SELECT 42")
            owner, actor = f"{schema}_owner", f"{schema}_actor"
            if interference == "authorization":
                execute(other, f"CREATE ROLE {owner}")
                execute(other, f"CREATE ROLE {actor}")
                execute(other, f"GRANT {owner} TO {actor}")
                execute(other, f"GRANT {owner} TO materialize")
                execute(other, f"GRANT {actor} TO materialize")
                with c.sql_connection(port=6877, user="mz_system") as system:
                    execute(system, f"GRANT USAGE ON DATABASE materialize TO {actor}")
                    execute(system, f"GRANT USAGE ON CLUSTER quickstart TO {actor}")
                execute(other, f"GRANT ALL PRIVILEGES ON SCHEMA {schema} TO {actor}")
                # Keep the first operation authorized so a later failure must
                # roll back an otherwise valid prefix.
                execute(other, f"ALTER VIEW {schema}.sentinel OWNER TO {actor}")
                execute(other, f"ALTER VIEW {schema}.target OWNER TO {owner}")
            user = actor if interference == "authorization" else "materialize"
            with c.sql_connection(user=user) as txn:
                batch = [
                    f"ALTER VIEW {schema}.{source} RENAME TO {destination}"
                    for source, destination in renames
                ]
                # The generated suffix resolves names created by the prefix,
                # with interference between statement execution and commit.
                split = 2
                if concurrent:
                    execute(txn, "BEGIN")
                    for statement in batch[:split]:
                        execute(txn, statement)
                if interference == "replacement":
                    execute(other, f"DROP VIEW {schema}.target")
                    execute(other, f"CREATE VIEW {schema}.target AS SELECT 43")
                elif interference == "rename_away":
                    execute(other, f"ALTER VIEW {schema}.target RENAME TO away")
                elif interference == "destination":
                    execute(other, f"CREATE VIEW {schema}.target_pending AS SELECT 99")
                elif interference == "authorization":
                    execute(other, f"REVOKE {owner} FROM {actor}")
                else:
                    raise AssertionError(interference)
                before = view_ids(other, schema)
                if concurrent:
                    state = outcome(txn, [*batch[split:], "COMMIT"])
                else:
                    state = outcome(txn, ["BEGIN", *batch, "COMMIT"])
                after = view_ids(other, schema)
                values = {
                    name: execute(other, f"SELECT * FROM {schema}.{name}").fetchall()
                    for name in after
                }
                print(
                    f"schedule={schema} outcome={state} ids={after} rows={values}",
                    flush=True,
                )
                if interference == "replacement":
                    assert state is None, (schema, state)
                    assert after == {
                        names[0]: before["sentinel"],
                        names[1]: before["target"],
                    }, (schema, before, after)
                    assert values == {names[0]: [(7,)], names[1]: [(43,)]}, values
                else:
                    assert state is not None, f"{schema}: unexpectedly committed"
                    assert after == before, (schema, before, after)
                    expected = {"sentinel": [(7,)]}
                    expected["away" if interference == "rename_away" else "target"] = [
                        (42,)
                    ]
                    if interference == "destination":
                        expected["target_pending"] = [(99,)]
                    assert values == expected, (schema, values)
                results.append(
                    (state[0], state[1].replace(schema, "<schema>"))
                    if state is not None
                    else None
                )
    assert results[0] == results[1], (
        f"{interference} seed={seed} renames={renames}: "
        f"serial outcome={results[0]}, concurrent outcome={results[1]}"
    )


def rename_noop(c: Composition) -> None:
    for create_missing in (False, True):
        schema = f"ddl_rename_noop_{int(create_missing)}"
        with c.sql_connection() as txn, c.sql_connection() as other:
            execute(other, f"CREATE SCHEMA {schema}")
            execute(other, f"CREATE VIEW {schema}.sentinel AS SELECT 7")
            notices = []
            txn.add_notice_handler(
                lambda notice: notices.append((notice.sqlstate, notice.message_primary))
            )
            execute(txn, "BEGIN")
            execute(txn, f"ALTER VIEW {schema}.sentinel RENAME TO pending")
            execute(txn, f"ALTER VIEW IF EXISTS {schema}.missing RENAME TO renamed")
            missing_notices = list(notices)
            assert len(missing_notices) == 1, missing_notices
            assert "missing" in (missing_notices[0][1] or ""), missing_notices
            name = "missing" if create_missing else "unrelated"
            execute(other, f"CREATE VIEW {schema}.{name} AS SELECT 42")
            before = view_ids(other, schema)
            state = outcome(txn, ["COMMIT"])
            after = view_ids(other, schema)
            if create_missing:
                assert state is not None and state[0] == "40001", state
                assert after == before, (before, after)
                sentinel = "sentinel"
            else:
                assert state is None, state
                assert after == {
                    "pending": before["sentinel"],
                    "unrelated": before["unrelated"],
                }, (before, after)
                sentinel = "pending"
            assert execute(other, f"SELECT * FROM {schema}.{sentinel}").fetchall() == [
                (7,)
            ]
            assert execute(other, f"SELECT * FROM {schema}.{name}").fetchall() == [
                (42,)
            ]
            # outcome's ROLLBACK can emit a separate no-transaction warning.
            assert [n for n in notices if n == missing_notices[0]] == missing_notices


def schema_swap(c: Composition) -> None:
    results = []
    for concurrent in (False, True):
        prefix = f"ddl_swap_{int(concurrent)}"
        left, right = f"{prefix}_left", f"{prefix}_right"
        with c.sql_connection() as txn, c.sql_connection() as other:
            for schema, value in ((left, 7), (right, 42)):
                execute(other, f"CREATE SCHEMA {schema}")
                execute(
                    other, f"CREATE VIEW {schema}.target AS SELECT {value} AS value"
                )
                execute(
                    other,
                    f"CREATE VIEW {schema}.dependent AS SELECT * FROM {schema}.target",
                )
            old_right_id = execute(
                other, f"SELECT id FROM mz_schemas WHERE name = '{right}'"
            ).fetchone()
            batch = [
                f"ALTER SCHEMA {left} SWAP WITH {right}",
                f"ALTER VIEW {left}.target RENAME TO final",
            ]
            if concurrent:
                execute(txn, "BEGIN")
                execute(txn, batch[0])
            execute(other, f"DROP SCHEMA {right} CASCADE")
            execute(other, f"CREATE SCHEMA {right}")
            execute(other, f"CREATE VIEW {right}.target AS SELECT 43 AS value")
            execute(
                other, f"CREATE VIEW {right}.dependent AS SELECT * FROM {right}.target"
            )
            execute(
                other,
                f"CREATE VIEW {left}.late_dependent AS SELECT * FROM {right}.target",
            )
            before = {schema: view_ids(other, schema) for schema in (left, right)}
            schema_ids = dict(
                execute(
                    other,
                    f"SELECT name, id FROM mz_schemas WHERE name IN ('{left}', '{right}')",
                ).fetchall()
            )
            assert (schema_ids[right],) != old_right_id
            state = outcome(
                txn, [batch[1], "COMMIT"] if concurrent else ["BEGIN", *batch, "COMMIT"]
            )
            assert state is None, (prefix, state)
            after_schema_ids = dict(
                execute(
                    other,
                    f"SELECT name, id FROM mz_schemas WHERE name IN ('{left}', '{right}')",
                ).fetchall()
            )
            assert after_schema_ids == {
                left: schema_ids[right],
                right: schema_ids[left],
            }
            assert view_ids(other, left) == {
                "final": before[right]["target"],
                "dependent": before[right]["dependent"],
            }
            assert view_ids(other, right) == before[left]
            definitions = {}
            for schema, name, value, target in (
                (left, "final", 43, None),
                (left, "dependent", 43, f"{left}.final"),
                (right, "target", 7, None),
                (right, "dependent", 7, f"{right}.target"),
                (right, "late_dependent", 43, f"{left}.final"),
            ):
                assert execute(other, f"SELECT * FROM {schema}.{name}").fetchall() == [
                    (value,)
                ]
                definition = execute(
                    other, f"SHOW CREATE VIEW {schema}.{name}"
                ).fetchone()
                assert definition is not None
                if target is not None:
                    assert target in definition[1].replace('"', ""), definition
                definitions[f"{schema}.{name}".replace(prefix, "<schema>")] = (
                    definition[1].replace(prefix, "<schema>")
                )
            results.append(definitions)
    assert results[0] == results[1], (results[0], results[1])


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


def source_late_binding(c: Composition, interference: str, before_commit: bool) -> None:
    results = []
    for concurrent in (False, True):
        schema = (
            f"ddl_source_binding_{interference}_{int(before_commit)}_{int(concurrent)}"
        )
        with c.sql_connection() as txn, c.sql_connection() as other:
            execute(other, f"CREATE SCHEMA {schema}")
            execute(other, f"CREATE VIEW {schema}.sentinel AS SELECT 7")
            execute(
                other,
                f"CREATE SOURCE {schema}.source FROM LOAD GENERATOR COUNTER "
                "(AS OF 0, UP TO 1)",
            )
            batch = [
                f"ALTER VIEW {schema}.sentinel RENAME TO pending",
                f"CREATE TABLE {schema}.export FROM SOURCE {schema}.source",
                f"ALTER TABLE {schema}.export RENAME TO final",
            ]
            split = len(batch) if before_commit else 2
            if concurrent:
                execute(txn, "BEGIN")
                for statement in batch[:split]:
                    execute(txn, statement)
            if interference == "replacement":
                execute(other, f"DROP SOURCE {schema}.source")
                execute(
                    other,
                    f"CREATE SOURCE {schema}.source FROM LOAD GENERATOR COUNTER "
                    "(AS OF 0, UP TO 3)",
                )
            elif interference == "rename_away":
                execute(other, f"ALTER SOURCE {schema}.source RENAME TO away")
            else:
                raise AssertionError(interference)
            objects = (
                "SELECT id, name FROM mz_objects WHERE schema_id = "
                f"(SELECT id FROM mz_schemas WHERE name = '{schema}')"
            )
            before = set(execute(other, objects).fetchall())
            statements = (
                [*batch[split:], "COMMIT"]
                if concurrent
                else ["BEGIN", *batch, "COMMIT"]
            )
            state = outcome(txn, statements)
            after = set(execute(other, objects).fetchall())
            print(
                f"schedule={schema} outcome={state} before={before} after={after}",
                flush=True,
            )
            if interference == "replacement":
                assert state is None, (schema, state)
                expected = {
                    (id, "pending" if name == "sentinel" else name)
                    for id, name in before
                }
                assert {(id, name) for id, name in after if name != "final"} == expected
                assert len(after) == len(before) + 1, (schema, before, after)
                assert execute(other, f"SELECT * FROM {schema}.pending").fetchall() == [
                    (7,)
                ]
                final_id = next(id for id, name in after if name == "final")
                deadline = time.monotonic() + 120
                # DDL completion does not imply that the bounded generator has finished.
                while True:
                    frontier = execute(
                        other,
                        "SELECT write_frontier IS NULL FROM mz_internal.mz_frontiers "
                        f"WHERE object_id = '{final_id}'",
                    ).fetchall()
                    if frontier == [(True,)]:
                        break
                    assert time.monotonic() < deadline, (schema, final_id, frontier)
                    time.sleep(0.1)
                # A sealed frontier can be visible before the read oracle reaches the
                # final record's timestamp. Timestamp::MAX reads the completed trace.
                rows = execute(
                    other,
                    f"SELECT * FROM {schema}.final ORDER BY 1 AS OF 18446744073709551615",
                ).fetchall()
                if rows != [(1,), (2,), (3,)]:
                    for statement in [
                        f"SHOW CREATE SOURCE {schema}.source",
                        f"SHOW CREATE TABLE {schema}.final",
                        f"SELECT * FROM mz_internal.mz_frontiers WHERE object_id = '{final_id}'",
                    ]:
                        print(execute(other, statement).fetchall(), flush=True)
                assert rows == [(1,), (2,), (3,)], (schema, rows)
            else:
                assert state is not None, f"{schema}: unexpectedly committed"
                assert after == before, (schema, before, after)
                assert execute(
                    other, f"SELECT * FROM {schema}.sentinel"
                ).fetchall() == [(7,)]
            results.append(
                (state[0], state[1].replace(schema, "<schema>"))
                if state is not None
                else None
            )
    assert results[0] == results[1], (interference, before_commit, results)


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
            raise AssertionError("commit changed the skipped source creation outcome")
        assert execute(other, "SELECT * FROM ddl_noop_sentinel").fetchall() == [(7,)]
        assert (
            execute(
                other,
                "SELECT name FROM mz_objects WHERE name IN ('ddl_noop_pending', 'ddl_existing')",
            ).fetchall()
            == []
        )


def source_drop_conflicts(c: Composition) -> None:
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
            batch = [
                f"ALTER VIEW {name}_sentinel RENAME TO {name}_pending",
                f"CREATE SOURCE {name}.source IN CLUSTER {name} "
                "FROM POSTGRES CONNECTION ddl_pg (PUBLICATION 'ddl_pub') FOR ALL TABLES",
            ]
            with c.sql_connection() as reference:
                serial = outcome(reference, ["BEGIN", *batch, "COMMIT"])
            assert serial is not None, f"dropped {dropped}: serial execution succeeded"
            concurrent = outcome(txn, [batch[1], "COMMIT"])
            assert concurrent == serial, (dropped, serial, concurrent)
            assert execute(other, f"SELECT * FROM {name}_sentinel").fetchall() == [(7,)]
            assert (
                execute(
                    other,
                    f"SELECT name FROM mz_objects WHERE name IN ('{name}_pending', 'source') "
                    f"AND schema_id IN (SELECT id FROM mz_schemas WHERE name IN ('public', '{name}'))",
                ).fetchall()
                == []
            )


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
                    print(f"hammer worker={worker} round={iteration}", flush=True)
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
