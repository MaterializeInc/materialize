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
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from psycopg import Connection, Cursor, Error
from psycopg.errors import SerializationFailure
from psycopg.pq import ExecStatus

from materialize.mzcompose.composition import Composition


def execute(conn: Connection, statement: str) -> Cursor:
    """Execute SQL containing only harness-controlled values."""
    print(f"{conn.info.backend_pid}: {statement}", flush=True)
    return conn.execute(statement.encode())


def explain_ddl_rejection(c: Composition) -> None:
    with c.sql_connection() as txn:
        execute(txn, "CREATE SCHEMA ddl_explain_rejection")
        execute(txn, "CREATE VIEW ddl_explain_rejection.original AS SELECT 42 AS value")
        execute(txn, "CREATE TABLE ddl_explain_rejection.input (value INT)")
        execute(
            txn,
            "CREATE CONNECTION ddl_explain_rejection.kafka TO KAFKA "
            "(BROKER 'localhost:1', SECURITY PROTOCOL PLAINTEXT) WITH (VALIDATE = false)",
        )
        execute(
            txn,
            "CREATE CONNECTION ddl_explain_rejection.registry TO CONFLUENT SCHEMA REGISTRY "
            "(URL 'http://localhost:1') WITH (VALIDATE = false)",
        )
        for explain in (
            "EXPLAIN RAW PLAN FOR VIEW {}",
            "EXPLAIN RAW PLAN FOR SELECT * FROM {}",
            "EXPLAIN VALUE SCHEMA FOR CREATE SINK ddl_explain_rejection.sink "
            "FROM ddl_explain_rejection.input "
            "INTO KAFKA CONNECTION ddl_explain_rejection.kafka (TOPIC 'explain-rejection') "
            "FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION ddl_explain_rejection.registry "
            "ENVELOPE DEBEZIUM",
        ):
            standalone = explain.format("ddl_explain_rejection.original")
            assert execute(txn, standalone).fetchall()
            execute(txn, "BEGIN")
            assert execute(txn, standalone).fetchall()
            execute(txn, "COMMIT")
            for mode in ("direct", "prepared", "implicit"):
                rename = "ALTER VIEW ddl_explain_rejection.original RENAME TO observed"
                statement = explain.format("ddl_explain_rejection.observed")
                if mode == "implicit":
                    statement = f"{rename}; {statement}"
                else:
                    execute(txn, "BEGIN")
                    execute(txn, rename)
                try:
                    if mode == "prepared":
                        # SQL PREPARE rejects EXPLAIN, so use protocol preparation.
                        result = txn.execute(statement.encode(), prepare=True)
                    else:
                        result = execute(txn, statement)
                    while result.nextset():
                        pass
                except Error as error:
                    assert error.sqlstate == "25000", error
                    assert "restricted to just modifying objects" in str(error)
                else:
                    raise AssertionError(f"EXPLAIN was allowed in DDL mode: {mode}")
                finally:
                    execute(txn, "ROLLBACK")
                assert execute(
                    txn, "SELECT * FROM ddl_explain_rejection.original"
                ).fetchall() == [(42,)]


def catalog_result_outcome(c: Composition) -> None:
    cases = [
        (show_before_ddl, interference, False)
        for show_before_ddl in (True, False)
        for interference in ("definition", "unrelated", "equivalent_replacement")
    ]
    cases.append((True, "definition", True))
    for show_before_ddl, interference, change_before_ddl in cases:
        schema = (
            f"ddl_catalog_result_{int(show_before_ddl)}_"
            f"{interference}_{int(change_before_ddl)}"
        )
        with c.sql_connection() as txn, c.sql_connection() as other:
            execute(other, f"CREATE SCHEMA {schema}")
            execute(other, f"CREATE TABLE {schema}.original (value INT)")
            execute(other, f"CREATE TABLE {schema}.unrelated (value INT)")
            execute(txn, "BEGIN")
            if not show_before_ddl:
                execute(txn, f"ALTER TABLE {schema}.original RENAME TO observed")
            name = "original" if show_before_ddl else "observed"
            statement = f"SHOW CREATE TABLE {schema}.{name}"
            observed = execute(txn, statement).fetchone()
            assert observed is not None
            assert "value" in observed[1] and "added" not in observed[1]
            if change_before_ddl:
                execute(other, f"ALTER TABLE {schema}.original ADD COLUMN added TEXT")
            if show_before_ddl:
                execute(txn, f"ALTER TABLE {schema}.original RENAME TO observed")
            execute(txn, f"ALTER TABLE {schema}.observed RENAME TO final")
            if interference == "equivalent_replacement":
                execute(other, f"DROP TABLE {schema}.original")
                execute(other, f"CREATE TABLE {schema}.original (value INT)")
            elif not change_before_ddl:
                target = "original" if interference == "definition" else "unrelated"
                execute(other, f"ALTER TABLE {schema}.{target} ADD COLUMN added TEXT")
            try:
                execute(txn, "COMMIT")
            except SerializationFailure:
                assert interference == "definition"
            else:
                assert (
                    interference != "definition"
                ), "changed catalog result must conflict"
            names = execute(
                other,
                "SELECT t.name FROM mz_tables t JOIN mz_schemas s ON s.id = t.schema_id "
                f"WHERE s.name = '{schema}' ORDER BY t.name",
            ).fetchall()
            expected = "original" if interference == "definition" else "final"
            assert names == [(expected,), ("unrelated",)], (schema, names)

    for continuation in ("select", "insert", "rollback"):
        schema = f"ddl_inspection_without_ddl_{continuation}"
        with c.sql_connection() as txn, c.sql_connection() as other:
            execute(other, f"CREATE SCHEMA {schema}")
            execute(other, f"CREATE TABLE {schema}.original (value INT)")
            execute(other, f"CREATE TABLE {schema}.data (value INT)")
            execute(other, f"INSERT INTO {schema}.data VALUES (42)")
            execute(txn, "BEGIN")
            before = execute(txn, f"SHOW CREATE TABLE {schema}.original").fetchone()
            assert before is not None and "added" not in before[1]
            if continuation == "select":
                assert execute(txn, f"SELECT * FROM {schema}.data").fetchall() == [
                    (42,)
                ]
            elif continuation == "insert":
                execute(txn, f"INSERT INTO {schema}.data VALUES (7)")
            execute(other, f"ALTER TABLE {schema}.original ADD COLUMN added TEXT")
            execute(txn, "ROLLBACK" if continuation == "rollback" else "COMMIT")
            expected_rows = [(7,), (42,)] if continuation == "insert" else [(42,)]
            assert (
                execute(other, f"SELECT * FROM {schema}.data ORDER BY value").fetchall()
                == expected_rows
            )

            # A finished transaction must not constrain the next DDL transaction.
            execute(txn, "BEGIN")
            execute(txn, f"ALTER TABLE {schema}.original RENAME TO final")
            execute(txn, "COMMIT")
            after = execute(other, f"SHOW CREATE TABLE {schema}.final").fetchone()
            assert after is not None and "added" in after[1]


def description_outcome(c: Composition) -> None:
    for api in ("prepare", "declare", "protocol"):
        for wildcard in (False, True):
            for timing in ("after_ddl", "before_ddl", "change_before_ddl"):
                schema = f"ddl_description_{api}_{int(wildcard)}_{timing}"
                with c.sql_connection() as txn, c.sql_connection() as other:
                    execute(other, f"CREATE SCHEMA {schema}")
                    execute(other, f"CREATE TABLE {schema}.original (value INT)")
                    execute(txn, "BEGIN")
                    if timing == "after_ddl":
                        execute(
                            txn, f"ALTER TABLE {schema}.original RENAME TO observed"
                        )
                    projection = "*" if wildcard else "value"
                    name = "observed" if timing == "after_ddl" else "original"
                    query = f"SELECT {projection} FROM {schema}.{name}"
                    if api == "prepare":
                        execute(txn, f"PREPARE described AS {query}")
                    elif api == "declare":
                        execute(txn, f"DECLARE described CURSOR FOR {query}")
                    else:
                        prepared = txn.pgconn.prepare(b"described", query.encode())
                        assert (
                            prepared.status == ExecStatus.COMMAND_OK
                        ), prepared.error_message
                        described = txn.pgconn.describe_prepared(b"described")
                        assert (
                            described.status == ExecStatus.COMMAND_OK
                        ), described.error_message
                        assert described.nfields == 1
                        assert described.fname(0) == b"value"
                    if api in ("prepare", "declare"):
                        wrapper = (
                            "EXECUTE described"
                            if api == "prepare"
                            else "FETCH described"
                        )
                        prepared = txn.pgconn.prepare(b"wrapper", wrapper.encode())
                        assert (
                            prepared.status == ExecStatus.COMMAND_OK
                        ), prepared.error_message
                        described = txn.pgconn.describe_prepared(b"wrapper")
                        assert (
                            described.status == ExecStatus.COMMAND_OK
                        ), described.error_message
                        assert described.nfields == 1
                        nested = f"DECLARE nested CURSOR FOR {wrapper}"
                        prepared = txn.pgconn.prepare(
                            b"nested_wrapper", nested.encode()
                        )
                        assert (
                            prepared.status == ExecStatus.COMMAND_OK
                        ), prepared.error_message
                    if timing == "change_before_ddl":
                        execute(
                            other,
                            f"ALTER TABLE {schema}.original ADD COLUMN added TEXT",
                        )
                    if timing != "after_ddl":
                        execute(
                            txn, f"ALTER TABLE {schema}.original RENAME TO observed"
                        )
                    execute(txn, f"ALTER TABLE {schema}.observed RENAME TO final")
                    if timing != "change_before_ddl":
                        execute(
                            other,
                            f"ALTER TABLE {schema}.original ADD COLUMN added TEXT",
                        )
                    try:
                        execute(txn, "COMMIT")
                    except SerializationFailure as error:
                        assert error.sqlstate == "40001", error
                        assert wildcard, "explicit projection's schema is unchanged"
                    else:
                        assert (
                            not wildcard
                        ), "expanded wildcard changed the described schema"
                    expected = "original" if wildcard else "final"
                    names = execute(
                        other,
                        "SELECT t.name FROM mz_tables t JOIN mz_schemas s ON s.id = t.schema_id "
                        f"WHERE s.name = '{schema}'",
                    ).fetchall()
                    assert names == [(expected,)], (api, wildcard, timing, names)

                    # Neither successful nor failed COMMIT may retain descriptions.
                    execute(txn, "BEGIN")
                    execute(txn, f"ALTER TABLE {schema}.{expected} RENAME TO cleaned")
                    execute(txn, "COMMIT")
                    assert execute(
                        other, f"SELECT count(*) FROM {schema}.cleaned"
                    ).fetchone() == (0,)

    for continuation in ("select", "insert", "rollback"):
        schema = f"ddl_description_without_ddl_{continuation}"
        with c.sql_connection() as txn, c.sql_connection() as other:
            execute(other, f"CREATE SCHEMA {schema}")
            execute(other, f"CREATE TABLE {schema}.original (value INT)")
            execute(other, f"CREATE TABLE {schema}.data (value INT)")
            execute(other, f"INSERT INTO {schema}.data VALUES (42)")
            execute(txn, "BEGIN")
            execute(txn, f"PREPARE described AS SELECT * FROM {schema}.original")
            if continuation == "select":
                assert execute(txn, f"SELECT * FROM {schema}.data").fetchall() == [
                    (42,)
                ]
            elif continuation == "insert":
                execute(txn, f"INSERT INTO {schema}.data VALUES (7)")
            execute(other, f"ALTER TABLE {schema}.original ADD COLUMN added TEXT")
            execute(txn, "ROLLBACK" if continuation == "rollback" else "COMMIT")
            expected_rows = [(7,), (42,)] if continuation == "insert" else [(42,)]
            assert (
                execute(other, f"SELECT * FROM {schema}.data ORDER BY value").fetchall()
                == expected_rows
            )

            # A finished transaction must not constrain the next DDL transaction.
            execute(txn, "BEGIN")
            execute(txn, f"ALTER TABLE {schema}.original RENAME TO final")
            execute(txn, "COMMIT")
            after = execute(other, f"SHOW CREATE TABLE {schema}.final").fetchone()
            assert after is not None and "added" in after[1]

    with c.sql_connection() as txn, c.sql_connection() as other:
        execute(other, "CREATE SCHEMA ddl_empty_description")
        execute(other, "CREATE TABLE ddl_empty_description.original (value INT)")
        execute(other, "CREATE TABLE ddl_empty_description.unexecuted (value INT)")
        execute(txn, "BEGIN")
        execute(txn, "ALTER TABLE ddl_empty_description.original RENAME TO final")
        prepared = txn.pgconn.prepare(
            b"unexecuted",
            b"ALTER TABLE ddl_empty_description.unexecuted RENAME TO unused",
        )
        assert prepared.status == ExecStatus.COMMAND_OK, prepared.error_message
        execute(other, "DROP TABLE ddl_empty_description.unexecuted")
        execute(txn, "COMMIT")
        assert execute(
            other, "SELECT count(*) FROM ddl_empty_description.final"
        ).fetchone() == (0,)


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
