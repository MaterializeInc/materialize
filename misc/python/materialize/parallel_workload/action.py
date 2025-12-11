# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import copy
import datetime
import json
import random
import threading
import time
import urllib.parse
from collections.abc import Callable
from typing import TYPE_CHECKING

import psycopg
import requests
import websocket
from pg8000.native import identifier
from psycopg import Connection
from psycopg.errors import OperationalError

import materialize.parallel_workload.database
from materialize.data_ingest.data_type import (
    NUMBER_TYPES,
    Boolean,
    Text,
    TextTextMap,
)
from materialize.data_ingest.query_error import QueryError
from materialize.data_ingest.row import Operation
from materialize.mzcompose import get_default_system_parameters
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import (
    LEADER_STATUS_HEALTHCHECK,
    DeploymentStatus,
    Materialized,
)
from materialize.mzcompose.services.minio import minio_blob_uri
from materialize.parallel_workload.database import (
    DATA_TYPES,
    DB,
    MAX_CLUSTERS,
    MAX_COLUMNS,
    MAX_DBS,
    MAX_INDEXES,
    MAX_KAFKA_SINKS,
    MAX_KAFKA_SOURCES,
    MAX_MYSQL_SOURCES,
    MAX_POSTGRES_SOURCES,
    MAX_ROLES,
    MAX_ROWS,
    MAX_SCHEMAS,
    MAX_SQL_SERVER_SOURCES,
    MAX_TABLES,
    MAX_VIEWS,
    MAX_WEBHOOK_SOURCES,
    Cluster,
    ClusterReplica,
    Column,
    Database,
    DBObject,
    Index,
    KafkaSink,
    KafkaSource,
    MySqlSource,
    PostgresSource,
    Role,
    Schema,
    SqlServerSource,
    Table,
    View,
    WebhookSource,
)
from materialize.parallel_workload.executor import Executor, Http
from materialize.parallel_workload.expression import ExprKind, expression
from materialize.parallel_workload.settings import (
    ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS,
    Complexity,
    Scenario,
)
from materialize.sqlsmith import known_errors

if TYPE_CHECKING:
    from materialize.parallel_workload.worker import Worker


def ws_connect(ws: websocket.WebSocket, host, port, user: str) -> tuple[int, int]:
    thread_name = threading.current_thread().getName()
    ws.connect(f"ws://{host}:{port}/api/experimental/sql", origin=thread_name)
    ws.send(
        json.dumps(
            {
                "user": user,
                "password": "",
                "options": {
                    "application_name": thread_name,
                    "max_query_result_size": "1000000",
                    "cluster": "quickstart",
                    "database": "materialize",
                    "search_path": "public",
                },
            }
        )
    )
    ws_conn_id = -1
    ws_secret_key = -1
    ws_ready = False
    while True:
        result = json.loads(ws.recv())
        result_type = result["type"]
        if result_type == "ParameterStatus":
            continue
        elif result_type == "BackendKeyData":
            ws_conn_id = result["payload"]["conn_id"]
            ws_secret_key = result["payload"]["secret_key"]
        elif result_type == "ReadyForQuery":
            ws_ready = True
        elif result_type == "Notice":
            assert "connected to Materialize" in result["payload"]["message"], result
            break
        else:
            raise RuntimeError(f"Unexpected result type: {result_type} in: {result}")
    assert ws_ready
    return (ws_conn_id, ws_secret_key)


# TODO: CASCADE in DROPs, keep track of what will be deleted
class Action:
    rng: random.Random
    composition: Composition | None
    stmt_id: int

    def __init__(self, rng: random.Random, composition: Composition | None):
        self.rng = rng
        self.composition = composition
        self.stmt_id = 0

    def run(self, exe: Executor) -> bool:
        raise NotImplementedError

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = [
            "permission denied for",
            "must be owner of",
            "HTTP read timeout",
            "result exceeds max size of",
            "timestamp out of range",
            "numeric field overflow",
            "division by zero",
            "out of range",
            "is only defined for finite arguments",
            "Window function performance issue",  # TODO: Remove when https://github.com/MaterializeInc/database-issues/issues/9644 is fixed
            "unknown cluster 'dont_exist'",  # Set intentionally to find panics
        ]
        if exe.db.complexity in (Complexity.DDL, Complexity.DDLOnly):
            result.extend(
                [
                    "query could not complete",
                    "cached plan must not change result type",
                    "violates not-null constraint",
                    "unknown catalog item",  # Expected, see database-issues#6124
                    "was concurrently dropped",  # role was dropped
                    "unknown cluster",  # cluster was dropped
                    "unknown schema",  # schema was dropped
                    "the transaction's active cluster has been dropped",  # cluster was dropped
                    "was removed",  # dependency was removed, started with moving optimization off main thread, see database-issues#7285
                    "real-time source dropped before ingesting the upstream system's visible frontier",  # Expected, see https://buildkite.com/materialize/nightly/builds/9399#0191be17-1f4c-4321-9b51-edc4b08b71c5
                    "object state changed while transaction was in progress",  # Old error msg, can remove this ignore later
                    "another session modified the catalog while this DDL transaction was open",
                    "was dropped while executing a statement",
                    "' was dropped",  # ConcurrentDependencyDrop (collection, schema, etc.)
                ]
            )
        if exe.db.scenario == Scenario.Cancel:
            result.extend(
                [
                    "canceling statement due to user request",
                ]
            )
        if exe.db.scenario == Scenario.ZeroDowntimeDeploy:
            result.extend(
                [
                    "cannot write in read-only mode",
                    "500: internal storage failure! ReadOnly",
                ]
            )
        if exe.db.scenario in (
            Scenario.Kill,
            Scenario.BackupRestore,
            Scenario.ZeroDowntimeDeploy,
        ):
            result.extend(
                [
                    # psycopg
                    "server closed the connection unexpectedly",
                    "Can't create a connection to host",
                    "Connection refused",
                    "Cursor closed",
                    # websockets
                    "Connection to remote host was lost.",
                    "socket is already closed.",
                    "Broken pipe",
                    "WS connect",
                    "Connection reset by peer",
                    # http
                    "Remote end closed connection without response",
                    "Connection aborted",
                    "Connection refused",
                    "Connection broken: IncompleteRead",
                ]
            )
        if exe.db.scenario in (Scenario.Kill, Scenario.ZeroDowntimeDeploy):
            # Expected, see database-issues#6156
            result.extend(
                ["unknown catalog item", "unknown schema", "unknown database"]
            )
        if exe.db.scenario == Scenario.Rename:
            result.extend(["unknown schema", "ambiguous reference to schema name"])
        if materialize.parallel_workload.database.NAUGHTY_IDENTIFIERS:
            result.extend(["identifier length exceeds 255 bytes"])
        return result

    def generate_select_query(self, exe: Executor, expr_kind: ExprKind) -> str:
        obj = self.rng.choice(exe.db.db_objects())
        column = self.rng.choice(obj.columns)
        obj2 = self.rng.choice(exe.db.db_objects_without_views())
        obj_name = str(obj)
        obj2_name = str(obj2)
        columns = [
            c
            for c in obj2.columns
            if c.data_type == column.data_type and c.data_type != TextTextMap
        ]

        join = obj_name != obj2_name and obj not in exe.db.views and columns

        if join:
            all_columns = list(obj.columns) + list(obj2.columns)
        else:
            all_columns = obj.columns

        if self.rng.random() < 0.9:
            expressions = ", ".join(
                [
                    expression(
                        self.rng.choice(list(DATA_TYPES)),
                        all_columns,
                        self.rng,
                        expr_kind,
                    )
                    for i in range(self.rng.randint(1, 10))
                ]
            )
            if self.rng.choice([True, False]):
                column1 = self.rng.choice(all_columns)
                column2 = self.rng.choice(all_columns)
                column3 = self.rng.choice(all_columns)
                fns = [
                    "COUNT({})",
                    # "LIST_AGG({})",
                    # "JSONB_AGG({})",
                ]
                # if column1.data_type == Text:
                #     fns.extend(["STRING_AGG({}, ',')"])
                # if column1.data_type not in [TextTextMap, IntArray, IntList]:
                #     fns.extend(["ARRAY_AGG({})"])
                if column1.data_type in NUMBER_TYPES:
                    fns.extend(
                        [
                            "SUM({})",
                            "AVG({})",
                            "MAX({})",
                            "MIN({})",
                            "STDDEV({})",
                            "STDDEV_POP({})",
                            "STDDEV_SAMP({})",
                            "VAR_SAMP({})",
                            "VAR_POP({})",
                        ]
                    )
                elif column1.data_type == Boolean:
                    fns.extend(["BOOL_AND({})", "BOOL_OR({})"])
                window_fn = self.rng.choice(fns)
                expressions += f", {window_fn.format(column1)} OVER (PARTITION BY {column2} ORDER BY {column3})"
        else:
            expressions = "*"

        query = f"SELECT {expressions} FROM {obj_name}"

        if join:
            column2 = self.rng.choice(columns)
            query += f" JOIN {obj2_name} ON {column} = {column2}"

        if self.rng.choice([True, False]):
            query += f" WHERE {expression(Boolean, all_columns, self.rng, expr_kind)}"

        if self.rng.choice([True, False]):
            query += f" UNION ALL SELECT {expressions} FROM {obj_name}"

            if join:
                column2 = self.rng.choice(columns)
                query += f" JOIN {obj2_name} ON {column} = {column2}"

            if self.rng.choice([True, False]):
                query += (
                    f" WHERE {expression(Boolean, all_columns, self.rng, expr_kind)}"
                )

        query += f" LIMIT {self.rng.randint(0, 100)}"
        return query

    def exe_prepared(self, query: str, stmt_name: str, exe: Executor) -> None:
        # TODO: Parameters
        exe.execute(
            f"PREPARE {stmt_name} AS {query}",
            explainable=False,
            http=Http.NO,
            fetch=False,
        )
        exe.execute(
            f"EXECUTE {stmt_name}", explainable=False, http=Http.NO, fetch=False
        )
        exe.execute(
            f"DEALLOCATE {stmt_name}", explainable=False, http=Http.NO, fetch=False
        )


class FetchAction(Action):
    def __init__(self, rng: random.Random, composition: Composition | None):
        super().__init__(rng, composition)
        self.i = 0

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        result.extend(
            [
                "is not of expected type",  # TODO(def-) Remove when database-issues#7857 is fixed
                "cached plan must not change result type",  # Expected, see database-issues#9666
            ]
        )
        if exe.db.complexity == Complexity.DDL:
            result.extend(
                [
                    "does not exist",
                    "subscribe has been terminated because underlying relation",
                    "subscribe has been terminated because underlying cluster",
                ]
            )
        return result

    def run(self, exe: Executor) -> bool:
        self.i += 1
        # Unsupported via this API
        # See https://github.com/MaterializeInc/database-issues/issues/6159
        (
            exe.rollback(http=Http.NO)
            if self.rng.choice([True, False])
            else exe.commit(http=Http.NO)
        )
        query = "SUBSCRIBE "
        if self.rng.choice([True, False]):
            obj = self.rng.choice(exe.db.db_objects())
            query += f"{obj}"

            if self.rng.choice([True, False]):
                envelope = "UPSERT" if self.rng.choice([True, False]) else "DEBEZIUM"
                columns = self.rng.sample(obj.columns, len(obj.columns))
                key = ", ".join(column.name(True) for column in columns)
                query += f" ENVELOPE {envelope} (KEY ({key}))"
        else:
            query += f"({self.generate_select_query(exe, ExprKind.MATERIALIZABLE)})"

        exe.execute(f"DECLARE c{self.i} CURSOR FOR {query}", http=Http.NO)
        while True:
            rows = self.rng.choice(["ALL", self.rng.randrange(1000)])
            timeout = self.rng.randrange(10)
            query = f"FETCH {rows} c{self.i} WITH (timeout='{timeout}s')"

            if self.rng.choice([True, False]):
                self.stmt_id += 1
                self.exe_prepared(query, f"fetch{self.stmt_id}", exe)
            else:
                exe.execute(query, http=Http.NO, fetch=True)
            if self.rng.choice([True, False]):
                break
        (
            exe.rollback(http=Http.NO)
            if self.rng.choice([True, False])
            else exe.commit(http=Http.NO)
        )
        return True


class SelectOneAction(Action):
    def run(self, exe: Executor) -> bool:
        exe.execute("SELECT 1", explainable=True, http=Http.RANDOM, fetch=True)
        return True


class SelectAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        result.extend(
            [
                "in the same timedomain",
                'is not allowed from the "mz_catalog_server" cluster',
                "timed out before ingesting the source's visible frontier when real-time-recency query issued",
            ]
        )
        if exe.db.complexity == Complexity.DDL:
            result.extend(
                [
                    "does not exist",
                ]
            )
        return result

    def run(self, exe: Executor) -> bool:
        query = self.generate_select_query(exe, ExprKind.ALL)
        rtr = self.rng.choice([True, False])
        if rtr:
            exe.execute("SET REAL_TIME_RECENCY TO TRUE", explainable=False)
        if self.rng.choice([True, False]):
            self.stmt_id += 1
            self.exe_prepared(query, f"select{self.stmt_id}", exe)
        else:
            exe.execute(query, explainable=True, http=Http.RANDOM, fetch=True)
        if rtr:
            exe.execute("SET REAL_TIME_RECENCY TO FALSE", explainable=False)
        return True


class SQLsmithAction(Action):
    composition: Composition
    queries: list[str]

    def __init__(self, rng: random.Random, composition: Composition | None):
        super().__init__(rng, composition)
        self.queries = []
        assert self.composition

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        result.extend(known_errors)
        result.extend(
            [
                "in the same timedomain",
                'is not allowed from the "mz_catalog_server" cluster',
            ]
        )
        if exe.db.complexity == Complexity.DDL:
            result.extend(
                [
                    "does not exist",
                ]
            )
        return result

    def refill_sqlsmith(self, exe: Executor) -> None:
        self.composition.silent = True
        seed = self.rng.randrange(2**31)
        try:
            result = self.composition.run(
                "sqlsmith",
                "--max-joins=0",
                "--target=host=materialized port=6875 dbname=materialize user=materialize",
                "--read-state",
                "--dry-run",
                "--max-queries=100",
                f"--seed={seed}",
                stdin=exe.db.sqlsmith_state,
                capture=True,
                capture_stderr=True,
                rm=True,
            )
            if result.returncode != 0:
                raise ValueError(
                    f"SQLsmith failed: {result.returncode} (seed {seed})\nStderr: {result.stderr}\nState: {exe.db.sqlsmith_state}"
                )
            try:
                data = json.loads(result.stdout)
                self.queries.extend(data["queries"])
            except:
                print(f"Loading json failed: {result.stdout}")
                # TODO(def-) SQLsmith sporadically fails to output
                # the entire json, I believe this to be a bug in the C++
                # json library used or the interaction with Python reading from
                # it. Ignore for now
                return
        except:
            if exe.db.scenario not in (
                Scenario.Kill,
                Scenario.BackupRestore,
                Scenario.ZeroDowntimeDeploy,
            ):
                raise
        finally:
            self.composition.silent = False

    def run(self, exe: Executor) -> bool:
        while not self.queries:
            self.refill_sqlsmith(exe)
        query = self.queries.pop()
        exe.execute(query, explainable=True, http=Http.RANDOM, fetch=True)
        return True


class CopyToS3Action(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        result.extend(
            [
                "in the same timedomain",
                'is not allowed from the "mz_catalog_server" cluster',
                "copy has been terminated because underlying relation",
                "Relation contains unimplemented arrow types",
                "Cannot encode the following columns/types",
                "timeout: error trying to connect",
                "cannot represent decimal value",  # parquet limitation
                "Cannot represent special numeric value",  # parquet limitation
            ]
        )
        if exe.db.complexity == Complexity.DDL:
            result.extend(
                [
                    "does not exist",
                ]
            )
        return result

    def run(self, exe: Executor) -> bool:
        obj = self.rng.choice(exe.db.db_objects())
        obj_name = str(obj)
        with exe.db.lock:
            location = exe.db.s3_path
            exe.db.s3_path += 1
        format = "csv" if self.rng.choice([True, False]) else "parquet"
        if self.rng.random() < 0.9:
            expressions = ", ".join(
                [
                    expression(self.rng.choice(list(DATA_TYPES)), obj.columns, self.rng)
                    for i in range(self.rng.randint(1, 10))
                ]
            )
        else:
            expressions = "*"
        query = f"COPY (SELECT {expressions} FROM {obj_name} WHERE {expression(Boolean, obj.columns, self.rng)} LIMIT {self.rng.randint(0, 100)}) TO 's3://copytos3/{location}' WITH (AWS CONNECTION = aws_conn, FORMAT = '{format}')"

        exe.execute(query, explainable=False, http=Http.NO, fetch=False)
        return True


class InsertAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        if exe.db.complexity == Complexity.DDL:
            result.extend(
                [
                    "does not exist",
                ]
            )
        return result

    def run(self, exe: Executor) -> bool:
        table = None
        if exe.insert_table is not None:
            for t in exe.db.tables:
                if t.table_id == exe.insert_table:
                    table = t
                    if table.num_rows >= MAX_ROWS:
                        (
                            exe.commit()
                            if self.rng.choice([True, False])
                            else exe.rollback()
                        )
                        table = None
                    break
            else:
                exe.commit() if self.rng.choice([True, False]) else exe.rollback()
        if not table:
            tables = [table for table in exe.db.tables if table.num_rows < MAX_ROWS]
            if not tables:
                return False
            table = self.rng.choice(tables)

        column_names = ", ".join(column.name(True) for column in table.columns)
        column_values = []
        max_rows = min(100, MAX_ROWS - table.num_rows)
        for i in range(self.rng.randrange(1, max_rows + 1)):
            column_values.append(
                ", ".join(column.value(self.rng, True) for column in table.columns)
            )
        all_column_values = ", ".join(f"({v})" for v in column_values)
        query = f"INSERT INTO {table} ({column_names}) VALUES {all_column_values}"
        # TODO: Use INSERT INTO {} SELECT {} (only works for tables)
        if self.rng.choice([True, False]):
            self.stmt_id += 1
            self.exe_prepared(query, f"insert{self.stmt_id}", exe)
        else:
            exe.execute(query, http=Http.RANDOM)
        table.num_rows += len(column_values)
        exe.insert_table = table.table_id
        return True


class CopyFromStdinAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        if exe.db.complexity == Complexity.DDL:
            result.extend(
                [
                    "COPY FROM's target table",
                ]
            )
        return result

    def run(self, exe: Executor) -> bool:
        table = None
        if exe.insert_table is not None:
            for t in exe.db.tables:
                if t.table_id == exe.insert_table:
                    table = t
                    if table.num_rows >= MAX_ROWS:
                        (
                            exe.commit()
                            if self.rng.choice([True, False])
                            else exe.rollback()
                        )
                        table = None
                    break
            else:
                exe.commit() if self.rng.choice([True, False]) else exe.rollback()
        if not table:
            tables = [table for table in exe.db.tables if table.num_rows < MAX_ROWS]
            if not tables:
                return False
            table = self.rng.choice(tables)

        values = []
        max_rows = min(100, MAX_ROWS - table.num_rows)
        for i in range(self.rng.randrange(1, max_rows + 1)):
            values.append([column.value(self.rng, False) for column in table.columns])
        query = f"COPY INTO {table} FROM STDIN"
        exe.copy(query, values)
        table.num_rows += len(values)
        exe.insert_table = table.table_id
        return True


class InsertReturningAction(Action):
    def run(self, exe: Executor) -> bool:
        table = None
        if exe.insert_table is not None:
            for t in exe.db.tables:
                if t.table_id == exe.insert_table:
                    table = t
                    if table.num_rows >= MAX_ROWS:
                        (
                            exe.commit()
                            if self.rng.choice([True, False])
                            else exe.rollback()
                        )
                        table = None
                    break
            else:
                exe.commit() if self.rng.choice([True, False]) else exe.rollback()
        if not table:
            tables = [table for table in exe.db.tables if table.num_rows < MAX_ROWS]
            if not tables:
                return False
            table = self.rng.choice(tables)

        column_names = ", ".join(column.name(True) for column in table.columns)
        column_values = []
        max_rows = min(100, MAX_ROWS - table.num_rows)
        for i in range(self.rng.randrange(1, max_rows + 1)):
            column_values.append(
                ", ".join(column.value(self.rng, True) for column in table.columns)
            )
        all_column_values = ", ".join(f"({v})" for v in column_values)
        query = f"INSERT INTO {table} ({column_names}) VALUES {all_column_values}"
        # TODO: Use INSERT INTO {} SELECT {} (only works for tables)
        returning_exprs = []
        if self.rng.random() < 0.5:
            returning_exprs += [
                expression(
                    self.rng.choice(list(DATA_TYPES)),
                    table.columns,
                    self.rng,
                    kind=ExprKind.WRITE,
                )
                for i in range(self.rng.randint(1, 10))
            ]
        elif self.rng.choice([True, False]):
            returning_exprs.append("*")
        if returning_exprs:
            query += f" RETURNING {', '.join(returning_exprs)}"
        if self.rng.choice([True, False]):
            self.stmt_id += 1
            self.exe_prepared(query, f"insert_returning{self.stmt_id}", exe)
        else:
            exe.execute(query, http=Http.RANDOM)
        table.num_rows += len(column_values)
        exe.insert_table = table.table_id
        return True


class SourceInsertAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            sources = [
                source
                for source in exe.db.kafka_sources
                + exe.db.postgres_sources
                + exe.db.mysql_sources
                + exe.db.sql_server_sources
                if source.num_rows < MAX_ROWS
            ]
            if not sources:
                return False
            source = self.rng.choice(sources)
        with source.lock:
            if source not in [
                *exe.db.kafka_sources,
                *exe.db.postgres_sources,
                *exe.db.mysql_sources,
                *exe.db.sql_server_sources,
            ]:
                return False

            transaction = next(source.generator)
            for row_list in transaction.row_lists:
                for row in row_list.rows:
                    if row.operation == Operation.INSERT:
                        source.num_rows += 1
                    elif row.operation == Operation.DELETE:
                        source.num_rows -= 1
            source.executor.run(transaction, logging_exe=exe)
        return True


class UpdateAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        result.extend(
            [
                "canceling statement due to statement timeout",
            ]
        )

        if exe.db.complexity == Complexity.DDL:
            result.extend(
                [
                    "does not exist",
                ]
            )
        return result

    def run(self, exe: Executor) -> bool:
        table = None
        if exe.insert_table is not None:
            for t in exe.db.tables:
                if t.table_id == exe.insert_table:
                    table = t
                    break
        if not table:
            table = self.rng.choice(exe.db.tables)

        table.columns[0]
        column2 = self.rng.choice(table.columns)
        query = f"UPDATE {table} SET {column2.name(True)} = {expression(column2.data_type, table.columns, self.rng, kind=ExprKind.WRITE)} WHERE {expression(Boolean, table.columns, self.rng, kind=ExprKind.WRITE)}"
        if self.rng.choice([True, False]):
            self.stmt_id += 1
            self.exe_prepared(query, f"update{self.stmt_id}", exe)
        else:
            exe.execute(query, http=Http.RANDOM)
        exe.insert_table = table.table_id
        return True


class DeleteAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "canceling statement due to statement timeout",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        table = self.rng.choice(exe.db.tables)
        query = f"DELETE FROM {table}"
        if self.rng.random() < 0.95:
            query += f" WHERE {expression(Boolean, table.columns, self.rng, kind=ExprKind.WRITE)}"
        if self.rng.choice([True, False]):
            self.stmt_id += 1
            self.exe_prepared(query, f"delete{self.stmt_id}", exe)
        else:
            exe.execute(query, http=Http.RANDOM)
        exe.commit()
        result = exe.cur.rowcount
        table.num_rows -= result
        return True


class CommentAction(Action):
    def run(self, exe: Executor) -> bool:
        table = self.rng.choice(exe.db.tables)

        if self.rng.choice([True, False]):
            column = self.rng.choice(table.columns)
            query = f"COMMENT ON COLUMN {column} IS '{Text.random_value(self.rng)}'"
        else:
            query = f"COMMENT ON TABLE {table} IS '{Text.random_value(self.rng)}'"

        exe.execute(query, http=Http.RANDOM)
        return True


class CreateIndexAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "already exists",  # TODO: Investigate
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        if len(exe.db.indexes) >= MAX_INDEXES:
            return False

        obj = self.rng.choice(exe.db.db_objects())
        columns = self.rng.sample(obj.columns, len(obj.columns))
        columns_str = "_".join(column.name() for column in columns)
        # columns_str may exceed 255 characters, so it is converted to a positive number with hash
        index = Index(f"idx_{obj.name()}_{abs(hash(columns_str))}")
        index_elems = []
        for column in columns:
            order = self.rng.choice(["ASC", "DESC"])
            index_elems.append(f"{column.name(True)} {order}")
        index_str = ", ".join(index_elems)
        query = f"CREATE INDEX {index} ON {obj} ({index_str})"
        exe.execute(query, http=Http.RANDOM)
        with exe.db.lock:
            exe.db.indexes.add(index)
        return True


class DropIndexAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.indexes:
                return False
            index = self.rng.choice(list(exe.db.indexes))
        with index.lock:
            if index not in exe.db.indexes:
                return False

            query = f"DROP INDEX {index}"
            exe.execute(query, http=Http.RANDOM)
            exe.db.indexes.remove(index)
            return True


class CreateTableAction(Action):
    def run(self, exe: Executor) -> bool:
        if len(exe.db.tables) >= MAX_TABLES:
            return False
        table_id = exe.db.table_id
        exe.db.table_id += 1
        try:
            schema = self.rng.choice(exe.db.schemas)
        except IndexError:
            # We mostly prevent index errors, but we don't want to lock too
            # much since that would reduce our chance of finding race
            # conditions in production code, so ignore the rare case where
            # we accidentally removed all objects.
            return False
        with schema.lock:
            if schema not in exe.db.schemas:
                return False
            table = Table(self.rng, table_id, schema)
            table.create(exe)
        exe.db.tables.append(table)
        return True


class DropTableAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.tables) <= 2:
                return False
            table = self.rng.choice(exe.db.tables)
        with table.lock:
            # Was dropped while we were acquiring lock
            if table not in exe.db.tables:
                return False
            if len(exe.db.tables) <= 2:
                return False

            query = f"DROP TABLE {table}"
            exe.execute(query, http=Http.RANDOM)
            exe.db.tables.remove(table)
        return True


class RenameTableAction(Action):
    def run(self, exe: Executor) -> bool:
        if exe.db.scenario != Scenario.Rename:
            return False
        with exe.db.lock:
            if not exe.db.tables:
                return False
            table = self.rng.choice(exe.db.tables)
        with table.lock:
            old_name = str(table)
            table.rename += 1
            try:
                exe.execute(
                    f"ALTER TABLE {old_name} RENAME TO {identifier(table.name())}",
                    # http=Http.RANDOM,  # Fails, see https://buildkite.com/materialize/nightly/builds/7362#018ecc56-787f-4cc2-ac54-1c8437af164b
                )
            except:
                table.rename -= 1
                raise
        return True


class AlterTableAddColumnAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.tables:
                return False
            if exe.db.flags.get("enable_alter_table_add_column", "FALSE") != "TRUE":
                return False
            table = self.rng.choice(exe.db.tables)
        with table.lock:
            # Allow adding more a few more columns than the max for additional coverage.
            if len(table.columns) >= MAX_COLUMNS + 3:
                return False

            # TODO(alter_table): Support adding non-nullable columns with a default value.
            new_column = Column(
                self.rng, len(table.columns), self.rng.choice(DATA_TYPES), table
            )
            new_column.raw_name = f"{new_column.raw_name}-altered"
            new_column.nullable = True
            new_column.default = None

            try:
                exe.execute(
                    f"ALTER TABLE {str(table)} ADD COLUMN {new_column.create()}"
                )
            except:
                raise
            table.columns.append(new_column)
        return True


class RenameViewAction(Action):
    def run(self, exe: Executor) -> bool:
        if exe.db.scenario != Scenario.Rename:
            return False
        with exe.db.lock:
            if not exe.db.views:
                return False
            view = self.rng.choice(exe.db.views)
        with view.lock:
            if view not in exe.db.views:
                return False

            old_name = str(view)
            view.rename += 1
            try:
                exe.execute(
                    f"ALTER {'MATERIALIZED VIEW' if view.materialized else 'VIEW'} {old_name} RENAME TO {identifier(view.name())}",
                    # http=Http.RANDOM,  # Fails, see https://buildkite.com/materialize/nightly/builds/7362#018ecc56-787f-4cc2-ac54-1c8437af164b
                )
            except:
                view.rename -= 1
                raise
        return True


class RenameSinkAction(Action):
    def run(self, exe: Executor) -> bool:
        if exe.db.scenario != Scenario.Rename:
            return False
        with exe.db.lock:
            if not exe.db.kafka_sinks:
                return False
            try:
                sink = self.rng.choice(exe.db.kafka_sinks)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with sink.lock:
            if sink not in exe.db.kafka_sinks:
                return False

            old_name = str(sink)
            sink.rename += 1
            try:
                exe.execute(
                    f"ALTER SINK {old_name} RENAME TO {identifier(sink.name())}",
                    # http=Http.RANDOM,  # Fails
                )
            except:
                sink.rename -= 1
                raise
        return True


class AlterKafkaSinkFromAction(Action):
    def run(self, exe: Executor) -> bool:
        if exe.db.scenario in (Scenario.Kill, Scenario.ZeroDowntimeDeploy):
            # Does not work reliably with kills, see database-issues#8421
            return False
        with exe.db.lock:
            if not exe.db.kafka_sinks:
                return False
            try:
                sink = self.rng.choice(exe.db.kafka_sinks)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with sink.lock, sink.base_object.lock:
            if sink not in exe.db.kafka_sinks:
                return False

            old_object = sink.base_object
            if sink.key != "":
                # key requires same column names, low chance of even having that
                return False
            elif sink.format in ["FORMAT BYTES", "FORMAT TEXT"]:
                # single column formats
                objs = [
                    o
                    for o in exe.db.db_objects_without_views()
                    if len(o.columns) == 1
                    and o.columns[0].data_type == old_object.columns[0].data_type
                ]
            elif sink.format in ["FORMAT JSON"]:
                # We should be able to format all data types as JSON, and they have no
                # particular backwards-compatiblility requirements.
                objs = [o for o in exe.db.db_objects_without_views()]
            else:
                # Avro schema migration checking can be quite strict, and we need to be not only
                # compatible with the latest object's schema but all previous schemas.
                # Only allow a conservative case for now: where all types and names match.
                objs = []
                old_cols = {c.name(True): c.data_type for c in old_object.columns}
                for o in exe.db.db_objects_without_views():
                    if isinstance(old_object, WebhookSource):
                        continue
                    if isinstance(o, WebhookSource):
                        continue
                    new_cols = {c.name(True): c.data_type for c in o.columns}
                    if old_cols == new_cols:
                        objs.append(o)
            if not objs:
                return False
            sink.base_object = self.rng.choice(objs)

            try:
                exe.execute(
                    f"ALTER SINK {sink} SET FROM {sink.base_object}",
                    # http=Http.RANDOM,  # Fails
                )
            except:
                sink.base_object = old_object
                raise
        return True


class CreateDatabaseAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.dbs) >= MAX_DBS:
                return False
            db_id = exe.db.db_id
            exe.db.db_id += 1
        db = DB(exe.db.seed, db_id)
        db.create(exe)
        exe.db.dbs.append(db)
        return True


class DropDatabaseAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "cannot be dropped with RESTRICT while it contains schemas",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.dbs) <= 1:
                return False
            try:
                db = self.rng.choice(exe.db.dbs)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with db.lock:
            # Was dropped while we were acquiring lock
            if db not in exe.db.dbs:
                return False
            if len(exe.db.dbs) <= 1:
                return False

            query = f"DROP DATABASE {db} RESTRICT"
            exe.execute(query, http=Http.RANDOM)
            exe.db.dbs.remove(db)
        return True


class CreateSchemaAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.schemas) >= MAX_SCHEMAS:
                return False
            schema_id = exe.db.schema_id
            exe.db.schema_id += 1
        try:
            schema = Schema(self.rng.choice(exe.db.dbs), schema_id)
        except IndexError:
            # We mostly prevent index errors, but we don't want to lock too
            # much since that would reduce our chance of finding race
            # conditions in production code, so ignore the rare case where
            # we accidentally removed all objects.
            return False
        schema.create(exe)
        exe.db.schemas.append(schema)
        return True


class DropSchemaAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "cannot be dropped without CASCADE while it contains objects",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.schemas) <= 1:
                return False
            try:
                schema = self.rng.choice(exe.db.schemas)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with schema.lock:
            # Was dropped while we were acquiring lock
            if schema not in exe.db.schemas:
                return False
            if len(exe.db.schemas) <= 1:
                return False

            query = f"DROP SCHEMA {schema}"
            exe.execute(query, http=Http.RANDOM)
            exe.db.schemas.remove(schema)
        return True


class RenameSchemaAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "ambiguous reference to schema named"  # see https://github.com/MaterializeInc/materialize/pull/22551#pullrequestreview-1691876923
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        if exe.db.scenario != Scenario.Rename:
            return False
        with exe.db.lock:
            try:
                schema = self.rng.choice(exe.db.schemas)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with schema.lock:
            if schema not in exe.db.schemas:
                return False
            old_name = str(schema)
            schema.rename += 1
            try:
                exe.execute(
                    f"ALTER SCHEMA {old_name} RENAME TO {identifier(schema.name())}",
                    # http=Http.RANDOM,  # Fails
                )
            except:
                schema.rename -= 1
                raise
        return True


class SwapSchemaAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "object state changed while transaction was in progress",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        if exe.db.scenario != Scenario.Rename:
            return False
        with exe.db.lock:
            try:
                db = self.rng.choice(exe.db.dbs)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
            schemas = [
                schema for schema in exe.db.schemas if schema.db.db_id == db.db_id
            ]
            if len(schemas) < 2:
                return False
            schema_ids = sorted(self.rng.sample(range(0, len(schemas)), 2))
            schema1 = schemas[schema_ids[0]]
            schema2 = schemas[schema_ids[1]]
        with schema1.lock, schema2.lock:
            if schema1 not in exe.db.schemas:
                return False
            if schema2 not in exe.db.schemas:
                return False
            if self.rng.choice([True, False]):
                exe.execute(
                    f"ALTER SCHEMA {schema1} SWAP WITH {identifier(schema2.name())}"
                )
            else:
                exe.cur.connection.autocommit = False
                try:
                    exe.execute(f"ALTER SCHEMA {schema1} RENAME TO tmp_schema")
                    exe.execute(
                        f"ALTER SCHEMA {schema2} RENAME TO {identifier(schema1.name())}"
                    )
                    exe.execute(
                        f"ALTER SCHEMA tmp_schema RENAME TO {identifier(schema1.name())}"
                    )
                    exe.commit()
                finally:
                    try:
                        exe.cur.connection.autocommit = True
                    except:
                        exe.reconnect_next = True
            schema1.schema_id, schema2.schema_id = schema2.schema_id, schema1.schema_id
            schema1.rename, schema2.rename = schema2.rename, schema1.rename
        return True


class TransactionIsolationAction(Action):
    def run(self, exe: Executor) -> bool:
        level = self.rng.choice(["SERIALIZABLE", "STRICT SERIALIZABLE"])
        exe.set_isolation(level)
        return True


class CommitRollbackAction(Action):
    def run(self, exe: Executor) -> bool:
        if not exe.action_run_since_last_commit_rollback:
            return False

        if self.rng.random() < 0.7:
            exe.commit()
        else:
            exe.rollback()
        exe.action_run_since_last_commit_rollback = False
        return True


class FlipFlagsAction(Action):
    def __init__(
        self,
        rng: random.Random,
        composition: Composition | None,
    ):
        super().__init__(rng, composition)

        BOOLEAN_FLAG_VALUES = ["TRUE", "FALSE"]

        self.flags_with_values: dict[str, list[str]] = dict()
        self.flags_with_values["persist_blob_target_size"] = (
            # 1 MiB, 16 MiB, 128 MiB
            ["1048576", "16777216", "134217728"]
        )
        for flag in ["catalog", "source", "snapshot", "txn"]:
            self.flags_with_values[f"persist_use_critical_since_{flag}"] = (
                BOOLEAN_FLAG_VALUES
            )
        self.flags_with_values["persist_claim_unclaimed_compactions"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["persist_optimize_ignored_data_fetch"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["enable_variadic_left_join_lowering"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["enable_eager_delta_joins"] = BOOLEAN_FLAG_VALUES
        self.flags_with_values["persist_batch_structured_key_lower_len"] = [
            "0",
            "1",
            "512",
            "1000",
            "50000",
        ]
        self.flags_with_values["persist_batch_max_run_len"] = [
            "2",
            "3",
            "4",
            "16",
            "1000",
        ]
        self.flags_with_values["persist_compaction_memory_bound_bytes"] = [
            # 64 MiB, 1 * 128 MiB, 4 * 128 MiB, 8 * 128 MiB
            "67108864",
            "134217728",
            "536870912",
            "1073741824",
        ]
        self.flags_with_values["persist_part_decode_format"] = [
            "row_with_validate",
            "arrow",
        ]
        self.flags_with_values["persist_encoding_enable_dictionary"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["persist_enable_incremental_compaction"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["persist_stats_audit_percent"] = [
            "0",
            "1",
            "2",
            "10",
            "100",
        ]
        self.flags_with_values["persist_stats_audit_panic"] = BOOLEAN_FLAG_VALUES
        # Note: it's not safe to re-enable this flag after writing with `persist_validate_part_bounds_on_write`,
        # since those new-style parts may fail our old-style validation.
        self.flags_with_values["persist_validate_part_bounds_on_read"] = ["FALSE"]
        self.flags_with_values["persist_validate_part_bounds_on_write"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["compute_apply_column_demands"] = BOOLEAN_FLAG_VALUES
        self.flags_with_values["enable_compute_temporal_bucketing"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["enable_alter_table_add_column"] = BOOLEAN_FLAG_VALUES
        self.flags_with_values["enable_compute_peek_response_stash"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["compute_peek_response_stash_threshold_bytes"] = [
            "0",  # "force enabled"
            "1048576",  # 1 MiB, an in-between value
            "314572800",  # 300 MiB, the production value
        ]
        self.flags_with_values["cluster"] = ["quickstart", "dont_exist"]
        self.flags_with_values["enable_frontend_peek_sequencing"] = [
            "true",
            "false",
        ]

        # If you are adding a new config flag in Materialize, consider using it
        # here instead of just marking it as uninteresting to silence the
        # linter. parallel-workload randomly flips the flags in
        # `flags_with_values` while running. If a new flag has interesting
        # behavior, you should add it. Feature flags which turn on/off
        # externally visible features should not be flipped.
        self.uninteresting_flags: list[str] = [
            "enable_mz_join_core",
            "enable_compute_correction_v2",
            "linear_join_yielding",
            "enable_lgalloc",
            "enable_lgalloc_eager_reclamation",
            "lgalloc_background_interval",
            "lgalloc_file_growth_dampener",
            "lgalloc_local_buffer_bytes",
            "lgalloc_slow_clear_bytes",
            "memory_limiter_interval",
            "memory_limiter_usage_bias",
            "memory_limiter_burst_factor",
            "enable_columnation_lgalloc",
            "enable_columnar_lgalloc",
            "compute_server_maintenance_interval",
            "compute_dataflow_max_inflight_bytes",
            "compute_dataflow_max_inflight_bytes_cc",
            "compute_flat_map_fuel",
            "consolidating_vec_growth_dampener",
            "compute_hydration_concurrency",
            "copy_to_s3_parquet_row_group_file_ratio",
            "copy_to_s3_arrow_builder_buffer_ratio",
            "copy_to_s3_multipart_part_size_bytes",
            "enable_compute_replica_expiration",
            "compute_replica_expiration_offset",
            "enable_compute_render_fueled_as_specific_collection",
            "compute_temporal_bucketing_summary",
            "enable_compute_logical_backpressure",
            "compute_logical_backpressure_max_retained_capabilities",
            "compute_logical_backpressure_inflight_slack",
            "persist_fetch_semaphore_cost_adjustment",
            "persist_fetch_semaphore_permit_adjustment",
            "persist_pubsub_client_enabled",
            "persist_pubsub_push_diff_enabled",
            "persist_pubsub_same_process_delegate_enabled",
            "persist_pubsub_connect_attempt_timeout",
            "persist_pubsub_request_timeout",
            "persist_pubsub_connect_max_backoff",
            "persist_pubsub_client_sender_channel_size",
            "persist_pubsub_client_receiver_channel_size",
            "persist_pubsub_server_connection_channel_size",
            "persist_pubsub_state_cache_shard_ref_channel_size",
            "persist_pubsub_reconnect_backoff",
            "persist_batch_delete_enabled",
            "persist_encoding_compression_format",
            "persist_batch_max_runs",
            "persist_inline_writes_single_max_bytes",
            "persist_inline_writes_total_max_bytes",
            "persist_write_combine_inline_writes",
            "storage_source_decode_fuel",
            "persist_reader_lease_duration",
            "persist_consensus_connection_pool_max_size",
            "persist_consensus_connection_pool_max_wait",
            "persist_consensus_connection_pool_ttl",
            "persist_consensus_connection_pool_ttl_stagger",
            "crdb_connect_timeout",
            "crdb_tcp_user_timeout",
            "use_global_txn_cache_source",
            "persist_batch_builder_max_outstanding_parts",
            "persist_compaction_heuristic_min_inputs",
            "persist_compaction_heuristic_min_parts",
            "persist_compaction_heuristic_min_updates",
            "persist_gc_blob_delete_concurrency_limit",
            "persist_state_versions_recent_live_diffs_limit",
            "persist_usage_state_fetch_concurrency_limit",
            "persist_blob_operation_timeout",
            "persist_blob_operation_attempt_timeout",
            "persist_blob_connect_timeout",
            "persist_blob_read_timeout",
            "persist_stats_collection_enabled",
            "persist_stats_filter_enabled",
            "persist_stats_budget_bytes",
            "persist_stats_untrimmable_columns_equals",
            "persist_stats_untrimmable_columns_prefix",
            "persist_stats_untrimmable_columns_suffix",
            "persist_catalog_force_compaction_fuel",
            "persist_catalog_force_compaction_wait",
            "persist_expression_cache_force_compaction_fuel",
            "persist_expression_cache_force_compaction_wait",
            "persist_blob_cache_mem_limit_bytes",
            "persist_blob_cache_scale_with_threads",
            "persist_blob_cache_scale_factor_bytes",
            "persist_claim_compaction_percent",
            "persist_claim_compaction_min_version",
            "persist_next_listen_batch_retryer_fixed_sleep",
            "persist_next_listen_batch_retryer_initial_backoff",
            "persist_next_listen_batch_retryer_multiplier",
            "persist_next_listen_batch_retryer_clamp",
            "persist_rollup_threshold",
            "persist_rollup_fallback_threshold_ms",
            "persist_rollup_use_active_rollup",
            "persist_gc_fallback_threshold_ms",
            "persist_gc_use_active_gc",
            "persist_gc_min_versions",
            "persist_gc_max_versions",
            "persist_compaction_minimum_timeout",
            "persist_compaction_check_process_flag",
            "balancerd_sigterm_connection_wait",
            "balancerd_sigterm_listen_wait",
            "balancerd_inject_proxy_protocol_header_http",
            "balancerd_log_filter",
            "balancerd_opentelemetry_filter",
            "balancerd_log_filter_defaults",
            "balancerd_opentelemetry_filter_defaults",
            "balancerd_sentry_filters",
            "persist_enable_s3_lgalloc_cc_sizes",
            "persist_enable_s3_lgalloc_noncc_sizes",
            "persist_enable_arrow_lgalloc_cc_sizes",
            "persist_enable_arrow_lgalloc_noncc_sizes",
            "controller_past_generation_replica_cleanup_retry_interval",
            "enable_0dt_deployment_sources",
            "enable_0dt_caught_up_replica_status_check",
            "wallclock_lag_recording_interval",
            "wallclock_lag_histogram_period_interval",
            "enable_timely_zero_copy",
            "enable_timely_zero_copy_lgalloc",
            "timely_zero_copy_limit",
            "arrangement_exert_proportionality",
            "txn_wal_apply_ensure_schema_match",
            "persist_txns_data_shard_retryer_initial_backoff",
            "persist_txns_data_shard_retryer_multiplier",
            "persist_txns_data_shard_retryer_clamp",
            "storage_cluster_shutdown_grace_period",
            "storage_dataflow_delay_sources_past_rehydration",
            "storage_dataflow_suspendable_sources",
            "storage_downgrade_since_during_finalization",
            "replica_metrics_history_retention_interval",
            "wallclock_lag_history_retention_interval",
            "wallclock_global_lag_histogram_retention_interval",
            "kafka_client_id_enrichment_rules",
            "kafka_poll_max_wait",
            "kafka_default_metadata_fetch_interval",
            "kafka_default_aws_privatelink_endpoint_identification_algorithm",
            "kafka_buffered_event_resize_threshold_elements",
            "mysql_replication_heartbeat_interval",
            "mysql_offset_known_interval",
            "postgres_fetch_slot_resume_lsn_interval",
            "pg_offset_known_interval",
            "pg_schema_validation_interval",
            "storage_enforce_external_addresses",
            "storage_upsert_prevent_snapshot_buffering",
            "storage_rocksdb_use_merge_operator",
            "storage_upsert_max_snapshot_batch_buffering",
            "storage_rocksdb_cleanup_tries",
            "storage_suspend_and_restart_delay",
            "storage_reclock_to_latest",
            "storage_use_continual_feedback_upsert",
            "storage_server_maintenance_interval",
            "storage_sink_progress_search",
            "storage_sink_ensure_topic_config",
            "ore_overflowing_behavior",
            "sql_server_max_lsn_wait",
            "sql_server_snapshot_progress_report_interval",
            "sql_server_cdc_poll_interval",
            "sql_server_cdc_cleanup_change_table",
            "sql_server_cdc_cleanup_change_table_max_deletes",
            "sql_server_offset_known_interval",
            "allow_user_sessions",
            "enable_0dt_deployment",
            "with_0dt_deployment_max_wait",
            "with_0dt_deployment_ddl_check_interval",
            "enable_0dt_deployment_panic_after_timeout",
            "enable_0dt_caught_up_check",
            "with_0dt_caught_up_check_allowed_lag",
            "with_0dt_caught_up_check_cutoff",
            "enable_statement_lifecycle_logging",
            "enable_introspection_subscribes",
            "plan_insights_notice_fast_path_clusters_optimize_duration",
            "enable_continual_task_builtins",
            "enable_expression_cache",
            "enable_multi_replica_sources",
            "enable_password_auth",
            "constraint_based_timestamp_selection",
            "persist_fast_path_order",
            "mz_metrics_lgalloc_map_refresh_interval",
            "mz_metrics_lgalloc_refresh_interval",
            "mz_metrics_rusage_refresh_interval",
            "compute_peek_stash_num_batches",
            "compute_peek_stash_batch_size",
            "compute_peek_response_stash_batch_max_runs",
            "compute_peek_response_stash_read_batch_size_bytes",
            "compute_peek_response_stash_read_memory_budget_bytes",
            "storage_statistics_retention_duration",
            "enable_paused_cluster_readhold_downgrade",
            "enable_with_ordinality_legacy_fallback",
            "kafka_retry_backoff",
            "kafka_retry_backoff_max",
            "kafka_reconnect_backoff",
            "kafka_reconnect_backoff_max",
            "pg_source_validate_timeline",
        ]

    def run(self, exe: Executor) -> bool:
        flag_name = self.rng.choice(list(self.flags_with_values.keys()))

        # TODO: Remove when database-issues#8352 is fixed
        if exe.db.scenario == Scenario.ZeroDowntimeDeploy and flag_name.startswith(
            "persist_use_critical_since_"
        ):
            return False

        flag_value = self.rng.choice(self.flags_with_values[flag_name])

        conn = None

        try:
            conn = self.create_system_connection(exe)
            self.flip_flag(conn, flag_name, flag_value)
            exe.db.flags[flag_name] = flag_value
            return True
        except OperationalError:
            if conn is not None:
                conn.close()

            # ignore it
            return False
        except Exception as e:
            raise QueryError(str(e), "FlipFlags")

    def create_system_connection(
        self, exe: Executor, num_attempts: int = 10
    ) -> Connection:
        try:
            conn = psycopg.connect(
                host=exe.db.host,
                port=exe.db.ports[
                    "mz_system" if exe.mz_service == "materialized" else "mz_system2"
                ],
                user="mz_system",
                dbname="materialize",
            )
            conn.autocommit = True
            return conn
        except:
            if num_attempts == 0:
                raise
            else:
                time.sleep(1)
                return self.create_system_connection(exe, num_attempts - 1)

    def flip_flag(self, conn: Connection, flag_name: str, flag_value: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                f"ALTER SYSTEM SET {flag_name} = {flag_value};".encode(),
            )


class CreateViewAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        errors = super().errors_to_ignore(exe)
        if exe.db.scenario == Scenario.Rename:
            # Columns could have been renamed, we don't lock the base objects
            # to get more interesting race conditions
            errors += ["does not exist"]
        return errors

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.views) >= MAX_VIEWS:
                return False
            view_id = exe.db.view_id
            exe.db.view_id += 1
        # Don't use views for now since LIMIT 1 and statement_timeout are
        # not effective yet at preventing long-running queries and OoMs.
        base_object = self.rng.choice(exe.db.db_objects_without_views())
        base_object2: DBObject | None = self.rng.choice(
            exe.db.db_objects_without_views()
        )
        if self.rng.choice([True, False]) or base_object2 == base_object:
            base_object2 = None
        try:
            schema = self.rng.choice(exe.db.schemas)
        except IndexError:
            # We mostly prevent index errors, but we don't want to lock too
            # much since that would reduce our chance of finding race
            # conditions in production code, so ignore the rare case where
            # we accidentally removed all objects.
            return False
        with schema.lock:
            if schema not in exe.db.schemas:
                return False
            view = View(
                self.rng,
                view_id,
                base_object,
                base_object2,
                schema,
            )
            view.create(exe)
        exe.db.views.append(view)
        return True


class DropViewAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.views:
                return False
            view = self.rng.choice(exe.db.views)
        with view.lock:
            # Was dropped while we were acquiring lock
            if view not in exe.db.views:
                return False

            if view.materialized:
                query = f"DROP MATERIALIZED VIEW {view}"
            else:
                query = f"DROP VIEW {view}"
            exe.execute(query, http=Http.RANDOM)
            exe.db.views.remove(view)
        return True


class CreateRoleAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.roles) >= MAX_ROLES:
                return False
            role_id = exe.db.role_id
            exe.db.role_id += 1
        role = Role(role_id)
        role.create(exe)
        exe.db.roles.append(role)
        return True


class DropRoleAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "cannot be dropped because some objects depend on it",
            "current role cannot be dropped",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.roles:
                return False
            role = self.rng.choice(exe.db.roles)
        with role.lock:
            # Was dropped while we were acquiring lock
            if role not in exe.db.roles:
                return False

            query = f"DROP ROLE {role}"
            try:
                exe.execute(query, http=Http.RANDOM)
            except QueryError as e:
                # expected, see database-issues#6156
                if (
                    exe.db.scenario not in (Scenario.Kill, Scenario.ZeroDowntimeDeploy)
                    or "unknown role" not in e.msg
                ):
                    raise e
            exe.db.roles.remove(role)
        return True


class CreateClusterAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.clusters) >= MAX_CLUSTERS:
                return False
            cluster_id = exe.db.cluster_id
            exe.db.cluster_id += 1
        cluster = Cluster(
            cluster_id,
            managed=self.rng.choice([True, False]),
            size=self.rng.choice(["1", "2"]),
            replication_factor=self.rng.choice([1, 2]),
            introspection_interval="1s",
        )
        cluster.create(exe)
        exe.db.clusters.append(cluster)
        return True


class DropClusterAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            # cannot drop cluster "..." because other objects depend on it
            "because other objects depend on it",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.clusters) <= 1:
                return False
            # Keep cluster 0 with 1 replica for sources/sinks
            self.rng.randrange(1, len(exe.db.clusters))
            try:
                cluster = self.rng.choice(exe.db.clusters)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with cluster.lock:
            # Was dropped while we were acquiring lock
            if cluster not in exe.db.clusters:
                return False

            # Avoid removing all clusters
            if len(exe.db.clusters) <= 1:
                return False

            query = f"DROP CLUSTER {cluster}"
            try:
                exe.execute(query, http=Http.RANDOM)
            except QueryError as e:
                # expected, see database-issues#6156
                if (
                    exe.db.scenario not in (Scenario.Kill, Scenario.ZeroDowntimeDeploy)
                    or "unknown cluster" not in e.msg
                ):
                    raise e
            exe.db.clusters.remove(cluster)
        return True


class SwapClusterAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "object state changed while transaction was in progress",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        if exe.db.scenario != Scenario.Rename:
            return False
        with exe.db.lock:
            if len(exe.db.clusters) < 2:
                return False
            cluster_ids = sorted(self.rng.sample(range(0, len(exe.db.clusters)), 2))
            cluster1 = exe.db.clusters[cluster_ids[0]]
            cluster2 = exe.db.clusters[cluster_ids[1]]
        with cluster1.lock, cluster2.lock:
            if cluster1 not in exe.db.clusters:
                return False
            if cluster2 not in exe.db.clusters:
                return False

            if self.rng.choice([True, False]):
                exe.execute(
                    f"ALTER CLUSTER {cluster1} SWAP WITH {identifier(cluster2.name())}",
                    # http=Http.RANDOM,  # Fails, see https://buildkite.com/materialize/nightly/builds/7362#018ecc56-787f-4cc2-ac54-1c8437af164b
                )
            else:
                exe.cur.connection.autocommit = False
                try:
                    exe.execute(f"ALTER SCHEMA {cluster1} RENAME TO tmp_cluster")
                    exe.execute(
                        f"ALTER SCHEMA {cluster2} RENAME TO {identifier(cluster1.name())}"
                    )
                    exe.execute(
                        f"ALTER SCHEMA tmp_cluster RENAME TO {identifier(cluster1.name())}"
                    )
                    exe.commit()
                finally:
                    try:
                        exe.cur.connection.autocommit = True
                    except:
                        exe.reconnect_next = True
            cluster1.cluster_id, cluster2.cluster_id = (
                cluster2.cluster_id,
                cluster1.cluster_id,
            )
            cluster1.rename, cluster2.rename = cluster2.rename, cluster1.rename
        return True


class SetClusterAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "SET cluster cannot be called in an active transaction",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.clusters:
                return False
            try:
                cluster = self.rng.choice(exe.db.clusters)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        http = self.rng.choice([Http.NO, Http.YES])
        if self.rng.choice([True, False]):
            exe.commit(http=http)
        else:
            exe.rollback(http=http)
        query = f"SET CLUSTER = {cluster}"
        exe.execute(query, http=http)
        return True


class CreateClusterReplicaAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            # Keep cluster 0 with 1 replica for sources/sinks
            unmanaged_clusters = [c for c in exe.db.clusters[1:] if not c.managed]
            if not unmanaged_clusters:
                return False
            cluster = self.rng.choice(unmanaged_clusters)
            cluster.replica_id += 1
        with cluster.lock:
            if cluster not in exe.db.clusters or not cluster.managed:
                return False

            replica = ClusterReplica(
                cluster.replica_id,
                size=self.rng.choice(["1", "2"]),
                cluster=cluster,
            )
            replica.create(exe)
            cluster.replicas.append(replica)
            return True


class DropClusterReplicaAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            # Keep cluster 0 with 1 replica for sources/sinks
            unmanaged_clusters = [c for c in exe.db.clusters[1:] if not c.managed]
            if not unmanaged_clusters:
                return False
            cluster = self.rng.choice(unmanaged_clusters)
            # Avoid "has no replicas available to service request" error
            if len(cluster.replicas) <= 1:
                return False
            replica = self.rng.choice(cluster.replicas)

        with cluster.lock, replica.lock:
            # Was dropped while we were acquiring lock
            if replica not in cluster.replicas:
                return False
            if cluster not in exe.db.clusters:
                return False
            # Avoid "has no replicas available to service request" error
            if len(cluster.replicas) <= 1:
                return False

            query = f"DROP CLUSTER REPLICA {cluster}.{replica}"
            try:
                exe.execute(query, http=Http.RANDOM)
            except QueryError as e:
                # expected, see database-issues#6156
                if (
                    exe.db.scenario not in (Scenario.Kill, Scenario.ZeroDowntimeDeploy)
                    or "has no CLUSTER REPLICA named" not in e.msg
                ):
                    raise e
            cluster.replicas.remove(replica)
        return True


class GrantPrivilegesAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.roles:
                return False
            role = self.rng.choice(exe.db.roles)
            privilege = self.rng.choice(["SELECT", "INSERT", "UPDATE", "ALL"])
            tables_views: list[DBObject] = [*exe.db.tables, *exe.db.views]
            table = self.rng.choice(tables_views)
        with table.lock, role.lock:
            if table not in [*exe.db.tables, *exe.db.views]:
                return False
            if role not in exe.db.roles:
                return False

            query = f"GRANT {privilege} ON {table} TO {role}"
            try:
                exe.execute(query, http=Http.RANDOM)
            except QueryError as e:
                # expected, see database-issues#6156
                if (
                    exe.db.scenario not in (Scenario.Kill, Scenario.ZeroDowntimeDeploy)
                    or "unknown role" not in e.msg
                ):
                    raise e
            exe.db.roles.remove(role)
        return True


class RevokePrivilegesAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.roles:
                return False
            role = self.rng.choice(exe.db.roles)
            privilege = self.rng.choice(["SELECT", "INSERT", "UPDATE", "ALL"])
            tables_views: list[DBObject] = [*exe.db.tables, *exe.db.views]
            table = self.rng.choice(tables_views)
        with table.lock, role.lock:
            if table not in [*exe.db.tables, *exe.db.views]:
                return False
            if role not in exe.db.roles:
                return False

            query = f"REVOKE {privilege} ON {table} FROM {role}"
            try:
                exe.execute(query, http=Http.RANDOM)
            except QueryError as e:
                # expected, see database-issues#6156
                if (
                    exe.db.scenario not in (Scenario.Kill, Scenario.ZeroDowntimeDeploy)
                    or "unknown role" not in e.msg
                ):
                    raise e
            exe.db.roles.remove(role)
        return True


# TODO: Should factor this out so can easily use it without action
class ReconnectAction(Action):
    def __init__(
        self,
        rng: random.Random,
        composition: Composition | None,
        random_role: bool = True,
    ):
        super().__init__(rng, composition)
        self.random_role = random_role

    def run(self, exe: Executor) -> bool:
        exe.mz_service = "materialized"
        exe.log("reconnecting")
        host = exe.db.host
        port = exe.db.ports[exe.mz_service]
        with exe.db.lock:
            if self.random_role and exe.db.roles:
                user = self.rng.choice(
                    ["materialize", str(self.rng.choice(exe.db.roles))]
                )
            else:
                user = "materialize"
            conn = exe.cur.connection

        if exe.ws and exe.use_ws:
            try:
                exe.ws.close()
            except:
                pass

        try:
            exe.cur.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass

        NUM_ATTEMPTS = 20
        if exe.ws:
            threading.current_thread().getName()
            for i in range(
                NUM_ATTEMPTS
                if exe.db.scenario != Scenario.ZeroDowntimeDeploy
                else 1000000
            ):
                exe.ws = websocket.WebSocket()
                try:
                    ws_conn_id, ws_secret_key = ws_connect(
                        exe.ws,
                        host,
                        exe.db.ports[
                            "http" if exe.mz_service == "materialized" else "http2"
                        ],
                        user,
                    )
                except Exception as e:
                    if exe.db.scenario == Scenario.ZeroDowntimeDeploy:
                        exe.mz_service = (
                            "materialized2"
                            if exe.mz_service == "materialized"
                            else "materialized"
                        )
                        continue
                    if i < NUM_ATTEMPTS - 1:
                        time.sleep(1)
                        continue
                    raise QueryError(str(e), "WS connect")
                if exe.use_ws:
                    exe.pg_pid = ws_conn_id
                break

        for i in range(
            NUM_ATTEMPTS if exe.db.scenario != Scenario.ZeroDowntimeDeploy else 1000000
        ):
            try:
                conn = psycopg.connect(
                    host=host, port=port, user=user, dbname="materialize"
                )
                conn.autocommit = exe.autocommit
                cur = conn.cursor()
                exe.cur = cur
                exe.set_isolation("SERIALIZABLE")
                cur.execute("SELECT pg_backend_pid()")
                if not exe.use_ws:
                    exe.pg_pid = cur.fetchall()[0][0]
            except Exception as e:
                if exe.db.scenario == Scenario.ZeroDowntimeDeploy:
                    exe.mz_service = (
                        "materialized2"
                        if exe.mz_service == "materialized"
                        else "materialized"
                    )
                    continue
                if i < NUM_ATTEMPTS - 1 and (
                    "server closed the connection unexpectedly" in str(e)
                    or "Can't create a connection to host" in str(e)
                    or "Connection refused" in str(e)
                    or "connection timeout expired" in str(e)
                ):
                    time.sleep(1)
                    continue
                raise QueryError(str(e), "connect")
            else:
                break
        return True


class CancelAction(Action):
    workers: list["Worker"]

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "must be a member of",
        ] + super().errors_to_ignore(exe)

    def __init__(
        self,
        rng: random.Random,
        composition: Composition | None,
        workers: list["Worker"],
    ):
        super().__init__(rng, composition)
        self.workers = workers

    def run(self, exe: Executor) -> bool:
        pid = self.rng.choice(
            [worker.exe.pg_pid for worker in self.workers if worker.exe and worker.exe.pg_pid != -1]  # type: ignore
        )
        worker = None
        for i in range(len(self.workers)):
            worker_exe = self.workers[i].exe
            if worker_exe and worker_exe.pg_pid == pid:
                worker = f"worker_{i}"
                break
        assert worker
        exe.execute(
            f"SELECT pg_cancel_backend({pid})",
            extra_info=f"Canceling {worker}",
            http=Http.RANDOM,
        )
        time.sleep(self.rng.uniform(0.1, 10))
        return True


class KillAction(Action):
    def __init__(
        self,
        rng: random.Random,
        composition: Composition | None,
        azurite: bool,
        sanity_restart: bool,
        system_param_fn: Callable[[dict[str, str]], dict[str, str]] = lambda x: x,
    ):
        super().__init__(rng, composition)
        self.system_param_fn = system_param_fn
        self.system_parameters = copy.deepcopy(ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS)
        self.azurite = azurite
        self.sanity_restart = sanity_restart

    def run(self, exe: Executor) -> bool:
        assert self.composition
        self.composition.kill("materialized")
        self.system_parameters = self.system_param_fn(self.system_parameters)
        with self.composition.override(
            Materialized(
                restart="on-failure",
                external_blob_store=True,
                blob_store_is_azure=self.azurite,
                external_metadata_store="toxiproxy",
                ports=["6975:6875", "6976:6876", "6977:6877"],
                sanity_restart=self.sanity_restart,
                additional_system_parameter_defaults=self.system_parameters,
                metadata_store="cockroach",
                default_replication_factor=1,
            )
        ):
            self.composition.up("materialized", detach=True)
        time.sleep(self.rng.uniform(60, 120))
        return True


class ZeroDowntimeDeployAction(Action):
    def __init__(
        self,
        rng: random.Random,
        composition: Composition | None,
        azurite: bool,
        sanity_restart: bool,
    ):
        super().__init__(rng, composition)
        self.azurite = azurite
        self.sanity_restart = sanity_restart
        self.deploy_generation = 0

    def run(self, exe: Executor) -> bool:
        assert self.composition

        self.deploy_generation += 1

        if self.deploy_generation % 2 == 0:
            mz_service = "materialized"
            ports = ["6975:6875", "6976:6876", "6977:6877"]
        else:
            mz_service = "materialized2"
            ports = ["7075:6875", "7076:6876", "7077:6877"]

        print(f"Deploying generation {self.deploy_generation} on {mz_service}")

        with self.composition.override(
            Materialized(
                name=mz_service,
                # TODO: Retry with toxiproxy on azurite
                external_blob_store=True,
                blob_store_is_azure=self.azurite,
                external_metadata_store="toxiproxy",
                ports=ports,
                sanity_restart=self.sanity_restart,
                deploy_generation=self.deploy_generation,
                system_parameter_defaults=get_default_system_parameters(),
                restart="on-failure",
                healthcheck=LEADER_STATUS_HEALTHCHECK,
                metadata_store="cockroach",
                default_replication_factor=1,
                additional_system_parameter_defaults=ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS,
            ),
        ):
            self.composition.up(mz_service, detach=True)
            self.composition.await_mz_deployment_status(
                DeploymentStatus.READY_TO_PROMOTE, mz_service, timeout=1800
            )
            self.composition.promote_mz(mz_service)
            self.composition.await_mz_deployment_status(
                DeploymentStatus.IS_LEADER, mz_service
            )

        time.sleep(self.rng.uniform(60, 120))
        return True


# TODO: Don't restore immediately, keep copy of Database objects
class BackupRestoreAction(Action):
    composition: Composition
    db: Database
    num: int

    def __init__(
        self, rng: random.Random, composition: Composition | None, db: Database
    ):
        super().__init__(rng, composition)
        self.db = db
        self.num = 0
        assert self.composition

    def run(self, exe: Executor) -> bool:
        self.num += 1
        time.sleep(self.rng.uniform(10, 240))

        with self.db.lock:
            # Backup
            self.composition.exec("mc", "mc", "mb", f"persist/crdb-backup{self.num}")
            self.composition.exec(
                "cockroach",
                "cockroach",
                "sql",
                "--insecure",
                "-e",
                f"""
               CREATE EXTERNAL CONNECTION backup_bucket{self.num} AS 's3://persist/crdb-backup{self.num}?AWS_ENDPOINT=http://minio:9000/&AWS_REGION=minio&AWS_ACCESS_KEY_ID=minioadmin&AWS_SECRET_ACCESS_KEY=minioadmin';
               BACKUP INTO 'external://backup_bucket{self.num}';
            """,
            )
            self.composition.kill("materialized")

            # Restore
            self.composition.exec(
                "cockroach",
                "cockroach",
                "sql",
                "--insecure",
                "-e",
                f"""
                DROP DATABASE defaultdb;
                RESTORE DATABASE defaultdb FROM LATEST IN 'external://backup_bucket{self.num}';
                SELECT shard, min(sequence_number), max(sequence_number)
                FROM consensus.consensus GROUP BY 1 ORDER BY 2 DESC, 3 DESC, 1 ASC LIMIT 32;
            """,
            )
            self.composition.run(
                "persistcli",
                "admin",
                "--commit",
                "restore-blob",
                f"--blob-uri={minio_blob_uri()}",
                "--consensus-uri=postgres://root@cockroach:26257?options=--search_path=consensus",
            )
            self.composition.up("materialized")
        return True


class CreateWebhookSourceAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.webhook_sources) >= MAX_WEBHOOK_SOURCES:
                return False
            webhook_source_id = exe.db.webhook_source_id
            exe.db.webhook_source_id += 1
            try:
                cluster = self.rng.choice(exe.db.clusters)
                schema = self.rng.choice(exe.db.schemas)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with schema.lock, cluster.lock:
            if schema not in exe.db.schemas:
                return False
            if cluster not in exe.db.clusters:
                return False

            source = WebhookSource(webhook_source_id, cluster, schema, self.rng)
            source.create(exe)
            exe.db.webhook_sources.append(source)
        return True


class DropWebhookSourceAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.webhook_sources:
                return False
            try:
                source = self.rng.choice(exe.db.webhook_sources)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with source.lock:
            # Was dropped while we were acquiring lock
            if source not in exe.db.webhook_sources:
                return False

            query = f"DROP SOURCE {source}"
            exe.execute(query, http=Http.RANDOM)
            exe.db.webhook_sources.remove(source)
        return True


class CreateKafkaSourceAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.kafka_sources) >= MAX_KAFKA_SOURCES:
                return False
            source_id = exe.db.kafka_source_id
            exe.db.kafka_source_id += 1
            try:
                cluster = self.rng.choice(exe.db.clusters)
                schema = self.rng.choice(exe.db.schemas)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with schema.lock, cluster.lock:
            if schema not in exe.db.schemas:
                return False
            if cluster not in exe.db.clusters:
                return False

            try:
                source = KafkaSource(
                    source_id,
                    cluster,
                    schema,
                    exe.db.ports,
                    self.rng,
                )
                source.create(exe)
                exe.db.kafka_sources.append(source)
            except:
                if exe.db.scenario not in (
                    Scenario.Kill,
                    Scenario.ZeroDowntimeDeploy,
                ):
                    raise
        return True


class DropKafkaSourceAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.kafka_sources:
                return False
            try:
                source = self.rng.choice(exe.db.kafka_sources)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with source.lock:
            # Was dropped while we were acquiring lock
            if source not in exe.db.kafka_sources:
                return False

            query = f"DROP SOURCE {source}"
            exe.execute(query, http=Http.RANDOM)
            exe.db.kafka_sources.remove(source)
            source.executor.mz_conn.close()
        return True


class CreateMySqlSourceAction(Action):
    def run(self, exe: Executor) -> bool:
        # See database-issues#6881, not expected to work
        if exe.db.scenario == Scenario.BackupRestore:
            return False

        with exe.db.lock:
            if len(exe.db.mysql_sources) >= MAX_MYSQL_SOURCES:
                return False
            source_id = exe.db.mysql_source_id
            exe.db.mysql_source_id += 1
            try:
                schema = self.rng.choice(exe.db.schemas)
                cluster = self.rng.choice(exe.db.clusters)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with schema.lock, cluster.lock:
            if schema not in exe.db.schemas:
                return False
            if cluster not in exe.db.clusters:
                return False

            try:
                source = MySqlSource(
                    source_id,
                    cluster,
                    schema,
                    exe.db.ports,
                    self.rng,
                )
                source.create(exe)
                exe.db.mysql_sources.append(source)
            except:
                if exe.db.scenario not in (
                    Scenario.Kill,
                    Scenario.ZeroDowntimeDeploy,
                ):
                    raise
        return True


class DropMySqlSourceAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.mysql_sources:
                return False
            try:
                source = self.rng.choice(exe.db.mysql_sources)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with source.lock:
            # Was dropped while we were acquiring lock
            if source not in exe.db.mysql_sources:
                return False

            query = f"DROP SOURCE {source.executor.source}"
            exe.execute(query, http=Http.RANDOM)
            exe.db.mysql_sources.remove(source)
            source.executor.mz_conn.close()
        return True


class CreatePostgresSourceAction(Action):
    def run(self, exe: Executor) -> bool:
        # See database-issues#6881, not expected to work
        if exe.db.scenario == Scenario.BackupRestore:
            return False

        with exe.db.lock:
            if len(exe.db.postgres_sources) >= MAX_POSTGRES_SOURCES:
                return False
            source_id = exe.db.postgres_source_id
            exe.db.postgres_source_id += 1
            try:
                schema = self.rng.choice(exe.db.schemas)
                cluster = self.rng.choice(exe.db.clusters)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with schema.lock, cluster.lock:
            if schema not in exe.db.schemas:
                return False
            if cluster not in exe.db.clusters:
                return False

            try:
                source = PostgresSource(
                    source_id,
                    cluster,
                    schema,
                    exe.db.ports,
                    self.rng,
                )
                source.create(exe)
                exe.db.postgres_sources.append(source)
            except:
                if exe.db.scenario not in (
                    Scenario.Kill,
                    Scenario.ZeroDowntimeDeploy,
                ):
                    raise
        return True


class DropPostgresSourceAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.postgres_sources:
                return False
            try:
                source = self.rng.choice(exe.db.postgres_sources)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with source.lock:
            # Was dropped while we were acquiring lock
            if source not in exe.db.postgres_sources:
                return False

            query = f"DROP SOURCE {source.executor.source}"
            exe.execute(query, http=Http.RANDOM)
            exe.db.postgres_sources.remove(source)
            source.executor.mz_conn.close()
        return True


class CreateSqlServerSourceAction(Action):
    def run(self, exe: Executor) -> bool:
        # See database-issues#6881, not expected to work
        if exe.db.scenario == Scenario.BackupRestore:
            return False

        with exe.db.lock:
            if len(exe.db.sql_server_sources) >= MAX_SQL_SERVER_SOURCES:
                return False
            source_id = exe.db.sql_server_source_id
            exe.db.sql_server_source_id += 1
            try:
                schema = self.rng.choice(exe.db.schemas)
                cluster = self.rng.choice(exe.db.clusters)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with schema.lock, cluster.lock:
            if schema not in exe.db.schemas:
                return False
            if cluster not in exe.db.clusters:
                return False

            try:
                assert self.composition
                source = SqlServerSource(
                    source_id,
                    cluster,
                    schema,
                    exe.db.ports,
                    self.rng,
                    self.composition,
                )
                source.create(exe)
                exe.db.sql_server_sources.append(source)
            except:
                if exe.db.scenario not in (
                    Scenario.Kill,
                    Scenario.ZeroDowntimeDeploy,
                ):
                    raise
        return True


class DropSqlServerSourceAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.sql_server_sources:
                return False
            try:
                source = self.rng.choice(exe.db.sql_server_sources)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with source.lock:
            # Was dropped while we were acquiring lock
            if source not in exe.db.sql_server_sources:
                return False

            query = f"DROP SOURCE {source.executor.source}"
            exe.execute(query, http=Http.RANDOM)
            exe.db.sql_server_sources.remove(source)
            source.executor.mz_conn.close()
        return True


class CreateKafkaSinkAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "BYTES format with non-encodable type",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.kafka_sinks) >= MAX_KAFKA_SINKS:
                return False
            sink_id = exe.db.kafka_sink_id
            exe.db.kafka_sink_id += 1
            try:
                cluster = self.rng.choice(exe.db.clusters)
                schema = self.rng.choice(exe.db.schemas)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with schema.lock, cluster.lock:
            if schema not in exe.db.schemas:
                return False
            if cluster not in exe.db.clusters:
                return False

            sink = KafkaSink(
                sink_id,
                cluster,
                schema,
                self.rng.choice(exe.db.db_objects_without_views()),
                self.rng,
            )
            sink.create(exe)
            exe.db.kafka_sinks.append(sink)
        return True


class DropKafkaSinkAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.kafka_sinks:
                return False
            try:
                sink = self.rng.choice(exe.db.kafka_sinks)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with sink.lock:
            # Was dropped while we were acquiring lock
            if sink not in exe.db.kafka_sinks:
                return False

            query = f"DROP SINK {sink}"
            exe.execute(query, http=Http.RANDOM)
            exe.db.kafka_sinks.remove(sink)
        return True


class HttpPostAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        if exe.db.scenario == Scenario.Rename:
            result.extend(["404: no object was found at the path"])
        return result

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.webhook_sources:
                return False

            sources = [
                source
                for source in exe.db.webhook_sources
                if source.num_rows < MAX_ROWS
            ]
            if not sources:
                return False
            source = self.rng.choice(sources)
        with source.lock:
            # Was dropped while we were acquiring lock
            if source not in exe.db.webhook_sources:
                return False

            url = f"http://{exe.db.host}:{exe.db.ports['http' if exe.mz_service == 'materialized' else 'http2']}/api/webhook/{urllib.parse.quote(source.schema.db.name(), safe='')}/{urllib.parse.quote(source.schema.name(), safe='')}/{urllib.parse.quote(source.name(), safe='')}"

            payload = source.body_format.to_data_type().random_value(self.rng)

            header_fields = source.explicit_include_headers
            if source.include_headers:
                header_fields.extend(["x-event-type", "signature", "x-mz-api-key"])

            headers = {
                header: (
                    f"{datetime.datetime.now()}"
                    if header == "timestamp"
                    else f'"{Text.random_value(self.rng)}"'.encode()
                )
                for header in self.rng.sample(header_fields, len(header_fields))
            }

            headers_strs = [f"{key}: {value}" for key, value in headers.items()]
            log = f"POST {url} Headers: {', '.join(headers_strs)} Body: {payload.encode('utf-8')}"
            exe.log(log)
            try:
                source.num_rows += 1
                result = requests.post(url, data=payload.encode(), headers=headers)
                if result.status_code != 200:
                    raise QueryError(f"{result.status_code}: {result.text}", log)
            except requests.exceptions.ConnectionError:
                # Expected when Mz is killed
                if exe.db.scenario not in (
                    Scenario.Kill,
                    Scenario.BackupRestore,
                    Scenario.ZeroDowntimeDeploy,
                ):
                    raise
            except QueryError as e:
                # expected, see database-issues#6156
                if exe.db.scenario not in (
                    Scenario.Kill,
                    Scenario.ZeroDowntimeDeploy,
                ) or ("404: no object was found at the path" not in e.msg):
                    raise e
        return True


class StatisticsAction(Action):
    def run(self, exe: Executor) -> bool:
        for typ, objs in [
            ("tables", exe.db.tables),
            ("views", exe.db.views),
            ("kafka_sources", exe.db.kafka_sources),
            ("postgres_sources", exe.db.postgres_sources),
            ("mysql_sources", exe.db.mysql_sources),
            ("sql_server_sources", exe.db.sql_server_sources),
            ("webhook_sources", exe.db.webhook_sources),
        ]:
            counts = []
            for t in objs:
                exe.execute(f"SELECT count(*) FROM {t}")
                counts.append(str(exe.cur.fetchall()[0][0]))
            print(f"{typ}: {' '.join(counts)}")
            time.sleep(10)
        return True


class ActionList:
    action_classes: list[type[Action]]
    weights: list[float]
    autocommit: bool

    def __init__(
        self, action_classes_weights: list[tuple[type[Action], int]], autocommit: bool
    ):
        self.action_classes = [action[0] for action in action_classes_weights]
        self.weights = [action[1] for action in action_classes_weights]
        self.autocommit = autocommit


read_action_list = ActionList(
    [
        (SelectAction, 100),
        (SelectOneAction, 1),
        # (SQLsmithAction, 30),  # Questionable use
        (
            CopyToS3Action,
            100,
        ),  # TODO: Reenable when https://github.com/MaterializeInc/database-issues/issues/9661 is fixed
        (SetClusterAction, 1),
        (CommitRollbackAction, 30),
        (ReconnectAction, 1),
        (FlipFlagsAction, 2),
    ],
    autocommit=False,
)

fetch_action_list = ActionList(
    [
        (FetchAction, 30),
        (SetClusterAction, 1),
        (ReconnectAction, 1),
        (FlipFlagsAction, 2),
    ],
    autocommit=False,
)

write_action_list = ActionList(
    [
        (InsertAction, 30),
        (CopyFromStdinAction, 20),
        (SelectOneAction, 1),  # can be mixed with writes
        (SetClusterAction, 1),
        (HttpPostAction, 5),
        (CommitRollbackAction, 10),
        (ReconnectAction, 1),
        (SourceInsertAction, 5),
        (FlipFlagsAction, 2),
    ],
    autocommit=False,
)

dml_nontrans_action_list = ActionList(
    [
        (DeleteAction, 10),
        (UpdateAction, 10),
        (InsertReturningAction, 10),
        (CommentAction, 5),
        (SetClusterAction, 1),
        (ReconnectAction, 1),
        (FlipFlagsAction, 2),
        # (TransactionIsolationAction, 1),
    ],
    autocommit=True,  # deletes can't be inside of transactions
)

ddl_action_list = ActionList(
    [
        (CreateIndexAction, 2),
        (DropIndexAction, 2),
        (CreateTableAction, 2),
        (DropTableAction, 2),
        (CreateViewAction, 8),
        (DropViewAction, 8),
        (CreateRoleAction, 2),
        (DropRoleAction, 2),
        (CreateClusterAction, 1),
        (DropClusterAction, 1),
        (SwapClusterAction, 10),
        (CreateClusterReplicaAction, 2),
        (DropClusterReplicaAction, 2),
        (SetClusterAction, 1),
        (CreateWebhookSourceAction, 2),
        (DropWebhookSourceAction, 2),
        (CreateKafkaSinkAction, 4),
        (DropKafkaSinkAction, 4),
        (CreateKafkaSourceAction, 4),
        (DropKafkaSourceAction, 4),
        # TODO: Reenable when database-issues#8237 is fixed
        # (CreateMySqlSourceAction, 4),
        # (DropMySqlSourceAction, 4),
        (CreatePostgresSourceAction, 4),
        (DropPostgresSourceAction, 4),
        # TODO: Reenable when database-issues#9620 is fixed
        # (CreateSqlServerSourceAction, 4),
        # (DropSqlServerSourceAction, 4),
        (GrantPrivilegesAction, 4),
        (RevokePrivilegesAction, 1),
        (ReconnectAction, 1),
        (CreateDatabaseAction, 1),
        (DropDatabaseAction, 1),
        (CreateSchemaAction, 1),
        (DropSchemaAction, 1),
        (RenameSchemaAction, 10),
        (RenameTableAction, 10),
        (RenameViewAction, 10),
        (RenameSinkAction, 10),
        (SwapSchemaAction, 10),
        (FlipFlagsAction, 2),
        # TODO: Reenable when database-issues#8813 is fixed.
        # (AlterTableAddColumnAction, 10),
        (AlterKafkaSinkFromAction, 8),
        # (TransactionIsolationAction, 1),
    ],
    autocommit=True,
)

action_lists = [
    read_action_list,
    fetch_action_list,
    write_action_list,
    dml_nontrans_action_list,
    ddl_action_list,
]
