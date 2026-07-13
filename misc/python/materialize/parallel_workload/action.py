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
import zlib
from collections.abc import Callable
from typing import TYPE_CHECKING

import psycopg
import requests
import websocket
from pg8000.native import identifier
from psycopg import Connection
from psycopg.errors import OperationalError

import materialize.parallel_workload.column
from materialize.data_ingest.data_type import (
    NUMBER_TYPES,
    RANGE_TYPES,
    UUID,
    Boolean,
    Bytea,
    Char,
    IntArray,
    IntList,
    Jsonb,
    Oid,
    Text,
    TextTextMap,
    Timestamp,
    TimestampTz,
    VarChar,
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
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.parallel_workload.database import (
    DATA_TYPES,
    DB,
    MAX_CLUSTER_REPLICAS,
    MAX_CLUSTERS,
    MAX_COLUMNS,
    MAX_DBS,
    MAX_ICEBERG_SINKS,
    MAX_INDEXES,
    MAX_KAFKA_SINKS,
    MAX_KAFKA_SOURCES,
    MAX_LOADGEN_SOURCES,
    MAX_MYSQL_SOURCES,
    MAX_NETWORK_POLICIES,
    MAX_POSTGRES_SOURCES,
    MAX_ROLES,
    MAX_ROWS,
    MAX_SCHEMAS,
    MAX_SQL_SERVER_SOURCES,
    MAX_TABLES,
    MAX_TYPES,
    MAX_VIEWS,
    MAX_WEBHOOK_SOURCES,
    Cluster,
    ClusterReplica,
    Column,
    Database,
    DBObject,
    IcebergSink,
    Index,
    KafkaSink,
    KafkaSource,
    LoadGeneratorSource,
    MultiLoadGeneratorSource,
    MySqlSource,
    MzTempSchema,
    NetworkPolicy,
    PostgresSource,
    Role,
    S3Object,
    Schema,
    SqlServerSource,
    Table,
    Type,
    View,
    WebhookSource,
)
from materialize.parallel_workload.executor import Executor, Http
from materialize.parallel_workload.expression import ExprKind, expression
from materialize.parallel_workload.negative_accumulation_errors import (
    NEGATIVE_ACCUMULATION_ERRORS,
)
from materialize.parallel_workload.settings import (
    ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS,
    COCKROACH_SCENARIOS,
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


def untrack_objects_in_schemas(exe: Executor, schemas: set[Schema]) -> None:
    """Remove every tracked object living in one of the given schemas.

    Called after a CASCADE drop of a schema or database. Source executor
    connections are closed. Cross-schema dependents are cascade-dropped
    server-side but not pruned here, they later surface as "does not exist",
    which is why the CASCADE actions only run in DDL complexity."""
    with exe.db.lock:
        exe.db.tables[:] = [t for t in exe.db.tables if t.schema not in schemas]
        exe.db.views[:] = [v for v in exe.db.views if v.schema not in schemas]
        exe.db.indexes = {i for i in exe.db.indexes if i.schema not in schemas}
        # Close the executor connections of the ingestion sources being
        # untracked so they don't leak.
        connected_sources = (
            exe.db.kafka_sources
            + exe.db.postgres_sources
            + exe.db.mysql_sources
            + exe.db.sql_server_sources
        )
        for src in connected_sources:
            if src.schema in schemas:
                try:
                    src.executor.mz_conn.close()
                except Exception:
                    pass
        exe.db.kafka_sources[:] = [
            s for s in exe.db.kafka_sources if s.schema not in schemas
        ]
        exe.db.postgres_sources[:] = [
            s for s in exe.db.postgres_sources if s.schema not in schemas
        ]
        exe.db.mysql_sources[:] = [
            s for s in exe.db.mysql_sources if s.schema not in schemas
        ]
        exe.db.sql_server_sources[:] = [
            s for s in exe.db.sql_server_sources if s.schema not in schemas
        ]
        exe.db.loadgen_sources[:] = [
            s for s in exe.db.loadgen_sources if s.schema not in schemas
        ]
        exe.db.multi_loadgen_sources[:] = [
            s for s in exe.db.multi_loadgen_sources if s.schema not in schemas
        ]
        exe.db.webhook_sources[:] = [
            s for s in exe.db.webhook_sources if s.schema not in schemas
        ]
        exe.db.kafka_sinks[:] = [
            s for s in exe.db.kafka_sinks if s.schema not in schemas
        ]
        exe.db.iceberg_sinks[:] = [
            s for s in exe.db.iceberg_sinks if s.schema not in schemas
        ]
        exe.db.types[:] = [t for t in exe.db.types if t.schema not in schemas]


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

    def applicable(self, exe: Executor) -> bool:
        """Whether this action can run at all in the current configuration.

        Inapplicable actions (e.g. wrong scenario) are skipped by the worker
        without counting as attempts, which keeps the end-of-run action
        coverage check meaningful."""
        return True

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
            # A persistent object (sink, non-temp view) referencing a temporary
            # one is correctly rejected. We still let the workload attempt it,
            # a path that wrongly accepts it instead panics the coordinator on
            # catalog apply, which surfaces as an unexpected failure.
            "non-temporary items cannot depend on temporary item",
            # generate_select_query occasionally emits a WITH MUTUALLY
            # RECURSIVE body with ERROR AT RECURSION LIMIT, which errors on
            # purpose when the iteration outruns the limit. Only our generated
            # queries produce this, so ignoring it globally is safe.
            "exceeded the recursion limit",
        ]
        if exe.statement_timeout_set:
            result.append("canceling statement due to statement timeout")
        if exe.db.complexity in (Complexity.DDL, Complexity.DDLOnly):
            result.extend(
                [
                    "query could not complete",
                    "cached plan must not change result type",
                    "violates not-null constraint",
                    "unknown catalog item",  # Expected, see database-issues#6124
                    "was concurrently dropped",  # role was dropped
                    # cluster was dropped. The trailing quote keeps this from
                    # matching "unknown cluster replica size" errors.
                    "unknown cluster '",
                    "unknown schema",  # schema was dropped
                    # database was dropped by DropDatabaseCascadeAction. Before
                    # that action, DROP DATABASE used RESTRICT and never
                    # removed a non-empty database, so this could not arise.
                    "unknown database",
                    "invalid database",  # CREATE SCHEMA wording for a vanished database
                    # The Pg/MySql/SqlServer source executor connects to the
                    # source's target database. A concurrent DropDatabaseCascade
                    # leaves that session with no schema, so the unqualified
                    # CREATE SECRET/CONNECTION it runs fails to resolve one.
                    "no valid schema selected",
                    "the transaction's active cluster has been dropped",  # cluster was dropped
                    "was removed",  # dependency was removed, started with moving optimization off main thread, see database-issues#7285
                    "real-time source dropped before ingesting the upstream system's visible frontier",  # Expected, see https://buildkite.com/materialize/nightly/builds/9399#0191be17-1f4c-4321-9b51-edc4b08b71c5
                    "object state changed while transaction was in progress",  # Old error msg, can remove this ignore later
                    "another session modified the catalog while this DDL transaction was open",
                    "was dropped while executing a statement",
                    "' was dropped",  # ConcurrentDependencyDrop (collection, schema, etc.)
                    "was concurrently modified",  # ConcurrentDependencyMutation (SQL-272)
                    "non-temporary items cannot depend on temporary item",  # TODO(def-): Fix?
                    "is not readable at any timestamp",  # Expected, due to object drops
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
                    "the connection is lost",
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
        if exe.db.scenario in (
            Scenario.Kill,
            Scenario.ZeroDowntimeDeploy,
            Scenario.BackupRestore,
        ):
            # Expected, see database-issues#6156. For BackupRestore the
            # restore rolls the catalog back to the backup point, so objects
            # created after the backup vanish while still being tracked.
            # "invalid database" is the CREATE SCHEMA wording for a database
            # that vanished the same way (CreateSchemaAction does not lock it).
            result.extend(
                [
                    "unknown catalog item",
                    "unknown schema",
                    "unknown database",
                    "invalid database",
                ]
            )
        if exe.db.scenario == Scenario.Rename:
            result.extend(["unknown schema", "ambiguous reference to schema name"])
        # Negative multiplicities arise two ways: `repeat_row` with a negative
        # count (RepeatRow scenario), and `DELETE .. USING`, which lowers to a
        # semijoin whose DistinctBy can leave a table with a net-negative row
        # (database-issues#9308). DELETE .. USING runs in the DML and DDL
        # complexities, and the corruption it leaves is then observed by any
        # later reader of that table (e.g. a COPY of a SELECT over it), not just
        # by the DELETE itself. So tolerate the whole class wherever either
        # source is active. Read and DDLOnly run neither, so a negative-
        # accumulation error there is still a genuine finding. The central list
        # lives in `negative_accumulation_errors.py`.
        if exe.db.scenario == Scenario.RepeatRow or exe.db.complexity in (
            Complexity.DML,
            Complexity.DDL,
        ):
            result.extend(NEGATIVE_ACCUMULATION_ERRORS)
        if materialize.parallel_workload.column.NAUGHTY_IDENTIFIERS:
            result.extend(["identifier length exceeds 255 bytes"])
        return result

    # MIN/MAX exist for every type except these (bytea, jsonb, map, list,
    # array, uuid, oid, and the range types).
    _MINMAX_EXCLUDED = (
        Bytea,
        Jsonb,
        TextTextMap,
        IntList,
        IntArray,
        UUID,
        Oid,
    ) + tuple(RANGE_TYPES)

    def aggregate_fns(self, column: Column) -> list[str]:
        """Aggregate function templates valid for the column's type.

        Used both in window position (OVER ..) and in GROUP BY position. The
        collection aggregates (array_agg/list_agg/jsonb_agg/string_agg)
        exercise the "collection" reduce rendering, distinct from the
        accumulable sum/count path. Type exclusions are empirically derived,
        e.g. array_agg rejects char and cannot nest map/list/array."""
        dt = column.data_type
        fns = ["COUNT({})"]
        if dt in NUMBER_TYPES:
            fns.extend(
                [
                    "SUM({})",
                    "AVG({})",
                    "STDDEV({})",
                    "STDDEV_POP({})",
                    "STDDEV_SAMP({})",
                    "VAR_SAMP({})",
                    "VAR_POP({})",
                ]
            )
        if dt == Boolean:
            fns.extend(["BOOL_AND({})", "BOOL_OR({})"])
        if dt not in self._MINMAX_EXCLUDED:
            fns.extend(["MAX({})", "MIN({})"])
        # Collection aggregates.
        fns.append("jsonb_agg({})")
        if dt != Char:
            fns.append("list_agg({})")
        if dt not in (Char, TextTextMap, IntList, IntArray):
            fns.append("array_agg({})")
        if dt in (Text, VarChar, Char):
            fns.append("string_agg({}, ',')")
        return fns

    def generate_select_query(self, exe: Executor, expr_kind: ExprKind) -> str:
        objects = exe.db.db_objects()
        if not objects:
            # A concurrent CASCADE drop removed every object. Nothing to query.
            return "SELECT 1"
        obj = self.rng.choice(objects)
        column = self.rng.choice(obj.columns)
        # db_objects_without_views() can momentarily be empty if a CASCADE drop
        # removed the last table/source/MV, leaving only plain views. Fall back
        # to obj (only used for an optional join, which is skipped for views).
        objs_without_views = exe.db.db_objects_without_views()
        obj2 = self.rng.choice(objs_without_views) if objs_without_views else obj
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

        # Self-contained iterative dataflow, cross joined with a real object.
        # The iteration bound and the recursion limit are drawn independently,
        # so the limit sometimes fires, exercising the RETURN/ERROR AT paths
        # on purpose ("exceeded the recursion limit" is an expected error).
        if self.rng.random() < 0.05:
            limit_kind = self.rng.choice(["RETURN AT", "ERROR AT"])
            expr = expression(
                self.rng.choice(list(DATA_TYPES)), obj.columns, self.rng, expr_kind
            )
            return (
                f"WITH MUTUALLY RECURSIVE ({limit_kind} RECURSION LIMIT {self.rng.randint(2, 200)}) "
                f"cnt (i int8) AS ("
                f"SELECT 1 UNION ALL SELECT i + 1 FROM cnt WHERE i < {self.rng.randint(1, 100)}"
                f") SELECT i, {expr} FROM cnt, {obj_name}"
                f" LIMIT {self.rng.randint(0, 100)}"
            )

        # Explicit LATERAL derived table correlated on a matching-type column.
        # Exercises correlated-subquery decorrelation.
        if self.rng.random() < 0.08:
            outer_col = self.rng.choice(obj.columns)
            inner_obj = self.rng.choice(exe.db.db_objects_without_views())
            match_cols = [
                c
                for c in inner_obj.columns
                if c.data_type == outer_col.data_type and c.data_type != TextTextMap
            ]
            corr = ""
            if match_cols:
                corr = f" WHERE {self.rng.choice(match_cols)} = {outer_col}"
            expr = expression(
                self.rng.choice(list(DATA_TYPES)), obj.columns, self.rng, expr_kind
            )
            return (
                f"SELECT {expr} FROM {obj_name}, LATERAL ("
                f"SELECT count(*) AS cnt FROM {inner_obj}{corr}"
                f") AS lat LIMIT {self.rng.randint(0, 100)}"
            )

        # Table function in FROM. unnest of a column reference is an
        # implicitly lateral call.
        if self.rng.random() < 0.1:
            int_list_columns = [c for c in obj.columns if c.data_type == IntList]
            if int_list_columns and self.rng.choice([True, False]):
                func = f"unnest({self.rng.choice(int_list_columns)})"
            else:
                func = f"generate_series(1, {self.rng.randint(1, 100)})"
            expr = expression(
                self.rng.choice(list(DATA_TYPES)), obj.columns, self.rng, expr_kind
            )
            if self.rng.choice([True, False]):
                tf = f"{func} WITH ORDINALITY AS tf(x, ord)"
            else:
                tf = f"{func} AS tf(x)"
            return (
                f"SELECT tf.x, {expr} FROM {obj_name}, {tf}"
                f" LIMIT {self.rng.randint(0, 100)}"
            )

        star = self.rng.random() >= 0.9
        exprs: list[tuple[type, str]] = []
        if not star:
            for i in range(self.rng.randint(1, 10)):
                dt = self.rng.choice(list(DATA_TYPES))
                exprs.append((dt, expression(dt, all_columns, self.rng, expr_kind)))

        if join:
            join_kind = self.rng.choice(
                ["JOIN", "JOIN", "JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"]
            )
            column2 = self.rng.choice(columns)
            join_clause = f" {join_kind} {obj2_name} ON {column} = {column2}"
            if self.rng.random() < 0.2:
                join_clause += (
                    f" AND {expression(Boolean, all_columns, self.rng, expr_kind)}"
                )
        else:
            join_clause = ""

        def where_clause() -> str:
            parts = []
            if self.rng.choice([True, False]):
                parts.append(expression(Boolean, all_columns, self.rng, expr_kind))
            if self.rng.random() < 0.2:
                obj3 = self.rng.choice(exe.db.db_objects_without_views())
                sub_columns = [
                    c
                    for c in obj3.columns
                    if c.data_type == column.data_type and c.data_type != TextTextMap
                ]
                sub_kind = self.rng.choice(["exists", "not exists", "in", "scalar"])
                if sub_kind in ("exists", "not exists"):
                    cond = expression(Boolean, obj3.columns, self.rng, expr_kind)
                    parts.append(
                        f"{'NOT ' if sub_kind == 'not exists' else ''}EXISTS (SELECT 1 FROM {obj3} WHERE {cond})"
                    )
                elif sub_columns:
                    sub_column = self.rng.choice(sub_columns)
                    if sub_kind == "in":
                        parts.append(f"{column} IN (SELECT {sub_column} FROM {obj3})")
                    else:
                        parts.append(
                            f"{column} = (SELECT {sub_column} FROM {obj3} LIMIT 1)"
                        )
            # Deliberate temporal filter, mz_now() has to be a top-level
            # conjunct for the temporal filter machinery to apply.
            ts_columns = [
                c for c in all_columns if c.data_type in (Timestamp, TimestampTz)
            ]
            if ts_columns and expr_kind != ExprKind.WRITE and self.rng.random() < 0.2:
                ts_column = self.rng.choice(ts_columns)
                op = self.rng.choice(["<=", ">="])
                parts.append(
                    f"mz_now() {op} {ts_column} + INTERVAL '{self.rng.randint(0, 100000)} seconds'"
                )
            if not parts:
                return ""
            return f" WHERE {' AND '.join(parts)}"

        distinct_on_expr = None
        group_by = bool(exprs) and self.rng.random() < 0.2
        if group_by:
            num_group = self.rng.randint(1, len(exprs))
            select_list = [expr for _, expr in exprs[:num_group]]
            for i in range(self.rng.randint(1, 3)):
                agg_column = self.rng.choice(all_columns)
                fn = self.rng.choice(self.aggregate_fns(agg_column))
                select_list.append(fn.format(agg_column))
            group_clause = (
                f" GROUP BY {', '.join(str(i + 1) for i in range(num_group))}"
            )
            # Clause order is fixed: HAVING before OPTIONS.
            if self.rng.random() < 0.3:
                group_clause += f" HAVING count(*) >= {self.rng.randint(0, 10)}"
            if self.rng.random() < 0.3:
                group_clause += f" OPTIONS (AGGREGATE INPUT GROUP SIZE = {self.rng.randint(1, 1000)})"
            arity = len(select_list)
            distinct_clause = ""
        else:
            select_list = [expr for _, expr in exprs] if exprs else ["*"]
            group_clause = ""
            distinct_clause = ""
            distinct_on_expr = None
            if exprs and self.rng.random() < 0.15:
                if exprs[0][0] == TextTextMap or self.rng.choice([True, False]):
                    distinct_clause = "DISTINCT "
                else:
                    # The DISTINCT ON expressions must be a prefix of the
                    # ORDER BY expressions. The leading ORDER BY item repeats
                    # this expression verbatim (not its ordinal) so the match
                    # holds even for bare literals.
                    distinct_on_expr = exprs[0][1]
                    distinct_clause = f"DISTINCT ON ({distinct_on_expr}) "
            if exprs and self.rng.choice([True, False]):
                column1 = self.rng.choice(all_columns)
                column2 = self.rng.choice(all_columns)
                column3 = self.rng.choice(all_columns)
                window_fn = self.rng.choice(self.aggregate_fns(column1))
                select_list.append(
                    f"{window_fn.format(column1)} OVER (PARTITION BY {column2} ORDER BY {column3})"
                )
            arity = len(select_list) if exprs else len(all_columns)

        expressions = ", ".join(select_list)
        query = f"SELECT {distinct_clause}{expressions} FROM {obj_name}"
        query += join_clause
        query += where_clause()
        query += group_clause

        if not distinct_clause and self.rng.choice([True, False]):
            set_op = self.rng.choice(
                ["UNION ALL"] * 4
                + ["UNION", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"]
            )
            query += f" {set_op} SELECT {expressions} FROM {obj_name}"
            query += join_clause
            query += where_clause()
            query += group_clause

        # Ordinals keep ORDER BY valid across set operations. Map-typed
        # output columns are skipped, same as in the join column selection.
        orderable = [
            i + 1
            for i in range(arity)
            if not exprs or i >= len(exprs) or exprs[i][0] != TextTextMap
        ]
        if distinct_on_expr is not None:
            order_by = [distinct_on_expr]
            for i in self.rng.sample(orderable, self.rng.randint(0, len(orderable))):
                if i != 1:
                    order_by.append(str(i))
            query += f" ORDER BY {', '.join(order_by)}"
        elif exprs and orderable and self.rng.random() < 0.3:
            order_by = []
            for i in self.rng.sample(orderable, self.rng.randint(1, len(orderable))):
                direction = self.rng.choice(["", " ASC", " DESC"])
                nulls = self.rng.choice(["", " NULLS FIRST", " NULLS LAST"])
                order_by.append(f"{i}{direction}{nulls}")
            query += f" ORDER BY {', '.join(order_by)}"

        query += f" LIMIT {self.rng.randint(0, 100)}"
        if self.rng.random() < 0.2:
            query += f" OFFSET {self.rng.randint(0, 100)}"

        if self.rng.random() < 0.15:
            query = f"WITH cte0 AS ({query}) SELECT * FROM cte0"
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
                "cached plan must not change result type",  # Expected, see database-issues#9666
            ]
        )
        if exe.db.complexity == Complexity.DDL:
            result.extend(
                [
                    "does not exist",
                    "query could not complete because relation",
                    "query could not complete because cluster",
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
        # NOTE: A bounded SUBSCRIBE (UP TO) over an object whose as_of has
        # advanced to the end of time (e.g. a finished bounded load generator
        # source) soft-panics the optimizer (CLU-169). See FINDINGS-BUGS.md. Left out
        # until that is fixed. AS OF AT LEAST 0 below is safe (empty until).
        query = "SUBSCRIBE "
        envelope_used = False
        if self.rng.choice([True, False]):
            obj = self.rng.choice(exe.db.db_objects())
            query += f"{obj}"

            if self.rng.choice([True, False]):
                envelope = "UPSERT" if self.rng.choice([True, False]) else "DEBEZIUM"
                columns = self.rng.sample(obj.columns, len(obj.columns))
                key = ", ".join(column.name(True) for column in columns)
                query += f" ENVELOPE {envelope} (KEY ({key}))"
                envelope_used = True
        else:
            query += f"({self.generate_select_query(exe, ExprKind.MATERIALIZABLE)})"

        options = []
        if self.rng.choice([True, False]):
            options.append("SNAPSHOT = false")
        if not envelope_used and self.rng.random() < 0.3:
            options.append("PROGRESS")
        if options:
            query += f" WITH ({', '.join(options)})"
        if self.rng.random() < 0.2:
            # AT LEAST always plans, no matter how far the since advanced.
            query += " AS OF AT LEAST 0"

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
        # The SET only applies to the pg session, so the RTR query has to run
        # there too (http=Http.NO). If the query fails, the staged SET is
        # discarded along with the worker's subsequent rollback, so no reset
        # is needed on the error path.
        if self.rng.choice([True, False]):
            self.stmt_id += 1
            self.exe_prepared(query, f"select{self.stmt_id}", exe)
        else:
            exe.execute(
                query,
                explainable=True,
                http=Http.NO if rtr else Http.RANDOM,
                fetch=True,
            )
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
                "Arrow interval type MonthDayNano to parquet that is not yet implemented",  # arrow-rs limitation
                "overflow i64 nanoseconds",  # arrow IntervalMonthDayNano limitation
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
        db_objs = exe.db.db_objects()
        if not db_objs:
            return False
        obj = self.rng.choice(db_objs)
        obj_name = str(obj)
        with exe.db.lock:
            location = exe.db.s3_path
            exe.db.s3_path += 1
        format = "csv" if self.rng.choice([True, False]) else "parquet"
        s3_obj = None
        if self.rng.random() < 0.9:
            dts = [
                self.rng.choice(list(DATA_TYPES))
                for _ in range(self.rng.randint(1, 10))
            ]
            expressions = ", ".join(
                [expression(dt, obj.columns, self.rng) for dt in dts]
            )
        else:
            expressions = "*"
            # A verbatim dump of a table can later be loaded back into it by
            # CopyFromS3Action: the file's column names and types match the
            # table's exactly. Temp tables are session-scoped, so other
            # workers could not COPY INTO them.
            if isinstance(obj, Table) and not obj.temp:
                s3_obj = S3Object(str(location), "copytos3", format, obj)
        to_query = f"COPY (SELECT {expressions} FROM {obj_name} WHERE {expression(Boolean, obj.columns, self.rng)} LIMIT {self.rng.randint(0, 100)}) TO 's3://copytos3/{location}' WITH (AWS CONNECTION = aws_conn, FORMAT = '{format}')"

        exe.execute(to_query, explainable=False, http=Http.NO, fetch=False)
        if s3_obj is not None:
            with exe.db.lock:
                exe.db.s3_objects.append(s3_obj)
        return True


class CopyFromS3Action(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        result.extend(
            [
                # CSV cannot distinguish NULL from the empty string, so the
                # roundtrip can produce NULLs for NOT NULL columns.
                "violates not-null constraint",
                "timeout: error trying to connect",
                # TODO: Remove when https://linear.app/materializeinc/issue/SS-341 is fixed
                "parquet error",
                # Same SS-341 class via the arrow record-batch path: COPY TO
                # writes a type (e.g. a daterange) that COPY FROM cannot decode.
                "failed to decode Row from a record batch",
                # COPY TO CSV writes a large-year date that COPY FROM CSV then
                # fails to parse back (SS-345). See FINDINGS-BUGS.md ("COPY FROM CSV
                # cannot decode a large-year date written by COPY TO").
                "expected_dur_like_tokens can only be called with",
            ]
        )
        if exe.db.complexity == Complexity.DDL:
            result.extend(
                [
                    "COPY FROM's target table",
                    "does not exist",
                    # A concurrent drop of the target table or the cluster
                    # retires an in-flight COPY FROM as canceled, see
                    # cancel_pending_copy
                    "canceling statement due to user request",
                ]
            )
        return result

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            # Prune entries whose source table was dropped concurrently.
            exe.db.s3_objects[:] = [
                o for o in exe.db.s3_objects if o.table in exe.db.tables
            ]
            candidates = [o for o in exe.db.s3_objects if o.table.num_rows < MAX_ROWS]
            if not candidates:
                return False
            s3_obj = self.rng.choice(candidates)
        table = s3_obj.table
        from_query = f"COPY INTO {table} FROM 's3://{s3_obj.bucket}/{s3_obj.key}' (FORMAT {s3_obj.format.upper()}, AWS CONNECTION = aws_conn)"
        exe.execute(from_query, explainable=False, http=Http.NO, fetch=False)
        # We don't know how many rows the file contained, resync the estimate
        # from the table itself.
        exe.execute(f"SELECT count(*) FROM {table}", http=Http.NO)
        table.num_rows = exe.cur.fetchall()[0][0]
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
            # Temp tables can only be written by their creating session
            tables = [
                table
                for table in exe.db.tables
                if table.num_rows < MAX_ROWS
                and (not table.temp or table in exe.temp_objects)
            ]
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
            # Temp tables can only be written by their creating session
            tables = [
                table
                for table in exe.db.tables
                if table.num_rows < MAX_ROWS
                and (not table.temp or table in exe.temp_objects)
            ]
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
            # Temp tables can only be written by their creating session
            tables = [
                table
                for table in exe.db.tables
                if table.num_rows < MAX_ROWS
                and (not table.temp or table in exe.temp_objects)
            ]
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


class InsertSelectAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        result.extend(
            [
                # A random source expression can evaluate to NULL even for a
                # NOT NULL target column, a legitimate rejection.
                "violates not-null constraint",
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
        # Temp tables can only be written by their creating session, and
        # other sessions' temp tables cannot be read either.
        tables = [
            table
            for table in exe.db.tables
            if not table.temp or table in exe.temp_objects
        ]
        writable = [table for table in tables if table.num_rows < MAX_ROWS]
        if not writable:
            return False
        table = self.rng.choice(writable)
        source_table = self.rng.choice(tables)
        limit = self.rng.randint(0, min(100, MAX_ROWS - table.num_rows))
        column_names = ", ".join(column.name(True) for column in table.columns)
        # Cast each projection to its target column's type. INSERT .. SELECT
        # requires the projection to assignment-cast to the target, and a few
        # types (notably bytea) come back from expression() as a bare text
        # literal that has no text->target assignment cast.
        select_exprs = ", ".join(
            "({})::{}".format(
                expression(
                    column.data_type,
                    source_table.columns,
                    self.rng,
                    kind=ExprKind.WRITE,
                ),
                column.data_type.name(),
            )
            for column in table.columns
        )
        query = f"INSERT INTO {table} ({column_names}) SELECT {select_exprs} FROM {source_table} LIMIT {limit}"
        exe.execute(query, http=Http.RANDOM)
        # The actual count depends on the source table's size, LIMIT is an
        # upper bound. Overestimating num_rows is fine, it only gates further
        # inserts.
        table.num_rows += limit
        return True


class CopyToStdoutAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        result.extend(
            [
                "in the same timedomain",
                # A prior statement in the read transaction routed it to
                # mz_catalog_server, where this COPY of a user object is
                # rejected.
                'is not allowed from the "mz_catalog_server" cluster',
                # BINARY format is deliberately kept in the mix even though
                # some types (e.g. map) have no binary output function.
                "no binary output function available for type",
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
        query = f"COPY (SELECT * FROM {obj} LIMIT {self.rng.randint(0, 100)}) TO STDOUT"
        if self.rng.choice([True, False]):
            format = self.rng.choice(["TEXT", "CSV", "BINARY"])
            query += f" WITH (FORMAT {format})"
        exe.copy_to_stdout(query)
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
                # A random SET expression can evaluate to NULL (e.g. a map-key
                # miss) even for a NOT NULL column. That is a legitimate
                # rejection, not a bug, and the column type can't be coerced
                # away without breaking bare-literal casts (e.g. text->bytea).
                # The base list ignores this only for DDL complexity, UPDATE
                # can hit it in any complexity.
                "violates not-null constraint",
            ]
        )

        if exe.db.complexity == Complexity.DDL or exe.db.scenario == Scenario.Rename:
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
            # Temp tables can only be written by their creating session
            tables = [
                table
                for table in exe.db.tables
                if not table.temp or table in exe.temp_objects
            ]
            if not tables:
                return False
            table = self.rng.choice(tables)

        set_columns = self.rng.sample(
            table.columns, self.rng.randint(1, len(table.columns))
        )
        set_clause = ", ".join(
            f"{c.name(True)} = {expression(c.data_type, table.columns, self.rng, kind=ExprKind.WRITE)}"
            for c in set_columns
        )
        query = f"UPDATE {table} SET {set_clause} WHERE {expression(Boolean, table.columns, self.rng, kind=ExprKind.WRITE)}"
        if self.rng.choice([True, False]):
            self.stmt_id += 1
            self.exe_prepared(query, f"update{self.stmt_id}", exe)
        else:
            exe.execute(query, http=Http.RANDOM)
        exe.insert_table = table.table_id
        return True


class DeleteAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        errors = (
            [
                "canceling statement due to statement timeout",
                # DELETE .. USING lowers to a semijoin whose DistinctBy can surface
                # negative-accumulation errors for some generated WHERE clauses,
                # outside the RepeatRow scenario. This is the known class tracked in
                # database-issues#9308. See FINDINGS-BUGS.md ("DELETE .. USING
                # surfaces a negative-accumulation error").
            ]
            + NEGATIVE_ACCUMULATION_ERRORS
            + super().errors_to_ignore(exe)
        )
        if exe.db.scenario == Scenario.Rename:
            errors += ["does not exist"]
        return errors

    def run(self, exe: Executor) -> bool:
        # Temp tables can only be written by their creating session
        tables = [
            table
            for table in exe.db.tables
            if not table.temp or table in exe.temp_objects
        ]
        if not tables:
            return False
        table = self.rng.choice(tables)
        query = f"DELETE FROM {table}"
        using_tables = [
            t
            for t in exe.db.tables
            if t != table and (not t.temp or t in exe.temp_objects)
        ]
        if using_tables and self.rng.random() < 0.2:
            using_table = self.rng.choice(using_tables)
            all_columns = list(table.columns) + list(using_table.columns)
            query += f" USING {using_table}"
            query += f" WHERE {expression(Boolean, all_columns, self.rng, kind=ExprKind.WRITE)}"
        elif self.rng.random() < 0.95:
            query += f" WHERE {expression(Boolean, table.columns, self.rng, kind=ExprKind.WRITE)}"
        if self.rng.choice([True, False]):
            self.stmt_id += 1
            self.exe_prepared(query, f"delete{self.stmt_id}", exe)
        else:
            exe.execute(query, http=Http.RANDOM)
        exe.commit()
        # The DELETE may have run over HTTP/WS or as a prepared statement, in
        # which case the pg cursor's rowcount is meaningless. Resync the row
        # count estimate from the table itself. It only gates insert-type
        # actions, so races with concurrent writers are fine.
        exe.execute(f"SELECT count(*) FROM {table}", http=Http.NO)
        table.num_rows = exe.cur.fetchall()[0][0]
        return True


class CommentAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "unknown role",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            candidates: list[tuple[str, str]] = []
            for table in exe.db.tables:
                candidates.append(("TABLE", str(table)))
                candidates.append(("COLUMN", str(self.rng.choice(table.columns))))
            for view in exe.db.views:
                candidates.append(
                    (
                        "MATERIALIZED VIEW" if view.materialized else "VIEW",
                        str(view),
                    )
                )
            # Kafka and webhook source objects are readable directly, the
            # others follow the source-table model: str(obj) names the table,
            # the ingestion source is a separate catalog item.
            for source in exe.db.kafka_sources + exe.db.webhook_sources:
                candidates.append(("SOURCE", str(source)))
            for source in (
                exe.db.postgres_sources
                + exe.db.mysql_sources
                + exe.db.sql_server_sources
            ):
                candidates.append(("TABLE", str(source)))
                candidates.append(
                    (
                        "SOURCE",
                        f"{source.schema}.{identifier(source.executor.source)}",
                    )
                )
            for source in exe.db.loadgen_sources:
                candidates.append(("TABLE", str(source)))
                candidates.append(
                    ("SOURCE", f"{source.schema}.{identifier(source.source_name())}")
                )
            for sink in exe.db.kafka_sinks + exe.db.iceberg_sinks:
                candidates.append(("SINK", str(sink)))
            for index in exe.db.indexes:
                candidates.append(("INDEX", str(index)))
            for schema in exe.db.schemas:
                candidates.append(("SCHEMA", str(schema)))
            for db in exe.db.dbs:
                candidates.append(("DATABASE", str(db)))
            for cluster in exe.db.clusters:
                candidates.append(("CLUSTER", str(cluster)))
            for role in exe.db.roles:
                candidates.append(("ROLE", str(role)))
            candidates.append(("SECRET", "materialize.public.pgpass"))
            candidates.append(("CONNECTION", "materialize.public.kafka_conn"))
            if not candidates:
                return False
            kind, name = self.rng.choice(candidates)

        comment = self.rng.choice([f"'{Text.random_value(self.rng)}'", "NULL"])
        query = f"COMMENT ON {kind} {name} IS {comment}"
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
        # columns_str may exceed 255 characters, so it is shortened to a
        # number. crc32 rather than hash() so index names are stable across
        # runs with the same seed (hash() of a str is salted per process).
        index = Index(
            f"idx_{obj.name()}_{zlib.crc32(columns_str.encode())}", obj.schema
        )
        index_elems = []
        for column in columns:
            order = self.rng.choice(["ASC", "DESC"])
            index_elems.append(f"{column.name(True)} {order}")
        index_str = ", ".join(index_elems)
        # The index name must be unqualified in CREATE INDEX, the index always
        # lands in the indexed object's schema.
        query = f"CREATE INDEX {identifier(index.name())} ON {obj} ({index_str})"
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
            try:
                exe.execute(query, http=Http.RANDOM)
            except QueryError:
                # The indexed object or its schema may have been dropped
                # concurrently, taking the index with it. Untrack the index
                # either way so stale entries don't fill up the set and choke
                # off CreateIndexAction. Use discard, not remove: a concurrent
                # CASCADE drop's untrack_objects_in_schemas may have already
                # removed it, and remove would raise KeyError.
                exe.db.indexes.discard(index)
                raise
            exe.db.indexes.discard(index)
            return True


class CreateTableAction(Action):
    def run(self, exe: Executor) -> bool:
        temp = self.rng.choice([True, False])
        if (
            not temp
            and len([table for table in exe.db.tables if not table.temp]) >= MAX_TABLES
        ):
            return False
        table_id = exe.db.table_id
        exe.db.table_id += 1
        if temp:
            schema = MzTempSchema(self.rng.choice(exe.db.dbs))
            table = Table(self.rng, table_id, schema, temp=True)
            table.create(exe)
        else:
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
        if temp:
            exe.temp_objects.append(table)
        return True


class DropTableAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            tables = [table for table in exe.db.tables if not table.temp]
            if not tables:
                return False
            table = self.rng.choice(tables)
            if (
                not table.temp
                and len([table for table in exe.db.tables if not table.temp]) <= 2
            ):
                return False
        with table.lock:
            # Was dropped while we were acquiring lock
            if table not in exe.db.tables:
                return False
            if len([table for table in exe.db.tables if not table.temp]) <= 2:
                return False

            query = f"DROP TABLE {table}"
            exe.execute(query, http=Http.RANDOM)
            # A concurrent CASCADE drop's untrack_objects_in_schemas may have
            # already filtered this table out of the list; tolerate that.
            try:
                exe.db.tables.remove(table)
            except ValueError:
                pass
        return True


class RenameTableAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        # Renaming a temporary item referenced by another temporary object is
        # refused as ambiguous: temp references are only 2-part (mz_temp.item),
        # and the item-rename check treats a non-3-part reference as ambiguous.
        return ["potentially used ambiguously"] + super().errors_to_ignore(exe)

    def applicable(self, exe: Executor) -> bool:
        return exe.db.scenario == Scenario.Rename

    def run(self, exe: Executor) -> bool:
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
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        # Renaming a temporary item referenced by another temporary object is
        # refused as ambiguous: temp references are only 2-part (mz_temp.item),
        # and the item-rename check treats a non-3-part reference as ambiguous.
        return ["potentially used ambiguously"] + super().errors_to_ignore(exe)

    def applicable(self, exe: Executor) -> bool:
        return exe.db.scenario == Scenario.Rename

    def run(self, exe: Executor) -> bool:
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


class RenameIcebergSinkAction(Action):
    def applicable(self, exe: Executor) -> bool:
        return exe.db.scenario == Scenario.Rename

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.iceberg_sinks:
                return False
            try:
                sink = self.rng.choice(exe.db.iceberg_sinks)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with sink.lock:
            if sink not in exe.db.iceberg_sinks:
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


class RenameKafkaSinkAction(Action):
    def applicable(self, exe: Executor) -> bool:
        return exe.db.scenario == Scenario.Rename

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


class ReplaceMaterializedViewAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        errors = [
            # A concurrent or leaked replacement of the same view
            "because it already has a replacement",
            "is sealed and thus cannot be replaced",
        ] + super().errors_to_ignore(exe)
        if exe.db.scenario == Scenario.Rename:
            # The view's rendered SELECT embeds qualified names captured at
            # creation time, renames invalidate them
            errors += ["does not exist"]
        return errors

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            mvs = [v for v in exe.db.views if v.materialized]
            if not mvs:
                return False
            view = self.rng.choice(mvs)

        # Views live in random schemas of random databases, so all names have
        # to be fully qualified. The replacement goes into the same schema as
        # the view it replaces.
        self.stmt_id += 1
        tmp_name = (
            f"{view.name()}_{threading.current_thread().getName()}_{self.stmt_id}"
        )
        tmp_mv = f"{view.schema}.{identifier(tmp_name)}"
        exe.execute(
            f"CREATE REPLACEMENT MATERIALIZED VIEW {tmp_mv} FOR {view} AS {view.get_select()}",
        )
        time.sleep(self.rng.random())
        try:
            exe.execute(f"ALTER MATERIALIZED VIEW {view} APPLY REPLACEMENT {tmp_mv}")
        except QueryError:
            # Clean up, a leaked replacement blocks all future replacements
            # of this view.
            try:
                exe.execute(f"DROP MATERIALIZED VIEW IF EXISTS {tmp_mv}")
            except QueryError:
                pass
            raise
        return True


class AlterIcebergSinkFromAction(Action):
    def applicable(self, exe: Executor) -> bool:
        # Does not work reliably with kills, see database-issues#8421
        return exe.db.scenario not in (Scenario.Kill, Scenario.ZeroDowntimeDeploy)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.iceberg_sinks:
                return False
            try:
                sink = self.rng.choice(exe.db.iceberg_sinks)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with sink.lock, sink.base_object.lock:
            if sink not in exe.db.iceberg_sinks:
                return False

            old_object = sink.base_object
            # Iceberg sinks always have a key, so only allow a conservative
            # case: all names, types, and nullabilities match, which also
            # guarantees the key columns exist in the new object.
            # TODO: Switch back when SS-344 is fixed to make sure it errors
            # instead of causing a stall
            objs = []
            old_cols = {
                c.name(True): (c.data_type, c.nullable) for c in old_object.columns
            }
            for o in exe.db.db_objects_for_sinks():
                if isinstance(old_object, WebhookSource):
                    continue
                if isinstance(o, WebhookSource):
                    continue
                new_cols = {c.name(True): (c.data_type, c.nullable) for c in o.columns}
                if old_cols == new_cols:
                    objs.append(o)
            # ALTER SINK ... SET FROM a temporary object panics the coordinator
            # (uncatchable) because the UpdateItem catalog path skips the
            # temp-dependency check that CREATE enforces. Exclude temp objects.
            objs = [o for o in objs if not getattr(o, "temp", False)]
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


class AlterKafkaSinkFromAction(Action):
    def applicable(self, exe: Executor) -> bool:
        # Does not work reliably with kills, see database-issues#8421
        return exe.db.scenario not in (Scenario.Kill, Scenario.ZeroDowntimeDeploy)

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
                    for o in exe.db.db_objects_for_sinks()
                    if len(o.columns) == 1
                    and o.columns[0].data_type == old_object.columns[0].data_type
                ]
            elif sink.format in ["FORMAT JSON"]:
                # We should be able to format all data types as JSON, and they have no
                # particular backwards-compatiblility requirements.
                objs = [o for o in exe.db.db_objects_for_sinks()]
            else:
                # Avro schema migration checking can be quite strict, and we need to be not only
                # compatible with the latest object's schema but all previous schemas.
                # Only allow a conservative case for now: where all names, types,
                # and nullabilities match. Nullability matters because it flips an
                # Avro field between a `["null", T]` union and a bare `T`, which the
                # schema registry rejects as an incompatible change for an existing
                # subject (the topic, hence subject, is unchanged by SET FROM).
                objs = []
                old_cols = {
                    c.name(True): (c.data_type, c.nullable) for c in old_object.columns
                }
                for o in exe.db.db_objects_for_sinks():
                    if isinstance(old_object, WebhookSource):
                        continue
                    if isinstance(o, WebhookSource):
                        continue
                    new_cols = {
                        c.name(True): (c.data_type, c.nullable) for c in o.columns
                    }
                    if old_cols == new_cols:
                        objs.append(o)
            # ALTER SINK ... SET FROM a temporary object panics the coordinator
            # (uncatchable) because the UpdateItem catalog path skips the
            # temp-dependency check that CREATE enforces. Exclude temp objects.
            objs = [o for o in objs if not getattr(o, "temp", False)]
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

    def applicable(self, exe: Executor) -> bool:
        return exe.db.scenario == Scenario.Rename

    def run(self, exe: Executor) -> bool:
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

    def applicable(self, exe: Executor) -> bool:
        return exe.db.scenario == Scenario.Rename

    def run(self, exe: Executor) -> bool:
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
                # Both schemas belong to the same database, the concurrent
                # swap of a disjoint pair uses a different tmp name.
                tmp_name = f"tmp_schema_{schema1.schema_id}_{schema2.schema_id}"
                exe.cur.connection.autocommit = False
                try:
                    exe.execute(
                        f"ALTER SCHEMA {schema1} RENAME TO {identifier(tmp_name)}"
                    )
                    exe.execute(
                        f"ALTER SCHEMA {schema2} RENAME TO {identifier(schema1.name())}"
                    )
                    exe.execute(
                        f"ALTER SCHEMA {schema1.db}.{identifier(tmp_name)} RENAME TO {identifier(schema2.name())}"
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


class ParameterizedQueryAction(Action):
    """PREPARE a query with $1..$n placeholders, then EXECUTE it with values.

    Exercises the parameter-type-inference and bind/assignment-cast path that
    the workload's other prepared statements (no parameters) never reach."""

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        result.extend(
            [
                "in the same timedomain",
                'is not allowed from the "mz_catalog_server" cluster',
            ]
        )
        if exe.db.complexity == Complexity.DDL:
            result.extend(["does not exist"])
        return result

    def run(self, exe: Executor) -> bool:
        obj = self.rng.choice(exe.db.db_objects())
        n = self.rng.randint(1, 4)
        param_types = [self.rng.choice(list(DATA_TYPES)) for _ in range(n)]
        projection = ", ".join(
            f"${i + 1}::{t.name()}" for i, t in enumerate(param_types)
        )
        self.stmt_id += 1
        name = f"pq{self.stmt_id}"
        query = f"SELECT {projection} FROM {obj} LIMIT {self.rng.randint(0, 10)}"
        # Each argument is cast to its parameter's declared type so the
        # assignment cast on EXECUTE always succeeds (e.g. a bytea parameter
        # rejects a bare text literal).
        values = ", ".join(
            f"({t.random_value(self.rng, in_query=True)})::{t.name()}"
            for t in param_types
        )
        # Run sequentially, not in a try/finally: if EXECUTE fails it aborts
        # the transaction, and a DEALLOCATE in a finally would then fail with
        # "current transaction is aborted", masking the real error. On failure
        # the worker rolls back, which discards the prepared statement anyway.
        exe.execute(f"PREPARE {name} AS {query}", http=Http.NO)
        exe.execute(f"EXECUTE {name} ({values})", http=Http.NO, fetch=True)
        exe.execute(f"DEALLOCATE {name}", http=Http.NO)
        return True


class BoundedStalenessReadAction(Action):
    """A read under `bounded staleness` isolation, a distinct timestamp-
    selection path that never blocks and returns 40001 when the freshness
    bound cannot be met. The isolation is set transiently around the read and
    reset, since bounded staleness is read-only and would break writes."""

    def applicable(self, exe: Executor) -> bool:
        return exe.db.flags.get("enable_bounded_staleness_isolation", "FALSE") == "TRUE"

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        result.extend(
            [
                # The flag was flipped off between applicable() and run().
                "is not available",
                # The freshness bound could not be met. Bounded staleness
                # never blocks, it errors instead.
                "not been materialized",
                "could not find a valid timestamp for the query",
                "cannot serve query under bounded staleness",
                # A leaked real_time_recency SET on the session (its own reset
                # was discarded on a prior query's error path) conflicts with
                # bounded staleness.
                "cannot be combined with bounded staleness",
                "in the same timedomain",
                'is not allowed from the "mz_catalog_server" cluster',
            ]
        )
        if exe.db.complexity == Complexity.DDL:
            result.extend(["does not exist"])
        return result

    def run(self, exe: Executor) -> bool:
        bound = self.rng.choice(["1s", "5s", "30s"])
        exe.execute(
            f"SET TRANSACTION_ISOLATION TO 'bounded staleness {bound}'",
            explainable=False,
        )
        try:
            query = self.generate_select_query(exe, ExprKind.ALL)
            exe.execute(query, http=Http.NO, fetch=True)
        finally:
            exe.execute(
                "SET TRANSACTION_ISOLATION TO 'strict serializable'",
                explainable=False,
            )
        return True


class ReadOnlyTransactionAction(Action):
    """A multi-statement `BEGIN READ ONLY` transaction. All reads run at one
    pinned timestamp (one timedomain), and holding the read pins compaction
    for the transaction's lifetime."""

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        result.extend(
            [
                "in the same timedomain",
                'is not allowed from the "mz_catalog_server" cluster',
            ]
        )
        if exe.db.complexity == Complexity.DDL:
            result.extend(["does not exist"])
        return result

    def run(self, exe: Executor) -> bool:
        isolation = self.rng.choice(
            [
                "",
                " ISOLATION LEVEL SERIALIZABLE",
                " ISOLATION LEVEL STRICT SERIALIZABLE",
            ]
        )
        exe.execute(f"BEGIN{isolation} READ ONLY", http=Http.NO)
        try:
            for _ in range(self.rng.randint(1, 3)):
                query = self.generate_select_query(exe, ExprKind.ALL)
                exe.execute(query, http=Http.NO, fetch=True)
        finally:
            end = "COMMIT" if self.rng.choice([True, False]) else "ROLLBACK"
            exe.execute(end, http=Http.NO)
        return True


class DDLTransactionAction(Action):
    """A DDL statement inside an explicit `BEGIN`/`COMMIT`. Materialize allows
    only a single statement in a DDL transaction, so the value over autocommit
    DDL is the open commit window: racing concurrent DDL against it exercises
    the "another session modified the catalog while this DDL transaction was
    open" serialization path."""

    def applicable(self, exe: Executor) -> bool:
        return exe.db.complexity in (Complexity.DDL, Complexity.DDLOnly)

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "another session modified the catalog",
            "unknown schema",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        if len([t for t in exe.db.tables if not t.temp]) >= MAX_TABLES:
            return False
        try:
            schema = self.rng.choice(exe.db.schemas)
        except IndexError:
            return False
        table_id = exe.db.table_id
        exe.db.table_id += 1
        table = Table(self.rng, table_id, schema)
        exe.execute("BEGIN", http=Http.NO)
        try:
            table.create(exe)
            exe.execute("COMMIT", http=Http.NO)
        except QueryError:
            try:
                exe.execute("ROLLBACK", http=Http.NO)
            except QueryError:
                pass
            raise
        with exe.db.lock:
            exe.db.tables.append(table)
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
        self.flags_with_values["persist_source_fetch_concurrency"] = [
            "1",
            "2",
            "8",
            "16",
        ]
        self.flags_with_values["enable_variadic_left_join_lowering"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["enable_eager_delta_joins"] = BOOLEAN_FLAG_VALUES
        self.flags_with_values["enable_public_metrics_endpoint"] = BOOLEAN_FLAG_VALUES
        self.flags_with_values["enable_scoped_system_parameters"] = BOOLEAN_FLAG_VALUES
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
        self.flags_with_values["persist_source_hydration_frontier_coalesce_bytes"] = [
            # 0 disables; otherwise coalesce frontier downgrades until this
            # many encoded bytes have been emitted (1 MiB, 16 MiB, 128 MiB).
            "0",
            "1048576",
            "16777216",
            "134217728",
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
        self.flags_with_values["persist_state_update_lease_timeout"] = [
            "'0s'",
            "'1s'",
            "'10s'",
        ]
        self.flags_with_values["compute_prometheus_introspection_scrape_interval"] = [
            "'0s'",
            "'1s'",
            "'10s'",
        ]
        self.flags_with_values["arrangement_size_history_collection_interval"] = [
            "'1s'",
            "'10s'",
            "'1h'",
        ]
        self.flags_with_values["arrangement_size_history_retention_period"] = [
            "'1min'",
            "'1h'",
            "'7d'",
        ]
        # Keep these generous: a tight timeout would abort the oracle's own
        # queries (they are retried, but it adds noise). "0s" leaves it unset.
        self.flags_with_values["pg_timestamp_oracle_statement_timeout"] = [
            "'0s'",
            "'30s'",
            "'60s'",
        ]
        # Note: it's not safe to re-enable this flag after writing with `persist_validate_part_bounds_on_write`,
        # since those new-style parts may fail our old-style validation.
        self.flags_with_values["persist_validate_part_bounds_on_read"] = ["FALSE"]
        self.flags_with_values["persist_validate_part_bounds_on_write"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["user_id_pool_batch_size"] = [
            "1",
            "5",
            "512",
            "1024",
        ]
        self.flags_with_values["compute_apply_column_demands"] = BOOLEAN_FLAG_VALUES
        self.flags_with_values["compute_correction_v2_chain_proportionality"] = [
            "2",
            "3",
        ]
        self.flags_with_values["compute_correction_v2_chunk_size"] = [
            "8192",
            "65536",
            "1048576",
        ]
        self.flags_with_values["enable_compute_temporal_bucketing"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["enable_alter_table_add_column"] = BOOLEAN_FLAG_VALUES
        self.flags_with_values["enable_bounded_staleness_isolation"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["enable_arrangement_dictionary_compression_alpha"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["enable_compute_peek_response_stash"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["compute_peek_response_stash_threshold_bytes"] = [
            "0",  # "force enabled"
            "1048576",  # 1 MiB, an in-between value
            "314572800",  # 300 MiB, the production value
        ]
        self.flags_with_values["compute_subscribe_snapshot_optimization"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["cluster"] = ["quickstart", "dont_exist"]
        # NOTE: enable_frontend_peek_sequencing is pinned off in
        # ADDITIONAL_SYSTEM_PARAMETER_DEFAULTS (frontend-peek read-hold vs
        # compaction race, SQL-520, see FINDINGS-BUGS.md), so it is not flipped
        # here.
        self.flags_with_values["enable_frontend_subscribes"] = [
            "true",
            "false",
        ]
        self.flags_with_values["enable_case_literal_transform"] = BOOLEAN_FLAG_VALUES
        self.flags_with_values["enable_cast_elimination"] = BOOLEAN_FLAG_VALUES
        self.flags_with_values["enable_fixed_correlated_cte_lowering"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["enable_upsert_v2"] = BOOLEAN_FLAG_VALUES
        self.flags_with_values["enable_coalesce_case_transform"] = BOOLEAN_FLAG_VALUES
        self.flags_with_values["enable_compute_sync_mv_sink"] = BOOLEAN_FLAG_VALUES
        self.flags_with_values["enable_column_paged_batcher"] = BOOLEAN_FLAG_VALUES
        self.flags_with_values["enable_column_paged_batcher_spill"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["column_paged_batcher_budget_fraction"] = [
            "0.0",
            "0.01",
            "0.05",
            "0.25",
        ]
        self.flags_with_values["column_paged_batcher_lz4"] = BOOLEAN_FLAG_VALUES
        self.flags_with_values["column_paged_batcher_swap_pageout"] = (
            BOOLEAN_FLAG_VALUES
        )
        self.flags_with_values["enable_upsert_paged_spill"] = BOOLEAN_FLAG_VALUES
        self.flags_with_values["webhook_max_request_size_bytes"] = [
            # 1 MiB, 5 MiB (default), 10 MiB
            "1048576",
            "5242880",
            "10485760",
        ]
        self.flags_with_values["aws_prefetch_sts_connect_timeout"] = [
            "'3100ms'",
            "'30s'",
            "'60s'",
        ]

        # If you are adding a new config flag in Materialize, consider using it
        # here instead of just marking it as uninteresting to silence the
        # linter. parallel-workload randomly flips the flags in
        # `flags_with_values` while running. If a new flag has interesting
        # behavior, you should add it. Feature flags which turn on/off
        # externally visible features should not be flipped.
        self.uninteresting_flags: list[str] = [
            "enable_compute_half_join2",
            "enable_mz_join_core",
            "enable_compute_correction_v2",
            "linear_join_yielding",
            "enable_lgalloc",
            "enable_lgalloc_eager_reclamation",
            "enable_s3_tables_region_check",
            "lgalloc_background_interval",
            "lgalloc_file_growth_dampener",
            "lgalloc_local_buffer_bytes",
            "lgalloc_slow_clear_bytes",
            "memory_limiter_interval",
            "memory_limiter_usage_bias",
            "memory_limiter_burst_factor",
            "enable_columnation_lgalloc",
            "enable_columnar_lgalloc",
            "catalog_info_metrics_reconcile_interval",
            "compute_server_maintenance_interval",
            "compute_dataflow_max_inflight_bytes",
            "compute_dataflow_max_inflight_bytes_cc",
            "compute_flat_map_fuel",
            "consolidating_vec_growth_dampener",
            "compute_hydration_concurrency",
            "copy_to_s3_parquet_row_group_file_ratio",
            "copy_to_s3_arrow_builder_buffer_ratio",
            "copy_to_s3_multipart_part_size_bytes",
            "enable_compute_prometheus_metrics",
            "enable_compute_replica_expiration",
            "compute_mv_sink_advance_persist_frontiers",
            "compute_replica_expiration_offset",
            "enable_compute_render_fueled_as_specific_collection",
            "compute_temporal_bucketing_summary",
            "enable_compute_logical_backpressure",
            "enable_replica_targeted_materialized_views",
            "compute_logical_backpressure_max_retained_capabilities",
            "compute_logical_backpressure_inflight_slack",
            "persist_fetch_semaphore_cost_adjustment",
            "persist_fetch_semaphore_permit_adjustment",
            "persist_pg_consensus_read_committed",  # Doesn't work against CRDB
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
            "persist_use_postgres_tuned_queries",
            "crdb_connect_timeout",
            "crdb_tcp_user_timeout",
            "crdb_keepalives_idle",
            "crdb_keepalives_interval",
            "crdb_keepalives_retries",
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
            "kafka_default_aws_privatelink_endpoint_identification_algorithm",
            "kafka_buffered_event_resize_threshold_elements",
            "kafka_low_watermark_check",
            "mysql_replication_heartbeat_interval",
            "postgres_fetch_slot_resume_lsn_interval",
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
            "sql_server_cdc_cleanup_change_table",
            "sql_server_cdc_cleanup_change_table_max_deletes",
            "default_timestamp_interval",
            "allow_user_sessions",
            "enable_0dt_deployment",
            "with_0dt_deployment_max_wait",
            "with_0dt_deployment_ddl_check_interval",
            "enable_0dt_deployment_panic_after_timeout",
            "enable_0dt_caught_up_check",
            "with_0dt_caught_up_check_allowed_lag",
            "with_0dt_caught_up_check_cutoff",
            "with_0dt_caught_up_check_stability_period",
            "enable_0dt_caught_up_stability_check",
            "enable_statement_lifecycle_logging",
            "enable_introspection_subscribes",
            "plan_insights_notice_fast_path_clusters_optimize_duration",
            "enable_expression_cache",
            "enable_password_auth",
            "persist_fast_path_order",
            "enable_mcp_agent",
            "enable_mcp_agent_query_tool",
            "enable_mcp_agent_read_data_product_tool",
            "enable_mcp_developer",
            "enable_mcp_developer_query_tool",
            "mcp_max_response_size",
            "mcp_request_timeout",
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
            "kafka_sink_message_max_bytes",
            "kafka_sink_batch_size",
            "kafka_sink_batch_num_messages",
            "pg_source_validate_timeline",
            "sql_server_source_validate_restore_history",
            "oidc_issuer",
            "oidc_audience",
            "oidc_authentication_claim",
            "oidc_group_role_sync_enabled",
            "oidc_group_claim",
            "oidc_group_role_sync_strict",
            "console_oidc_client_id",
            "console_oidc_scopes",
            "enable_cluster_controller",
            "cluster_controller_tick_interval",
            "enable_background_alter_cluster",
            "default_cluster_reconfiguration_timeout",
            # A safety bound on read-then-write dependency validation. Flipping
            # it low would make ordinary DELETE/UPDATE/INSERT ... SELECT fail,
            # which the workload does not expect.
            "read_then_write_max_dependencies",
        ]

    def run(self, exe: Executor) -> bool:
        flag_name = self.rng.choice(list(self.flags_with_values.keys()))

        # TODO: Remove when https://linear.app/materializeinc/issue/DB-138 is fixed
        if exe.db.scenario == Scenario.ZeroDowntimeDeploy and flag_name.startswith(
            "persist_use_critical_since_"
        ):
            return False

        # `persist_pg_consensus_read_committed` requires a Postgres consensus
        # backend. The external scenarios run against CockroachDB, where it
        # panics persist, so never flip it on there.
        if (
            flag_name == "persist_pg_consensus_read_committed"
            and exe.db.scenario in COCKROACH_SCENARIOS
        ):
            return False

        flag_value = self.rng.choice(self.flags_with_values[flag_name])
        # Occasionally restore the default instead, a distinct path from
        # SET-to-value.
        reset = self.rng.random() < 0.1

        conn = None

        try:
            conn = self.create_system_connection(exe)
            if reset:
                self.reset_flag(conn, flag_name)
                # Gates reading exe.db.flags fall back to their conservative
                # default when the key is absent.
                exe.db.flags.pop(flag_name, None)
            else:
                self.flip_flag(conn, flag_name, flag_value)
                exe.db.flags[flag_name] = flag_value
            return True
        except OperationalError:
            # ignore it
            return False
        except Exception as e:
            raise QueryError(str(e), "FlipFlags")
        finally:
            if conn is not None:
                conn.close()

    def flip_flag(self, conn: Connection, flag_name: str, flag_value: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                f"ALTER SYSTEM SET {flag_name} = {flag_value};".encode(),
            )

    def reset_flag(self, conn: Connection, flag_name: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                f"ALTER SYSTEM RESET {flag_name};".encode(),
            )


class CreateViewAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        errors = super().errors_to_ignore(exe)
        if exe.db.scenario == Scenario.Rename:
            # Columns could have been renamed, we don't lock the base objects
            # to get more interesting race conditions
            errors += ["does not exist"]
        errors += [
            "replica-targeted materialized views is not supported",
            "unknown cluster replica",
        ]
        return errors

    def run(self, exe: Executor) -> bool:
        temp = self.rng.choice([True, False])
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
        if temp:
            schema = MzTempSchema(self.rng.choice(exe.db.dbs))
            view = View(
                self.rng,
                view_id,
                base_object,
                base_object2,
                schema,
                scenario=exe.db.scenario,
                temp=True,
            )
            view.create(exe)
        else:
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
                    scenario=exe.db.scenario,
                )
                # Randomly make materialized views replica-targeted
                if (
                    view.materialized
                    and exe.db.flags.get(
                        "enable_replica_targeted_materialized_views", "FALSE"
                    )
                    == "TRUE"
                    and self.rng.choice([True, False])
                ):
                    clusters_with_replicas = [c for c in exe.db.clusters if c.replicas]
                    if clusters_with_replicas:
                        cluster = self.rng.choice(clusters_with_replicas)
                        view.target_replica = self.rng.choice(cluster.replicas)
                view.create(exe)
        exe.db.views.append(view)
        if temp:
            exe.temp_objects.append(view)
        return True


class DropViewAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            views = [view for view in exe.db.views if not view.temp]
            if not views:
                return False
            view = self.rng.choice(views)
        with view.lock:
            # Was dropped while we were acquiring lock
            if view not in exe.db.views:
                return False

            if view.materialized:
                query = f"DROP MATERIALIZED VIEW {view}"
            else:
                query = f"DROP VIEW {view}"
            exe.execute(query, http=Http.RANDOM)
            # A concurrent CASCADE drop's untrack_objects_in_schemas may have
            # already filtered this view out of the list; tolerate that.
            try:
                exe.db.views.remove(view)
            except ValueError:
                pass
        return True


class CreateOrReplaceViewAction(Action):
    """In-place swap of an existing view's definition via CREATE OR REPLACE.

    The body is unchanged, so dependents stay valid, but the coordinator still
    tears down and rebuilds the item (and the dataflow, for a materialized
    view). Racing that swap against reads and concurrent DDL is the point."""

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        errors = [
            # A dependent references a column the replacement would drop. The
            # body is unchanged here, but a concurrent replacement may have
            # changed it.
            "still depended upon by",
            "replica-targeted materialized views is not supported",
            "unknown cluster replica",
        ] + super().errors_to_ignore(exe)
        if exe.db.scenario == Scenario.Rename:
            # A base object was renamed, invalidating the captured body.
            errors += ["does not exist", "ambiguous reference to schema name"]
        return errors

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            views = [view for view in exe.db.views if not view.temp]
            if not views:
                return False
            view = self.rng.choice(views)
        with view.lock:
            if view not in exe.db.views:
                return False
            view.create(exe, or_replace=True)
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


class AlterRoleAction(Action):
    """ALTER ROLE ... SET / RESET a default session variable.

    Exercises the per-role session-default path (applied at session init) and
    role-name resolution on the ALTER path, neither of which CREATE/DROP ROLE
    touch. Part of broadening ALTER coverage (the ALTER paths are where several
    catalog bugs have hidden vs the well-worn CREATE/DROP)."""

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.roles:
                return False
            role = self.rng.choice(exe.db.roles)
        with role.lock:
            # Was dropped while we were acquiring the lock.
            if role not in exe.db.roles:
                return False
            var, value = self.rng.choice(
                [
                    ("cluster", "'quickstart'"),
                    ("transaction_isolation", "'serializable'"),
                    ("statement_timeout", "'120s'"),
                    ("search_path", "public"),
                ]
            )
            query = (
                f"ALTER ROLE {role} RESET {var}"
                if self.rng.choice([True, False])
                else f"ALTER ROLE {role} SET {var} = {value}"
            )
            try:
                exe.execute(query, http=Http.RANDOM)
            except QueryError as e:
                # Concurrent DROP ROLE, expected as with DropRoleAction.
                if (
                    exe.db.scenario not in (Scenario.Kill, Scenario.ZeroDowntimeDeploy)
                    or "unknown role" not in e.msg
                ):
                    raise e
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
            size=self.rng.choice(["scale=1,workers=1", "scale=1,workers=2"]),
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
            # Keep the first cluster with 1 replica for sources/sinks
            cluster = self.rng.choice(exe.db.clusters[1:])
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

    def applicable(self, exe: Executor) -> bool:
        return exe.db.scenario == Scenario.Rename

    def run(self, exe: Executor) -> bool:
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
                # A concurrent swap of a disjoint pair uses a different tmp
                # name.
                tmp_name = f"tmp_cluster_{cluster1.cluster_id}_{cluster2.cluster_id}"
                exe.cur.connection.autocommit = False
                try:
                    exe.execute(
                        f"ALTER CLUSTER {cluster1} RENAME TO {identifier(tmp_name)}"
                    )
                    exe.execute(
                        f"ALTER CLUSTER {cluster2} RENAME TO {identifier(cluster1.name())}"
                    )
                    exe.execute(
                        f"ALTER CLUSTER {identifier(tmp_name)} RENAME TO {identifier(cluster2.name())}"
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
            # Keep cluster 0 with 1 replica for sources/sinks. Only unmanaged
            # clusters support CREATE CLUSTER REPLICA. Without the
            # MAX_CLUSTER_REPLICAS cap the replica count random-walks upward
            # (drops skip at <= 1 replica) into max_replicas_per_cluster.
            unmanaged_clusters = [
                c
                for c in exe.db.clusters[1:]
                if not c.managed and len(c.replicas) < MAX_CLUSTER_REPLICAS
            ]
            if not unmanaged_clusters:
                return False
            cluster = self.rng.choice(unmanaged_clusters)
            replica_id = cluster.replica_id
            cluster.replica_id += 1
        with cluster.lock:
            if cluster not in exe.db.clusters or cluster.managed:
                return False

            replica = ClusterReplica(
                replica_id,
                size=self.rng.choice(["scale=1,workers=1", "scale=1,workers=2"]),
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
            privilege = self.rng.choice(["SELECT", "INSERT", "UPDATE", "DELETE", "ALL"])
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
        return True


class RevokePrivilegesAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.roles:
                return False
            role = self.rng.choice(exe.db.roles)
            privilege = self.rng.choice(["SELECT", "INSERT", "UPDATE", "DELETE", "ALL"])
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
        return True


class GrantRoleAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "unknown role",
            # Concurrent memberships can close a cycle, which is rejected.
            "is a member of role",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.roles) < 2:
                return False
            role1, role2 = self.rng.sample(exe.db.roles, 2)
        with role1.lock, role2.lock:
            if role1 not in exe.db.roles or role2 not in exe.db.roles:
                return False
            exe.execute(f"GRANT {role1} TO {role2}", http=Http.RANDOM)
        return True


class RevokeRoleAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "unknown role",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.roles) < 2:
                return False
            role1, role2 = self.rng.sample(exe.db.roles, 2)
        with role1.lock, role2.lock:
            if role1 not in exe.db.roles or role2 not in exe.db.roles:
                return False
            exe.execute(f"REVOKE {role1} FROM {role2}", http=Http.RANDOM)
        return True


class AlterOwnerAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = [
            "unknown role",
            "must be a member of",
        ] + super().errors_to_ignore(exe)
        if exe.db.complexity in (Complexity.DDL, Complexity.DDLOnly):
            result.extend(["does not exist"])
        return result

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.roles:
                return False
            role = self.rng.choice(exe.db.roles)
            candidates: list[tuple[str, str]] = []
            # Temp objects cannot change owner, they die with the session.
            for table in exe.db.tables:
                if not table.temp:
                    candidates.append(("TABLE", str(table)))
            for view in exe.db.views:
                if not view.temp:
                    candidates.append(
                        (
                            "MATERIALIZED VIEW" if view.materialized else "VIEW",
                            str(view),
                        )
                    )
            # Kafka and webhook source objects are readable directly, the
            # others follow the source-table model: str(obj) names the table,
            # the ingestion source is a separate catalog item.
            for source in exe.db.kafka_sources + exe.db.webhook_sources:
                candidates.append(("SOURCE", str(source)))
            for source in (
                exe.db.postgres_sources
                + exe.db.mysql_sources
                + exe.db.sql_server_sources
            ):
                candidates.append(("TABLE", str(source)))
                candidates.append(
                    (
                        "SOURCE",
                        f"{source.schema}.{identifier(source.executor.source)}",
                    )
                )
            for source in exe.db.loadgen_sources:
                candidates.append(("TABLE", str(source)))
                candidates.append(
                    ("SOURCE", f"{source.schema}.{identifier(source.source_name())}")
                )
            for sink in exe.db.kafka_sinks + exe.db.iceberg_sinks:
                candidates.append(("SINK", str(sink)))
            for schema in exe.db.schemas:
                candidates.append(("SCHEMA", str(schema)))
            for db in exe.db.dbs:
                candidates.append(("DATABASE", str(db)))
            for cluster in exe.db.clusters:
                candidates.append(("CLUSTER", str(cluster)))
            candidates.append(("SECRET", "materialize.public.pgpass"))
            # NOTE: No CONNECTION target. Changing a connection's owner emits a
            # Connection(Altered) implication, which re-alters every dependent
            # sink's export connection; that re-alter can fail with InvalidAlter
            # and panic the coordinator (SQL-517). See FINDINGS-BUGS.md ("Coordinator
            # panic re-altering a dependent sink's export connection").
            kind, name = self.rng.choice(candidates)
        with role.lock:
            if role not in exe.db.roles:
                return False
            exe.execute(f"ALTER {kind} {name} OWNER TO {role}", http=Http.RANDOM)
        return True


class AlterDefaultPrivilegesAction(Action):
    # Privileges per object type. The bool pair is (allows IN SCHEMA, allows IN
    # DATABASE): schema-scoped objects accept both, SCHEMAS only IN DATABASE,
    # DATABASES and CLUSTERS neither. Mixing e.g. ON DATABASES with IN DATABASE
    # is a plan error, not a race, so it is generated out.
    OBJECT_TYPES = {
        "TABLES": (["SELECT", "INSERT", "UPDATE", "DELETE", "ALL"], True, True),
        "TYPES": (["USAGE", "ALL"], True, True),
        "SECRETS": (["USAGE", "ALL"], True, True),
        "CONNECTIONS": (["USAGE", "ALL"], True, True),
        "SCHEMAS": (["USAGE", "CREATE", "ALL"], False, True),
        "DATABASES": (["USAGE", "CREATE", "ALL"], False, False),
        "CLUSTERS": (["USAGE", "CREATE", "ALL"], False, False),
    }

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = [
            "unknown role",
            "unknown schema",
            "unknown database",
            "must be a member of",
            # FOR ALL ROLES and system-adjacent grants require privileges the
            # (possibly reconnected-as-a-random-role) session may lack.
            "permission denied to",
        ] + super().errors_to_ignore(exe)
        return result

    def run(self, exe: Executor) -> bool:
        object_type = self.rng.choice(list(self.OBJECT_TYPES.keys()))
        privileges, allows_in_schema, allows_in_database = self.OBJECT_TYPES[
            object_type
        ]
        privilege = self.rng.choice(privileges)
        with exe.db.lock:
            if not exe.db.roles:
                return False
            role = self.rng.choice(exe.db.roles)
            for_clause = self.rng.choice(
                ["FOR ALL ROLES"]
                + [f"FOR ROLE {r}" for r in self.rng.sample(exe.db.roles, 1)]
            )
            in_clause = ""
            if allows_in_schema and exe.db.schemas and self.rng.random() < 0.3:
                in_clause = f" IN SCHEMA {self.rng.choice(exe.db.schemas)}"
            elif allows_in_database and exe.db.dbs and self.rng.random() < 0.3:
                in_clause = f" IN DATABASE {self.rng.choice(exe.db.dbs)}"
        with role.lock:
            if role not in exe.db.roles:
                return False
            if self.rng.choice([True, False]):
                query = f"ALTER DEFAULT PRIVILEGES {for_clause}{in_clause} GRANT {privilege} ON {object_type} TO {role}"
            else:
                query = f"ALTER DEFAULT PRIVILEGES {for_clause}{in_clause} REVOKE {privilege} ON {object_type} FROM {role}"
            exe.execute(query, http=Http.RANDOM)
        return True


class BroadPrivilegesAction(Action):
    """GRANT/REVOKE on object classes beyond the tables and views covered by
    Grant/RevokePrivilegesAction."""

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "unknown role",
            "unknown schema",
            "unknown database",
            "unknown cluster",
            # System privileges require superuser, which a session reconnected
            # as a random role does not have.
            "permission denied to",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.roles:
                return False
            role = self.rng.choice(exe.db.roles)
            targets: list[tuple[str, list[str]]] = [
                ("SYSTEM", ["CREATEDB", "CREATECLUSTER", "CREATEROLE", "ALL"]),
                ("SECRET materialize.public.pgpass", ["USAGE", "ALL"]),
                # NOTE: No CONNECTION target. GRANT/REVOKE on a connection emits
                # a Connection(Altered) implication, which re-alters every
                # dependent sink's export connection; that re-alter can fail
                # with InvalidAlter and panic the coordinator (SQL-517). See
                # FINDINGS-BUGS.md ("Coordinator panic re-altering a dependent
                # sink's export connection").
            ]
            if exe.db.schemas:
                targets.append(
                    (
                        f"SCHEMA {self.rng.choice(exe.db.schemas)}",
                        ["USAGE", "CREATE", "ALL"],
                    )
                )
            if exe.db.dbs:
                targets.append(
                    (
                        f"DATABASE {self.rng.choice(exe.db.dbs)}",
                        ["USAGE", "CREATE", "ALL"],
                    )
                )
            if exe.db.clusters:
                targets.append(
                    (
                        f"CLUSTER {self.rng.choice(exe.db.clusters)}",
                        ["USAGE", "CREATE", "ALL"],
                    )
                )
            target, privileges = self.rng.choice(targets)
            privilege = self.rng.choice(privileges)
        with role.lock:
            if role not in exe.db.roles:
                return False
            if self.rng.choice([True, False]):
                exe.execute(
                    f"GRANT {privilege} ON {target} TO {role}", http=Http.RANDOM
                )
            else:
                exe.execute(
                    f"REVOKE {privilege} ON {target} FROM {role}", http=Http.RANDOM
                )
        return True


class ShowAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        result.extend(
            [
                # SHOW CREATE CLUSTER only works for managed clusters. We only
                # target managed ones, this covers a managed->unmanaged race.
                "SHOW CREATE for unmanaged clusters not yet supported",
                # With auto_route_catalog_queries off, SHOW compiles to a
                # catalog query on the active cluster, so it shares the read
                # transaction's timedomain.
                "in the same timedomain",
                'is not allowed from the "mz_catalog_server" cluster',
            ]
        )
        if exe.db.complexity in (Complexity.DDL, Complexity.DDLOnly):
            result.extend(["does not exist"])
        return result

    def run(self, exe: Executor) -> bool:
        if self.rng.choice([True, False]):
            schema_scoped = [
                "SHOW TABLES",
                "SHOW VIEWS",
                "SHOW MATERIALIZED VIEWS",
                "SHOW SOURCES",
                "SHOW SINKS",
                "SHOW INDEXES",
                "SHOW OBJECTS",
                "SHOW SECRETS",
                "SHOW CONNECTIONS",
                "SHOW TYPES",
            ]
            other = [
                "SHOW CLUSTERS",
                "SHOW CLUSTER REPLICAS",
                "SHOW DATABASES",
                "SHOW SCHEMAS",
                "SHOW ROLES",
                "SHOW PRIVILEGES",
                "SHOW DEFAULT PRIVILEGES",
                "SHOW ROLE MEMBERSHIP",
                "SHOW ALL",
            ]
            if self.rng.choice([True, False]):
                query = self.rng.choice(schema_scoped)
                with exe.db.lock:
                    if exe.db.schemas and self.rng.choice([True, False]):
                        query += f" FROM {self.rng.choice(exe.db.schemas)}"
                if self.rng.random() < 0.2:
                    query += " LIKE '%1%'"
            else:
                query = self.rng.choice(other)
        else:
            with exe.db.lock:
                candidates: list[tuple[str, str]] = [
                    ("CONNECTION", "materialize.public.kafka_conn"),
                    ("CONNECTION", "materialize.public.csr_conn"),
                ]
                for table in exe.db.tables:
                    candidates.append(("TABLE", str(table)))
                for view in exe.db.views:
                    candidates.append(
                        (
                            "MATERIALIZED VIEW" if view.materialized else "VIEW",
                            str(view),
                        )
                    )
                # Kafka and webhook sources are readable directly; the others
                # follow the source-table model where str() is the table and
                # the ingestion source is a separate catalog item.
                for source in exe.db.kafka_sources + exe.db.webhook_sources:
                    candidates.append(("SOURCE", str(source)))
                for source in (
                    exe.db.postgres_sources
                    + exe.db.mysql_sources
                    + exe.db.sql_server_sources
                ):
                    candidates.append(("TABLE", str(source)))
                    candidates.append(
                        (
                            "SOURCE",
                            f"{source.schema}.{identifier(source.executor.source)}",
                        )
                    )
                for source in exe.db.loadgen_sources:
                    candidates.append(("TABLE", str(source)))
                    candidates.append(
                        (
                            "SOURCE",
                            f"{source.schema}.{identifier(source.source_name())}",
                        )
                    )
                for sink in exe.db.kafka_sinks + exe.db.iceberg_sinks:
                    candidates.append(("SINK", str(sink)))
                for index in exe.db.indexes:
                    candidates.append(("INDEX", str(index)))
                for cluster in exe.db.clusters:
                    # SHOW CREATE CLUSTER is not supported for unmanaged
                    # clusters.
                    if cluster.managed:
                        candidates.append(("CLUSTER", str(cluster)))
                kind, name = self.rng.choice(candidates)
            # SHOW REDACTED CREATE CLUSTER is not supported
            redacted = (
                "REDACTED "
                if kind != "CLUSTER" and self.rng.choice([True, False])
                else ""
            )
            query = f"SHOW {redacted}CREATE {kind} {name}"
        exe.execute(query, http=Http.RANDOM, fetch=True)
        return True


class SetSessionVariableAction(Action):
    def __init__(self, rng: random.Random, composition: Composition | None):
        super().__init__(rng, composition)
        self.vars_with_values: dict[str, list[str]] = {
            "statement_timeout": ["'30s'", "'60s'", "'0s'"],
            "application_name": ["'parallel-workload'", "''"],
            "client_min_messages": ["debug1", "info", "notice", "warning", "error"],
            "max_query_result_size": ["100000", "1000000", "1000000000"],
            "emit_timestamp_notice": ["true", "false"],
            "emit_trace_id_notice": ["true", "false"],
            # Only UTC is accepted, the rejection of other time zones is
            # deliberate error path coverage.
            "timezone": ["'UTC'", "'America/New_York'"],
        }

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "invalid value for parameter",
            "cannot have value",
            "unrecognized configuration parameter",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        var = self.rng.choice(list(self.vars_with_values.keys()))
        if self.rng.random() < 0.2:
            exe.execute(f"RESET {var}", http=Http.RANDOM)
            return True
        value = self.rng.choice(self.vars_with_values[var])
        local = "LOCAL " if self.rng.random() < 0.1 else ""
        exe.execute(f"SET {local}{var} = {value}", http=Http.RANDOM)
        if var == "statement_timeout":
            # Statement timeouts on this session are expected from here on.
            # Kept sticky even across RESET, an in-flight statement can still
            # hit the old timeout.
            exe.statement_timeout_set = True
        return True


class DiscardAction(Action):
    def run(self, exe: Executor) -> bool:
        # The session's temp objects die with DISCARD, drop them from the
        # tracked state so other workers stop querying them, mirroring
        # ReconnectAction.
        if exe.temp_objects:
            with exe.db.lock:
                exe.db.tables[:] = [
                    t for t in exe.db.tables if t not in exe.temp_objects
                ]
                exe.db.views[:] = [v for v in exe.db.views if v not in exe.temp_objects]
            exe.temp_objects.clear()
        # Only DISCARD TEMP, not DISCARD ALL. DISCARD ALL (like DEALLOCATE ALL)
        # deallocates every prepared statement, including the ones psycopg
        # transparently auto-prepares. psycopg's client-side cache would then
        # be stale and the next reuse of such a statement fails with
        # "prepared statement ... does not exist". DISCARD TEMP still exercises
        # the temp-object teardown, which is the interesting path here.
        exe.execute("DISCARD TEMP", http=Http.NO)
        return True


class ValidateConnectionAction(Action):
    # aws_conn is excluded, MinIO's STS support for validation is unclear.
    CONNECTIONS = [
        "kafka_conn",
        "csr_conn",
        "postgres_conn",
        "mysql_conn",
        "sql_server_conn",
        "polaris_conn",
    ]

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "timeout: error trying to connect",
            # A concurrent ALTER SECRET rotation (the workload only rotates a
            # secret to its own value) can transiently expose an empty secret,
            # so VALIDATE CONNECTION sends an empty password and Postgres
            # rejects it (secret-rotation atomicity). See FINDINGS-BUGS.md.
            # TODO: Remove when SS-347 is fixed.
            "empty password returned by client",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        name = self.rng.choice(self.CONNECTIONS)
        exe.execute(f"VALIDATE CONNECTION materialize.public.{name}", http=Http.NO)
        return True


class AlterConnectionAction(Action):
    # The SET clause per connection, setting the option to the value the
    # connection already has. That still exercises the full reconfiguration
    # path (restarting dependent sources and sinks) without breaking them.
    # NOTE: BROKER takes no `=` (it is parsed specially), HOST/URL do.
    SET_CLAUSES = {
        "kafka_conn": "BROKER 'kafka:9092'",
        "csr_conn": "URL = 'http://schema-registry:8081'",
        "postgres_conn": "HOST = 'postgres'",
        "mysql_conn": "HOST = 'mysql'",
        "sql_server_conn": "HOST = 'sql-server'",
    }

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "timeout: error trying to connect",
            # The storage controller can refuse an in-place connection change
            # depending on the connection's current state or dependents.
            "cannot be altered in the requested way",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        name = self.rng.choice(list(self.SET_CLAUSES.keys()))
        set_clause = self.SET_CLAUSES[name]
        query = f"ALTER CONNECTION materialize.public.{name} SET ({set_clause})"
        if self.rng.choice([True, False]):
            validate = self.rng.choice(["true", "false"])
            query += f" WITH (VALIDATE = {validate})"
        exe.execute(query, http=Http.RANDOM)
        return True


class AlterSecretAction(Action):
    def run(self, exe: Executor) -> bool:
        # Rotate to the same value, exercising the rotation path (including
        # dependent connections picking up the new secret version) without
        # breaking the credentials.
        name, value = self.rng.choice(
            [
                ("pgpass", "postgres"),
                ("mypass", MySql.DEFAULT_ROOT_PASSWORD),
                ("sql_server_pass", SqlServer.DEFAULT_SA_PASSWORD),
                ("minio", "minioadmin"),
            ]
        )
        exe.execute(
            f"ALTER SECRET materialize.public.{name} AS '{value}'", http=Http.RANDOM
        )
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
        # The connection's temp objects die with it, drop them from the
        # tracked state so other workers stop querying them.
        if exe.temp_objects:
            with exe.db.lock:
                exe.db.tables[:] = [
                    t for t in exe.db.tables if t not in exe.temp_objects
                ]
                exe.db.views[:] = [v for v in exe.db.views if v not in exe.temp_objects]
            exe.temp_objects.clear()
        host = exe.db.host

        def pg_port() -> int:
            # System workers (e.g. the Cancel worker) live on the internal
            # port, everyone else on the external one of the current service.
            if exe.user == "mz_system":
                return exe.db.ports[
                    "mz_system" if exe.mz_service == "materialized" else "mz_system2"
                ]
            return exe.db.ports[exe.mz_service]

        with exe.db.lock:
            if self.random_role and exe.db.roles:
                user = self.rng.choice(
                    ["materialize", str(self.rng.choice(exe.db.roles))]
                )
            else:
                # Keep the executor's original user, e.g. the Cancel worker
                # must stay mz_system or its cancels fail with "must be a
                # member of"
                user = exe.user
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
                # Recompute the port each attempt, mz_service flips between
                # the services during zero-downtime deploys.
                conn = psycopg.connect(
                    host=host, port=pg_port(), user=user, dbname="materialize"
                )
                conn.autocommit = exe.autocommit
                cur = conn.cursor()
                exe.cur = cur
                exe.set_isolation("SERIALIZABLE")
                # Reapply the session settings from Worker.run, they don't
                # survive the reconnect.
                cur.execute("SET auto_route_catalog_queries TO false")
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


class CreateLoadGeneratorSourceAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.loadgen_sources) >= MAX_LOADGEN_SOURCES:
                return False
            source_id = exe.db.loadgen_source_id
            exe.db.loadgen_source_id += 1
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

            source = LoadGeneratorSource(source_id, cluster, schema, self.rng)
            source.create(exe)
            exe.db.loadgen_sources.append(source)
        return True


class DropLoadGeneratorSourceAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.loadgen_sources:
                return False
            try:
                source = self.rng.choice(exe.db.loadgen_sources)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with source.lock:
            # Was dropped while we were acquiring lock
            if source not in exe.db.loadgen_sources:
                return False

            exe.execute(f"DROP TABLE IF EXISTS {source}")
            exe.execute(
                f"DROP SOURCE {source.schema}.{identifier(source.source_name())} CASCADE",
                http=Http.RANDOM,
            )
            exe.db.loadgen_sources.remove(source)
        return True


class CreateMultiLoadGeneratorSourceAction(Action):
    GENERATORS = ["AUCTION", "TPCH", "MARKETING"]

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        # A rare cross-type subsource-name collision, or a concurrent create of
        # the same generator type.
        return ["already exists"] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            present = {s.generator for s in exe.db.multi_loadgen_sources}
            available = [g for g in self.GENERATORS if g not in present]
            if not available:
                return False
            generator = self.rng.choice(available)
            source_id = exe.db.multi_loadgen_source_id
            exe.db.multi_loadgen_source_id += 1
            try:
                cluster = self.rng.choice(exe.db.clusters)
                schema = self.rng.choice(exe.db.schemas)
            except IndexError:
                return False
        with schema.lock, cluster.lock:
            if schema not in exe.db.schemas:
                return False
            if cluster not in exe.db.clusters:
                return False
            source = MultiLoadGeneratorSource(
                source_id, cluster, schema, generator, self.rng
            )
            source.create(exe)
            with exe.db.lock:
                exe.db.multi_loadgen_sources.append(source)
        return True


class DropMultiLoadGeneratorSourceAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.multi_loadgen_sources:
                return False
            source = self.rng.choice(exe.db.multi_loadgen_sources)
        with source.lock:
            if source not in exe.db.multi_loadgen_sources:
                return False
            exe.execute(f"DROP SOURCE {source} CASCADE", http=Http.RANDOM)
            exe.db.multi_loadgen_sources.remove(source)
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
    def applicable(self, exe: Executor) -> bool:
        # See database-issues#6881, not expected to work
        return exe.db.scenario != Scenario.BackupRestore

    def run(self, exe: Executor) -> bool:
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
                # Creation can fail after CREATE CONNECTION but before the
                # source is appended, orphaning the connection (the mypass
                # secret is shared). Best-effort drop by name so it doesn't
                # accumulate toward max_mysql_connections.
                for stmt in (
                    f"DROP SOURCE IF EXISTS {schema}.{identifier(f'mysql_source{source_id}')} CASCADE",
                    f"DROP CONNECTION IF EXISTS mysql{source_id}",
                ):
                    try:
                        exe.execute(stmt, http=Http.NO)
                    except QueryError:
                        pass
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

            # The source's table (CREATE TABLE ... FROM SOURCE) depends on
            # it, drop the table first, it fails with "still depended upon
            # by" while other objects reference it. The CASCADE only sweeps
            # the source's own progress subsource.
            exe.execute(f"DROP TABLE IF EXISTS {source}", http=Http.RANDOM)
            query = f"DROP SOURCE {source.schema}.{identifier(source.executor.source)} CASCADE"
            exe.execute(query, http=Http.RANDOM)
            exe.db.mysql_sources.remove(source)
            source.executor.mz_conn.close()
            source.executor.mysql_conn.close()
            # The executor's per-source connection would otherwise accumulate
            # in materialize.public until max_objects_per_schema is hit (its
            # secret mypass is shared between sources)
            exe.execute(
                f"DROP CONNECTION IF EXISTS mysql{source.executor.num}",
                http=Http.RANDOM,
            )
        return True


class CreatePostgresSourceAction(Action):
    def applicable(self, exe: Executor) -> bool:
        # See database-issues#6881, not expected to work
        return exe.db.scenario != Scenario.BackupRestore

    def run(self, exe: Executor) -> bool:
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
                # Creation can fail after CREATE SECRET/CONNECTION but before
                # the source is appended, so DropPostgresSourceAction never
                # reclaims them and they accumulate toward
                # max_postgres_connections / max_objects_per_schema. Best-effort
                # drop what this source id would have created, by name.
                for stmt in (
                    f"DROP SOURCE IF EXISTS {schema}.{identifier(f'postgres_source{source_id}')} CASCADE",
                    f"DROP CONNECTION IF EXISTS pg{source_id}",
                    f"DROP SECRET IF EXISTS pgpass{source_id}",
                ):
                    try:
                        exe.execute(stmt, http=Http.NO)
                    except QueryError:
                        pass
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

            # The source's table (CREATE TABLE ... FROM SOURCE) depends on
            # it, drop the table first, it fails with "still depended upon
            # by" while other objects reference it. The CASCADE only sweeps
            # the source's own progress subsource.
            exe.execute(f"DROP TABLE IF EXISTS {source}", http=Http.RANDOM)
            query = f"DROP SOURCE {source.schema}.{identifier(source.executor.source)} CASCADE"
            exe.execute(query, http=Http.RANDOM)
            exe.db.postgres_sources.remove(source)
            source.executor.mz_conn.close()
            source.executor.pg_conn.close()
            # The executor's per-source connection and secret would otherwise
            # accumulate in materialize.public until max_objects_per_schema
            # is hit
            exe.execute(
                f"DROP CONNECTION IF EXISTS pg{source.executor.num}",
                http=Http.RANDOM,
            )
            exe.execute(
                f"DROP SECRET IF EXISTS pgpass{source.executor.num}",
                http=Http.RANDOM,
            )
        return True


class CreateSqlServerSourceAction(Action):
    def applicable(self, exe: Executor) -> bool:
        # See database-issues#6881, not expected to work
        return exe.db.scenario != Scenario.BackupRestore

    def run(self, exe: Executor) -> bool:
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
                # Creation can fail after CREATE CONNECTION but before the
                # source is appended, orphaning the connection (the
                # sql_server_pass secret is shared). Best-effort drop by name
                # so it doesn't accumulate toward max_sql_server_connections.
                for stmt in (
                    f"DROP SOURCE IF EXISTS {schema}.{identifier(f'sql_server_source{source_id}')} CASCADE",
                    f"DROP CONNECTION IF EXISTS sql_server{source_id}",
                ):
                    try:
                        exe.execute(stmt, http=Http.NO)
                    except QueryError:
                        pass
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

            # The source's table (CREATE TABLE ... FROM SOURCE) depends on
            # it, drop the table first, it fails with "still depended upon
            # by" while other objects reference it. The CASCADE only sweeps
            # the source's own progress subsource.
            exe.execute(f"DROP TABLE IF EXISTS {source}", http=Http.RANDOM)
            query = f"DROP SOURCE {source.schema}.{identifier(source.executor.source)} CASCADE"
            exe.execute(query, http=Http.RANDOM)
            exe.db.sql_server_sources.remove(source)
            source.executor.mz_conn.close()
            # The executor's per-source connection would otherwise accumulate
            # in materialize.public until max_objects_per_schema is hit (its
            # secret sql_server_pass is shared between sources)
            exe.execute(
                f"DROP CONNECTION IF EXISTS sql_server{source.executor.num}",
                http=Http.RANDOM,
            )
        return True


class CreateIcebergSinkAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "BYTES format with non-encodable type",
            "cannot be used as an Iceberg equality delete key",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.iceberg_sinks) >= MAX_ICEBERG_SINKS:
                return False
            sink_id = exe.db.iceberg_sink_id
            exe.db.iceberg_sink_id += 1
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

            sink = IcebergSink(
                sink_id,
                cluster,
                schema,
                self.rng.choice(exe.db.db_objects_for_sinks()),
                self.rng,
            )
            sink.create(exe)
            exe.db.iceberg_sinks.append(sink)
        return True


class DropIcebergSinkAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.iceberg_sinks:
                return False
            try:
                sink = self.rng.choice(exe.db.iceberg_sinks)
            except IndexError:
                # We mostly prevent index errors, but we don't want to lock too
                # much since that would reduce our chance of finding race
                # conditions in production code, so ignore the rare case where
                # we accidentally removed all objects.
                return False
        with sink.lock:
            # Was dropped while we were acquiring lock
            if sink not in exe.db.iceberg_sinks:
                return False

            query = f"DROP SINK {sink}"
            exe.execute(query, http=Http.RANDOM)
            exe.db.iceberg_sinks.remove(sink)
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
                self.rng.choice(exe.db.db_objects_for_sinks()),
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
        # DropSchemaCascadeAction / DropDatabaseCascadeAction can drop a
        # webhook source concurrently without taking its per-object lock, so a
        # POST that already picked the source can 404.
        if exe.db.complexity in (Complexity.DDL, Complexity.DDLOnly):
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

            # Copy, extending the source's list would grow it on every post.
            header_fields = list(source.explicit_include_headers)
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


class AlterClusterSetAction(Action):
    """Live reconfigure of a managed cluster (SIZE / REPLICATION FACTOR).

    Resizing or changing the replica count of a cluster hosting indexes, MVs,
    sources, and sinks forces rehydration and replica teardown/spin-up under
    concurrent DDL and DML."""

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            # A concurrent graceful reconfiguration of the same cluster.
            "cannot be modified while a reconfiguration is in progress",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            # Cluster 0 stays fixed, it hosts sources and sinks.
            managed = [c for c in exe.db.clusters[1:] if c.managed]
            if not managed:
                return False
            cluster = self.rng.choice(managed)
        with cluster.lock:
            if cluster not in exe.db.clusters or not cluster.managed:
                return False
            choice = self.rng.choice(["size", "replication_factor", "reset_rf"])
            if choice == "size":
                new_size = self.rng.choice(["scale=1,workers=1", "scale=1,workers=2"])
                exe.execute(
                    f"ALTER CLUSTER {cluster} SET (SIZE = '{new_size}')",
                    http=Http.RANDOM,
                )
                cluster.size = new_size
                for replica in cluster.replicas:
                    replica.size = new_size
            elif choice == "replication_factor":
                rf = self.rng.choice([1, 2])
                exe.execute(
                    f"ALTER CLUSTER {cluster} SET (REPLICATION FACTOR = {rf})",
                    http=Http.RANDOM,
                )
                self._resize_replicas(cluster, rf)
            else:
                exe.execute(
                    f"ALTER CLUSTER {cluster} RESET (REPLICATION FACTOR)",
                    http=Http.RANDOM,
                )
                self._resize_replicas(cluster, 1)
        return True

    def _resize_replicas(self, cluster: Cluster, count: int) -> None:
        # Managed cluster replicas are server-named (r1..rN). We keep the
        # tracked list at the right length only so the replica-targeted-MV
        # picker sees the right count.
        cluster.replicas = [
            ClusterReplica(i, cluster.size, cluster) for i in range(count)
        ]
        cluster.replica_id = count


class DropSchemaCascadeAction(Action):
    """DROP SCHEMA .. CASCADE, an atomic multi-object catalog mutation.

    Only enabled in DDL complexity: cross-schema dependents are cascade-dropped
    server-side but stay tracked until they surface as "does not exist", which
    DDL complexity ignores."""

    def applicable(self, exe: Executor) -> bool:
        return exe.db.complexity in (Complexity.DDL, Complexity.DDLOnly)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.schemas) <= 1:
                return False
            schema = self.rng.choice(exe.db.schemas)
            # Keep at least two non-temp tables alive outside the dropped
            # schema. Query generation picks from the non-view objects, and a
            # CASCADE that emptied that set would crash it. This mirrors
            # DropTableAction's minimum.
            if (
                len([t for t in exe.db.tables if not t.temp and t.schema is not schema])
                < 2
            ):
                return False
        with schema.lock:
            if schema not in exe.db.schemas:
                return False
            if len(exe.db.schemas) <= 1:
                return False
            exe.execute(f"DROP SCHEMA {schema} CASCADE", http=Http.RANDOM)
            exe.db.schemas.remove(schema)
        untrack_objects_in_schemas(exe, {schema})
        return True


class DropDatabaseCascadeAction(Action):
    """DROP DATABASE .. CASCADE, an atomic multi-object catalog mutation.

    DDL-complexity only, for the same reason as DropSchemaCascadeAction."""

    def applicable(self, exe: Executor) -> bool:
        return exe.db.complexity in (Complexity.DDL, Complexity.DDLOnly)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.dbs) <= 1:
                return False
            db = self.rng.choice(exe.db.dbs)
            # Keep at least two non-temp tables alive outside the dropped
            # database, so query generation always has a non-view object.
            if (
                len([t for t in exe.db.tables if not t.temp and t.schema.db is not db])
                < 2
            ):
                return False
        with db.lock:
            if db not in exe.db.dbs:
                return False
            if len(exe.db.dbs) <= 1:
                return False
            exe.execute(f"DROP DATABASE {db} CASCADE", http=Http.RANDOM)
            exe.db.dbs.remove(db)
        with exe.db.lock:
            dropped = {s for s in exe.db.schemas if s.db is db}
            exe.db.schemas[:] = [s for s in exe.db.schemas if s.db is not db]
        untrack_objects_in_schemas(exe, dropped)
        return True


class CreateTypeAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.types) >= MAX_TYPES:
                return False
            type_id = exe.db.type_id
            exe.db.type_id += 1
            try:
                schema = self.rng.choice(exe.db.schemas)
            except IndexError:
                return False
        with schema.lock:
            if schema not in exe.db.schemas:
                return False
            typ = Type(type_id, schema, self.rng)
            typ.create(exe)
            exe.db.types.append(typ)
        return True


class DropTypeAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
            "cannot be dropped",
            # Another worker (or a CASCADE drop of the schema/database) can
            # drop the type first.
            "does not exist",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.types:
                return False
            typ = self.rng.choice(exe.db.types)
        with typ.lock:
            if typ not in exe.db.types:
                return False
            exe.execute(f"DROP TYPE {typ}", http=Http.RANDOM)
            exe.db.types.remove(typ)
        return True


class CreateNetworkPolicyAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.network_policies) >= MAX_NETWORK_POLICIES:
                return False
            policy_id = exe.db.network_policy_id
            exe.db.network_policy_id += 1
        policy = NetworkPolicy(policy_id, self.rng)
        policy.create(exe)
        with exe.db.lock:
            exe.db.network_policies.append(policy)
        return True


class AlterNetworkPolicyAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.network_policies:
                return False
            policy = self.rng.choice(exe.db.network_policies)
        with policy.lock:
            if policy not in exe.db.network_policies:
                return False
            policy.num_rules = self.rng.randint(1, 3)
            exe.execute(
                f"ALTER NETWORK POLICY {policy} SET ({policy.rules_clause()})",
                http=Http.RANDOM,
            )
        return True


class DropNetworkPolicyAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            # The policy is installed as a default somewhere (should not happen,
            # we never install ours, but be safe).
            "cannot be dropped",
            # Another worker dropped the same policy first. The error carries
            # the raw name ('netpol-N', not the quoted form), so DROP resolves
            # the name correctly; this is a concurrency race, not the ALTER
            # NETWORK POLICY quoted-name bug.
            "unknown network policy",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if not exe.db.network_policies:
                return False
            policy = self.rng.choice(exe.db.network_policies)
        with policy.lock:
            if policy not in exe.db.network_policies:
                return False
            exe.execute(f"DROP NETWORK POLICY {policy}", http=Http.RANDOM)
            exe.db.network_policies.remove(policy)
        return True


class SystemCatalogReadAction(Action):
    """Read a random system-catalog / introspection relation while DDL churns.

    Exercises catalog-read-vs-write consistency and the auto-route path.
    Relations that the `materialize` user cannot read (mz_notices,
    mz_recent_activity_log_redacted) are left out."""

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            # An introspection view over many objects can outrun
            # statement_timeout, never a real bug in a stress run.
            "canceling statement due to statement timeout",
            # Reading a system-catalog relation inside a read transaction that
            # already touched user objects crosses timedomains.
            "in the same timedomain",
            'is not allowed from the "mz_catalog_server" cluster',
        ] + super().errors_to_ignore(exe)

    RELATIONS = [
        "mz_catalog.mz_objects",
        "mz_catalog.mz_columns",
        "mz_catalog.mz_indexes",
        "mz_catalog.mz_sources",
        "mz_catalog.mz_sinks",
        "mz_catalog.mz_materialized_views",
        "mz_catalog.mz_views",
        "mz_catalog.mz_tables",
        "mz_catalog.mz_audit_events",
        "mz_catalog.mz_databases",
        "mz_catalog.mz_schemas",
        "mz_catalog.mz_roles",
        "mz_catalog.mz_clusters",
        "mz_catalog.mz_cluster_replicas",
        "mz_internal.mz_frontiers",
        "mz_internal.mz_hydration_statuses",
        "mz_internal.mz_compute_dependencies",
        "mz_internal.mz_source_statuses",
        "mz_internal.mz_sink_statuses",
        "mz_internal.mz_materialization_lag",
        "mz_internal.mz_wallclock_global_lag_recent_history",
        "mz_internal.mz_cluster_replica_statuses",
        "mz_internal.mz_object_dependencies",
        "mz_internal.mz_object_transitive_dependencies",
        "mz_internal.mz_show_all_objects",
        "mz_internal.mz_comments",
    ]

    def run(self, exe: Executor) -> bool:
        relation = self.rng.choice(self.RELATIONS)
        exe.execute(
            f"SELECT * FROM {relation} LIMIT {self.rng.randint(1, 100)}",
            http=Http.RANDOM,
            fetch=True,
        )
        return True


class ExplainAnalyzeAction(Action):
    """EXPLAIN ANALYZE against a live materialized view or index dataflow.

    Runs generated introspection queries on the active cluster. Racing it
    against drop/replace of the target probes the introspection path."""

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            # The active cluster has more than one replica.
            "log source reads must target a replica",
            "does not exist",
            "not been hydrated",
            "not been materialized",
            # A concurrent DROP/reconfigure of the targeted replica retires the
            # introspection query. No panic in services.log, just a race.
            "target replica failed or was dropped",
            # Introspection over a large dataflow can outrun statement_timeout.
            "canceling statement due to statement timeout",
            # The generated introspection queries can cross timedomains or be
            # routed to mz_catalog_server.
            "in the same timedomain",
            'is not allowed from the "mz_catalog_server" cluster',
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        mvs = [v for v in exe.db.views if v.materialized]
        with exe.db.lock:
            indexes = list(exe.db.indexes)
        candidates: list[tuple[str, DBObject | Index]] = []
        for v in mvs:
            candidates.append(("MATERIALIZED VIEW", v))
        for i in indexes:
            candidates.append(("INDEX", i))
        if not candidates:
            return False
        kind, obj = self.rng.choice(candidates)
        analysis = self.rng.choice(
            ["MEMORY", "CPU", "MEMORY WITH SKEW", "CPU WITH SKEW", "HINTS"]
        )
        with obj.lock:
            if kind == "MATERIALIZED VIEW" and obj not in exe.db.views:
                return False
            if kind == "INDEX" and obj not in exe.db.indexes:
                return False
            exe.execute(
                f"EXPLAIN ANALYZE {analysis} FOR {kind} {obj}",
                http=Http.NO,
                fetch=True,
            )
        return True


class ExplainFilterPushdownAction(Action):
    """EXPLAIN FILTER PUSHDOWN, which inspects durable persist state to compute
    which parts a query's filters would read."""

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        result.extend(
            [
                "in the same timedomain",
                'is not allowed from the "mz_catalog_server" cluster',
                # Scanning persist part stats can outrun statement_timeout.
                "canceling statement due to statement timeout",
            ]
        )
        if exe.db.complexity == Complexity.DDL:
            result.extend(["does not exist"])
        return result

    def run(self, exe: Executor) -> bool:
        mvs = [v for v in exe.db.views if v.materialized]
        if mvs and self.rng.choice([True, False]):
            view = self.rng.choice(mvs)
            with view.lock:
                if view not in exe.db.views:
                    return False
                exe.execute(
                    f"EXPLAIN FILTER PUSHDOWN FOR MATERIALIZED VIEW {view}",
                    http=Http.NO,
                    fetch=True,
                )
        else:
            query = self.generate_select_query(exe, ExprKind.ALL)
            exe.execute(
                f"EXPLAIN FILTER PUSHDOWN FOR {query}", http=Http.NO, fetch=True
            )
        return True


class SourceSinkStallCheckAction(Action):
    def applicable(self, exe: Executor) -> bool:
        return exe.db.scenario not in (
            Scenario.Kill,
            Scenario.ZeroDowntimeDeploy,
            Scenario.BackupRestore,
        )

    def run(self, exe: Executor) -> bool:
        exe.execute(
            "SELECT name, error FROM mz_internal.mz_sink_statuses WHERE status = 'stalled'"
        )
        stalled_sinks = exe.cur.fetchall()
        if stalled_sinks:
            details = "; ".join(f"{name}: {error}" for name, error in stalled_sinks)
            raise ValueError(f"Sinks in stalled state: {details}")

        exe.execute(
            "SELECT name, error FROM mz_internal.mz_source_statuses WHERE status = 'stalled'"
        )
        stalled_sources = exe.cur.fetchall()
        if stalled_sources:
            details = "; ".join(f"{name}: {error}" for name, error in stalled_sources)
            raise ValueError(f"Sources in stalled state: {details}")

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
            ("loadgen_sources", exe.db.loadgen_sources),
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
        (ParameterizedQueryAction, 20),
        # (SQLsmithAction, 30),  # Questionable use
        (
            CopyToS3Action,
            100,
        ),
        (CopyToStdoutAction, 20),
        (ShowAction, 10),
        (SystemCatalogReadAction, 10),
        # TODO: Reenable once EXPLAIN FILTER PUSHDOWN can no longer panic the
        # coordinator when a referenced compute collection is concurrently
        # dropped. sequence_explain_pushdown -> acquire_read_holds().expect(
        # "missing compute collection") at read_policy.rs:389 (normal peeks and
        # EXPLAIN ANALYZE handle the drop gracefully). See SQL-519 /
        # FINDINGS-BUGS.md.
        # (ExplainFilterPushdownAction, 5),
        (SetClusterAction, 1),
        (CommitRollbackAction, 30),
        (ReconnectAction, 1),
        (FlipFlagsAction, 2),
        (SetSessionVariableAction, 2),
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
        # INSERT INTO .. SELECT runs as a read-then-write transaction, it
        # can't run inside a transaction block
        (InsertSelectAction, 10),
        # COPY FROM is oneshot ingestion, it can't run inside a transaction
        (CopyFromS3Action, 10),
        (CommentAction, 5),
        (SetClusterAction, 1),
        (ReconnectAction, 1),
        (FlipFlagsAction, 2),
        (SetSessionVariableAction, 2),
        (DiscardAction, 2),
        # TODO: Reenable when SS-193 and SS-325 are fixed
        # (SourceSinkStallCheckAction, 4),
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
        (CreateOrReplaceViewAction, 4),
        (CreateRoleAction, 2),
        (DropRoleAction, 2),
        (AlterRoleAction, 2),
        (CreateClusterAction, 1),
        (DropClusterAction, 1),
        (AlterClusterSetAction, 3),
        (SwapClusterAction, 10),
        (CreateClusterReplicaAction, 2),
        (DropClusterReplicaAction, 2),
        (SetClusterAction, 1),
        (CreateWebhookSourceAction, 2),
        (DropWebhookSourceAction, 2),
        (CreateKafkaSinkAction, 4),
        (DropKafkaSinkAction, 4),
        (CreateIcebergSinkAction, 4),
        (DropIcebergSinkAction, 4),
        (CreateKafkaSourceAction, 4),
        (DropKafkaSourceAction, 4),
        (CreateLoadGeneratorSourceAction, 4),
        (DropLoadGeneratorSourceAction, 4),
        (CreateMultiLoadGeneratorSourceAction, 2),
        (DropMultiLoadGeneratorSourceAction, 2),
        # TODO: Reenable when https://linear.app/materializeinc/issue/SS-307 is fixed
        # (CreateMySqlSourceAction, 4),
        # (DropMySqlSourceAction, 4),
        (CreatePostgresSourceAction, 4),
        (DropPostgresSourceAction, 4),
        # TODO: Reenable when https://linear.app/materializeinc/issue/SS-290 is fixed
        # (CreateSqlServerSourceAction, 4),
        # (DropSqlServerSourceAction, 4),
        (GrantPrivilegesAction, 4),
        (RevokePrivilegesAction, 1),
        (GrantRoleAction, 2),
        (RevokeRoleAction, 1),
        (AlterOwnerAction, 2),
        (AlterDefaultPrivilegesAction, 2),
        (BroadPrivilegesAction, 2),
        (ShowAction, 4),
        (ValidateConnectionAction, 2),
        # TODO: Reenable once altering a connection that sinks or sources depend
        # on can no longer panic the coordinator. Re-altering a dependent sink's
        # export connection after the txn fails with InvalidAlter, which
        # unwrap_or_terminate turns into a panic (SQL-517). See FINDINGS-BUGS.md
        # ("Coordinator panic re-altering a dependent sink's export
        # connection").
        # (AlterConnectionAction, 2),
        (AlterSecretAction, 2),
        (ReconnectAction, 1),
        (CreateDatabaseAction, 1),
        (DropDatabaseAction, 1),
        # TODO: Reenable once a concurrent DROP DATABASE CASCADE can no longer
        # panic the coordinator. A staged create (e.g. the source executor's
        # CREATE SECRET) whose target database is dropped between staging and
        # finish hits resolve_full_name -> get_database (panicking OrdMap index)
        # in catalog transact_op. Only CASCADE can drop a non-empty database,
        # so this is the precise trigger (SQL-518). See FINDINGS-BUGS.md ("Coordinator
        # panic resolving a name whose database was concurrently dropped").
        # (DropDatabaseCascadeAction, 1),
        (CreateSchemaAction, 1),
        (DropSchemaAction, 1),
        (DropSchemaCascadeAction, 1),
        (CreateTypeAction, 2),
        (DropTypeAction, 2),
        (CreateNetworkPolicyAction, 1),
        # TODO: Reenable once ALTER NETWORK POLICY resolves quoted (e.g.
        # hyphenated) names. It looks the policy up by its quoted display form,
        # so it fails with "unknown network policy" for any name that requires
        # quoting, even though CREATE and DROP work (CLO-143). See FINDINGS-BUGS.md
        # ("ALTER NETWORK POLICY cannot resolve a quoted (hyphenated) name").
        # (AlterNetworkPolicyAction, 1),
        (DropNetworkPolicyAction, 1),
        (RenameSchemaAction, 10),
        (RenameTableAction, 10),
        (RenameViewAction, 10),
        (RenameKafkaSinkAction, 10),
        (RenameIcebergSinkAction, 10),
        (SwapSchemaAction, 10),
        (ReplaceMaterializedViewAction, 20),
        (TransactionIsolationAction, 1),
        (BoundedStalenessReadAction, 2),
        (ReadOnlyTransactionAction, 3),
        (DDLTransactionAction, 2),
        (SystemCatalogReadAction, 4),
        (ExplainAnalyzeAction, 4),
        # TODO: Reenable with EXPLAIN FILTER PUSHDOWN's coordinator panic on a
        # concurrently-dropped compute collection (read_policy.rs:389,
        # SQL-519). See FINDINGS-BUGS.md.
        # (ExplainFilterPushdownAction, 2),
        (FlipFlagsAction, 2),
        # TODO: Reenable when https://linear.app/materializeinc/issue/SQL-405 is fixed.
        # (AlterTableAddColumnAction, 10),
        (AlterIcebergSinkFromAction, 8),
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
