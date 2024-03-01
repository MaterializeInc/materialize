# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import datetime
import json
import random
import time
import urllib.parse
from collections.abc import Callable
from typing import TYPE_CHECKING

import pg8000
import requests
from pg8000.native import identifier

import materialize.parallel_workload.database
from materialize.data_ingest.data_type import NUMBER_TYPES, Text, TextTextMap
from materialize.data_ingest.query_error import QueryError
from materialize.data_ingest.row import Operation
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import minio_blob_uri
from materialize.parallel_workload.database import (
    DB,
    MAX_CLUSTER_REPLICAS,
    MAX_CLUSTERS,
    MAX_DBS,
    MAX_INDEXES,
    MAX_KAFKA_SINKS,
    MAX_KAFKA_SOURCES,
    MAX_MYSQL_SOURCES,
    MAX_POSTGRES_SOURCES,
    MAX_ROLES,
    MAX_ROWS,
    MAX_SCHEMAS,
    MAX_TABLES,
    MAX_VIEWS,
    MAX_WEBHOOK_SOURCES,
    Cluster,
    ClusterReplica,
    Database,
    DBObject,
    Index,
    KafkaSink,
    KafkaSource,
    MySqlSource,
    PostgresSource,
    Role,
    Schema,
    Table,
    View,
    WebhookSource,
)
from materialize.parallel_workload.executor import Executor
from materialize.parallel_workload.settings import Complexity, Scenario
from materialize.sqlsmith import known_errors

if TYPE_CHECKING:
    from materialize.parallel_workload.worker import Worker

# TODO: CASCADE in DROPs, keep track of what will be deleted
class Action:
    rng: random.Random
    composition: Composition | None

    def __init__(self, rng: random.Random, composition: Composition | None):
        self.rng = rng
        self.composition = composition

    def run(self, exe: Executor) -> bool:
        raise NotImplementedError

    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = [
            "permission denied for",
            "must be owner of",
            "network error",  # #21954, remove when fixed when fixed
        ]
        if exe.db.complexity == Complexity.DDL:
            result.extend(
                [
                    "query could not complete",
                    "cached plan must not change result type",
                    "violates not-null constraint",
                    "result exceeds max size of",
                    "unknown catalog item",  # Expected, see #20381
                    "was concurrently dropped",  # role was dropped
                    "unknown cluster",  # cluster was dropped
                    "the transaction's active cluster has been dropped",  # cluster was dropped
                    "was removed",  # dependency was removed, started with moving optimization off main thread, see #24367
                ]
            )
        if exe.db.scenario == Scenario.Cancel:
            result.extend(
                [
                    "canceling statement due to user request",
                ]
            )
        if exe.db.scenario in (
            Scenario.Kill,
            Scenario.TogglePersistTxn,
            Scenario.BackupRestore,
        ):
            result.extend(
                [
                    "network error",
                    "Can't create a connection to host",
                    "Connection refused",
                ]
            )
        if exe.db.scenario in (Scenario.Kill, Scenario.TogglePersistTxn):
            # Expected, see #20465
            result.extend(["unknown catalog item", "unknown schema"])
        if exe.db.scenario == Scenario.Rename:
            result.extend(["unknown schema", "ambiguous reference to schema name"])
        if materialize.parallel_workload.database.NAUGHTY_IDENTIFIERS:
            result.extend(["identifier length exceeds 255 bytes"])
        return result


class FetchAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        if exe.db.complexity == Complexity.DDL:
            result.extend(
                [
                    "does not exist",
                    "subscribe has been terminated because underlying relation",
                ]
            )
        return result

    def run(self, exe: Executor) -> bool:
        obj = self.rng.choice(exe.db.db_objects())
        # See https://github.com/MaterializeInc/materialize/issues/20474
        exe.rollback() if self.rng.choice([True, False]) else exe.commit()
        query = f"DECLARE c CURSOR FOR SUBSCRIBE {obj}"
        exe.execute(query)
        while True:
            rows = self.rng.choice(["ALL", self.rng.randrange(1000)])
            timeout = self.rng.randrange(10)
            query = f"FETCH {rows} c WITH (timeout='{timeout}s')"
            exe.execute(query)
            exe.cur.fetchall()
            if self.rng.choice([True, False]):
                break
        exe.rollback() if self.rng.choice([True, False]) else exe.commit()
        return True


class SelectOneAction(Action):
    def run(self, exe: Executor) -> bool:
        exe.execute("SELECT 1", explainable=True)
        exe.cur.fetchall()
        return True


class SelectAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        result.extend(
            [
                "in the same timedomain",
                'is not allowed from the "mz_introspection" cluster',
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

        if self.rng.choice([True, False]):
            expressions = ", ".join(
                str(column)
                for column in self.rng.sample(
                    all_columns, k=self.rng.randint(1, len(all_columns))
                )
            )
            if self.rng.choice([True, False]):
                column1 = self.rng.choice(all_columns)
                column2 = self.rng.choice(all_columns)
                column3 = self.rng.choice(all_columns)
                fns = ["COUNT"]
                if column1.data_type in NUMBER_TYPES:
                    fns.extend(["SUM", "AVG", "MAX", "MIN"])
                window_fn = self.rng.choice(fns)
                expressions += f", {window_fn}({column1}) OVER (PARTITION BY {column2} ORDER BY {column3})"
        else:
            expressions = "*"

        query = f"SELECT {expressions} FROM {obj_name} "

        if join:
            column2 = self.rng.choice(columns)
            query += f"JOIN {obj2_name} ON {column} = {column2}"

        query += " LIMIT 1"

        exe.execute(query, explainable=True)
        exe.cur.fetchall()
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
                'is not allowed from the "mz_introspection" cluster',
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
                Scenario.TogglePersistTxn,
                Scenario.BackupRestore,
            ):
                raise
        finally:
            self.composition.silent = False

    def run(self, exe: Executor) -> bool:
        if exe.db.fast_startup:
            return False

        while not self.queries:
            self.refill_sqlsmith(exe)
        query = self.queries.pop()
        exe.execute(query, explainable=True)
        exe.cur.fetchall()
        return True


class InsertAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "cannot be run inside a transaction block",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        table = None
        if exe.insert_table is not None:
            for t in exe.db.tables:
                if t.table_id == exe.insert_table:
                    table = t
                    if table.num_rows >= MAX_ROWS:
                        exe.commit() if self.rng.choice(
                            [True, False]
                        ) else exe.rollback()
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
        if self.rng.choice([True, False]):
            returning_exprs = []
            if self.rng.choice([True, False]):
                returning_exprs.append("0")
            elif self.rng.choice([True, False]):
                returning_exprs.append("*")
            else:
                returning_exprs.append(column_names)
            if returning_exprs:
                query += f" RETURNING {', '.join(returning_exprs)}"
        exe.execute(query)
        table.num_rows += len(column_values)
        exe.insert_table = table.table_id
        return True


class SourceInsertAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            sources = [
                source
                for source in exe.db.kafka_sources + exe.db.postgres_sources
                if source.num_rows < MAX_ROWS
            ]
            if not sources:
                return False
            source = self.rng.choice(sources)
        with source.lock:
            if source not in [*exe.db.kafka_sources, *exe.db.postgres_sources]:
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
        return [
            "canceling statement due to statement timeout",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        table = None
        if exe.insert_table is not None:
            for t in exe.db.tables:
                if t.table_id == exe.insert_table:
                    table = t
                    break
        if not table:
            table = self.rng.choice(exe.db.tables)

        column1 = table.columns[0]
        column2 = self.rng.choice(table.columns)
        query = f"UPDATE {table} SET {column2.name(True)} = {column2.value(self.rng, True)} WHERE "
        if column1.data_type == TextTextMap:
            query += f"map_length({column1.name(True)}) = map_length({column1.value(self.rng, True)})"
        else:
            query += f"{column1.name(True)} = {column1.value(self.rng, True)}"
        exe.execute(query)
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
            query += " WHERE true"
            # TODO: Generic expression generator
            for column in table.columns:
                if column.data_type == TextTextMap:
                    query += f" AND map_length({column.name(True)}) = map_length({column.value(self.rng, True)})"
                else:
                    query += (
                        f" AND {column.name(True)} = {column.value(self.rng, True)}"
                    )
        exe.execute(query)
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

        exe.execute(query)
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
        exe.execute(query)
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
            exe.execute(query)
            exe.db.indexes.remove(index)
            return True


class CreateTableAction(Action):
    def run(self, exe: Executor) -> bool:
        if len(exe.db.tables) >= MAX_TABLES:
            return False
        table_id = exe.db.table_id
        exe.db.table_id += 1
        schema = self.rng.choice(exe.db.schemas)
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

            query = f"DROP TABLE {table}"
            exe.execute(query)
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
                    f"ALTER TABLE {old_name} RENAME TO {identifier(table.name())}"
                )
            except:
                table.rename -= 1
                raise
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
                    f"ALTER {'MATERIALIZED VIEW' if view.materialized else 'VIEW'} {old_name} RENAME TO {identifier(view.name())}"
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
            sink = self.rng.choice(exe.db.kafka_sinks)
        with sink.lock:
            if sink not in exe.db.kafka_sinks:
                return False

            old_name = str(sink)
            sink.rename += 1
            try:
                exe.execute(
                    f"ALTER SINK {old_name} RENAME TO {identifier(sink.name())}"
                )
            except:
                sink.rename -= 1
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
            db = self.rng.choice(exe.db.dbs)
        with db.lock:
            # Was dropped while we were acquiring lock
            if db not in exe.db.dbs:
                return False

            query = f"DROP DATABASE {db} RESTRICT"
            exe.execute(query)
            exe.db.dbs.remove(db)
        return True


class CreateSchemaAction(Action):
    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.schemas) >= MAX_SCHEMAS:
                return False
            schema_id = exe.db.schema_id
            exe.db.schema_id += 1
        schema = Schema(self.rng.choice(exe.db.dbs), schema_id)
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
            schema = self.rng.choice(exe.db.schemas)
        with schema.lock:
            # Was dropped while we were acquiring lock
            if schema not in exe.db.schemas:
                return False

            query = f"DROP SCHEMA {schema}"
            exe.execute(query)
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
            schema = self.rng.choice(exe.db.schemas)
        with schema.lock:
            if schema not in exe.db.schemas:
                return False
            old_name = str(schema)
            schema.rename += 1
            try:
                exe.execute(
                    f"ALTER SCHEMA {old_name} RENAME TO {identifier(schema.name())}"
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
            db = self.rng.choice(exe.db.dbs)
            schemas = [
                schema for schema in exe.db.schemas if schema.db.db_id == db.db_id
            ]
            if len(schemas) < 2:
                return False
            schema1, schema2 = self.rng.sample(schemas, 2)
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
                try:
                    exe.cur._c.autocommit = False
                    exe.execute(f"ALTER SCHEMA {schema1} RENAME TO tmp_schema")
                    exe.execute(
                        f"ALTER SCHEMA {schema2} RENAME TO {identifier(schema1.name())}"
                    )
                    exe.execute(
                        f"ALTER SCHEMA tmp_schema RENAME TO {identifier(schema1.name())}"
                    )
                    exe.commit()
                finally:
                    exe.cur._c.autocommit = True
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


class CreateViewAction(Action):
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
        schema = self.rng.choice(exe.db.schemas)
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
            exe.execute(query)
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
                exe.execute(query)
            except QueryError as e:
                # expected, see #20465
                if (
                    exe.db.scenario not in (Scenario.Kill, Scenario.TogglePersistTxn)
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
            introspection_interval=self.rng.choice(["0", "1s", "10s"]),
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
            cluster = self.rng.choice(exe.db.clusters)
        with cluster.lock:
            # Was dropped while we were acquiring lock
            if cluster not in exe.db.clusters:
                return False

            query = f"DROP CLUSTER {cluster}"
            try:
                exe.execute(query)
            except QueryError as e:
                # expected, see #20465
                if (
                    exe.db.scenario not in (Scenario.Kill, Scenario.TogglePersistTxn)
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
            cluster1, cluster2 = self.rng.sample(exe.db.clusters, 2)
        with cluster1.lock, cluster2.lock:
            if cluster1 not in exe.db.clusters:
                return False
            if cluster2 not in exe.db.clusters:
                return False

            if self.rng.choice([True, False]):
                exe.execute(
                    f"ALTER CLUSTER {cluster1} SWAP WITH {identifier(cluster2.name())}"
                )
            else:
                try:
                    exe.cur._c.autocommit = False
                    exe.execute(f"ALTER SCHEMA {cluster1} RENAME TO tmp_cluster")
                    exe.execute(
                        f"ALTER SCHEMA {cluster2} RENAME TO {identifier(cluster1.name())}"
                    )
                    exe.execute(
                        f"ALTER SCHEMA tmp_cluster RENAME TO {identifier(cluster1.name())}"
                    )
                    exe.commit()
                finally:
                    exe.cur._c.autocommit = True
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
            cluster = self.rng.choice(exe.db.clusters)
        query = f"SET CLUSTER = {cluster}"
        exe.execute(query)
        return True


class CreateClusterReplicaAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = [
            "cannot create more than one replica of a cluster containing sources or sinks",
        ] + super().errors_to_ignore(exe)

        return result

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            # Keep cluster 0 with 1 replica for sources/sinks
            unmanaged_clusters = [c for c in exe.db.clusters[1:] if not c.managed]
            if not unmanaged_clusters:
                return False
            cluster = self.rng.choice(unmanaged_clusters)
            if len(cluster.replicas) >= MAX_CLUSTER_REPLICAS:
                return False
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
                exe.execute(query)
            except QueryError as e:
                # expected, see #20465
                if (
                    exe.db.scenario not in (Scenario.Kill, Scenario.TogglePersistTxn)
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
            exe.execute(query)
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
            exe.execute(query)
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
        autocommit = exe.cur._c.autocommit
        host = exe.db.host
        port = exe.db.ports["materialized"]
        with exe.db.lock:
            if self.random_role and exe.db.roles:
                user = self.rng.choice(
                    ["materialize", str(self.rng.choice(exe.db.roles))]
                )
            else:
                user = "materialize"
            conn = exe.cur._c

        try:
            exe.cur.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass

        NUM_ATTEMPTS = 20
        for i in range(NUM_ATTEMPTS):
            try:
                conn = pg8000.connect(
                    host=host, port=port, user=user, database="materialize"
                )
                conn.autocommit = autocommit
                cur = conn.cursor()
                exe.cur = cur
                exe.set_isolation("SERIALIZABLE")
                cur.execute("SELECT pg_backend_pid()")
                exe.pg_pid = cur.fetchall()[0][0]
            except Exception as e:
                if i < NUM_ATTEMPTS - 1 and (
                    "network error" in str(e)
                    or "Can't create a connection to host" in str(e)
                    or "Connection refused" in str(e)
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
            f"SELECT pg_cancel_backend({pid})", extra_info=f"Canceling {worker}"
        )
        # Sleep less often to work around #22228 / #2392
        time.sleep(self.rng.uniform(1, 10))
        return True


class KillAction(Action):
    def __init__(
        self,
        rng: random.Random,
        composition: Composition | None,
        catalog_store: str,
        sanity_restart: bool,
        system_param_fn: Callable[[dict[str, str]], dict[str, str]] = lambda x: x,
    ):
        super().__init__(rng, composition)
        self.system_param_fn = system_param_fn
        self.system_parameters = {}
        self.catalog_store = catalog_store
        self.sanity_restart = sanity_restart

    def run(self, exe: Executor) -> bool:
        assert self.composition
        self.composition.kill("materialized")
        # Otherwise getting failure on "up" locally
        time.sleep(1)
        self.system_parameters = self.system_param_fn(self.system_parameters)
        with self.composition.override(
            Materialized(
                restart="on-failure",
                external_minio="toxiproxy",
                external_cockroach="toxiproxy",
                ports=["6975:6875", "6976:6876", "6977:6877"],
                catalog_store=self.catalog_store,
                sanity_restart=self.sanity_restart,
                additional_system_parameter_defaults=self.system_parameters,
            )
        ):
            self.composition.up("materialized", detach=True)
        time.sleep(self.rng.uniform(120, 240))
        return True


# TODO: Don't restore immediately, keep copy Database objects
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
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        if exe.db.scenario in (Scenario.Kill, Scenario.TogglePersistTxn):
            result.extend(
                ["cannot create source in cluster with more than one replica"]
            )
        return result

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.webhook_sources) >= MAX_WEBHOOK_SOURCES:
                return False
            webhook_source_id = exe.db.webhook_source_id
            exe.db.webhook_source_id += 1
            potential_clusters = [c for c in exe.db.clusters if len(c.replicas) == 1]
            cluster = self.rng.choice(potential_clusters)
            schema = self.rng.choice(exe.db.schemas)
        with schema.lock, cluster.lock:
            if schema not in exe.db.schemas:
                return False
            if cluster not in exe.db.clusters or len(cluster.replicas) != 1:
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
            source = self.rng.choice(exe.db.webhook_sources)
        with source.lock:
            # Was dropped while we were acquiring lock
            if source not in exe.db.webhook_sources:
                return False

            query = f"DROP SOURCE {source}"
            exe.execute(query)
            exe.db.webhook_sources.remove(source)
        return True


class CreateKafkaSourceAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        if exe.db.scenario in (Scenario.Kill, Scenario.TogglePersistTxn):
            result.extend(
                ["cannot create source in cluster with more than one replica"]
            )
        return result

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.kafka_sources) >= MAX_KAFKA_SOURCES:
                return False
            source_id = exe.db.kafka_source_id
            exe.db.kafka_source_id += 1
            potential_clusters = [c for c in exe.db.clusters if len(c.replicas) == 1]
            cluster = self.rng.choice(potential_clusters)
            schema = self.rng.choice(exe.db.schemas)
        with schema.lock, cluster.lock:
            if schema not in exe.db.schemas:
                return False
            if cluster not in exe.db.clusters or len(cluster.replicas) != 1:
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
                if exe.db.scenario not in (Scenario.Kill, Scenario.TogglePersistTxn):
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
            source = self.rng.choice(exe.db.kafka_sources)
        with source.lock:
            # Was dropped while we were acquiring lock
            if source not in exe.db.kafka_sources:
                return False

            query = f"DROP SOURCE {source}"
            exe.execute(query)
            exe.db.kafka_sources.remove(source)
        return True


class CreateMySqlSourceAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        if exe.db.scenario in (Scenario.Kill, Scenario.TogglePersistTxn):
            result.extend(
                ["cannot create source in cluster with more than one replica"]
            )
        return result

    def run(self, exe: Executor) -> bool:
        # TODO: Reenable when #22770 is fixed
        if exe.db.scenario == Scenario.BackupRestore:
            return False

        with exe.db.lock:
            if len(exe.db.mysql_sources) >= MAX_MYSQL_SOURCES:
                return False
            source_id = exe.db.mysql_source_id
            exe.db.mysql_source_id += 1
            potential_clusters = [c for c in exe.db.clusters if len(c.replicas) == 1]
            schema = self.rng.choice(exe.db.schemas)
            cluster = self.rng.choice(potential_clusters)
        with schema.lock, cluster.lock:
            if schema not in exe.db.schemas:
                return False
            if cluster not in exe.db.clusters or len(cluster.replicas) != 1:
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
                if exe.db.scenario not in (Scenario.Kill, Scenario.TogglePersistTxn):
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
            source = self.rng.choice(exe.db.mysql_sources)
        with source.lock:
            # Was dropped while we were acquiring lock
            if source not in exe.db.mysql_sources:
                return False

            query = f"DROP SOURCE {source.executor.source}"
            exe.execute(query)
            exe.db.mysql_sources.remove(source)
        return True


class CreatePostgresSourceAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        if exe.db.scenario in (Scenario.Kill, Scenario.TogglePersistTxn):
            result.extend(
                ["cannot create source in cluster with more than one replica"]
            )
        return result

    def run(self, exe: Executor) -> bool:
        # TODO: Reenable when #22770 is fixed
        if exe.db.scenario == Scenario.BackupRestore:
            return False

        with exe.db.lock:
            if len(exe.db.postgres_sources) >= MAX_POSTGRES_SOURCES:
                return False
            source_id = exe.db.postgres_source_id
            exe.db.postgres_source_id += 1
            potential_clusters = [c for c in exe.db.clusters if len(c.replicas) == 1]
            schema = self.rng.choice(exe.db.schemas)
            cluster = self.rng.choice(potential_clusters)
        with schema.lock, cluster.lock:
            if schema not in exe.db.schemas:
                return False
            if cluster not in exe.db.clusters or len(cluster.replicas) != 1:
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
                if exe.db.scenario not in (Scenario.Kill, Scenario.TogglePersistTxn):
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
            source = self.rng.choice(exe.db.postgres_sources)
        with source.lock:
            # Was dropped while we were acquiring lock
            if source not in exe.db.postgres_sources:
                return False

            query = f"DROP SOURCE {source.executor.source}"
            exe.execute(query)
            exe.db.postgres_sources.remove(source)
        return True


class CreateKafkaSinkAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            # Another replica can be created in parallel
            "cannot create sink in cluster with more than one replica",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> bool:
        with exe.db.lock:
            if len(exe.db.kafka_sinks) >= MAX_KAFKA_SINKS:
                return False
            sink_id = exe.db.kafka_sink_id
            exe.db.kafka_sink_id += 1
            potential_clusters = [c for c in exe.db.clusters if len(c.replicas) == 1]
            cluster = self.rng.choice(potential_clusters)
            schema = self.rng.choice(exe.db.schemas)
        with schema.lock, cluster.lock:
            if schema not in exe.db.schemas:
                return False
            if cluster not in exe.db.clusters or len(cluster.replicas) != 1:
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
            sink = self.rng.choice(exe.db.kafka_sinks)
        with sink.lock:
            # Was dropped while we were acquiring lock
            if sink not in exe.db.kafka_sinks:
                return False

            query = f"DROP SINK {sink}"
            exe.execute(query)
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

            url = f"http://{exe.db.host}:{exe.db.ports['http']}/api/webhook/{urllib.parse.quote(source.schema.db.name(), safe='')}/{urllib.parse.quote(source.schema.name(), safe='')}/{urllib.parse.quote(source.name(), safe='')}"

            payload = source.body_format.to_data_type().random_value(self.rng)

            header_fields = source.explicit_include_headers
            if source.include_headers:
                header_fields.extend(["x-event-type", "signature", "x-mz-api-key"])

            headers = {
                header: f"{datetime.datetime.now()}"
                if header == "timestamp"
                else f'"{Text.random_value(self.rng)}"'.encode()
                for header in self.rng.sample(header_fields, len(header_fields))
            }

            headers_strs = [f"{key}: {value}" for key, value in headers.items()]
            log = f"POST {url} Headers: {', '.join(headers_strs)} Body: {payload.encode('utf-8')}"
            exe.log(log)
            try:
                source.num_rows += 1
                result = requests.post(
                    url, data=payload.encode("utf-8"), headers=headers
                )
                if result.status_code != 200:
                    raise QueryError(f"{result.status_code}: {result.text}", log)
            except (requests.exceptions.ConnectionError):
                # Expected when Mz is killed
                if exe.db.scenario not in (
                    Scenario.Kill,
                    Scenario.TogglePersistTxn,
                    Scenario.BackupRestore,
                ):
                    raise
            except QueryError as e:
                # expected, see #20465
                if exe.db.scenario not in (
                    Scenario.Kill,
                    Scenario.TogglePersistTxn,
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
        (SQLsmithAction, 30),
        # (SetClusterAction, 1),  # SET cluster cannot be called in an active transaction
        (CommitRollbackAction, 30),
        (ReconnectAction, 1),
    ],
    autocommit=False,
)

fetch_action_list = ActionList(
    [
        (FetchAction, 30),
        # (SetClusterAction, 1),  # SET cluster cannot be called in an active transaction
        (ReconnectAction, 1),
    ],
    autocommit=False,
)

write_action_list = ActionList(
    [
        (InsertAction, 50),
        (SelectOneAction, 1),  # can be mixed with writes
        # (SetClusterAction, 1),  # SET cluster cannot be called in an active transaction
        (HttpPostAction, 50),
        (CommitRollbackAction, 10),
        (ReconnectAction, 1),
        (SourceInsertAction, 50),
    ],
    autocommit=False,
)

dml_nontrans_action_list = ActionList(
    [
        (DeleteAction, 10),
        (UpdateAction, 10),
        (CommentAction, 5),
        (SetClusterAction, 1),
        (ReconnectAction, 1),
        # (TransactionIsolationAction, 1),
    ],
    autocommit=True,  # deletes can't be inside of transactions
)

ddl_action_list = ActionList(
    [
        (CreateIndexAction, 2),
        (DropIndexAction, 1),
        (CreateTableAction, 2),
        (DropTableAction, 1),
        (CreateViewAction, 8),
        (DropViewAction, 4),
        (CreateRoleAction, 2),
        (DropRoleAction, 1),
        (CreateClusterAction, 2),
        (DropClusterAction, 1),
        (SwapClusterAction, 10),
        (CreateClusterReplicaAction, 4),
        (DropClusterReplicaAction, 2),
        (SetClusterAction, 1),
        (CreateWebhookSourceAction, 2),
        (DropWebhookSourceAction, 1),
        (CreateKafkaSinkAction, 4),
        (DropKafkaSinkAction, 1),
        (CreateKafkaSourceAction, 4),
        (DropKafkaSourceAction, 1),
        (CreateMySqlSourceAction, 4),
        (DropMySqlSourceAction, 1),
        (CreatePostgresSourceAction, 4),
        (DropPostgresSourceAction, 1),
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
