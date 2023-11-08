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
from typing import TYPE_CHECKING

import pg8000
import requests
from pg8000.native import identifier

import materialize.parallel_workload.database
from materialize.data_ingest.data_type import NUMBER_TYPES, Text, TextTextMap
from materialize.data_ingest.query_error import QueryError
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.minio import MINIO_BLOB_URI
from materialize.parallel_workload.database import (
    DB,
    MAX_CLUSTER_REPLICAS,
    MAX_CLUSTERS,
    MAX_DBS,
    MAX_KAFKA_SINKS,
    MAX_KAFKA_SOURCES,
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
    KafkaSink,
    KafkaSource,
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

    def run(self, exe: Executor) -> None:
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
                ]
            )
        if exe.db.scenario == Scenario.Cancel:
            result.extend(
                [
                    "canceling statement due to user request",
                ]
            )
        if exe.db.scenario in (Scenario.Kill, Scenario.BackupRestore):
            result.extend(
                [
                    "network error",
                    "Can't create a connection to host",
                    "Connection refused",
                ]
            )
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
                ]
            )
        return result

    def run(self, exe: Executor) -> None:
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


class SelectAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        if exe.db.complexity in (Complexity.DML, Complexity.DDL):
            result.extend(
                [
                    "in the same timedomain",
                ]
            )
        if exe.db.complexity == Complexity.DDL:
            result.extend(
                [
                    "does not exist",
                ]
            )
        return result

    def run(self, exe: Executor) -> None:
        obj = self.rng.choice(exe.db.db_objects())
        column = self.rng.choice(obj.columns)
        obj2 = self.rng.choice(exe.db.db_objects())
        obj_name = str(obj)
        obj2_name = str(obj2)
        columns = [c for c in obj2.columns if c.data_type == column.data_type]

        if obj_name != obj2_name and columns:
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

        if obj_name != obj2_name and columns:
            column2 = self.rng.choice(columns)
            query += f"JOIN {obj2_name} ON "
            if column.data_type == TextTextMap:
                query += f"map_length({column}) = map_length({column2})"
            else:
                query += f"{column} = {column2}"

        query += " LIMIT 1"

        exe.execute(query, explainable=True)
        exe.cur.fetchall()


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

        if exe.db.complexity in (Complexity.DML, Complexity.DDL):
            result.extend(
                [
                    "in the same timedomain",
                ]
            )
        if exe.db.complexity == Complexity.DDL:
            result.extend(
                [
                    "does not exist",
                ]
            )
        return result

    def run(self, exe: Executor) -> None:
        if not self.queries:
            with exe.db.lock:
                self.composition.silent = True
                try:
                    result = self.composition.run(
                        "sqlsmith",
                        "--max-joins=1",
                        "--target=host=materialized port=6875 dbname=materialize user=materialize",
                        "--read-state",
                        "--dry-run",
                        "--max-queries=100",
                        stdin=exe.db.sqlsmith_state,
                        capture=True,
                        capture_stderr=True,
                        rm=True,
                    )
                    data = json.loads(result.stdout)
                    self.queries.extend(data["queries"])
                except:
                    if exe.db.scenario not in (Scenario.Kill, Scenario.BackupRestore):
                        raise
                    else:
                        return
                finally:
                    self.composition.silent = False

        query = self.queries.pop()
        exe.execute(query, explainable=True)
        exe.cur.fetchall()


class InsertAction(Action):
    def run(self, exe: Executor) -> None:
        table = None
        if exe.insert_table != None:
            for t in exe.db.tables:
                if t.table_id == exe.insert_table:
                    table = t
                    break
            else:
                exe.commit() if self.rng.choice([True, False]) else exe.rollback()
        if not table:
            table = self.rng.choice(
                [table for table in exe.db.tables if table.num_rows <= MAX_ROWS]
            )

        column_names = ", ".join(column.name(True) for column in table.columns)
        column_values = ", ".join(
            column.value(self.rng, True) for column in table.columns
        )
        query = f"INSERT INTO {table} ({column_names}) VALUES ({column_values})"
        exe.execute(query)
        exe.insert_table = table.table_id
        table.num_rows += 1


class SourceInsertAction(Action):
    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            sources = exe.db.kafka_sources + exe.db.postgres_sources
            if not sources:
                return
            source = self.rng.choice(sources)
        with source.lock:
            transaction = next(source.generator)
            source.num_rows += sum(
                [len(row_list.rows) for row_list in transaction.row_lists]
            )
            source.executor.run(transaction)


class UpdateAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "canceling statement due to statement timeout",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> None:
        table = None
        if exe.insert_table != None:
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


class DeleteAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "canceling statement due to statement timeout",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> None:
        table = self.rng.choice(exe.db.tables)
        query = f"DELETE FROM {table}"
        if self.rng.random() < 0.95:
            column = self.rng.choice(table.columns)
            query += " WHERE "
            # TODO: Generic expression generator
            if column.data_type == TextTextMap:
                query += f"map_length({column.name(True)}) = map_length({column.value(self.rng, True)})"
            else:
                query += f"{column.name(True)} = {column.value(self.rng, True)}"
            exe.execute(query)
        else:
            exe.execute(query)
            # Only after a commit we can be sure that the table is empty again,
            # so for now have to trigger them manually here.
            if self.rng.choice([True, False]):
                exe.commit()
                with exe.db.lock:
                    table.num_rows = 0


class CommentAction(Action):
    def run(self, exe: Executor) -> None:
        table = self.rng.choice(exe.db.tables)

        if self.rng.choice([True, False]):
            column = self.rng.choice(table.columns)
            query = f"COMMENT ON COLUMN {column} IS '{Text.random_value(self.rng)}'"
        else:
            query = f"COMMENT ON TABLE {table} IS '{Text.random_value(self.rng)}'"

        exe.execute(query)


class CreateIndexAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "already exists",  # TODO: Investigate
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> None:
        tables_views: list[DBObject] = [*exe.db.tables, *exe.db.views]
        table = self.rng.choice(tables_views)
        columns = self.rng.sample(table.columns, len(table.columns))
        columns_str = "_".join(column.name() for column in columns)
        # columns_str may exceed 255 characters, so it is converted to a positive number with hash
        index_name = f"idx_{table.name()}_{abs(hash(columns_str))}"
        index_elems = []
        for column in columns:
            order = self.rng.choice(["ASC", "DESC"])
            index_elems.append(f"{column.name(True)} {order}")
        index_str = ", ".join(index_elems)
        query = f"CREATE INDEX {identifier(index_name)} ON {table} ({index_str})"
        exe.execute(query)
        with exe.db.lock:
            exe.db.indexes.add(index_name)


class DropIndexAction(Action):
    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if not exe.db.indexes:
                return
            index_name = self.rng.choice(list(exe.db.indexes))
            query = f"DROP INDEX {identifier(index_name)}"
            try:
                exe.execute(query)
            except QueryError as e:
                # expected, see #20465
                if exe.db.scenario != Scenario.Kill or (
                    "unknown catalog item" not in e.msg
                    and "unknown schema" not in e.msg
                ):
                    raise e
            exe.db.indexes.remove(index_name)


class CreateTableAction(Action):
    def run(self, exe: Executor) -> None:
        if len(exe.db.tables) > MAX_TABLES:
            return
        table_id = exe.db.table_id
        exe.db.table_id += 1
        table = Table(self.rng, table_id, self.rng.choice(exe.db.schemas))
        table.create(exe)
        exe.db.tables.append(table)


class DropTableAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if len(exe.db.tables) <= 2:
                return
            table = self.rng.choice(exe.db.tables)
            query = f"DROP TABLE {table}"
            try:
                exe.execute(query)
            except QueryError as e:
                # expected, see #20465
                if exe.db.scenario != Scenario.Kill or (
                    "unknown catalog item" not in e.msg
                    and "unknown schema" not in e.msg
                ):
                    raise e
            exe.db.tables.remove(table)


class RenameTableAction(Action):
    def run(self, exe: Executor) -> None:
        if exe.db.scenario != Scenario.Rename:
            return
        with exe.db.lock:
            if not exe.db.tables:
                return
            table = self.rng.choice(exe.db.tables)
            old_name = str(table)
            table.rename += 1
            try:
                exe.execute(
                    f"ALTER TABLE {old_name} RENAME TO {identifier(table.name())}"
                )
            except:
                table.rename -= 1
                raise


class RenameViewAction(Action):
    def run(self, exe: Executor) -> None:
        if exe.db.scenario != Scenario.Rename:
            return
        with exe.db.lock:
            if not exe.db.views:
                return
            view = self.rng.choice(exe.db.views)
            old_name = str(view)
            view.rename += 1
            try:
                exe.execute(
                    f"ALTER {'MATERIALIZED VIEW' if view.materialized else 'VIEW'} {old_name} RENAME TO {identifier(view.name())}"
                )
            except:
                view.rename -= 1
                raise


class RenameSinkAction(Action):
    def run(self, exe: Executor) -> None:
        if exe.db.scenario != Scenario.Rename:
            return
        with exe.db.lock:
            if not exe.db.kafka_sinks:
                return
            sink = self.rng.choice(exe.db.kafka_sinks)
            old_name = str(sink)
            sink.rename += 1
            try:
                exe.execute(
                    f"ALTER SINK {old_name} RENAME TO {identifier(sink.name())}"
                )
            except:
                sink.rename -= 1
                raise


class CreateDatabaseAction(Action):
    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if len(exe.db.dbs) > MAX_DBS:
                return
            db_id = exe.db.db_id
            exe.db.db_id += 1
        db = DB(exe.db.seed, db_id)
        db.create(exe)
        exe.db.dbs.append(db)


class DropDatabaseAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "cannot be dropped with RESTRICT while it contains schemas",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if len(exe.db.dbs) <= 1:
                return
            db_id = self.rng.randrange(len(exe.db.dbs))
            db = exe.db.dbs[db_id]
            query = f"DROP DATABASE {db} RESTRICT"
            exe.execute(query)
            del exe.db.dbs[db_id]


class CreateSchemaAction(Action):
    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if len(exe.db.schemas) > MAX_SCHEMAS:
                return
            schema_id = exe.db.schema_id
            exe.db.schema_id += 1
            schema = Schema(self.rng.choice(exe.db.dbs), schema_id)
            schema.create(exe)
            exe.db.schemas.append(schema)


class DropSchemaAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "cannot be dropped without CASCADE while it contains objects",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if len(exe.db.schemas) <= 1:
                return
            schema_id = self.rng.randrange(len(exe.db.schemas))
            schema = exe.db.schemas[schema_id]
            query = f"DROP SCHEMA {schema}"
            try:
                exe.execute(query)
            except QueryError as e:
                # expected, see #20465
                if exe.db.scenario != Scenario.Kill or "unknown schema" not in e.msg:
                    raise e
            del exe.db.schemas[schema_id]


class RenameSchemaAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "ambiguous reference to schema named"  # see https://github.com/MaterializeInc/materialize/pull/22551#pullrequestreview-1691876923
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> None:
        if exe.db.scenario != Scenario.Rename:
            return
        with exe.db.lock:
            schema = self.rng.choice(exe.db.schemas)
            old_name = str(schema)
            schema.rename += 1
            try:
                exe.execute(
                    f"ALTER SCHEMA {old_name} RENAME TO {identifier(schema.name())}"
                )
            except:
                schema.rename -= 1
                raise


class SwapSchemaAction(Action):
    def run(self, exe: Executor) -> None:
        if exe.db.scenario != Scenario.Rename:
            return
        with exe.db.lock:
            db = self.rng.choice(exe.db.dbs)
            schemas = [
                schema for schema in exe.db.schemas if schema.db.db_id == db.db_id
            ]
            if len(schemas) < 2:
                return
            schema1, schema2 = self.rng.sample(schemas, 2)
            exe.execute(
                f"ALTER SCHEMA {schema1} SWAP WITH {identifier(schema2.name())}"
            )
            schema1.schema_id, schema2.schema_id = schema2.schema_id, schema1.schema_id
            schema1.rename, schema2.rename = schema2.rename, schema1.rename


class TransactionIsolationAction(Action):
    def run(self, exe: Executor) -> None:
        level = self.rng.choice(["SERIALIZABLE", "STRICT SERIALIZABLE"])
        exe.set_isolation(level)


class CommitRollbackAction(Action):
    def run(self, exe: Executor) -> None:
        if self.rng.choice([True, False]):
            exe.commit()
        else:
            exe.rollback()


class CreateViewAction(Action):
    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if len(exe.db.views) > MAX_VIEWS:
                return
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
        view = View(
            self.rng,
            view_id,
            base_object,
            base_object2,
            self.rng.choice(exe.db.schemas),
        )
        view.create(exe)
        exe.db.views.append(view)


class DropViewAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if not exe.db.views:
                return
            view_id = self.rng.randrange(len(exe.db.views))
            view = exe.db.views[view_id]
            if view.materialized:
                query = f"DROP MATERIALIZED VIEW {view}"
            else:
                query = f"DROP VIEW {view}"
            try:
                exe.execute(query)
            except QueryError as e:
                # expected, see #20465
                if exe.db.scenario != Scenario.Kill or (
                    "unknown catalog item" not in e.msg
                    and "unknown schema" not in e.msg
                ):
                    raise e
            del exe.db.views[view_id]


class CreateRoleAction(Action):
    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if len(exe.db.roles) > MAX_ROLES:
                return
            role_id = exe.db.role_id
            exe.db.role_id += 1
        role = Role(role_id)
        role.create(exe)
        exe.db.roles.append(role)


class DropRoleAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "cannot be dropped because some objects depend on it",
            "current role cannot be dropped",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if not exe.db.roles:
                return
            role = self.rng.choice(exe.db.roles)
            query = f"DROP ROLE {role}"
            try:
                exe.execute(query)
            except QueryError as e:
                # expected, see #20465
                if exe.db.scenario != Scenario.Kill or "unknown role" not in e.msg:
                    raise e
            exe.db.roles.remove(role)


class CreateClusterAction(Action):
    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if len(exe.db.clusters) > MAX_CLUSTERS:
                return
            cluster_id = exe.db.cluster_id
            exe.db.cluster_id += 1
        cluster = Cluster(
            cluster_id,
            managed=self.rng.choice([True, False]),
            size=self.rng.choice(["1", "2", "4"]),
            replication_factor=self.rng.choice([1, 2, 4, 5]),
            introspection_interval=self.rng.choice(["0", "1s", "10s"]),
        )
        cluster.create(exe)
        exe.db.clusters.append(cluster)


class DropClusterAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            # cannot drop cluster "..." because other objects depend on it
            "because other objects depend on it",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if len(exe.db.clusters) <= 1:
                return
            # Keep cluster 0 with 1 replica for sources/sinks
            cluster_id = self.rng.randrange(1, len(exe.db.clusters))
            cluster = exe.db.clusters[cluster_id]
            query = f"DROP CLUSTER {cluster}"
            try:
                exe.execute(query)
            except QueryError as e:
                # expected, see #20465
                if exe.db.scenario != Scenario.Kill or "unknown cluster" not in e.msg:
                    raise e
            del exe.db.clusters[cluster_id]


class SwapClusterAction(Action):
    def run(self, exe: Executor) -> None:
        if exe.db.scenario != Scenario.Rename:
            return
        with exe.db.lock:
            if len(exe.db.clusters) < 2:
                return
            cluster1, cluster2 = self.rng.sample(exe.db.clusters, 2)
            exe.execute(
                f"ALTER CLUSTER {cluster1} SWAP WITH {identifier(cluster2.name())}"
            )
            cluster1.cluster_id, cluster2.cluster_id = (
                cluster2.cluster_id,
                cluster1.cluster_id,
            )
            cluster1.rename, cluster2.rename = cluster2.rename, cluster1.rename


class SetClusterAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "SET cluster cannot be called in an active transaction",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if not exe.db.clusters:
                return
            cluster = self.rng.choice(exe.db.clusters)
        query = f"SET CLUSTER = {cluster}"
        exe.execute(query)


class CreateClusterReplicaAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = [
            "cannot create more than one replica of a cluster containing sources or sinks",
            # Can happen with reduced locking
            "cannot create multiple replicas named",
        ] + super().errors_to_ignore(exe)

        return result

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            # Keep cluster 0 with 1 replica for sources/sinks
            unmanaged_clusters = [c for c in exe.db.clusters[1:] if not c.managed]
            if not unmanaged_clusters:
                return
            cluster = self.rng.choice(unmanaged_clusters)
            if len(cluster.replicas) > MAX_CLUSTER_REPLICAS:
                return
            replica = ClusterReplica(
                cluster.replica_id,
                size=self.rng.choice(["1", "2", "4"]),
                cluster=cluster,
            )
            cluster.replica_id += 1
            try:
                replica.create(exe)
                cluster.replicas.append(replica)
            except:
                cluster.replica_id -= 1
                raise


class DropClusterReplicaAction(Action):
    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            # Keep cluster 0 with 1 replica for sources/sinks
            unmanaged_clusters = [c for c in exe.db.clusters[1:] if not c.managed]
            if not unmanaged_clusters:
                return
            cluster = self.rng.choice(unmanaged_clusters)
            # Avoid "has no replicas available to service request" error
            if len(cluster.replicas) <= 1:
                return
            replica = self.rng.choice(cluster.replicas)
            query = f"DROP CLUSTER REPLICA {cluster}.{replica}"
            try:
                exe.execute(query)
            except QueryError as e:
                # expected, see #20465
                if (
                    exe.db.scenario != Scenario.Kill
                    or "has no CLUSTER REPLICA named" not in e.msg
                ):
                    raise e
            cluster.replicas.remove(replica)


class GrantPrivilegesAction(Action):
    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if not exe.db.roles:
                return
            role = self.rng.choice(exe.db.roles)
        privilege = self.rng.choice(["SELECT", "INSERT", "UPDATE", "ALL"])
        tables_views: list[DBObject] = [*exe.db.tables, *exe.db.views]
        table = self.rng.choice(tables_views)
        query = f"GRANT {privilege} ON {table} TO {role}"
        exe.execute(query)


class RevokePrivilegesAction(Action):
    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if not exe.db.roles:
                return
            role = self.rng.choice(exe.db.roles)
        privilege = self.rng.choice(["SELECT", "INSERT", "UPDATE", "ALL"])
        tables_views: list[DBObject] = [*exe.db.tables, *exe.db.views]
        table = self.rng.choice(tables_views)
        query = f"REVOKE {privilege} ON {table} FROM {role}"
        exe.execute(query)


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

    def run(self, exe: Executor) -> None:
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

    def run(self, exe: Executor) -> None:
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


class KillAction(Action):
    def __init__(
        self,
        rng: random.Random,
        composition: Composition | None,
    ):
        super().__init__(rng, composition)

    def run(self, exe: Executor) -> None:
        assert self.composition
        self.composition.kill("materialized")
        # Otherwise getting failure on "up" locally
        time.sleep(1)
        self.composition.up("materialized", detach=True)
        time.sleep(self.rng.uniform(120, 240))


# TODO: Don't restore immediately, keep copy Database objects
class BackupRestoreAction(Action):
    composition: Composition
    db: Database
    num: int

    def __init__(
        self, rng: random.Random, composition: Composition | None, db: Database
    ) -> None:
        super().__init__(rng, composition)
        self.db = db
        self.num = 0
        assert self.composition

    def run(self, exe: Executor) -> None:
        self.num += 1
        time.sleep(self.rng.uniform(10, 240))
        self.db.lock.acquire()

        try:
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
                f"--blob-uri={MINIO_BLOB_URI}",
                "--consensus-uri=postgres://root@cockroach:26257?options=--search_path=consensus",
            )
            self.composition.up("materialized")

        finally:
            self.db.lock.release()


class CreateWebhookSourceAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        if exe.db.scenario == Scenario.Kill:
            result.extend(
                ["cannot create source in cluster with more than one replica"]
            )
        return result

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if len(exe.db.webhook_sources) > MAX_WEBHOOK_SOURCES:
                return
            webhook_source_id = exe.db.webhook_source_id
            exe.db.webhook_source_id += 1
            potential_clusters = [c for c in exe.db.clusters if len(c.replicas) == 1]
            cluster = self.rng.choice(potential_clusters)
            schema = self.rng.choice(exe.db.schemas)
            source = WebhookSource(webhook_source_id, cluster, schema, self.rng)
            source.create(exe)
            exe.db.webhook_sources.append(source)


class DropWebhookSourceAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if not exe.db.webhook_sources:
                return
            source_id = self.rng.randrange(len(exe.db.webhook_sources))
            source = exe.db.webhook_sources[source_id]
            query = f"DROP SOURCE {source}"
            try:
                exe.execute(query)
            except QueryError as e:
                # expected, see #20465
                if exe.db.scenario != Scenario.Kill or (
                    "unknown catalog item" not in e.msg
                    and "unknown schema" not in e.msg
                ):
                    raise e
            del exe.db.webhook_sources[source_id]


class CreateKafkaSourceAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        if exe.db.scenario == Scenario.Kill:
            result.extend(
                ["cannot create source in cluster with more than one replica"]
            )
        return result

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if len(exe.db.kafka_sources) > MAX_KAFKA_SOURCES:
                return
            source_id = exe.db.kafka_source_id
            exe.db.kafka_source_id += 1
            potential_clusters = [c for c in exe.db.clusters if len(c.replicas) == 1]
            cluster = self.rng.choice(potential_clusters)
            schema = self.rng.choice(exe.db.schemas)
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
                if exe.db.scenario != Scenario.Kill:
                    raise


class DropKafkaSourceAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if not exe.db.kafka_sources:
                return
            source_id = self.rng.randrange(len(exe.db.kafka_sources))
            source = exe.db.kafka_sources[source_id]
            query = f"DROP SOURCE {source}"
            try:
                exe.execute(query)
            except QueryError as e:
                # expected, see #20465
                if exe.db.scenario != Scenario.Kill or (
                    "unknown catalog item" not in e.msg
                    and "unknown schema" not in e.msg
                ):
                    raise e
            del exe.db.kafka_sources[source_id]


class CreatePostgresSourceAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        if exe.db.scenario == Scenario.Kill:
            result.extend(
                ["cannot create source in cluster with more than one replica"]
            )
        return result

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if len(exe.db.postgres_sources) > MAX_POSTGRES_SOURCES:
                return
            source_id = exe.db.postgres_source_id
            exe.db.postgres_source_id += 1
            potential_clusters = [c for c in exe.db.clusters if len(c.replicas) == 1]
            schema = self.rng.choice(exe.db.schemas)
            cluster = self.rng.choice(potential_clusters)
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
                if exe.db.scenario != Scenario.Kill:
                    raise


class DropPostgresSourceAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if not exe.db.postgres_sources:
                return
            source_id = self.rng.randrange(len(exe.db.postgres_sources))
            source = exe.db.postgres_sources[source_id]
            query = f"DROP SOURCE {source.executor.source}"
            try:
                exe.execute(query)
            except QueryError as e:
                # expected, see #20465
                if exe.db.scenario != Scenario.Kill or (
                    "unknown catalog item" not in e.msg
                    and "unknown schema" not in e.msg
                ):
                    raise e
            del exe.db.postgres_sources[source_id]


class CreateKafkaSinkAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            # Another replica can be created in parallel
            "cannot create sink in cluster with more than one replica",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if len(exe.db.kafka_sinks) > MAX_KAFKA_SINKS:
                return
            sink_id = exe.db.kafka_sink_id
            exe.db.kafka_sink_id += 1
            potential_clusters = [c for c in exe.db.clusters if len(c.replicas) == 1]
            cluster = self.rng.choice(potential_clusters)
            schema = self.rng.choice(exe.db.schemas)
        sink = KafkaSink(
            sink_id,
            cluster,
            schema,
            self.rng.choice(exe.db.db_objects_without_views()),
            self.rng,
        )
        sink.create(exe)
        exe.db.kafka_sinks.append(sink)


class DropKafkaSinkAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore(exe)

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if not exe.db.kafka_sinks:
                return
            sink_id = self.rng.randrange(len(exe.db.kafka_sinks))
            sink = exe.db.kafka_sinks[sink_id]
            query = f"DROP SINK {sink}"
            try:
                exe.execute(query)
            except QueryError as e:
                # expected, see #20465
                if exe.db.scenario != Scenario.Kill or (
                    "unknown catalog item" not in e.msg
                    and "unknown schema" not in e.msg
                ):
                    raise e
            del exe.db.kafka_sinks[sink_id]


class HttpPostAction(Action):
    def errors_to_ignore(self, exe: Executor) -> list[str]:
        result = super().errors_to_ignore(exe)
        if exe.db.scenario == Scenario.Rename:
            result.extend(["POST failed: 404, no object was found at the path"])
        return result

    def run(self, exe: Executor) -> None:
        with exe.db.lock:
            if not exe.db.webhook_sources:
                return

            source = self.rng.choice(exe.db.webhook_sources)
            url = f"http://{exe.db.host}:{exe.db.ports['http']}/api/webhook/{source.schema.db.name()}/{source.schema.name()}/{source.name()}"

            payload = source.body_format.to_data_type().random_value(self.rng)

            header_fields = source.explicit_include_headers
            if source.include_headers:
                header_fields.extend(
                    ["x-event-type", "signature", "x-mz-api-key"]
                )

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
                    raise QueryError(
                        f"POST failed: {result.status_code}, {result.text}", log
                    )
            except (requests.exceptions.ConnectionError):
                # Expected when Mz is killed
                if exe.db.scenario not in (Scenario.Kill, Scenario.BackupRestore):
                    raise


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
        (SQLsmithAction, 30),
        # (SetClusterAction, 1),  # SET cluster cannot be called in an active transaction
        (CommitRollbackAction, 1),
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
        (InsertAction, 100),
        # (SetClusterAction, 1),  # SET cluster cannot be called in an active transaction
        (HttpPostAction, 50),
        (CommitRollbackAction, 1),
        (ReconnectAction, 1),
        (SourceInsertAction, 100),
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
