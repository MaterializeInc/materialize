# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import time
from typing import TYPE_CHECKING

import pg8000
import requests
from pg8000.native import identifier

from materialize.data_ingest.data_type import NUMBER_TYPES, Text, TextTextMap
from materialize.mzcompose.composition import Composition
from materialize.parallel_workload.database import (
    MAX_CLUSTER_REPLICAS,
    MAX_CLUSTERS,
    MAX_KAFKA_SINKS,
    MAX_KAFKA_SOURCES,
    MAX_POSTGRES_SOURCES,
    MAX_ROLES,
    MAX_ROWS,
    MAX_SCHEMAS,
    MAX_TABLES,
    MAX_VIEWS,
    MAX_WEBHOOK_SOURCES,
    NAUGHTY_IDENTIFIERS,
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
from materialize.parallel_workload.executor import Executor, QueryError
from materialize.parallel_workload.settings import Complexity, Scenario

if TYPE_CHECKING:
    from materialize.parallel_workload.worker import Worker

# TODO: In kill scenario drops can be successful, but we might never know, see
# https://github.com/MaterializeInc/materialize/issues/20465 We should handle
# this by rescanning objects we expect to be there and removing the ones that
# were dropped. This also has the risk that objects get lost as a bug though.

# TODO: CASCADE in DROPs, keep track of what will be deleted
class Action:
    rng: random.Random
    db: Database

    def __init__(self, rng: random.Random, db: Database):
        self.rng = rng
        self.db = db

    def run(self, exe: Executor) -> None:
        raise NotImplementedError

    def errors_to_ignore(self) -> list[str]:
        result = [
            "permission denied for",
            "must be owner of",
        ]
        if self.db.complexity == Complexity.DDL:
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
        if self.db.scenario == Scenario.Cancel:
            result.extend(
                [
                    "canceling statement due to user request",
                ]
            )
        if self.db.scenario == Scenario.Kill:
            result.extend(
                [
                    "network error",
                    "Can't create a connection to host",
                    "Connection refused",
                ]
            )
        if self.db.scenario == Scenario.Rename:
            result.extend(["unknown schema", "ambiguous reference to schema name"])
        if NAUGHTY_IDENTIFIERS:
            result.extend(["identifier length exceeds 255 bytes"])
        return result


class FetchAction(Action):
    def errors_to_ignore(self) -> list[str]:
        result = super().errors_to_ignore()
        if self.db.complexity == Complexity.DDL:
            result.extend(
                [
                    "does not exist",
                ]
            )
        return result

    def run(self, exe: Executor) -> None:
        obj = self.rng.choice(self.db.db_objects())
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
    def errors_to_ignore(self) -> list[str]:
        result = super().errors_to_ignore()
        if self.db.complexity in (Complexity.DML, Complexity.DDL):
            result.extend(
                [
                    "in the same timedomain",
                ]
            )
        if self.db.complexity == Complexity.DDL:
            result.extend(
                [
                    "does not exist",
                ]
            )
        return result

    def run(self, exe: Executor) -> None:
        obj = self.rng.choice(self.db.db_objects())
        column = self.rng.choice(obj.columns)
        obj2 = self.rng.choice(self.db.db_objects())
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


class InsertAction(Action):
    def run(self, exe: Executor) -> None:
        table = None
        if exe.insert_table != None:
            for t in self.db.tables:
                if t.table_id == exe.insert_table:
                    table = t
                    break
            else:
                exe.commit() if self.rng.choice([True, False]) else exe.rollback()
        if not table:
            table = self.rng.choice(self.db.tables)

        column_names = ", ".join(column.name(True) for column in table.columns)
        column_values = ", ".join(
            column.value(self.rng, True) for column in table.columns
        )
        query = f"INSERT INTO {table} ({column_names}) VALUES ({column_values})"
        if table.num_rows > MAX_ROWS:
            return
        exe.execute(query)
        exe.insert_table = table.table_id
        with self.db.lock:
            table.num_rows += 1


class SourceInsertAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            sources = self.db.kafka_sources + self.db.postgres_sources
            if not sources:
                return
            source = self.rng.choice(sources)
            transaction = next(source.generator)
            with source.lock:
                source.executor.run(transaction)


class UpdateAction(Action):
    def errors_to_ignore(self) -> list[str]:
        return [
            "canceling statement due to statement timeout",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        table = None
        if exe.insert_table != None:
            for t in self.db.tables:
                if t.table_id == exe.insert_table:
                    table = t
                    break
        if not table:
            table = self.rng.choice(self.db.tables)

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
    def errors_to_ignore(self) -> list[str]:
        return [
            "canceling statement due to statement timeout",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        table = self.rng.choice(self.db.tables)
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
                with self.db.lock:
                    table.num_rows = 0


class CommentAction(Action):
    def run(self, exe: Executor) -> None:
        table = self.rng.choice(self.db.tables)

        if self.rng.choice([True, False]):
            column = self.rng.choice(table.columns)
            query = f"COMMENT ON COLUMN {column} IS '{Text.random_value(self.rng)}'"
        else:
            query = f"COMMENT ON TABLE {table} IS '{Text.random_value(self.rng)}'"

        exe.execute(query)


class CreateIndexAction(Action):
    def errors_to_ignore(self) -> list[str]:
        return [
            "already exists",  # TODO: Investigate
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        tables_views: list[DBObject] = [*self.db.tables, *self.db.views]
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
        with self.db.lock:
            self.db.indexes.add(index_name)


class DropIndexAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.indexes:
                return
            index_name = self.rng.choice(list(self.db.indexes))
            query = f"DROP INDEX {identifier(index_name)}"
            exe.execute(query)
            self.db.indexes.remove(index_name)


class CreateTableAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.tables) > MAX_TABLES:
                return
            table_id = self.db.table_id
            self.db.table_id += 1
            table = Table(self.rng, table_id, self.rng.choice(self.db.schemas))
            table.create(exe)
            self.db.tables.append(table)


class DropTableAction(Action):
    def errors_to_ignore(self) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.tables) <= 2:
                return
            table_id = self.rng.randrange(len(self.db.tables))
            table = self.db.tables[table_id]
            query = f"DROP TABLE {table}"
            exe.execute(query)
            del self.db.tables[table_id]


class RenameTableAction(Action):
    def run(self, exe: Executor) -> None:
        if self.db.scenario != Scenario.Rename:
            return
        with self.db.lock:
            if not self.db.tables:
                return
            table = self.rng.choice(self.db.tables)
            old_name = str(table)
            table.rename += 1
            try:
                exe.execute(
                    f"ALTER TABLE {old_name} RENAME TO {identifier(table.name())}"
                )
            except:
                table.rename -= 1
                raise


class CreateSchemaAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.schemas) > MAX_SCHEMAS:
                return
            schema_id = self.db.schema_id
            self.db.schema_id += 1
            schema = Schema(self.rng, schema_id)
            schema.create(exe)
            self.db.schemas.append(schema)


class DropSchemaAction(Action):
    def errors_to_ignore(self) -> list[str]:
        return [
            "cannot be dropped without CASCADE while it contains objects",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.schemas) <= 1:
                return
            schema_id = self.rng.randrange(len(self.db.schemas))
            schema = self.db.schemas[schema_id]
            query = f"DROP SCHEMA {schema}"
            exe.execute(query)
            del self.db.schemas[schema_id]


class RenameSchemaAction(Action):
    def run(self, exe: Executor) -> None:
        if self.db.scenario != Scenario.Rename:
            return
        with self.db.lock:
            schema = self.rng.choice(self.db.schemas)
            old_name = str(schema)
            schema.rename += 1
            try:
                exe.execute(f"ALTER SCHEMA {old_name} RENAME TO {schema}")
            except:
                schema.rename -= 1
                raise


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
        with self.db.lock:
            if len(self.db.views) > MAX_VIEWS:
                return
            view_id = self.db.view_id
            self.db.view_id += 1
            # Don't use views for now since LIMIT 1 and statement_timeout are
            # not effective yet at preventing long-running queries and OoMs.
            base_object = self.rng.choice(self.db.db_objects())
            base_object2: DBObject | None = self.rng.choice(self.db.db_objects())
            if self.rng.choice([True, False]) or base_object2 == base_object:
                base_object2 = None
            view = View(
                self.rng,
                view_id,
                base_object,
                base_object2,
                self.rng.choice(self.db.schemas),
            )
            view.create(exe)
            self.db.views.append(view)


class DropViewAction(Action):
    def errors_to_ignore(self) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.views:
                return
            view_id = self.rng.randrange(len(self.db.views))
            view = self.db.views[view_id]
            if view.materialized:
                query = f"DROP MATERIALIZED VIEW {view}"
            else:
                query = f"DROP VIEW {view}"
            exe.execute(query)
            del self.db.views[view_id]


class CreateRoleAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.roles) > MAX_ROLES:
                return
            role_id = self.db.role_id
            self.db.role_id += 1
            role = Role(role_id)
            role.create(exe)
            self.db.roles.append(role)


class DropRoleAction(Action):
    def errors_to_ignore(self) -> list[str]:
        return [
            "cannot be dropped because some objects depend on it",
            "current role cannot be dropped",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.roles:
                return
            role_id = self.rng.randrange(len(self.db.roles))
            role = self.db.roles[role_id]
            query = f"DROP ROLE {role}"
            exe.execute(query)
            del self.db.roles[role_id]


class CreateClusterAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.clusters) > MAX_CLUSTERS:
                return
            cluster_id = self.db.cluster_id
            self.db.cluster_id += 1
            cluster = Cluster(
                cluster_id,
                managed=self.rng.choice([True, False]),
                size=self.rng.choice(["1", "2", "4"]),
                replication_factor=self.rng.choice([1, 2, 4, 5]),
                introspection_interval=self.rng.choice(["0", "1s", "10s"]),
            )
            cluster.create(exe)
            self.db.clusters.append(cluster)


class DropClusterAction(Action):
    def errors_to_ignore(self) -> list[str]:
        return [
            "cannot drop cluster with active objects",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.clusters) <= 1:
                return
            # Keep cluster 0 with 1 replica for sources/sinks
            cluster_id = self.rng.randrange(1, len(self.db.clusters))
            cluster = self.db.clusters[cluster_id]
            query = f"DROP CLUSTER {cluster}"
            exe.execute(query)
            del self.db.clusters[cluster_id]


class SetClusterAction(Action):
    def errors_to_ignore(self) -> list[str]:
        return [
            "SET cluster cannot be called in an active transaction",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.clusters:
                return
            cluster = self.rng.choice(self.db.clusters)
            query = f"SET CLUSTER = {cluster}"
            exe.execute(query)


class CreateClusterReplicaAction(Action):
    def errors_to_ignore(self) -> list[str]:
        return [
            "cannot create more than one replica of a cluster containing sources or sinks"
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            # Keep cluster 0 with 1 replica for sources/sinks
            unmanaged_clusters = [c for c in self.db.clusters[1:] if not c.managed]
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
            replica.create(exe)
            cluster.replicas.append(replica)
            cluster.replica_id += 1


class DropClusterReplicaAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            # Keep cluster 0 with 1 replica for sources/sinks
            unmanaged_clusters = [c for c in self.db.clusters[1:] if not c.managed]
            if not unmanaged_clusters:
                return
            cluster = self.rng.choice(unmanaged_clusters)
            # Avoid "has no replicas available to service request" error
            if len(cluster.replicas) <= 1:
                return
            replica_id = self.rng.randrange(len(cluster.replicas))
            replica = cluster.replicas[replica_id]
            query = f"DROP CLUSTER REPLICA {cluster}.{replica}"
            exe.execute(query)
            del cluster.replicas[replica_id]


class GrantPrivilegesAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.roles:
                return
            role = self.rng.choice(self.db.roles)
            privilege = self.rng.choice(["SELECT", "INSERT", "UPDATE", "ALL"])
            tables_views: list[DBObject] = [*self.db.tables, *self.db.views]
            table = self.rng.choice(tables_views)
            query = f"GRANT {privilege} ON {table} TO {role}"
            exe.execute(query)


class RevokePrivilegesAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.roles:
                return
            role = self.rng.choice(self.db.roles)
            privilege = self.rng.choice(["SELECT", "INSERT", "UPDATE", "ALL"])
            tables_views: list[DBObject] = [*self.db.tables, *self.db.views]
            table = self.rng.choice(tables_views)
            query = f"REVOKE {privilege} ON {table} FROM {role}"
            exe.execute(query)


# TODO: Should factor this out so can easily use it without action
class ReconnectAction(Action):
    def __init__(
        self,
        rng: random.Random,
        db: Database,
        random_role: bool = True,
    ):
        super().__init__(rng, db)
        self.random_role = random_role

    def run(self, exe: Executor) -> None:
        autocommit = exe.cur._c.autocommit
        host = self.db.host
        port = self.db.ports["materialized"]
        with self.db.lock:
            if self.random_role and self.db.roles:
                user = self.rng.choice(
                    ["materialize", str(self.rng.choice(self.db.roles))]
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
                    host=host, port=port, user=user, database=self.db.name()
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

    def errors_to_ignore(self) -> list[str]:
        return [
            "must be a member of",
        ] + super().errors_to_ignore()

    def __init__(
        self,
        rng: random.Random,
        db: Database,
        workers: list["Worker"],
    ):
        super().__init__(rng, db)
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
    composition: Composition

    def __init__(
        self,
        rng: random.Random,
        db: Database,
        composition: Composition,
    ):
        super().__init__(rng, db)
        self.composition = composition

    def run(self, exe: Executor) -> None:
        self.composition.kill("materialized")
        # Otherwise getting failure on "up" locally
        time.sleep(1)
        self.composition.up("materialized", detach=True)
        time.sleep(self.rng.uniform(20, 60))


class CreateWebhookSourceAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.webhook_sources) > MAX_WEBHOOK_SOURCES:
                return
            webhook_source_id = self.db.webhook_source_id
            self.db.webhook_source_id += 1
            potential_clusters = [c for c in self.db.clusters if len(c.replicas) == 1]
            cluster = self.rng.choice(potential_clusters)
            schema = self.rng.choice(self.db.schemas)
            source = WebhookSource(webhook_source_id, cluster, schema, self.rng)
            source.create(exe)
            self.db.webhook_sources.append(source)


class DropWebhookSourceAction(Action):
    def errors_to_ignore(self) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.webhook_sources:
                return
            source_id = self.rng.randrange(len(self.db.webhook_sources))
            source = self.db.webhook_sources[source_id]
            query = f"DROP SOURCE {source}"
            exe.execute(query)
            del self.db.webhook_sources[source_id]


class CreateKafkaSourceAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.kafka_sources) > MAX_KAFKA_SOURCES:
                return
            source_id = self.db.kafka_source_id
            self.db.kafka_source_id += 1
            potential_clusters = [c for c in self.db.clusters if len(c.replicas) == 1]
            cluster = self.rng.choice(potential_clusters)
            schema = self.rng.choice(self.db.schemas)
            source = KafkaSource(
                self.db.name(), source_id, cluster, schema, self.db.ports, self.rng
            )
            source.create(exe)
            self.db.kafka_sources.append(source)


class DropKafkaSourceAction(Action):
    def errors_to_ignore(self) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.kafka_sources:
                return
            source_id = self.rng.randrange(len(self.db.kafka_sources))
            source = self.db.kafka_sources[source_id]
            query = f"DROP SOURCE {source}"
            exe.execute(query)
            del self.db.kafka_sources[source_id]


class CreatePostgresSourceAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.postgres_sources) > MAX_POSTGRES_SOURCES:
                return
            source_id = self.db.postgres_source_id
            self.db.postgres_source_id += 1
            potential_clusters = [c for c in self.db.clusters if len(c.replicas) == 1]
            schema = self.rng.choice(self.db.schemas)
            cluster = self.rng.choice(potential_clusters)
            source = PostgresSource(
                self.db.name(), source_id, cluster, schema, self.db.ports, self.rng
            )
            source.create(exe)
            self.db.postgres_sources.append(source)


class DropPostgresSourceAction(Action):
    def errors_to_ignore(self) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.postgres_sources:
                return
            source_id = self.rng.randrange(len(self.db.postgres_sources))
            source = self.db.postgres_sources[source_id]
            query = f"DROP SOURCE {source.executor.source}"
            exe.execute(query)
            del self.db.postgres_sources[source_id]


class CreateKafkaSinkAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.kafka_sinks) > MAX_KAFKA_SINKS:
                return
            sink_id = self.db.kafka_sink_id
            self.db.kafka_sink_id += 1
            potential_clusters = [c for c in self.db.clusters if len(c.replicas) == 1]
            cluster = self.rng.choice(potential_clusters)
            schema = self.rng.choice(self.db.schemas)
            sink = KafkaSink(
                sink_id,
                cluster,
                schema,
                self.rng.choice(self.db.db_objects_without_views()),
                self.rng,
            )
            sink.create(exe)
            self.db.kafka_sinks.append(sink)


class DropKafkaSinkAction(Action):
    def errors_to_ignore(self) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.kafka_sinks:
                return
            sink_id = self.rng.randrange(len(self.db.kafka_sinks))
            sink = self.db.kafka_sinks[sink_id]
            query = f"DROP SINK {sink}"
            exe.execute(query)
            del self.db.kafka_sinks[sink_id]


class HttpPostAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.webhook_sources:
                return

            source = self.rng.choice(self.db.webhook_sources)
            url = f"http://{self.db.host}:{self.db.ports['http']}/api/webhook/{self.db}/public/{source}"

            payload = source.body_format.to_data_type().random_value(self.rng)

            header_fields = source.explicit_include_headers
            if source.include_headers:
                header_fields.extend(
                    ["timestamp", "x-event-type", "signature", "x-mz-api-key"]
                )

            headers = {
                header: f'"{Text.random_value(self.rng)}"'.encode()
                for header in self.rng.sample(header_fields, len(header_fields))
            }

            headers_strs = [f"{key}: {value}" for key, value in enumerate(headers)]
            exe.log(
                f"POST Headers: {', '.join(headers_strs)} Body: {payload.encode('utf-8')}"
            )
            requests.post(url, data=payload.encode("utf-8"), headers=headers)


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
        (SetClusterAction, 1),
        (CommitRollbackAction, 1),
        (ReconnectAction, 1),
    ],
    autocommit=False,
)

fetch_action_list = ActionList(
    [
        (FetchAction, 30),
        (SetClusterAction, 1),
        (ReconnectAction, 1),
    ],
    autocommit=False,
)

write_action_list = ActionList(
    [
        (InsertAction, 100),
        (SetClusterAction, 1),
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
        (CreateClusterReplicaAction, 8),
        (DropClusterReplicaAction, 4),
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
        (CreateSchemaAction, 1),
        (DropSchemaAction, 1),
        (RenameSchemaAction, 10),
        (RenameTableAction, 10),
        # (TransactionIsolationAction, 1),
    ],
    autocommit=True,
)
