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

from materialize.mzcompose.composition import Composition
from materialize.parallel_workload.data_type import Text, TextTextMap
from materialize.parallel_workload.database import (
    MAX_CLUSTER_REPLICAS,
    MAX_COMPUTE_CLUSTERS,
    MAX_ROLES,
    MAX_ROWS,
    MAX_SOURCES,
    MAX_TABLES,
    MAX_VIEWS,
    Cluster,
    ClusterReplica,
    ClusterType,
    Database,
    DBObject,
    Role,
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
        objects: list[DBObject] = [*self.db.tables, *self.db.views, *self.db.sources]
        obj = self.rng.choice(objects)
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
        objects: list[DBObject] = [*self.db.tables, *self.db.views, *self.db.sources]
        obj = self.rng.choice(objects)
        column = self.rng.choice(obj.columns)
        obj2 = self.rng.choice(objects)
        obj_name = str(obj)
        obj2_name = str(obj2)
        columns = [c for c in obj2.columns if c.data_type == column.data_type]
        if obj_name != obj2_name and columns:
            column2 = self.rng.choice(columns)
            query = f"SELECT * FROM {obj_name} JOIN {obj2_name} ON "
            if column.data_type == TextTextMap:
                query += f"map_length({column}) = map_length({column2})"
            else:
                query += f"{column} = {column2}"
            query += " LIMIT 1"
            exe.execute(query, explainable=True)
            exe.cur.fetchall()
        else:
            if self.rng.choice([True, False]):
                expressions = ", ".join(
                    str(column)
                    for column in random.sample(
                        obj.columns, k=random.randint(1, len(obj.columns))
                    )
                )
            else:
                expressions = "*"
            query = f"SELECT {expressions} FROM {obj} LIMIT 1"
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
        index_name = f"idx_{table}_{abs(hash(columns_str))}"
        index_elems = []
        for i, column in enumerate(columns):
            order = self.rng.choice(["ASC", "DESC"])
            index_elems.append(f"{column.name(True)} {order}")
        index_str = ", ".join(index_elems)
        query = f"CREATE INDEX {index_name} ON {table} ({index_str})"
        exe.execute(query)
        with self.db.lock:
            self.db.indexes.add(index_name)


class DropIndexAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.indexes:
                return
            index_name = self.rng.choice(list(self.db.indexes))
            query = f"DROP INDEX {index_name}"
            exe.execute(query)
            self.db.indexes.remove(index_name)


class CreateTableAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.tables) > MAX_TABLES:
                return
            table_id = self.db.table_id
            self.db.table_id += 1
            table = Table(self.rng, table_id)
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
        with self.db.lock:
            if not self.db.tables:
                return
            table = self.rng.choice(self.db.tables)
            old_name = str(table)
            table.rename += 1
            try:
                exe.execute(f"ALTER TABLE {old_name} RENAME TO {table}")
            finally:
                table.rename -= 1


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
            base_object = self.rng.choice(self.db.tables + self.db.sources)
            base_object2: DBObject | None = self.rng.choice(
                self.db.tables + self.db.sources
            )
            if self.rng.choice([True, False]) or base_object2 == base_object:
                base_object2 = None
            view = View(self.rng, view_id, base_object, base_object2)
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


class CreateComputeClusterAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.compute_clusters) > MAX_COMPUTE_CLUSTERS:
                return
            cluster_id = self.db.compute_cluster_id
            self.db.compute_cluster_id += 1
            cluster = Cluster(
                ClusterType.COMPUTE,
                cluster_id,
                managed=self.rng.choice([True, False]),
                size=self.rng.choice(["1", "2", "4"]),
                replication_factor=self.rng.choice([1, 2, 4, 5]),
                introspection_interval=self.rng.choice(["0", "1s", "10s"]),
            )
            cluster.create(exe)
            self.db.compute_clusters.append(cluster)


class DropComputeClusterAction(Action):
    def errors_to_ignore(self) -> list[str]:
        return [
            "cannot drop cluster with active objects",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.compute_clusters:
                return
            cluster_id = self.rng.randrange(len(self.db.compute_clusters))
            cluster = self.db.compute_clusters[cluster_id]
            query = f"DROP CLUSTER {cluster}"
            exe.execute(query)
            del self.db.compute_clusters[cluster_id]


class SetComputeClusterAction(Action):
    def errors_to_ignore(self) -> list[str]:
        return [
            "SET cluster cannot be called in an active transaction",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.compute_clusters:
                return
            cluster = self.rng.choice(self.db.compute_clusters)
            query = f"SET CLUSTER = {cluster}"
            exe.execute(query)


class CreateComputeClusterReplicaAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            unmanaged_clusters = [c for c in self.db.compute_clusters if not c.managed]
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


class DropComputeClusterReplicaAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            unmanaged_clusters = [c for c in self.db.compute_clusters if not c.managed]
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
        port = self.db.port
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

        while True:
            try:
                conn = pg8000.connect(
                    host=host, port=port, user=user, database=str(self.db)
                )
                conn.autocommit = autocommit
                cur = conn.cursor()
                exe.cur = cur
                exe.set_isolation("SERIALIZABLE")
                cur.execute("SELECT pg_backend_pid()")
                exe.pg_pid = cur.fetchall()[0][0]
            except Exception as e:
                if (
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
        time.sleep(self.rng.uniform(0, 3))


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
            if len(self.db.sources) > MAX_SOURCES:
                return
            source_id = self.db.source_id
            self.db.source_id += 1
            cluster = self.rng.choice(self.db.storage_clusters)
            source = WebhookSource(source_id, cluster, self.rng)
            source.create(exe)
            self.db.sources.append(source)


class DropWebhookSourceAction(Action):
    def errors_to_ignore(self) -> list[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.sources:
                return
            source_id = self.rng.randrange(len(self.db.sources))
            source = self.db.sources[source_id]
            query = f"DROP SOURCE {source}"
            exe.execute(query)
            del self.db.sources[source_id]


class HttpPostAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.sources:
                return

            source = self.rng.choice(self.db.sources)
            url = f"http://{self.db.host}:{self.db.http_port}/api/webhook/{self.db}/public/{source}"

            payload = source.body_format.to_data_type().value(self.rng)

            header_fields = source.explicit_include_headers
            if source.include_headers:
                header_fields.extend(
                    ["timestamp", "x-event-type", "signature", "x-mz-api-key"]
                )

            headers = {
                header: f'"{Text.value(self.rng)}"'.encode()
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
        (SetComputeClusterAction, 1),
        (CommitRollbackAction, 1),
        (ReconnectAction, 1),
    ],
    autocommit=False,
)

fetch_action_list = ActionList(
    [
        (FetchAction, 30),
        (SetComputeClusterAction, 1),
        (ReconnectAction, 1),
    ],
    autocommit=False,
)

write_action_list = ActionList(
    [
        (InsertAction, 100),
        (SetComputeClusterAction, 1),
        (HttpPostAction, 50),
        (CommitRollbackAction, 1),
        (ReconnectAction, 1),
    ],
    autocommit=False,
)

dml_nontrans_action_list = ActionList(
    [
        (DeleteAction, 10),
        (UpdateAction, 10),
        (SetComputeClusterAction, 1),
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
        (CreateComputeClusterAction, 2),
        (DropComputeClusterAction, 1),
        (CreateComputeClusterReplicaAction, 8),
        (DropComputeClusterReplicaAction, 4),
        (SetComputeClusterAction, 1),
        (CreateWebhookSourceAction, 2),
        (DropWebhookSourceAction, 1),
        (GrantPrivilegesAction, 2),
        (RevokePrivilegesAction, 1),
        (ReconnectAction, 1),
        # (RenameTableAction, 1),  # TODO(def-) enable
        # (TransactionIsolationAction, 1),
    ],
    autocommit=True,
)
