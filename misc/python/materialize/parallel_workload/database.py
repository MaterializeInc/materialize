# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import threading
from copy import copy
from typing import List, Optional, Set, Type

from materialize.parallel_workload.data_type import DATA_TYPES, DataType
from materialize.parallel_workload.executor import Executor

MAX_COLUMNS = 100
MAX_ROWS = 1000
MAX_CLUSTERS = 10
MAX_CLUSTER_REPLICAS = 4
MAX_TABLES = 100
MAX_VIEWS = 100
MAX_ROLES = 100

MAX_INITIAL_CLUSTERS = 1
MAX_INITIAL_TABLES = 10
MAX_INITIAL_VIEWS = 10
MAX_INITIAL_ROLES = 3


# TODO: Create/Drop source (load generator, pg circle from Mz)
class Column:
    column_id: int
    data_type: Type[DataType]
    db_object: "DBObject"
    nullable: bool
    default: Optional[str]
    _name: str

    def __init__(
        self,
        rng: random.Random,
        column_id: int,
        data_type: Type[DataType],
        db_object: "DBObject",
    ):
        self.column_id = column_id
        self.data_type = data_type
        self.db_object = db_object
        self.nullable = rng.choice([True, False])
        self.default = rng.choice([None, str(data_type.value(rng))])
        self._name = f"c{self.column_id}_{self.data_type.name()}"

    def __str__(self) -> str:
        return f"{self.db_object}.{self._name}"

    def name(self) -> str:
        return self._name

    def set_name(self, new_name: str) -> None:
        self._name = new_name

    def value(self, rng: random.Random, in_query: bool = False) -> str:
        return str(self.data_type.value(rng, in_query))

    def create(self) -> str:
        result = f"{self.name()} {self.data_type.name()}"
        if self.default:
            result += f" DEFAULT {self.default}"
        if not self.nullable:
            result += " NOT NULL"
        return result


class DBObject:
    columns: List[Column]


class Table(DBObject):
    table_id: int
    rename: int
    num_rows: int

    def __init__(self, rng: random.Random, table_id: int):
        self.table_id = table_id
        self.columns = [
            Column(rng, i, rng.choice(DATA_TYPES), self)
            for i in range(rng.randint(2, MAX_COLUMNS))
        ]
        self.num_rows = 0
        self.rename = 0

    def __str__(self) -> str:
        if self.rename:
            return f"t{self.table_id}_{self.rename}"
        return f"t{self.table_id}"

    def create(self, exe: Executor) -> None:
        query = f"CREATE TABLE {self}("
        query += ",\n    ".join(column.create() for column in self.columns)
        query += ")"
        exe.execute(query)
        query = f"CREATE DEFAULT INDEX ON {self}"
        exe.execute(query)


class View(DBObject):
    view_id: int
    base_object: DBObject
    base_object2: Optional[DBObject]
    columns: List[Column]
    source_columns: List[Column]
    materialized: bool
    join_column: Optional[Column]
    join_column2: Optional[Column]

    def __init__(
        self,
        rng: random.Random,
        view_id: int,
        base_object: DBObject,
        base_object2: Optional[DBObject],
    ):
        self.view_id = view_id
        self.base_object = base_object
        self.base_object2 = base_object2
        all_columns = base_object.columns + (
            base_object2.columns if base_object2 else []
        )
        self.source_columns = [
            column
            for column in rng.sample(all_columns, k=rng.randint(1, len(all_columns)))
        ]
        self.columns = [copy(column) for column in self.source_columns]
        for column in self.columns:
            column.set_name(f"{column.name()}_{column.db_object}")
            column.db_object = self

        self.materialized = rng.choice([True, False])

        if base_object2:
            self.join_column = rng.choice(base_object.columns)
            self.join_column2 = None
            columns = [
                c
                for c in base_object2.columns
                if c.data_type == self.join_column.data_type
            ]
            if columns:
                self.join_column2 = rng.choice(columns)

    def __str__(self) -> str:
        return f"v{self.view_id}"

    def create(self, exe: Executor) -> None:
        if self.materialized:
            query = "CREATE MATERIALIZED VIEW"
        else:
            query = "CREATE VIEW"
        columns_str = ", ".join(
            f"{source_column} AS {column.name()}"
            for source_column, column in zip(self.source_columns, self.columns)
        )
        query += f" {self} AS SELECT {columns_str} FROM {self.base_object}"
        if self.base_object2:
            query += f" JOIN {self.base_object2}"
            if self.join_column2:
                query += f" ON {self.join_column} = {self.join_column2}"
            else:
                query += " ON TRUE"

        exe.execute(query)
        query = f"CREATE DEFAULT INDEX ON {self}"
        exe.execute(query)


class Role(DBObject):
    role_id: int

    def __init__(self, role_id: int):
        self.role_id = role_id

    def __str__(self) -> str:
        return f"role{self.role_id}"

    def create(self, exe: Executor) -> None:
        exe.execute(f"CREATE ROLE {self}")


class ClusterReplica(DBObject):
    replica_id: int
    size: str
    cluster: "Cluster"

    def __init__(self, replica_id: int, size: str, cluster: "Cluster"):
        self.replica_id = replica_id
        self.size = size
        self.cluster = cluster

    def __str__(self) -> str:
        return f"r{self.replica_id+1}"

    def create(self, exe: Executor) -> None:
        # TODO: More Cluster Replica settings
        exe.execute(
            f"CREATE CLUSTER REPLICA {self.cluster}.{self} SIZE = '{self.size}'"
        )


class Cluster(DBObject):
    cluster_id: int
    managed: bool
    size: str
    replicas: List[ClusterReplica]
    replica_id: int
    introspection_interval: str

    def __init__(
        self,
        cluster_id: int,
        managed: bool,
        size: str,
        replication_factor: int,
        introspection_interval: str,
    ):
        self.cluster_id = cluster_id
        self.managed = managed
        self.size = size
        self.replicas = [
            ClusterReplica(i, size, self) for i in range(replication_factor)
        ]
        self.replica_id = len(self.replicas)
        self.introspection_interval = introspection_interval

    def __str__(self) -> str:
        return f"cluster{self.cluster_id}"

    def create(self, exe: Executor) -> None:
        query = f"CREATE CLUSTER {self} "
        if self.managed:
            query += f"SIZE = '{self.size}', REPLICATION FACTOR = {len(self.replicas)}, INTROSPECTION INTERVAL = '{self.introspection_interval}'"
        else:
            query += "REPLICAS("
            query += ", ".join(
                f"{replica} (SIZE = '{replica.size}')" for replica in self.replicas
            )
            query += ")"
        exe.execute(query)


class Database:
    seed: str
    complexity: str
    scenario: str
    host: str
    port: int
    system_port: int
    tables: List[Table]
    table_id: int
    views: List[View]
    view_id: int
    roles: List[Role]
    role_id: int
    clusters: List[Cluster]
    cluster_id: int
    indexes: Set[str]
    lock: threading.Lock

    def __init__(
        self,
        rng: random.Random,
        seed: str,
        host: str,
        port: int,
        system_port: int,
        complexity: str,
        scenario: str,
    ):
        self.seed = seed
        self.host = host
        self.port = port
        self.complexity = complexity
        self.scenario = scenario

        self.tables = [Table(rng, i) for i in range(rng.randint(2, MAX_INITIAL_TABLES))]
        self.table_id = len(self.tables)
        self.views = []
        for i in range(rng.randint(2, MAX_INITIAL_VIEWS)):
            # Only use tables for now since LIMIT 1 and statement_timeout are
            # not effective yet at preventing long-running queries and OoMs.
            base_object = rng.choice(self.tables)
            base_object2: Optional[Table] = rng.choice(self.tables)
            if rng.choice([True, False]) or base_object2 == base_object:
                base_object2 = None
            view = View(rng, i, base_object, base_object2)
            self.views.append(view)
        self.view_id = len(self.views)
        self.roles = [Role(i) for i in range(rng.randint(0, MAX_INITIAL_ROLES))]
        self.role_id = len(self.roles)
        self.clusters = [
            Cluster(
                i,
                managed=rng.choice([True, False]),
                size=rng.choice(["1", "2", "4"]),
                replication_factor=rng.choice([1, 2, 4, 5]),
                introspection_interval=rng.choice(["0", "1s", "10s"]),
            )
            for i in range(rng.randint(0, MAX_INITIAL_CLUSTERS))
        ]
        self.cluster_id = len(self.clusters)
        self.indexes = set()
        self.lock = threading.Lock()

    def __str__(self) -> str:
        return f"db_pw_{self.seed}"

    def drop(self, exe: Executor) -> None:
        exe.execute(f"DROP DATABASE IF EXISTS {self}")

    def create(self, exe: Executor) -> None:
        self.drop(exe)
        exe.execute(f"CREATE DATABASE {self}")

    def create_relations(self, exe: Executor) -> None:
        exe.execute("SELECT name FROM mz_clusters WHERE name LIKE 'c%'")
        for row in exe.cur.fetchall():
            exe.execute(f"DROP CLUSTER {row[0]} CASCADE")

        for cluster in self.clusters:
            cluster.create(exe)

        for table in self.tables:
            table.create(exe)

        for view in self.views:
            view.create(exe)

        exe.execute("SELECT name FROM mz_roles WHERE name LIKE 'r%'")
        for row in exe.cur.fetchall():
            exe.execute(f"DROP ROLE {row[0]}")

        for role in self.roles:
            role.create(exe)
