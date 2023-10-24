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
from collections.abc import Iterator, Sequence
from copy import copy
from enum import Enum

from pg8000.native import identifier, literal

from materialize.data_ingest.data_type import (
    DATA_TYPES,
    DATA_TYPES_FOR_AVRO,
    Bytea,
    DataType,
    Jsonb,
    Text,
    TextTextMap,
)
from materialize.data_ingest.executor import KafkaExecutor, PgExecutor
from materialize.data_ingest.field import Field
from materialize.data_ingest.transaction import Transaction
from materialize.data_ingest.workload import WORKLOADS
from materialize.parallel_workload.executor import Executor
from materialize.parallel_workload.settings import Complexity, Scenario
from materialize.util import naughty_strings

MAX_COLUMNS = 100
MAX_ROWS = 1000
MAX_CLUSTERS = 10
MAX_CLUSTER_REPLICAS = 4
MAX_SCHEMAS = 10
MAX_TABLES = 100
MAX_VIEWS = 100
MAX_ROLES = 100
MAX_WEBHOOK_SOURCES = 20
MAX_KAFKA_SOURCES = 20
MAX_POSTGRES_SOURCES = 20
MAX_KAFKA_SINKS = 20
MAX_INCLUDE_HEADERS = 5

MAX_INITIAL_SCHEMAS = 1
MAX_INITIAL_CLUSTERS = 2
MAX_INITIAL_TABLES = 10
MAX_INITIAL_VIEWS = 10
MAX_INITIAL_ROLES = 3
MAX_INITIAL_WEBHOOK_SOURCES = 3
MAX_INITIAL_KAFKA_SOURCES = 3
MAX_INITIAL_POSTGRES_SOURCES = 3
MAX_INITIAL_KAFKA_SINKS = 3

MAX_IDENTIFIER_LENGTH = 255

NAUGHTY_IDENTIFIERS = False


def naughtify(name: str) -> str:
    """Makes a string into a naughty identifier, always returns the same
    identifier when called with the same input."""
    global NAUGHTY_IDENTIFIERS

    if not NAUGHTY_IDENTIFIERS:
        return name

    strings = naughty_strings()
    # This rng is just to get a more interesting integer for the name
    index = abs(hash(name)) % len(strings)
    # Keep them short so we can combine later with other identifiers, 255 char limit
    naughty_suffix = strings[index].encode("utf-8")[:8].decode("utf-8", "ignore")

    naughty_identifier = f"{name}_{naughty_suffix}"
    assert (
        len(naughty_identifier.encode("utf-8")) <= MAX_IDENTIFIER_LENGTH
    ), f"Naughty identifier exceeds length limit of {MAX_IDENTIFIER_LENGTH} chars (name={name}, naughty_suffix={naughty_suffix})"
    return naughty_identifier


class BodyFormat(Enum):
    TEXT = 1
    JSON = 2
    BYTES = 3

    def to_data_type(self) -> type[DataType]:
        if self == BodyFormat.JSON:
            return Jsonb
        if self == BodyFormat.TEXT:
            return Text
        if self == BodyFormat.BYTES:
            return Bytea
        raise ValueError(f"Unknown body format {self.name}")


class Column:
    column_id: int
    data_type: type[DataType]
    db_object: "DBObject"
    nullable: bool
    default: str | None
    _name: str

    def __init__(
        self,
        rng: random.Random,
        column_id: int,
        data_type: type[DataType],
        db_object: "DBObject",
    ):
        self.column_id = column_id
        self.data_type = data_type
        self.db_object = db_object
        self.nullable = rng.choice([True, False])
        self.default = rng.choice(
            [None, str(data_type.random_value(rng, in_query=True))]
        )
        self._name = f"c-{self.column_id}-{self.data_type.name()}"

    def __str__(self) -> str:
        return f"{self.db_object}.{self.name(True)}"

    def name(self, in_query: bool = False) -> str:
        return identifier(naughtify(self._name)) if in_query else naughtify(self._name)

    def set_name(self, new_name: str) -> None:
        self._name = new_name

    def value(self, rng: random.Random, in_query: bool = False) -> str:
        return str(self.data_type.random_value(rng, in_query=in_query))

    def create(self) -> str:
        result = f"{self.name(True)} {self.data_type.name()}"
        if self.default:
            result += f" DEFAULT {self.default}"
        if not self.nullable:
            result += " NOT NULL"
        return result


class Schema:
    schema_id: int
    rename: int

    def __init__(self, rng: random.Random, schema_id: int):
        self.schema_id = schema_id
        self.rename = 0

    def name(self) -> str:
        if self.rename:
            return naughtify(f"s-{self.schema_id}-{self.rename}")
        return naughtify(f"s-{self.schema_id}")

    def __str__(self) -> str:
        return identifier(self.name())

    def create(self, exe: Executor) -> None:
        query = f"CREATE SCHEMA {self}"
        exe.execute(query)


class DBObject:
    columns: Sequence[Column]

    def name(self) -> str:
        raise NotImplementedError

    def create(self, exe: Executor) -> None:
        raise NotImplementedError


class Table(DBObject):
    table_id: int
    rename: int
    num_rows: int
    schema: Schema

    def __init__(self, rng: random.Random, table_id: int, schema: Schema):
        self.table_id = table_id
        self.schema = schema
        self.columns = [
            Column(rng, i, rng.choice(DATA_TYPES), self)
            for i in range(rng.randint(2, MAX_COLUMNS))
        ]
        self.num_rows = 0
        self.rename = 0

    def name(self) -> str:
        if self.rename:
            return naughtify(f"t-{self.table_id}-{self.rename}")
        return naughtify(f"t-{self.table_id}")

    def __str__(self) -> str:
        return f"{self.schema}.{identifier(self.name())}"

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
    base_object2: DBObject | None
    source_columns: list[Column]
    materialized: bool
    join_column: Column | None
    join_column2: Column | None
    assert_not_null: list[Column]
    schema: Schema

    def __init__(
        self,
        rng: random.Random,
        view_id: int,
        base_object: DBObject,
        base_object2: DBObject | None,
        schema: Schema,
    ):
        self.view_id = view_id
        self.base_object = base_object
        self.base_object2 = base_object2
        self.schema = schema
        all_columns = list(base_object.columns) + (
            list(base_object2.columns) if base_object2 else []
        )
        self.source_columns = [
            column
            for column in rng.sample(all_columns, k=rng.randint(1, len(all_columns)))
        ]
        self.columns = [copy(column) for column in self.source_columns]
        for column in self.columns:
            column.set_name(f"{column.name()}-{column.db_object}")
            column.db_object = self

        self.materialized = rng.choice([True, False])

        self.assert_not_null = (
            [
                column
                for column in rng.sample(
                    self.columns, k=rng.randint(1, len(self.columns))
                )
                if not column.nullable
            ]
            if self.materialized
            else []
        )

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

    def name(self) -> str:
        return naughtify(f"v-{self.view_id}")

    def __str__(self) -> str:
        return f"{self.schema}.{identifier(self.name())}"

    def create(self, exe: Executor) -> None:
        if self.materialized:
            query = "CREATE MATERIALIZED VIEW"
        else:
            query = "CREATE VIEW"
        columns_str = ", ".join(
            f"{source_column} AS {column.name(True)}"
            for source_column, column in zip(self.source_columns, self.columns)
        )

        query += f" {self}"

        if self.assert_not_null:
            assert_not_null_strs = [
                f"ASSERT NOT NULL {c.name(True)}" for c in self.assert_not_null
            ]
            query += f" WITH ({', '.join(assert_not_null_strs)})"

        query += f" AS SELECT {columns_str} FROM {self.base_object}"
        if self.base_object2:
            query += f" JOIN {self.base_object2}"
            if self.join_column2:
                query += " ON "
                # TODO: Generic expression generator
                if self.join_column2.data_type == TextTextMap:
                    query += f"map_length({self.join_column}) = map_length({self.join_column2})"
                else:
                    query += f"{self.join_column} = {self.join_column2}"
            else:
                query += " ON TRUE"

        exe.execute(query)
        query = f"CREATE DEFAULT INDEX ON {self}"
        exe.execute(query)


class WebhookColumn(Column):
    def __init__(
        self, name: str, data_type: type[DataType], nullable: bool, db_object: DBObject
    ):
        self._name = name
        self.data_type = data_type
        self.nullable = nullable
        self.db_object = db_object

    def name(self, in_query: bool = False) -> str:
        return identifier(self._name) if in_query else self._name


class WebhookSource(DBObject):
    source_id: int
    rename: int
    cluster: "Cluster"
    body_format: BodyFormat
    include_headers: bool
    explicit_include_headers: list[str]
    check: str | None
    schema: Schema

    def __init__(
        self, source_id: int, cluster: "Cluster", schema: Schema, rng: random.Random
    ):
        self.source_id = source_id
        self.cluster = cluster
        self.schema = schema
        self.rename = 0
        self.body_format = rng.choice([e for e in BodyFormat])
        self.include_headers = rng.choice([True, False])
        self.explicit_include_headers = []
        self.columns = [
            WebhookColumn(
                "body",
                self.body_format.to_data_type(),
                False,
                self,
            )
        ]

        if self.include_headers:
            self.columns.append(WebhookColumn("headers", TextTextMap, False, self))

        for i in range(rng.randint(0, MAX_INCLUDE_HEADERS)):
            # naughtify: UnicodeEncodeError: 'ascii' codec can't encode characters
            self.explicit_include_headers.append(f"ih{i}")
        self.columns += [
            WebhookColumn(include_header, Text, True, self)
            for include_header in self.explicit_include_headers
        ]

        self.check_expr = None
        if rng.choice([True, False]):
            # TODO: More general expressions, failing expressions
            self.check_expr = (
                "BODY = BODY AND map_length(HEADERS) = map_length(HEADERS)"
            )
        # TODO: CHECK WITH SECRET
        # TODO: NOT IN INCLUDE HEADERS

    def name(self) -> str:
        if self.rename:
            return naughtify(f"wh-{self.source_id}-{self.rename}")
        return naughtify(f"wh-{self.source_id}")

    def __str__(self) -> str:
        return f"{self.schema}.{identifier(self.name())}"

    def create(self, exe: Executor) -> None:
        query = f"CREATE SOURCE {self} IN CLUSTER {self.cluster} FROM WEBHOOK BODY FORMAT {self.body_format.name}"
        if self.include_headers:
            query += " INCLUDE HEADERS"
        for include_header in self.explicit_include_headers:
            query += f" INCLUDE HEADER {literal(include_header)} as {identifier(include_header)}"
        if self.check_expr:
            query += f" CHECK (WITH (BODY, HEADERS) {self.check_expr})"
        exe.execute(query)


class KafkaColumn(Column):
    def __init__(
        self, name: str, data_type: type[DataType], nullable: bool, db_object: DBObject
    ):
        self._name = name
        self.data_type = data_type
        self.nullable = nullable
        self.db_object = db_object

    def name(self, in_query: bool = False) -> str:
        return identifier(self._name) if in_query else self._name


class KafkaSource(DBObject):
    source_id: int
    cluster: "Cluster"
    executor: KafkaExecutor
    generator: Iterator[Transaction]
    lock: threading.Lock
    columns: list[KafkaColumn]
    schema: Schema

    def __init__(
        self,
        database: str,
        source_id: int,
        cluster: "Cluster",
        schema: Schema,
        ports: dict[str, int],
        rng: random.Random,
    ):
        self.source_id = source_id
        self.cluster = cluster
        self.schema = schema
        fields = []
        for i in range(rng.randint(1, 10)):
            fields.append(
                # naughtify: Invalid schema
                Field(f"key{i}", rng.choice(DATA_TYPES_FOR_AVRO), True)
            )
        for i in range(rng.randint(0, 20)):
            fields.append(Field(f"value{i}", rng.choice(DATA_TYPES_FOR_AVRO), False))
        self.columns = [
            KafkaColumn(field.name, field.data_type, False, self) for field in fields
        ]
        self.executor = KafkaExecutor(
            self.source_id, ports, fields, database, schema.name()
        )
        self.generator = rng.choice(list(WORKLOADS))(None).generate(fields)
        self.lock = threading.Lock()

    def name(self) -> str:
        return self.executor.table

    def __str__(self) -> str:
        return f"{self.schema}.{self.name()}"

    def create(self, exe: Executor) -> None:
        self.executor.create()


class KafkaSink(DBObject):
    sink_id: int
    rename: int
    cluster: "Cluster"
    schema: Schema
    envelope: str
    format: str
    key: str

    def __init__(
        self,
        sink_id: int,
        cluster: "Cluster",
        schema: Schema,
        base_object: DBObject,
        rng: random.Random,
    ):
        self.sink_id = sink_id
        self.cluster = cluster
        self.schema = schema
        self.base_object = base_object
        self.format = rng.choice(
            ["AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn", "JSON"]
        )
        self.envelope = (
            "UPSERT" if self.format == "JSON" else rng.choice(["DEBEZIUM", "UPSERT"])
        )
        if self.envelope == "UPSERT":
            key_cols = ", ".join(
                [
                    column.name(True)
                    for column in rng.sample(
                        base_object.columns, k=rng.randint(1, len(base_object.columns))
                    )
                ]
            )
            self.key = f"KEY ({key_cols}) NOT ENFORCED"
        else:
            self.key = ""
        self.rename = 0

    def name(self) -> str:
        if self.rename:
            return naughtify(f"sink-{self.sink_id}-{self.rename}")
        return naughtify(f"sink-{self.sink_id}")

    def __str__(self) -> str:
        return f"{self.schema}.{identifier(self.name())}"

    def create(self, exe: Executor) -> None:
        topic = f"sink_topic{self.sink_id}"
        query = f"CREATE SINK {self} IN CLUSTER {self.cluster} FROM {self.base_object} INTO KAFKA CONNECTION kafka_conn (TOPIC {topic}) {self.key} FORMAT {self.format} ENVELOPE {self.envelope}"
        exe.execute(query)


class PostgresColumn(Column):
    def __init__(
        self, name: str, data_type: type[DataType], nullable: bool, db_object: DBObject
    ):
        self._name = name
        self.data_type = data_type
        self.nullable = nullable
        self.db_object = db_object

    def name(self, in_query: bool = False) -> str:
        return identifier(self._name) if in_query else self._name


class PostgresSource(DBObject):
    source_id: int
    cluster: "Cluster"
    executor: PgExecutor
    generator: Iterator[Transaction]
    lock: threading.Lock
    columns: list[PostgresColumn]
    schema: Schema

    def __init__(
        self,
        database: str,
        source_id: int,
        cluster: "Cluster",
        schema: Schema,
        ports: dict[str, int],
        rng: random.Random,
    ):
        self.source_id = source_id
        self.cluster = cluster
        self.schema = schema
        fields = []
        for i in range(rng.randint(1, 10)):
            fields.append(
                # naughtify: Postgres column identifiers are escaped differently for postgres sources: key3_ЁЂЃЄЅІЇЈЉЊЋЌЍЎЏА gets "", but pg8000.native.identifier() doesn't
                Field(f"key{i}", rng.choice(DATA_TYPES_FOR_AVRO), True)
            )
        for i in range(rng.randint(0, 20)):
            fields.append(Field(f"value{i}", rng.choice(DATA_TYPES_FOR_AVRO), False))
        self.columns = [
            PostgresColumn(field.name, field.data_type, False, self) for field in fields
        ]
        self.executor = PgExecutor(
            self.source_id, ports, fields, database, schema.name()
        )
        self.generator = rng.choice(list(WORKLOADS))(None).generate(fields)
        self.lock = threading.Lock()

    def name(self) -> str:
        return self.executor.table

    def __str__(self) -> str:
        return f"{self.schema}.{self.name()}"

    def create(self, exe: Executor) -> None:
        self.executor.create()


class Role:
    role_id: int

    def __init__(self, role_id: int):
        self.role_id = role_id

    def __str__(self) -> str:
        return f"role{self.role_id}"

    def create(self, exe: Executor) -> None:
        exe.execute(f"CREATE ROLE {self}")


class ClusterReplica:
    replica_id: int
    size: str
    cluster: "Cluster"

    def __init__(self, replica_id: int, size: str, cluster: "Cluster"):
        self.replica_id = replica_id
        self.size = size
        self.cluster = cluster

    def name(self) -> str:
        return naughtify(f"r-{self.replica_id+1}")

    def __str__(self) -> str:
        return identifier(self.name())

    def create(self, exe: Executor) -> None:
        # TODO: More Cluster Replica settings
        exe.execute(
            f"CREATE CLUSTER REPLICA {self.cluster}.{self} SIZE = '{self.size}'"
        )


class Cluster:
    cluster_id: int
    managed: bool
    size: str
    replicas: list[ClusterReplica]
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

    def name(self) -> str:
        return naughtify(f"cluster-{self.cluster_id}")

    def __str__(self) -> str:
        return identifier(self.name())

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
    complexity: Complexity
    scenario: Scenario
    host: str
    ports: dict[str, int]
    schemas: list[Schema]
    schema_id: int
    tables: list[Table]
    table_id: int
    views: list[View]
    view_id: int
    roles: list[Role]
    role_id: int
    clusters: list[Cluster]
    cluster_id: int
    indexes: set[str]
    webhook_sources: list[WebhookSource]
    webhook_source_id: int
    kafka_sources: list[KafkaSource]
    kafka_source_id: int
    postgres_sources: list[PostgresSource]
    postgres_source_id: int
    kafka_sinks: list[KafkaSink]
    kafka_sink_id: int
    lock: threading.Lock

    def __init__(
        self,
        rng: random.Random,
        seed: str,
        host: str,
        ports: dict[str, int],
        complexity: Complexity,
        scenario: Scenario,
        naughty_identifiers: bool,
    ):
        global NAUGHTY_IDENTIFIERS
        self.seed = seed
        self.host = host
        self.ports = ports
        self.complexity = complexity
        self.scenario = scenario
        NAUGHTY_IDENTIFIERS = naughty_identifiers

        self.schemas = [
            Schema(rng, i) for i in range(rng.randint(1, MAX_INITIAL_SCHEMAS))
        ]
        self.schema_id = len(self.schemas)
        self.tables = [
            Table(rng, i, rng.choice(self.schemas))
            for i in range(rng.randint(2, MAX_INITIAL_TABLES))
        ]
        self.table_id = len(self.tables)
        self.views = []
        for i in range(rng.randint(2, MAX_INITIAL_VIEWS)):
            # Only use tables for now since LIMIT 1 and statement_timeout are
            # not effective yet at preventing long-running queries and OoMs.
            base_object = rng.choice(self.tables)
            base_object2: Table | None = rng.choice(self.tables)
            if rng.choice([True, False]) or base_object2 == base_object:
                base_object2 = None
            view = View(rng, i, base_object, base_object2, rng.choice(self.schemas))
            self.views.append(view)
        self.view_id = len(self.views)
        self.roles = [Role(i) for i in range(rng.randint(0, MAX_INITIAL_ROLES))]
        self.role_id = len(self.roles)
        # At least one storage cluster required for WebhookSources
        self.clusters = [
            Cluster(
                i,
                managed=rng.choice([True, False]),
                size=rng.choice(["1", "2", "4"]),
                replication_factor=1,
                introspection_interval=rng.choice(["0", "1s", "10s"]),
            )
            for i in range(rng.randint(1, MAX_INITIAL_CLUSTERS))
        ]
        self.cluster_id = len(self.clusters)
        self.indexes = set()
        self.webhook_sources = [
            WebhookSource(i, rng.choice(self.clusters), rng.choice(self.schemas), rng)
            for i in range(rng.randint(0, MAX_INITIAL_WEBHOOK_SOURCES))
        ]
        self.webhook_source_id = len(self.webhook_sources)
        self.kafka_sources = [
            KafkaSource(
                self.name(),
                i,
                rng.choice(self.clusters),
                rng.choice(self.schemas),
                ports,
                rng,
            )
            for i in range(rng.randint(0, MAX_INITIAL_KAFKA_SOURCES))
        ]
        self.kafka_source_id = len(self.kafka_sources)
        self.postgres_sources = [
            PostgresSource(
                self.name(),
                i,
                rng.choice(self.clusters),
                rng.choice(self.schemas),
                ports,
                rng,
            )
            for i in range(rng.randint(0, MAX_INITIAL_POSTGRES_SOURCES))
        ]
        self.postgres_source_id = len(self.postgres_sources)
        self.kafka_sinks = [
            KafkaSink(
                i,
                rng.choice(self.clusters),
                rng.choice(self.schemas),
                rng.choice(self.db_objects_without_views()),
                rng,
            )
            for i in range(rng.randint(0, MAX_INITIAL_KAFKA_SINKS))
        ]
        self.kafka_sink_id = len(self.kafka_sinks)
        self.lock = threading.Lock()

    def name(self) -> str:
        return naughtify(f"db-pw-{self.seed}")

    def __str__(self) -> str:
        return identifier(self.name())

    def db_objects(
        self,
    ) -> list[WebhookSource | PostgresSource | KafkaSource | View | Table]:
        return (
            self.tables
            + self.views
            + self.kafka_sources
            + self.postgres_sources
            + self.webhook_sources
        )

    def db_objects_without_views(
        self,
    ) -> list[WebhookSource | PostgresSource | KafkaSource | View | Table]:
        return [
            obj for obj in self.db_objects() if type(obj) != View or obj.materialized
        ]

    def __iter__(self):
        """Returns all relations"""
        return (
            self.schemas + self.clusters + self.roles + self.db_objects()
        ).__iter__()

    def drop(self, exe: Executor) -> None:
        exe.execute(f"DROP DATABASE IF EXISTS {self}")

    def create(self, exe: Executor) -> None:
        self.drop(exe)
        exe.execute(f"CREATE DATABASE {self}")

    def create_relations(self, exe: Executor) -> None:
        exe.execute("SELECT name FROM mz_clusters WHERE name LIKE 'c%'")
        for row in exe.cur.fetchall():
            exe.execute(f"DROP CLUSTER {row[0]} CASCADE")

        exe.execute("SELECT name FROM mz_roles WHERE name LIKE 'r%'")
        for row in exe.cur.fetchall():
            exe.execute(f"DROP ROLE {row[0]}")

        exe.execute("CREATE CONNECTION kafka_conn FOR KAFKA BROKER 'kafka:9092'")
        exe.execute(
            "CREATE CONNECTION csr_conn FOR CONFLUENT SCHEMA REGISTRY URL 'http://schema-registry:8081'"
        )
        print("Created connections")

        exe.execute("CREATE SECRET pgpass AS 'postgres'")
        exe.execute(
            "CREATE CONNECTION postgres_conn FOR POSTGRES HOST 'postgres', DATABASE postgres, USER postgres, PASSWORD SECRET pgpass"
        )

        for relation in self:
            relation.create(exe)
