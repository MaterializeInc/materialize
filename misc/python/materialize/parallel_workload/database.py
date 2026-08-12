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
import uuid
from collections.abc import Iterator
from enum import Enum

from pg8000.native import identifier, literal

from materialize.data_ingest.data_type import (
    DATA_TYPES_FOR_AVRO,
    DATA_TYPES_FOR_COLUMNS,
    DATA_TYPES_FOR_KEY,
    DATA_TYPES_FOR_MYSQL,
    DATA_TYPES_FOR_SQL_SERVER,
    NUMBER_TYPES,
    Boolean,
    Bytea,
    DataType,
    Jsonb,
    Long,
    Text,
    TextTextMap,
)
from materialize.data_ingest.definition import Insert
from materialize.data_ingest.executor import (
    KafkaExecutor,
    MySqlExecutor,
    PgExecutor,
    SqlServerExecutor,
)
from materialize.data_ingest.field import Field
from materialize.data_ingest.transaction import Transaction
from materialize.data_ingest.workload import WORKLOADS
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.helpers.iceberg import setup_polaris_for_iceberg
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.parallel_workload.column import (
    Column,
    KafkaColumn,
    LoadGeneratorColumn,
    MySqlColumn,
    PostgresColumn,
    SqlServerColumn,
    WebhookColumn,
    naughtify,
    set_naughty_identifiers,
)
from materialize.parallel_workload.executor import Executor
from materialize.parallel_workload.expression import ExprKind, expression
from materialize.parallel_workload.settings import Complexity, Scenario

MAX_COLUMNS = 50
MAX_INCLUDE_HEADERS = 5
# The row count a table is held at. Measured with a view whose join was a cross
# product, one `COPY (SELECT * FROM view WHERE .. LIMIT 100) TO 's3://..'` over
# 500-row bases cost ~450 MiB of replica RSS with only four columns, and pw
# tables carry up to MAX_COLUMNS of them. A handful of such reads at once is
# what grew a clusterd to 19 GiB and had the kernel OOM-killer take it, plus
# environmentd and the Kafka broker, out of the one cgroup all of a run's
# containers share (nightlies 17660-17701). 100 keeps enough rows for the DML,
# persist and index paths to be interesting. See also JOIN_KEY_TYPES, which
# keeps a view's join from squaring this in the first place.
MAX_ROWS = 100
MAX_CLUSTERS = 4
MAX_CLUSTER_REPLICAS = 2
MAX_DBS = 50
MAX_SCHEMAS = 50
MAX_TABLES = 50
MAX_VIEWS = 150
MAX_INDEXES = 150
MAX_ROLES = 150
MAX_WEBHOOK_SOURCES = 50
MAX_KAFKA_SOURCES = 50
MAX_MYSQL_SOURCES = 50
MAX_SQL_SERVER_SOURCES = 50
MAX_POSTGRES_SOURCES = 50
MAX_LOADGEN_SOURCES = 50
MAX_KAFKA_SINKS = 50
MAX_ICEBERG_SINKS = 50
MAX_TYPES = 50
MAX_NETWORK_POLICIES = 30

MAX_INITIAL_DBS = 1
MAX_INITIAL_SCHEMAS = 1
MAX_INITIAL_TABLES = 2
MAX_INITIAL_VIEWS = 2
MAX_INITIAL_ROLES = 1
MAX_INITIAL_WEBHOOK_SOURCES = 1
MAX_INITIAL_KAFKA_SOURCES = 1
MAX_INITIAL_MYSQL_SOURCES = 1
MAX_INITIAL_SQL_SERVER_SOURCES = 1
MAX_INITIAL_POSTGRES_SOURCES = 1
MAX_INITIAL_LOADGEN_SOURCES = 1

# Types eligible as the equality anchor of a two-base view's join, see
# `View.join_key_expr`. A type qualifies when Materialize has `=` for it (map
# has none) and, less obviously, when its random values collide often enough
# that the equality still lets rows through: every type here draws one of a
# handful of sentinels (the type's min or max, 0, 1, one of five fixed strings)
# about a fifth of the time, so on the order of a percent of the cross product
# survives. An equality on uuid or timestamp columns would bound the join just
# as well but always produce an empty view, which tests nothing.
JOIN_KEY_TYPES = NUMBER_TYPES + [Text]


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


class DB:
    seed: str
    db_id: int
    lock: threading.Lock

    def __init__(self, seed: str, db_id: int):
        self.seed = seed
        self.db_id = db_id
        self.lock = threading.Lock()

    def name(self) -> str:
        return naughtify(f"db-pw-{self.seed}-{self.db_id}")

    def __str__(self) -> str:
        return identifier(self.name())

    def create(self, exe: Executor) -> None:
        exe.execute(f"CREATE DATABASE {self}")

    def drop(self, exe: Executor) -> None:
        exe.execute(f"DROP DATABASE IF EXISTS {self}")


class Schema:
    schema_id: int
    rename: int
    db: DB
    lock: threading.Lock

    def __init__(self, db: DB, schema_id: int):
        self.schema_id = schema_id
        self.db = db
        self.rename = 0
        self.lock = threading.Lock()

    def name(self) -> str:
        if self.rename:
            return naughtify(f"s-{self.schema_id}-{self.rename}")
        return naughtify(f"s-{self.schema_id}")

    def __str__(self) -> str:
        return f"{self.db}.{identifier(self.name())}"

    def create(self, exe: Executor) -> None:
        query = f"CREATE SCHEMA {self}"
        exe.execute(query)


class MzTempSchema(Schema):
    def __init__(self, db: DB):
        self.db = db
        self.lock = threading.Lock()

    def name(self) -> str:
        return "mz_temp"


class DBObject:
    columns: list[Column]
    lock: threading.Lock
    # Whether reading from this object can legitimately reach the empty
    # (sealed) frontier: its own shard seals in normal operation, or a
    # dataflow reading it sees an input frontier that becomes empty. Plain
    # tables and unbounded sources never seal, so the default is False.
    # Bounded (UP TO) load generators seal once they finish, and views
    # propagate sealing from their inputs, materialized or not.
    can_seal: bool = False

    def __init__(self):
        self.lock = threading.Lock()

    def name(self) -> str:
        raise NotImplementedError

    def create(self, exe: Executor) -> None:
        raise NotImplementedError


class Table(DBObject):
    table_id: int
    rename: int
    num_rows: int
    schema: Schema
    temp: bool

    def __init__(
        self, rng: random.Random, table_id: int, schema: Schema, temp: bool = False
    ):
        super().__init__()
        self.table_id = table_id
        self.schema = schema
        self.columns = [
            Column(rng, i, rng.choice(DATA_TYPES_FOR_COLUMNS), self)
            for i in range(rng.randint(2, MAX_COLUMNS))
        ]
        self.num_rows = 0
        self.rename = 0
        self.temp = temp

    def name(self) -> str:
        if self.rename:
            return naughtify(f"t-{self.table_id}-{self.rename}")
        return naughtify(f"t-{self.table_id}")

    def __str__(self) -> str:
        return f"{self.schema}.{identifier(self.name())}"

    def create(self, exe: Executor) -> None:
        query = "CREATE "
        if self.temp:
            query += "TEMP "
        query += f"TABLE {self}("
        query += ",\n    ".join(column.create() for column in self.columns)
        query += ")"
        exe.execute(query)


class View(DBObject):
    view_id: int
    base_object: DBObject
    base_object2: DBObject | None
    expressions: list[str]
    columns: list[Column]
    materialized: bool
    assert_not_null: list[Column]
    rename: int
    schema: Schema
    refresh: str | None
    temp: bool
    target_replica: "ClusterReplica | None"

    def __init__(
        self,
        rng: random.Random,
        view_id: int,
        base_object: DBObject,
        base_object2: DBObject | None,
        schema: Schema,
        scenario: Scenario = Scenario.Regression,
        temp: bool = False,
    ):
        super().__init__()
        self.rename = 0
        self.view_id = view_id
        # In the `RepeatRow` scenario, ~5% of views wrap their body with a
        # `repeat_row(-1)` cross join (constant-folded into a `Constant` MIR
        # node with negative diffs), and another ~5% are entirely replaced by
        # a hardcoded body that drives `repeat_row` from a column of the
        # `repeat_row_source` table (so the count is non-constant and can be
        # negative at runtime). The two modes are mutually exclusive. The
        # matching ignore list for negative-accumulation errors is wired
        # through `Action.errors_to_ignore`.
        self.repeat_row_const = scenario == Scenario.RepeatRow and rng.random() < 0.05
        self.repeat_row_table = (
            scenario == Scenario.RepeatRow
            and not self.repeat_row_const
            and rng.random() < 0.05
        )
        self.base_object = base_object
        self.base_object2 = base_object2
        self.schema = schema
        self.temp = temp
        self.target_replica = None
        all_columns = list(base_object.columns) + (
            list(base_object2.columns) if base_object2 else []
        )
        self.data_types = [
            rng.choice(list(DATA_TYPES_FOR_COLUMNS))
            for i in range(rng.randint(1, MAX_COLUMNS))
        ]
        self.expressions = [
            expression(data_type, all_columns, rng, kind=ExprKind.MATERIALIZABLE)
            for data_type in self.data_types
        ]
        self.columns = [
            Column(rng, i, data_type, self)
            for i, data_type in enumerate(self.data_types)
        ]

        self.materialized = not self.temp and rng.choice([True, False])

        self.refresh = (
            rng.choice(
                [
                    "ON COMMIT",
                    # TODO: Restore minute-scale intervals when CPU-196 is fixed
                    f"EVERY '{rng.randint(1, 15)} seconds'",
                    f"EVERY '{rng.randint(1, 15)} seconds' ALIGNED TO (mz_now())",
                    # Always in the future of all refreshes of previously generated MVs
                    "AT mz_now()::string::int8 + 1000",
                ]
            )
            if self.materialized
            else None
        )

        # A materialized view's shard seals (its write frontier advances to the
        # empty frontier) once it can never produce more output. That happens
        # for REFRESH AT views after their last refresh, for repeat_row(-1)
        # constant views on hydration, and transitively for any view that reads
        # from an input that itself seals. The replacement and sealed-shard
        # oracles key off this to tell legitimate seals from wrongly finalized
        # shards. Unmaterialized views have no shard, but a dataflow reading
        # one inlines its inputs, so sealing must propagate through them too.
        self.can_seal = (
            (self.refresh or "").startswith("AT")
            or self.repeat_row_const
            or base_object.can_seal
            or (base_object2 is not None and base_object2.can_seal)
        )

        if base_object2:
            # The random boolean alone references an arbitrary subset of the
            # columns, so it usually names only one side or neither, and the
            # join plans as a cross product whose output squares MAX_ROWS. The
            # equality anchor makes the output linear in the input instead.
            # Cross joins stay covered by the ad-hoc SELECTs, which carry a
            # LIMIT the optimizer can push into the join.
            join_key = self.join_key_expr(base_object, base_object2, rng)
            predicate = expression(
                Boolean, all_columns, rng, kind=ExprKind.MATERIALIZABLE
            )
            self.on_expr = f"{join_key} AND ({predicate})"

        self.union = rng.random() < 0.1
        if self.union:
            self.expressions2 = [
                expression(data_type, all_columns, rng, kind=ExprKind.MATERIALIZABLE)
                for data_type in self.data_types
            ]

        if self.repeat_row_table:
            # Replace the randomly generated shape with a hardcoded single
            # `bigint` column matching the body emitted by `get_select`. The
            # body reads `diff` from `repeat_row_source` and feeds it as the
            # `repeat_row` count, so the view's accumulation depends on the
            # current contents of that table.
            self.data_types = [Long]
            self.columns = [Column(rng, 0, Long, self)]
            self.union = False

    @staticmethod
    def join_key_expr(left: DBObject, right: DBObject, rng: random.Random) -> str:
        """An equality between one column of each side, so that the join has a
        key and its output stays linear in the input.

        Matches the join shape `SelectAction.generate_select_query` emits, with
        two additions. A `JOIN_KEY_TYPES` pair wins over an equally valid pair of
        some other type, whose values would never collide and leave the view
        permanently empty. And a pair of objects sharing no column type at all,
        which a query can answer by not joining but a two-base view cannot, falls
        back to comparing the columns as text. That bounds the join just as well,
        only with a near-empty result."""
        candidates = [
            (left_column, right_column)
            for left_column in left.columns
            for right_column in right.columns
            # Materialize has no `=` for map.
            if left_column.data_type is right_column.data_type
            and left_column.data_type is not TextTextMap
        ]
        colliding = [pair for pair in candidates if pair[0].data_type in JOIN_KEY_TYPES]
        if candidates:
            left_column, right_column = rng.choice(colliding or candidates)
            return f"{left_column} = {right_column}"
        return f"{rng.choice(left.columns)}::text = {rng.choice(right.columns)}::text"

    def name(self) -> str:
        if self.rename:
            return naughtify(f"v-{self.view_id}-{self.rename}")
        return naughtify(f"v-{self.view_id}")

    def __str__(self) -> str:
        return f"{self.schema}.{identifier(self.name())}"

    def get_select(self) -> str:
        if self.repeat_row_table:
            # Hardcoded body that drives `repeat_row` from a column whose
            # value is determined at runtime, so the count can be negative
            # depending on the current contents of `repeat_row_source`.
            col = self.columns[0].name(True)
            return (
                f"SELECT r.diff::bigint AS {col} "
                "FROM materialize.public.repeat_row_source r, repeat_row(r.diff)"
            )

        def select_str(exprs: str) -> str:
            select = f"SELECT {exprs} FROM {self.base_object}"
            if self.base_object2:
                select += f" JOIN {self.base_object2} ON {self.on_expr}"
            if self.repeat_row_const:
                # `repeat_row(-1)` retracts each input row exactly once,
                # producing a collection with a net-negative accumulation.
                # Hardcoded to `-1` to avoid blowing up the data size.
                select = f"SELECT * FROM ({select}) AS rr_inner, repeat_row(-1)"
            return select

        expressions_str = ", ".join(
            [
                f"({expression})::{data_type.name()} AS {column.name(True)}"
                for expression, data_type, column in zip(
                    self.expressions, self.data_types, self.columns
                )
            ]
        )

        query = select_str(expressions_str)

        if self.union:
            expressions_str2 = ", ".join(
                [
                    f"({expression})::{data_type.name()} AS {column.name(True)}"
                    for expression, data_type, column in zip(
                        self.expressions2, self.data_types, self.columns
                    )
                ]
            )

            query += f" UNION ALL {select_str(expressions_str2)}"

        return query

    def create(self, exe: Executor, or_replace: bool = False) -> None:
        query = "CREATE "
        # OR REPLACE keeps the same catalog item, swapping its definition and
        # rebuilding the dataflow. It is incompatible with TEMP.
        if or_replace and not self.temp:
            query += "OR REPLACE "
        if self.temp:
            query += "TEMP "
        if self.materialized:
            query += "MATERIALIZED "
        query += f"VIEW {self}"

        if self.target_replica is not None:
            replica = self.target_replica
            query += f" IN CLUSTER {replica.cluster} REPLICA {replica}"

        options = []

        if self.refresh:
            options.append(f"REFRESH {self.refresh}")

        if options:
            query += f" WITH ({', '.join(options)})"

        query += f" AS {self.get_select()}"

        exe.execute(query)


class WebhookSource(DBObject):
    source_id: int
    rename: int
    cluster: "Cluster"
    body_format: BodyFormat
    include_headers: bool
    explicit_include_headers: list[str]
    check: str | None
    schema: Schema
    num_rows: int

    def __init__(
        self, source_id: int, cluster: "Cluster", schema: Schema, rng: random.Random
    ):
        super().__init__()
        self.source_id = source_id
        self.cluster = cluster
        self.schema = schema
        self.rename = 0
        self.body_format = rng.choice([e for e in BodyFormat])
        self.include_headers = rng.choice([True, False])
        self.explicit_include_headers = []
        self.num_rows = 0
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
        # for testing now() in check
        if rng.choice([True, False]):
            self.explicit_include_headers.append("timestamp")
        self.columns += [
            WebhookColumn(include_header, Text, True, self)
            for include_header in self.explicit_include_headers
        ]

        self.check_expr = None
        if rng.choice([True, False]):
            # TODO: More general expressions, failing expressions
            exprs = [
                "BODY = BODY",
                "map_length(HEADERS) = map_length(HEADERS)",
            ]
            if "timestamp" in self.explicit_include_headers:
                exprs.append(
                    "(headers->'timestamp'::text)::timestamp + INTERVAL '10s' >= now()"
                )
            self.check_expr = " AND ".join(
                rng.sample(exprs, k=rng.randint(1, len(exprs)))
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
        source_id: int,
        cluster: "Cluster",
        schema: Schema,
        ports: dict[str, int],
        rng: random.Random,
    ):
        super().__init__()
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
            # Value columns are randomly nullable so their Avro type becomes a
            # `["null", T]` union, exercising union schema resolution in the
            # source decode path. Keys stay non-nullable.
            fields.append(
                Field(
                    f"value{i}",
                    rng.choice(DATA_TYPES_FOR_AVRO),
                    False,
                    nullable=rng.choice([True, False]),
                )
            )
        self.columns = [
            KafkaColumn(field.name, field.data_type, field.nullable, self)
            for field in fields
        ]
        # Sometimes evolve the schema before creating the source: write records
        # under a narrower (promotable) writer schema first, so the source must
        # promote them through union schema resolution. See
        # KafkaExecutor._write_promotion_preamble.
        self.executor = KafkaExecutor(
            self.source_id,
            ports,
            fields,
            schema.db.name(),
            schema.name(),
            cluster.name(),
            evolve_schema=rng.choice([True, False]),
        )
        workload = rng.choice(list(WORKLOADS))(azurite=False)
        for transaction_def in workload.cycle:
            for definition in transaction_def.operations:
                if type(definition) == Insert and definition.count > MAX_ROWS:
                    definition.count = 100
        self.generator = workload.generate(fields)
        self.lock = threading.Lock()

    def name(self) -> str:
        return self.executor.table

    def __str__(self) -> str:
        return f"{self.schema}.{self.name()}"

    def create(self, exe: Executor) -> None:
        self.executor.create(logging_exe=exe)


class IcebergSink(DBObject):
    sink_id: int
    rename: int
    cluster: "Cluster"
    schema: Schema
    base_object: DBObject
    mode: str
    key: str
    table_name: str

    def __init__(
        self,
        sink_id: int,
        cluster: "Cluster",
        schema: Schema,
        base_object: DBObject,
        rng: random.Random,
    ):
        super().__init__()
        self.sink_id = sink_id
        self.cluster = cluster
        self.schema = schema
        self.base_object = base_object
        self.mode = rng.choice(["UPSERT", "APPEND"])
        if self.mode == "UPSERT":
            key_cols = [
                column
                for column in rng.sample(
                    base_object.columns, k=rng.randint(1, len(base_object.columns))
                )
            ]
            key_col_names = [column.name(True) for column in key_cols]
            self.key = f"KEY ({', '.join(key_col_names)}) NOT ENFORCED"
        else:
            # APPEND mode does not permit a KEY.
            self.key = ""
        self.table_name = f"icesink_topic{self.sink_id}_{uuid.uuid4().hex[:8]}"
        self.rename = 0

    def name(self) -> str:
        if self.rename:
            return naughtify(f"icesink-{self.sink_id}-{self.rename}")
        return naughtify(f"icesink-{self.sink_id}")

    def __str__(self) -> str:
        return f"{self.schema}.{identifier(self.name())}"

    def create(self, exe: Executor) -> None:
        query = f"CREATE SINK {self} IN CLUSTER {self.cluster} FROM {self.base_object} INTO ICEBERG CATALOG CONNECTION polaris_conn (NAMESPACE 'default_namespace', TABLE '{self.table_name}') USING AWS CONNECTION aws_conn {self.key} MODE {self.mode} WITH (COMMIT INTERVAL '1s')"
        exe.execute(query)


class KafkaSink(DBObject):
    sink_id: int
    rename: int
    cluster: "Cluster"
    schema: Schema
    base_object: DBObject
    envelope: str
    key: str
    connection_options: list[str]
    no_snapshot: bool

    def __init__(
        self,
        sink_id: int,
        cluster: "Cluster",
        schema: Schema,
        base_object: DBObject,
        rng: random.Random,
    ):
        super().__init__()
        self.sink_id = sink_id
        self.cluster = cluster
        self.schema = schema
        self.base_object = base_object
        self.connection_options = []
        if rng.random() < 0.3:
            compression = rng.choice(["none", "gzip", "lz4", "zstd", "snappy"])
            self.connection_options.append(f"COMPRESSION TYPE = '{compression}'")
        if rng.random() < 0.2:
            self.connection_options.append(
                f"TRANSACTIONAL ID PREFIX 'pw-txn-{self.sink_id}'"
            )
        if rng.random() < 0.2:
            self.connection_options.append(
                f"PROGRESS GROUP ID PREFIX 'pw-progress-{self.sink_id}'"
            )
        self.no_snapshot = rng.random() < 0.2
        universal_formats = [
            "FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn",
            "FORMAT JSON",
        ]
        single_column_formats = ["FORMAT BYTES", "FORMAT TEXT"]
        formats = universal_formats.copy()
        if len(base_object.columns) == 1:
            formats.extend(single_column_formats)
        self.format = rng.choice(formats)
        self.envelope = rng.choice(["DEBEZIUM", "UPSERT"])
        if self.envelope == "UPSERT" or rng.choice([True, False]):
            key_cols = [
                column
                for column in rng.sample(
                    base_object.columns, k=rng.randint(1, len(base_object.columns))
                )
            ]
            key_col_names = [column.name(True) for column in key_cols]
            self.key = f"KEY ({', '.join(key_col_names)}) NOT ENFORCED"

            potential_partition_keys = [
                key_col for key_col in key_cols if key_col.data_type in NUMBER_TYPES
            ]
            if potential_partition_keys:
                self.partition_key = rng.choice(potential_partition_keys).name(True)
                self.partition_count = rng.randint(1, 10)
            else:
                self.partition_count = 0

            if rng.choice([True, False]):
                key_formats = universal_formats.copy()
                if len(key_cols) == 1:
                    key_formats.extend(single_column_formats)
                value_formats = universal_formats.copy()
                if len(base_object.columns) == 1:
                    value_formats.extend(single_column_formats)
                self.format = (
                    f"KEY {rng.choice(key_formats)} VALUE {rng.choice(value_formats)}"
                )
        else:
            self.key = ""
            self.partition_count = 0
        self.rename = 0

    def name(self) -> str:
        if self.rename:
            return naughtify(f"kafkasink-{self.sink_id}-{self.rename}")
        return naughtify(f"kafkasink-{self.sink_id}")

    def __str__(self) -> str:
        return f"{self.schema}.{identifier(self.name())}"

    def create(self, exe: Executor) -> None:
        topic = f"kafkasink_topic{self.sink_id}"
        maybe_partition = (
            f", TOPIC PARTITION COUNT {self.partition_count}, PARTITION BY {self.partition_key}"
            if self.partition_count
            else ""
        )
        options = "".join(f", {option}" for option in self.connection_options)
        query = f"CREATE SINK {self} IN CLUSTER {self.cluster} FROM {self.base_object} INTO KAFKA CONNECTION kafka_conn (TOPIC {topic}{maybe_partition}{options}) {self.key} {self.format} ENVELOPE {self.envelope}"
        if self.no_snapshot:
            query += " WITH (SNAPSHOT = false)"
        exe.execute(query)


class MySqlSource(DBObject):
    source_id: int
    cluster: "Cluster"
    executor: MySqlExecutor
    generator: Iterator[Transaction]
    lock: threading.Lock
    columns: list[MySqlColumn]
    schema: Schema

    def __init__(
        self,
        source_id: int,
        cluster: "Cluster",
        schema: Schema,
        ports: dict[str, int],
        rng: random.Random,
    ):
        super().__init__()
        self.source_id = source_id
        self.cluster = cluster
        self.schema = schema
        # Bias toward a single-column key: only tables with a single-column
        # integer primary key take the parallel PK-range snapshot path, the
        # rest fall back to a whole-table read by one worker.
        num_keys = rng.choice([1, 1, 1, 2, rng.randint(2, 10)])
        # Rows inserted upstream before CREATE SOURCE so the initial snapshot
        # has data to read and split across workers. Kept within the SMALLINT
        # range because every key column receives the row's negated sequence
        # number.
        self.prepopulate_rows = rng.choice([0, 100, rng.randint(1000, 30000)])
        fields = []
        for i in range(num_keys):
            fields.append(
                # naughtify: MySql column identifiers are escaped differently for MySql sources: key3_ЁЂЃЄЅІЇЈЉЊЋЌЍЎЏА gets "", but pg8000.native.identifier() doesn't
                Field(f"key{i}", rng.choice(DATA_TYPES_FOR_KEY), True)
            )
        for i in range(rng.randint(0, 20)):
            fields.append(Field(f"value{i}", rng.choice(DATA_TYPES_FOR_MYSQL), False))
        self.columns = [
            MySqlColumn(field.name, field.data_type, False, self) for field in fields
        ]
        self.executor = MySqlExecutor(
            self.source_id,
            ports,
            fields,
            schema.db.name(),
            schema.name(),
            cluster.name(),
        )
        self.generator = rng.choice(list(WORKLOADS))(azurite=False).generate(fields)
        self.lock = threading.Lock()

    def name(self) -> str:
        return self.executor.table

    def __str__(self) -> str:
        return f"{self.schema}.{self.name()}"

    def create(self, exe: Executor) -> None:
        self.executor.create(logging_exe=exe, prepopulate_rows=self.prepopulate_rows)


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
        source_id: int,
        cluster: "Cluster",
        schema: Schema,
        ports: dict[str, int],
        rng: random.Random,
    ):
        super().__init__()
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
            self.source_id,
            ports,
            fields,
            schema.db.name(),
            schema.name(),
            cluster.name(),
        )
        self.generator = rng.choice(list(WORKLOADS))(azurite=False).generate(fields)
        self.lock = threading.Lock()

    def name(self) -> str:
        return self.executor.table

    def __str__(self) -> str:
        return f"{self.schema}.{self.name()}"

    def create(self, exe: Executor) -> None:
        self.executor.create(logging_exe=exe)


class SqlServerSource(DBObject):
    source_id: int
    cluster: "Cluster"
    executor: SqlServerExecutor
    generator: Iterator[Transaction]
    lock: threading.Lock
    columns: list[SqlServerColumn]
    schema: Schema

    def __init__(
        self,
        source_id: int,
        cluster: "Cluster",
        schema: Schema,
        ports: dict[str, int],
        rng: random.Random,
        composition: Composition,
    ):
        super().__init__()
        self.source_id = source_id
        self.cluster = cluster
        self.schema = schema
        fields = []
        for i in range(rng.randint(1, 10)):
            fields.append(Field(f"key{i}", rng.choice(DATA_TYPES_FOR_KEY), True))
        for i in range(rng.randint(0, 20)):
            fields.append(
                Field(f"value{i}", rng.choice(DATA_TYPES_FOR_SQL_SERVER), False)
            )
        self.columns = [
            SqlServerColumn(field.name, field.data_type, False, self)
            for field in fields
        ]
        self.executor = SqlServerExecutor(
            self.source_id,
            ports,
            fields,
            schema.db.name(),
            schema.name(),
            cluster.name(),
            composition=composition,
        )
        self.generator = rng.choice(list(WORKLOADS))(azurite=False).generate(fields)
        self.lock = threading.Lock()

    def name(self) -> str:
        return self.executor.table

    def __str__(self) -> str:
        return f"{self.schema}.{self.name()}"

    def create(self, exe: Executor) -> None:
        self.executor.create(logging_exe=exe)


class LoadGeneratorSource(DBObject):
    """A COUNTER load generator source. Always bounded by UP TO so it cannot
    grow without limit, which also exercises the finished-source lifecycle
    state. The readable object (str(self)) is the table created from the
    source, matching the source-table model the other sources use."""

    # A finished (UP TO reached) counter advances the source table's frontier
    # to the empty antichain, so anything reading it seals legitimately.
    can_seal = True

    source_id: int
    cluster: "Cluster"
    schema: Schema
    columns: list[LoadGeneratorColumn]
    tick_interval: str
    up_to: int

    def __init__(
        self,
        source_id: int,
        cluster: "Cluster",
        schema: Schema,
        rng: random.Random,
    ):
        super().__init__()
        self.source_id = source_id
        self.cluster = cluster
        self.schema = schema
        self.tick_interval = rng.choice(["10ms", "100ms", "1s"])
        # Bounded near MAX_ROWS: this source-table is a readable relation that
        # views/MVs join over, and unlike user tables it is not capped by
        # MAX_ROWS. A large counter feeding a maintained join is a clusterd
        # memory risk, so keep it in the same size class as the other data.
        self.up_to = rng.randint(1, MAX_ROWS)
        self.columns = [LoadGeneratorColumn("counter", Long, False, self)]

    def name(self) -> str:
        return naughtify(f"lg-{self.source_id}")

    def source_name(self) -> str:
        return naughtify(f"lg-src-{self.source_id}")

    def __str__(self) -> str:
        return f"{self.schema}.{identifier(self.name())}"

    def create(self, exe: Executor) -> None:
        source = f"{self.schema}.{identifier(self.source_name())}"
        exe.execute(
            f"CREATE SOURCE {source} IN CLUSTER {self.cluster} FROM LOAD GENERATOR COUNTER (TICK INTERVAL '{self.tick_interval}', UP TO {self.up_to})"
        )
        exe.execute(f"CREATE TABLE {self} FROM SOURCE {source}")


class MultiLoadGeneratorSource:
    """A multi-subsource load generator (AUCTION / TPCH / MARKETING) created
    with FOR ALL TABLES. Tracked create/drop-only, not as a readable relation:
    its subsources have fixed names and hardcoded schemas, so wiring them into
    the general read/expression machinery is out of scope. The value is the
    multi-subsource source lifecycle and CASCADE teardown under concurrency.

    At most one source per generator type exists at a time, because FOR ALL
    TABLES names its subsources fixedly (a second AUCTION would collide on
    `auctions`, `bids`, ...)."""

    source_id: int
    cluster: "Cluster"
    schema: Schema
    generator: str
    tick_interval: str
    lock: threading.Lock

    def __init__(
        self,
        source_id: int,
        cluster: "Cluster",
        schema: Schema,
        generator: str,
        rng: random.Random,
    ):
        self.source_id = source_id
        self.cluster = cluster
        self.schema = schema
        self.generator = generator
        self.tick_interval = rng.choice(["100ms", "1s"])
        self.lock = threading.Lock()

    def name(self) -> str:
        return naughtify(f"mlg-{self.source_id}")

    def __str__(self) -> str:
        return f"{self.schema}.{identifier(self.name())}"

    def create(self, exe: Executor) -> None:
        options = [f"TICK INTERVAL '{self.tick_interval}'"]
        if self.generator == "TPCH":
            options.append("SCALE FACTOR 0.0001")
        query = (
            f"CREATE SOURCE {self} IN CLUSTER {self.cluster} "
            f"FROM LOAD GENERATOR {self.generator} ({', '.join(options)}) "
            f"FOR ALL TABLES"
        )
        exe.execute(query)


class S3Object(DBObject):
    """A COPY TO dump of `table` that CopyFromS3Action can load back into it.

    Only registered for verbatim (SELECT *) dumps of non-temp tables, so the
    file's column names and types match the table's."""

    key: str
    bucket: str
    format: str
    table: Table

    def __init__(
        self,
        key: str,
        bucket: str,
        format: str,
        table: Table,
    ):
        super().__init__()
        self.key = key
        self.bucket = bucket
        self.format = format
        self.table = table
        self.columns = table.columns

    def name(self) -> str:
        return f"{self.bucket}/{self.key}"

    def __str__(self) -> str:
        return self.name()


# A fixed name, never run through `naughtify`, in `materialize.public` rather
# than one of the workload's own databases: no action creates, renames, swaps or
# drops that database or schema, so the name resolves for a whole run and the
# end-of-run check is guaranteed to find the table.
READ_THEN_WRITE_COUNTER_NAME = "materialize.public.pw_rtw_counter"

# Error texts that prove an increment did not land.
#
# A concurrently modified dependency is reported when the coordinator rejects a
# statement whose dependency changed underneath it, before anything is appended:
# a plan it revalidates before sequencing, or a staged write that group commit
# drops because a concurrent ALTER TABLE left the rows stale against the table's
# current schema. The other error is a cluster-resolution failure during planning,
# which the workload provokes on purpose by pointing the default cluster at a
# nonexistent one, so the statement never reaches a write path at all. The trailing
# quote keeps it from matching "unknown cluster replica size" errors.
#
# Every other failure counts as unknown, a statement timeout and a cancellation
# included, because either can race a commit that did happen. A wrong entry here
# makes healthy runs fail, an unnecessary unknown only widens the upper bound.
DEFINITELY_NOT_COMMITTED_ERRORS = (
    "was concurrently modified",
    "unknown cluster '",
)


class ReadThenWriteCounter:
    """A single-row counter table that `ReadThenWriteCounterUpdateAction`
    increments with read-then-write UPDATEs, plus the tallies of the outcomes its
    workers saw.

    The table is registered in none of the `Database` object lists and lives
    outside the workload's own databases, so no other action writes to, renames,
    alters or drops it. That is what makes it an oracle: every increment of `v`
    comes from one recorded attempt, so

        definitely_committed <= v <= definitely_committed + unknown

    must hold at the end of a run. A violation means a lost update, an update
    applied twice, a success reported for a write that never landed, or an error
    reported for a write that did land. The lower bound is
    dropped in the backup-restore scenario, see `validate`.
    """

    lock: threading.Lock
    definitely_committed: int
    definitely_not_committed: int
    unknown: int

    def __init__(self):
        self.lock = threading.Lock()
        self.definitely_committed = 0
        self.definitely_not_committed = 0
        self.unknown = 0

    def __str__(self) -> str:
        return READ_THEN_WRITE_COUNTER_NAME

    def create(self, exe: Executor) -> None:
        """Creates and seeds the table, once per run.

        Assumes the harness has already granted all privileges on tables to
        PUBLIC, so that a worker which reconnected as a random role can still
        increment the counter."""
        exe.execute(f"DROP TABLE IF EXISTS {self} CASCADE")
        exe.execute(f"CREATE TABLE {self} (id int, v bigint)")
        exe.execute(f"INSERT INTO {self} VALUES (1, 0)")

    def drop(self, exe: Executor) -> None:
        exe.execute(f"DROP TABLE IF EXISTS {self} CASCADE")

    def record_committed(self) -> None:
        with self.lock:
            self.definitely_committed += 1

    def record_unknown(self) -> None:
        with self.lock:
            self.unknown += 1

    def record_failure(self, error: str) -> None:
        """Classifies a failed increment, see `DEFINITELY_NOT_COMMITTED_ERRORS`.
        An error this cannot classify counts as unknown."""
        if not any(known in error for known in DEFINITELY_NOT_COMMITTED_ERRORS):
            self.record_unknown()
            return
        with self.lock:
            self.definitely_not_committed += 1

    def validate(self, exe: Executor) -> None:
        """Checks the conservation invariant, raising if it is violated.

        Must run after all workers have joined, otherwise an in-flight increment
        can land between reading the tallies and reading the table."""
        with self.lock:
            committed = self.definitely_committed
            unknown = self.unknown
            tallies = (
                f"definitely_committed={committed} "
                f"definitely_not_committed={self.definitely_not_committed} "
                f"unknown={unknown}"
            )
        print(f"read-then-write counter tallies: {tallies}")

        # A restore rolls the whole instance back to the backup point, so
        # increments that committed after it are gone and the lower bound does
        # not hold in that scenario. Rolling back can only lose increments, never
        # invent them, so the upper bound holds everywhere and stays sharp.
        lower = 0 if exe.db.scenario == Scenario.BackupRestore else committed
        upper = committed + unknown

        # `FlipFlagsAction` points the system-wide default cluster at a
        # nonexistent one to hunt for panics, and it can still be doing that when
        # the run ends. This session inherits that default, so the read below
        # needs a cluster that is certainly there. quickstart is the only one the
        # harness guarantees: the workload creates and drops its own clusters,
        # but never that one.
        exe.execute("SET cluster = quickstart")
        exe.execute(f"SELECT id, v FROM {self}")
        rows = exe.cur.fetchall()
        if len(rows) != 1 or rows[0][0] != 1:
            message = (
                f"read-then-write counter table should hold exactly the seeded row "
                f"(1, v), "
                f"found {rows} ({tallies})"
            )
            print(f"+++ {message}")
            raise ValueError(message)
        value = rows[0][1]
        if not lower <= value <= upper:
            message = (
                f"read-then-write counter conservation invariant violated: "
                f"observed v={value}, "
                f"expected {lower} <= v <= {upper} ({tallies})"
            )
            print(f"+++ {message}")
            raise ValueError(message)
        print(
            f"read-then-write counter conservation invariant holds: "
            f"{lower} <= v={value} <= {upper}"
        )


class Index:
    _name: str
    schema: Schema
    lock: threading.Lock

    def __init__(self, name: str, schema: Schema):
        self._name = name
        # The index lives in the indexed object's schema. Referencing the
        # schema object (instead of a rendered string) keeps the reference
        # valid across schema renames.
        self.schema = schema
        self.lock = threading.Lock()

    def name(self) -> str:
        return self._name

    def __str__(self) -> str:
        return f"{self.schema}.{identifier(self.name())}"


class Role:
    role_id: int
    lock: threading.Lock

    def __init__(self, role_id: int):
        self.role_id = role_id
        self.lock = threading.Lock()

    def __str__(self) -> str:
        return f"role{self.role_id}"

    def create(self, exe: Executor) -> None:
        exe.execute(f"CREATE ROLE {self}")
        # Make the workload's own user (materialize) a member, so it can
        # manage the role, e.g. transfer object ownership to it: ALTER ..
        # OWNER TO <role> requires the executor to be a member of <role>.
        # The creating session has admin on the role it just created.
        exe.execute(f"GRANT {self} TO materialize")


class ClusterReplica:
    replica_id: int
    size: str
    cluster: "Cluster"
    rename: int
    lock: threading.Lock

    def __init__(self, replica_id: int, size: str, cluster: "Cluster"):
        self.replica_id = replica_id
        self.size = size
        self.cluster = cluster
        self.rename = 0
        self.lock = threading.Lock()

    def name(self) -> str:
        # A managed cluster's replicas are named by the controller as r1..rN,
        # never by us, so neither the rename counter nor naughtify applies.
        # Rendering our own name there would make every reference to the
        # replica, e.g. a replica-targeted materialized view, fail to resolve.
        if self.cluster.managed:
            return f"r{self.replica_id+1}"
        if self.rename:
            return naughtify(f"r-{self.replica_id+1}-{self.rename}")
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
    rename: int
    lock: threading.Lock

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
        self.rename = 0
        self.lock = threading.Lock()

    def name(self) -> str:
        if self.rename:
            return naughtify(f"cluster-{self.cluster_id}-{self.rename}")
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


class Type:
    """A user-defined type (row, list, or map). Schema-scoped."""

    type_id: int
    schema: Schema
    kind: str
    lock: threading.Lock
    rng: random.Random

    def __init__(self, type_id: int, schema: Schema, rng: random.Random):
        self.type_id = type_id
        self.schema = schema
        self.kind = rng.choice(["row", "list", "map"])
        self.rng = rng
        self.lock = threading.Lock()

    def name(self) -> str:
        return naughtify(f"type-{self.type_id}")

    def __str__(self) -> str:
        return f"{self.schema}.{identifier(self.name())}"

    def create(self, exe: Executor) -> None:
        if self.kind == "row":
            scalar_types = ["int4", "text", "bool", "float8", "timestamp"]
            fields = ", ".join(
                f"f{i} {self.rng.choice(scalar_types)}"
                for i in range(self.rng.randint(1, 4))
            )
            query = f"CREATE TYPE {self} AS ({fields})"
        elif self.kind == "list":
            element = self.rng.choice(["int4", "text", "float8"])
            query = f"CREATE TYPE {self} AS LIST (ELEMENT TYPE = {element})"
        else:
            value = self.rng.choice(["int4", "text", "float8"])
            query = f"CREATE TYPE {self} AS MAP (KEY TYPE = text, VALUE TYPE = {value})"
        exe.execute(query)


class NetworkPolicy:
    """A network policy. Top-level (not schema-scoped), like clusters.

    Rules are always allow-all so the workload can never lock itself out of
    its own connections. The policy is never installed as the active
    `network_policy` system parameter for the same reason."""

    policy_id: int
    num_rules: int
    lock: threading.Lock

    def __init__(self, policy_id: int, rng: random.Random):
        self.policy_id = policy_id
        self.num_rules = rng.randint(1, 3)
        self.lock = threading.Lock()

    def name(self) -> str:
        return naughtify(f"netpol-{self.policy_id}")

    def __str__(self) -> str:
        return identifier(self.name())

    def rules_clause(self) -> str:
        rules = ", ".join(
            f"r{i} (action='allow', direction='ingress', address='0.0.0.0/0')"
            for i in range(self.num_rules)
        )
        return f"RULES ({rules})"

    def create(self, exe: Executor) -> None:
        exe.execute(f"CREATE NETWORK POLICY {self} ({self.rules_clause()})")


# TODO: Can access both databases from same connection!
class Database:
    complexity: Complexity
    scenario: Scenario
    host: str
    ports: dict[str, int]
    dbs: list[DB]
    db_id: int
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
    indexes: set[Index]
    webhook_sources: list[WebhookSource]
    webhook_source_id: int
    kafka_sources: list[KafkaSource]
    kafka_source_id: int
    mysql_sources: list[MySqlSource]
    mysql_source_id: int
    postgres_sources: list[PostgresSource]
    postgres_source_id: int
    sql_server_sources: list[SqlServerSource]
    sql_server_source_id: int
    loadgen_sources: list[LoadGeneratorSource]
    loadgen_source_id: int
    multi_loadgen_sources: list[MultiLoadGeneratorSource]
    multi_loadgen_source_id: int
    iceberg_sinks: list[IcebergSink]
    iceberg_sink_id: int
    kafka_sinks: list[KafkaSink]
    kafka_sink_id: int
    types: list[Type]
    type_id: int
    network_policies: list[NetworkPolicy]
    network_policy_id: int
    s3_path: int
    s3_objects: list[S3Object]
    read_then_write_counter: ReadThenWriteCounter
    lock: threading.Lock
    seed: str
    sqlsmith_state: str
    flags: dict[str, str]
    num_threads: int

    def __init__(
        self,
        rng: random.Random,
        seed: str,
        host: str,
        ports: dict[str, int],
        complexity: Complexity,
        scenario: Scenario,
        naughty_identifiers: bool,
        num_threads: int,
    ):
        self.host = host
        self.ports = ports
        self.complexity = complexity
        self.scenario = scenario
        self.seed = seed
        self.num_threads = num_threads
        set_naughty_identifiers(naughty_identifiers)

        self.s3_path = 0
        self.dbs = [DB(seed, i) for i in range(rng.randint(1, MAX_INITIAL_DBS))]
        self.db_id = len(self.dbs)
        self.schemas = [
            Schema(rng.choice(self.dbs), i)
            for i in range(rng.randint(1, MAX_INITIAL_SCHEMAS))
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
            view = View(
                rng,
                i,
                base_object,
                base_object2,
                rng.choice(self.schemas),
                scenario=scenario,
            )
            self.views.append(view)
        self.view_id = len(self.views)
        self.roles = [Role(i) for i in range(rng.randint(0, MAX_INITIAL_ROLES))]
        self.role_id = len(self.roles)
        # Cluster 0 is the one cluster whose shape nothing churns: every cluster
        # action skips it and operates on `clusters[1:]`, so it stays available
        # for sources and sinks. Which cluster a given source or sink actually
        # lands on is still random, both here and in the Create*Action paths.
        #
        # Each of those actions needs a cluster of a specific kind past cluster
        # 0: managed for AlterClusterSetAction and FlipFlagsAction's per-cluster
        # arrangement compression, unmanaged for Create/DropClusterReplicaAction.
        # Drawing the count and the kinds the way the other initial objects are
        # drawn covers both in one run out of eight, and the rest spend the whole
        # run skipping. Nightly 17774 reported 369 AlterClusterSet, 129
        # ReconfigureCluster and 117 CreateClusterReplica attempts with not one
        # success between them. So the kinds are fixed and only cluster 0's is
        # left to the seed.
        self.clusters = [
            Cluster(
                cluster_id,
                managed=managed,
                size=rng.choice(["scale=1,workers=1", "scale=1,workers=2"]),
                # One replica each, which is what lets Create and
                # DropClusterReplicaAction take turns on the unmanaged cluster:
                # Create needs room below MAX_CLUSTER_REPLICAS, Drop needs more
                # than one replica so the cluster keeps serving requests.
                replication_factor=1,
                introspection_interval="1s",
            )
            for cluster_id, managed in enumerate(
                [rng.choice([True, False]), True, False]
            )
        ]
        self.cluster_id = len(self.clusters)
        self.indexes = set()
        self.webhook_sources = [
            WebhookSource(i, rng.choice(self.clusters), rng.choice(self.schemas), rng)
            for i in range(rng.randint(0, MAX_INITIAL_WEBHOOK_SOURCES))
        ]
        self.webhook_source_id = len(self.webhook_sources)
        # The CDC source lists are populated by `create_initial_sources`, not
        # here: such a source's executor connects to the source's database on
        # construction, and that database only exists once `create` has made it.
        self.kafka_sources = []
        self.mysql_sources = []
        self.postgres_sources = []
        self.sql_server_sources = []
        self.loadgen_sources = [
            LoadGeneratorSource(
                i, rng.choice(self.clusters), rng.choice(self.schemas), rng
            )
            for i in range(rng.randint(0, MAX_INITIAL_LOADGEN_SOURCES))
        ]
        self.iceberg_sinks = []
        self.kafka_sinks = []
        self.s3_objects = []
        self.kafka_source_id = len(self.kafka_sources)
        self.mysql_source_id = len(self.mysql_sources)
        self.postgres_source_id = len(self.postgres_sources)
        self.sql_server_source_id = len(self.sql_server_sources)
        self.loadgen_source_id = len(self.loadgen_sources)
        self.multi_loadgen_sources = []
        self.multi_loadgen_source_id = 0
        self.iceberg_sink_id = len(self.iceberg_sinks)
        self.kafka_sink_id = len(self.kafka_sinks)
        self.read_then_write_counter = ReadThenWriteCounter()
        self.types = []
        self.type_id = 0
        self.network_policies = []
        self.network_policy_id = 0
        self.lock = threading.Lock()
        self.sqlsmith_state = ""
        self.flags = {}

    def db_objects(
        self,
    ) -> list[
        WebhookSource
        | MySqlSource
        | PostgresSource
        | SqlServerSource
        | KafkaSource
        | LoadGeneratorSource
        | View
        | Table
    ]:
        # Temp objects are intentionally included. Referencing one from a
        # persistent object (sink, non-temp view) must be rejected by the
        # server, not accepted, so keeping them here lets the workload stress
        # that boundary and surface panics. The expected rejection ("non-
        # temporary items cannot depend on temporary item") and cross-session
        # "unknown catalog item" are ignored, see Action.errors_to_ignore.
        return (
            self.tables
            + self.views
            + self.kafka_sources
            + self.mysql_sources
            + self.postgres_sources
            + self.sql_server_sources
            + self.loadgen_sources
            + self.webhook_sources
        )

    def db_objects_without_views(
        self,
    ) -> list[
        WebhookSource
        | MySqlSource
        | PostgresSource
        | SqlServerSource
        | KafkaSource
        | LoadGeneratorSource
        | View
        | Table
    ]:
        return [
            obj for obj in self.db_objects() if type(obj) != View or obj.materialized
        ]

    def db_objects_for_sinks(
        self,
    ) -> list[
        WebhookSource
        | MySqlSource
        | PostgresSource
        | SqlServerSource
        | KafkaSource
        | View
        | Table
    ]:
        """Objects usable as a sink's input (base object or ALTER SINK SET
        FROM target). Load generator source tables are excluded: an
        ALTER SINK .. SET FROM one can trigger the sink stall of
        https://linear.app/materializeinc/issue/SS-344, which is worse for a
        continuously-producing input."""
        return [
            obj
            for obj in self.db_objects_without_views()
            if not isinstance(obj, LoadGeneratorSource)
        ]

    def __iter__(self):
        """Returns all relations"""
        return (
            self.schemas + self.clusters + self.roles + self.db_objects()
        ).__iter__()

    def create_initial_sources(self, exe: Executor) -> None:
        """Populate the CDC source lists, once the databases they connect to
        exist.

        SourceInsertAction is the only writer of a CDC source, and DML
        complexity gives the DDL action list weight 0, so nothing there ever
        runs a Create*SourceAction. Without a source from the start that action
        cannot succeed once in a whole run, which is why the Kafka source is
        unconditional rather than randomized down to zero like the other initial
        objects.

        Skipped for a 0dt deploy. A source executor connects on construction and
        picks one of the two environmentds at random every time it connects, and
        the second one only comes up mid-run when ZeroDowntimeDeployAction
        deploys it, so constructing a source here fails outright half the time.
        That scenario runs at DDL complexity, so its actions create the sources
        instead."""
        if self.scenario == Scenario.ZeroDowntimeDeploy:
            return
        self.kafka_sources = [
            KafkaSource(
                i,
                exe.rng.choice(self.clusters),
                exe.rng.choice(self.schemas),
                self.ports,
                exe.rng,
            )
            for i in range(MAX_INITIAL_KAFKA_SOURCES)
        ]
        self.kafka_source_id = len(self.kafka_sources)
        self.postgres_sources = [
            PostgresSource(
                i,
                exe.rng.choice(self.clusters),
                exe.rng.choice(self.schemas),
                self.ports,
                exe.rng,
            )
            # A Postgres source is not expected to survive a backup & restore,
            # see database-issues#6881 and
            # CreatePostgresSourceAction.applicable.
            for i in range(
                0
                if self.scenario == Scenario.BackupRestore
                else exe.rng.randint(0, MAX_INITIAL_POSTGRES_SOURCES)
            )
        ]
        self.postgres_source_id = len(self.postgres_sources)
        self.mysql_sources = [
            MySqlSource(
                i,
                exe.rng.choice(self.clusters),
                exe.rng.choice(self.schemas),
                self.ports,
                exe.rng,
            )
            for i in range(exe.rng.randint(0, MAX_INITIAL_MYSQL_SOURCES))
        ]
        self.mysql_source_id = len(self.mysql_sources)
        # MAX_INITIAL_SQL_SERVER_SOURCES stays unused: a SQL Server source
        # drives CPU far above the other source kinds (SS-290), enough to have
        # starved whole runs, and one in every run's initial database would put
        # that back for good.

    def create(self, exe: Executor, composition: Composition) -> None:
        for db in self.dbs:
            db.drop(exe)
            db.create(exe)

        exe.execute("SELECT name FROM mz_clusters WHERE name LIKE 'c%'")
        for row in exe.cur.fetchall():
            exe.execute(f"DROP CLUSTER {identifier(row[0])} CASCADE")

        exe.execute("DROP SECRET IF EXISTS pgpass CASCADE")
        exe.execute("DROP SECRET IF EXISTS mypass CASCADE")
        exe.execute("DROP SECRET IF EXISTS sql_server_pass CASCADE")
        exe.execute("DROP SECRET IF EXISTS minio CASCADE")

        exe.execute("SELECT name FROM mz_roles WHERE name LIKE 'r%'")
        for row in exe.cur.fetchall():
            exe.execute(f"DROP ROLE {identifier(row[0])}")

        # Network policies survive restarts and are top-level, so leftovers
        # from a killed run have to be swept before recreating.
        exe.execute("SELECT name FROM mz_internal.mz_network_policies")
        for row in exe.cur.fetchall():
            if row[0].startswith("netpol-"):
                exe.execute(f"DROP NETWORK POLICY {identifier(row[0])}")

        print("Creating connections")

        exe.execute(
            "CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER 'kafka:9092', SECURITY PROTOCOL PLAINTEXT"
        )
        exe.execute(
            "CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL 'http://schema-registry:8081'"
        )

        iceberg_credentials = setup_polaris_for_iceberg(composition)
        exe.execute(f"CREATE SECRET iceberg_secret AS '{iceberg_credentials[1]}'")
        exe.execute(
            f"CREATE CONNECTION aws_conn TO AWS (ACCESS KEY ID = '{iceberg_credentials[0]}', SECRET ACCESS KEY = SECRET iceberg_secret, ENDPOINT = 'http://minio:9000/', REGION = 'us-east-1');"
        )
        exe.execute(
            "CREATE CONNECTION polaris_conn TO ICEBERG CATALOG (CATALOG TYPE = 'REST', URL = 'http://polaris:8181/api/catalog', CREDENTIAL = 'root:root', WAREHOUSE = 'default_catalog', SCOPE = 'PRINCIPAL_ROLE:ALL');"
        )

        exe.execute("CREATE SECRET pgpass AS 'postgres'")
        exe.execute(
            "CREATE CONNECTION postgres_conn FOR POSTGRES HOST 'postgres', DATABASE postgres, USER postgres, PASSWORD SECRET pgpass"
        )

        exe.execute(f"CREATE SECRET mypass AS '{MySql.DEFAULT_ROOT_PASSWORD}'")
        exe.execute(
            "CREATE CONNECTION mysql_conn FOR MYSQL HOST 'mysql', USER root, PASSWORD SECRET mypass"
        )

        exe.execute(
            f"CREATE SECRET sql_server_pass AS '{SqlServer.DEFAULT_SA_PASSWORD}'"
        )
        exe.execute(
            f"CREATE CONNECTION sql_server_conn FOR SQL SERVER HOST 'sql-server', DATABASE test, USER {SqlServer.DEFAULT_USER}, PASSWORD SECRET sql_server_pass"
        )

        exe.execute("CREATE SECRET IF NOT EXISTS minio AS 'minioadmin'")
        exe.execute(
            "CREATE CONNECTION IF NOT EXISTS aws_conn TO AWS (ENDPOINT 'http://minio:9000/', REGION 'minio', ACCESS KEY ID 'minioadmin', SECRET ACCESS KEY SECRET minio)"
        )

        if self.scenario == Scenario.RepeatRow:
            # Hardcoded helper table for the table-driven `repeat_row(diff)`
            # mode in `View`. Must be created before relations because some
            # views reference it in their body.
            exe.execute(
                "DROP TABLE IF EXISTS materialize.public.repeat_row_source CASCADE"
            )
            exe.execute(
                "CREATE TABLE materialize.public.repeat_row_source (diff bigint NOT NULL)"
            )
            exe.execute(
                "INSERT INTO materialize.public.repeat_row_source VALUES (1), (1), (-1), (-1), (0)"
            )

        # Created and seeded exactly once per run: re-seeding mid-run would reset
        # `v` while the workers' tallies keep growing.
        self.read_then_write_counter.create(exe)

        print("Creating relations")

        self.create_initial_sources(exe)

        for relation in self:
            relation.create(exe)

        # Questionable use
        # result = composition.run(
        #     "sqlsmith",
        #     "--target=host=materialized port=6875 dbname=materialize user=materialize",
        #     "--exclude-catalog",
        #     "--dump-state",
        #     capture=True,
        #     capture_stderr=True,
        #     rm=True,
        # )
        # self.sqlsmith_state = result.stdout

    def drop(self, exe: Executor) -> None:
        # The counter table lives outside the workload's databases, so dropping
        # them does not reclaim it.
        self.read_then_write_counter.drop(exe)

        for db in self.dbs:
            print(f"Dropping database {db}")
            db.drop(exe)

        for src in self.kafka_sources:
            src.executor.mz_conn.close()
        for src in self.postgres_sources:
            src.executor.mz_conn.close()
        for src in self.mysql_sources:
            src.executor.mz_conn.close()
        for src in self.sql_server_sources:
            src.executor.mz_conn.close()

    def update_sqlsmith_state(self, composition: Composition) -> None:
        result = composition.run(
            "sqlsmith",
            "--target=host=materialized port=6875 dbname=materialize user=materialize",
            "--exclude-catalog",
            "--read-state",
            "--dump-state",
            stdin=self.sqlsmith_state,
            capture=True,
            capture_stderr=True,
            rm=True,
        )
        self.sqlsmith_state = result.stdout
