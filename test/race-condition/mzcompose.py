# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Tries to find race conditions in Materialize, mostly DDLs. Can find panics and wrong results.
"""

import datetime
import random
import time
from textwrap import dedent

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.util import all_subclasses

SERVICES = [
    Postgres(),
    MySql(),
    Zookeeper(),
    Kafka(
        auto_create_topics=False,
        ports=["30123:30123"],
        allow_host_ports=True,
        environment_extra=[
            "KAFKA_ADVERTISED_LISTENERS=HOST://localhost:30123,PLAINTEXT://kafka:9092",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        ],
    ),
    SchemaRegistry(),
    Minio(setup_materialize=True, additional_directories=["copytos3"]),
    Testdrive(no_reset=True, consistent_seed=True),
    Mc(),
    Materialized(),
]


class Object:
    name: str
    references: str | None
    can_refer: bool = True

    def __init__(self, name: str, references: str | None, rng: random.Random):
        self.name = name
        self.references = references

    def prepare(self) -> str:
        return ""

    def create(self) -> str:
        raise NotImplementedError

    def destroy(self) -> str:
        raise NotImplementedError

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class UpsertSource(Object):
    def prepare(self) -> str:
        return dedent(
            """
            $ set keyschema={
                "type": "record",
                "name": "Key",
                "fields": [
                    {"name": "b", "type": "string"},
                    {"name": "a", "type": "long"}
                ]
              }

            $ set schema={
                "type" : "record",
                "name" : "envelope",
                "fields" : [
                  {
                    "name": "before",
                    "type": [
                      {
                        "name": "row",
                        "type": "record",
                        "fields": [
                          {
                              "name": "a",
                              "type": "long"
                          },
                          {
                            "name": "data",
                            "type": "string"
                          },
                          {
                              "name": "b",
                              "type": "string"
                          }]
                       },
                       "null"
                     ]
                  },
                  {
                    "name": "after",
                    "type": ["row", "null"]
                  }
                ]
              }

            $ kafka-create-topic topic=dbzupsert partitions=1

            $ kafka-ingest format=avro topic=dbzupsert key-format=avro key-schema=${keyschema} schema=${schema} repeat=1000000
            {"b": "bdata", "a": ${kafka-ingest.iteration}} {"before": {"row": {"a": ${kafka-ingest.iteration}, "data": "fish", "b": "bdata"}}, "after": {"row": {"a": ${kafka-ingest.iteration}, "data": "fish2", "b": "bdata"}}}

            > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
              URL '${testdrive.schema-registry-url}');

            > CREATE CONNECTION IF NOT EXISTS kafka_conn
              TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

            > CREATE SOURCE IF NOT EXISTS upsert
              IN CLUSTER quickstart
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-${testdrive.seed}')"""
        )

    def create(self) -> str:
        return dedent(
            f"""
            > CREATE TABLE {self.name} FROM SOURCE upsert (REFERENCE "testdrive-dbzupsert-${{testdrive.seed}}")
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE DEBEZIUM"""
        )

    def destroy(self) -> str:
        return f"> DROP TABLE {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class Table(Object):
    def create(self) -> str:
        return f"> CREATE TABLE {self.name} (a TEXT, b TEXT)"

    def destroy(self) -> str:
        return f"> DROP TABLE {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


# TODO: How to handle things like clusters, replicas?

# TODO: Add more manipulations: inserts, updates, deletes, ALTER RENAME (twice)


class PostgresSource(Object):
    def prepare(self) -> str:
        return dedent(
            f"""
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            DROP USER IF EXISTS {self.name}_role;
            CREATE USER {self.name}_role WITH SUPERUSER PASSWORD 'postgres';
            ALTER USER {self.name}_role WITH replication;
            DROP PUBLICATION IF EXISTS {self.name}_source;
            DROP TABLE IF EXISTS {self.name}_table;
            CREATE TABLE {self.name}_table (a TEXT, b TEXT);
            ALTER TABLE {self.name}_table REPLICA IDENTITY FULL;
            CREATE PUBLICATION {self.name}_source FOR ALL TABLES;

            > CREATE SECRET IF NOT EXISTS {self.name}_pass AS 'postgres'
            > CREATE CONNECTION IF NOT EXISTS {self.name}_conn FOR POSTGRES
              HOST 'postgres',
              DATABASE postgres,
              USER {self.name}_role,
              PASSWORD SECRET {self.name}_pass
            > CREATE SOURCE IF NOT EXISTS {self.name}_source
              IN CLUSTER quickstart
              FROM POSTGRES CONNECTION {self.name}_conn
              (PUBLICATION '{self.name}_source')"""
        )

    def create(self) -> str:
        return f"> CREATE TABLE {self.name} FROM SOURCE {self.name}_source (REFERENCE {self.name}_table)"

    def destroy(self) -> str:
        return f"> DROP TABLE {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class MySqlSource(Object):
    def prepare(self) -> str:
        return dedent(
            f"""
            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

            $ mysql-execute name=mysql
            # create the database if it does not exist yet but do not drop it
            CREATE DATABASE IF NOT EXISTS public;
            USE public;

            CREATE USER IF NOT EXISTS {self.name}_role IDENTIFIED BY 'mysql';
            GRANT REPLICATION SLAVE ON *.* TO {self.name}_role;
            GRANT ALL ON public.* TO {self.name}_role;

            DROP TABLE IF EXISTS {self.name}_table;
            CREATE TABLE {self.name}_table (a TEXT, b TEXT);

            > CREATE SECRET IF NOT EXISTS {self.name}_pass AS 'mysql'
            > CREATE CONNECTION IF NOT EXISTS {self.name}_conn TO MYSQL (
                HOST 'mysql',
                USER {self.name}_role,
                PASSWORD SECRET {self.name}_pass
              )
            > CREATE SOURCE IF NOT EXISTS {self.name}_source
              IN CLUSTER quickstart
              FROM MYSQL CONNECTION {self.name}_conn;"""
        )

    def create(self) -> str:
        return f"> CREATE TABLE {self.name} FROM SOURCE {self.name}_source (REFERENCE public.{self.name}_table)"

    def destroy(self) -> str:
        return f"> DROP TABLE {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class LoadGeneratorSource(Object):
    def __init__(self, name: str, references: str | None, rng: random.Random):
        super().__init__(name, references, rng)
        self.tick_interval = rng.choice(["1ms", "10ms", "100ms", "1s", "10s"])

    def create(self) -> str:
        return f"> CREATE SOURCE {self.name} IN CLUSTER quickstart FROM LOAD GENERATOR COUNTER (TICK INTERVAL '{self.tick_interval}')"

    def destroy(self) -> str:
        return f"> DROP SOURCE {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class WebhookSource(Object):
    def __init__(self, name: str, references: str | None, rng: random.Random):
        super().__init__(name, references, rng)
        self.body_format = rng.choice(["TEXT", "JSON", "JSON ARRAY", "BYTES"])

    def create(self) -> str:
        return f"> CREATE SOURCE {self.name} IN CLUSTER quickstart FROM WEBHOOK BODY FORMAT {self.body_format}"

    def destroy(self) -> str:
        return f"> DROP SOURCE {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class KafkaSink(Object):
    def __init__(self, name: str, references: str | None, rng: random.Random):
        super().__init__(name, references, rng)
        self.envelope = rng.choice(["UPSERT", "DEBEZIUM"])
        self.format = rng.choice(
            [
                "JSON",
                "TEXT",
                "BYTES",
                "AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn",
            ]
        )

    def create(self) -> str:
        references = self.references or f"{self.name}_view"
        cmds = []
        if not self.references:
            cmds.append(
                f"> CREATE VIEW IF NOT EXISTS {references} AS SELECT 'foo' AS a, 'bar' AS b"
            )
        cmds.append(
            dedent(
                f"""
            > CREATE CONNECTION IF NOT EXISTS kafka_conn
              TO KAFKA (BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL PLAINTEXT);
            > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
                URL '${{testdrive.schema-registry-url}}'
              );
            > CREATE SINK {self.name}
              IN CLUSTER quickstart
              FROM {references}
              INTO KAFKA CONNECTION
              FORMAT {self.format}
              ENVELOPE {self.envelope}"""
            )
        )
        return "\n".join(cmds)

    def destroy(self) -> str:
        return f"> DROP SINK {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class View(Object):
    def create(self) -> str:
        return f'> CREATE VIEW {self.name} AS SELECT {"* FROM " + self.references if self.references else "'foo' AS a, 'bar' AS b"}'

    def destroy(self) -> str:
        return f"> DROP VIEW {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class MaterializedView(Object):
    def create(self) -> str:
        return f'> CREATE MATERIALIZED VIEW {self.name} AS SELECT {"* FROM " + self.references if self.references else "'foo' AS a, 'bar' AS b"}'

    def destroy(self) -> str:
        return f"> DROP MATERIALIZED VIEW {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class DefaultIndex(Object):
    can_refer: bool = False

    def create(self) -> str:
        return f"> CREATE DEFAULT INDEX ON {self.references}" if self.references else ""

    def destroy(self) -> str:
        return f"> DROP INDEX {self.references}_primary_idx" if self.references else ""

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class Executor:
    def execute(self, td: str) -> None:
        raise NotImplementedError


class Scenario:
    def __init__(self, c: Composition, rng: random.Random):
        self.c = c
        self.rng = rng

    def _impl(self, num_executions: int) -> str:
        raise NotImplementedError

    def print(self) -> None:
        print(self._impl(1))

    def run(self, num_executions: int) -> None:
        self.c.testdrive(self._impl(num_executions), quiet=True)


class Concurrent(Scenario):
    # set timing of when to start
    def _impl(self, num_executions: int) -> str:
        raise NotImplementedError
        # thread1.create()
        # sleep(...)
        # thread2.create()
        # threading.wait()
        # thread1.manipulate()
        # sleep(...)
        # thread2.manipulate()
        # thread1.destroy()
        # sleep(...)
        # thread2.destroy()


class Subsequent(Scenario):
    def __init__(self, c: Composition, rng: random.Random):
        super().__init__(c, rng)
        objects = list(all_subclasses(Object))
        self.o1 = rng.choice([o for o in objects if o.can_refer])("o1", None, rng)
        self.o2 = rng.choice(objects)("o2", self.o1.name, rng)
        self.o3 = rng.choice(objects)("o3", self.o1.name, rng)
        self.o4 = rng.choice(objects)("o4", self.o1.name, rng)
        self.o5 = rng.choice(objects)("o5", self.o1.name, rng)

    def _impl(self, num_executions: int) -> str:
        result = ""
        for i in range(num_executions):
            if i == 0:
                result += self.o1.prepare() + "\n"
                result += self.o2.prepare() + "\n"
                result += self.o3.prepare() + "\n"
                result += self.o4.prepare() + "\n"
                result += self.o5.prepare() + "\n"

            result += self.o1.create() + "\n"
            result += self.o2.create() + "\n"
            result += self.o2.manipulate(self.rng.randrange(100)) + "\n"
            result += self.o2.destroy() + "\n"
            result += self.o3.create() + "\n"
            result += self.o3.manipulate(self.rng.randrange(100)) + "\n"
            result += self.o3.destroy() + "\n"
            result += self.o4.create() + "\n"
            result += self.o4.manipulate(self.rng.randrange(100)) + "\n"
            result += self.o4.destroy() + "\n"
            result += self.o5.create() + "\n"
            result += self.o5.manipulate(self.rng.randrange(100)) + "\n"
            result += self.o5.destroy() + "\n"
            result += self.o1.destroy() + "\n"
        return result


class SubsequentChain(Scenario):
    def __init__(self, c: Composition, rng: random.Random):
        super().__init__(c, rng)
        objects = list(all_subclasses(Object))
        self.o1 = rng.choice([o for o in objects if o.can_refer])("o1", None, rng)
        self.o2 = rng.choice(objects)("o2", self.o1.name, rng)
        self.o3 = rng.choice(objects)("o3", self.o2.name, rng)
        self.o4 = rng.choice(objects)("o4", self.o3.name, rng)
        self.o5 = rng.choice(objects)("o5", self.o4.name, rng)

    def _impl(self, num_executions: int) -> str:
        result = ""
        for i in range(num_executions):
            if i == 0:
                result += self.o1.prepare() + "\n"
                result += self.o2.prepare() + "\n"
                result += self.o3.prepare() + "\n"

            result += self.o1.create() + "\n"
            result += self.o2.create() + "\n"
            result += self.o3.create() + "\n"
            result += self.o4.create() + "\n"
            result += self.o5.create() + "\n"
            result += self.o1.manipulate(self.rng.randrange(100)) + "\n"
            result += self.o2.manipulate(self.rng.randrange(100)) + "\n"
            result += self.o3.manipulate(self.rng.randrange(100)) + "\n"
            result += self.o4.manipulate(self.rng.randrange(100)) + "\n"
            result += self.o5.manipulate(self.rng.randrange(100)) + "\n"
            result += self.o5.destroy() + "\n"
            result += self.o4.destroy() + "\n"
            result += self.o4.destroy() + "\n"
            result += self.o3.destroy() + "\n"
            result += self.o2.destroy() + "\n"
            result += self.o1.destroy() + "\n"
        return result


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--seed", type=str, default=str(int(time.time())))
    parser.add_argument("--runtime", default=600, type=int, help="Runtime in seconds")
    parser.add_argument(
        "--repetitions", default=100, type=int, help="Repeatitions per scenario"
    )
    parser.add_argument(
        "--scenario",
        default="subsequent",
        type=str,
        choices=["subsequent", "subsequent_chain", "concurrent"],
    )
    args = parser.parse_args()

    print(f"--- Random seed is {args.seed}")
    service_names = [
        "postgres",
        "mysql",
        "zookeeper",
        "kafka",
        "schema-registry",
        # Still required for backups/s3 testing even when we use Azurite as blob store
        "minio",
        "materialized",
    ]

    rng = random.Random(args.seed)

    end_time = (
        datetime.datetime.now() + datetime.timedelta(seconds=args.runtime)
    ).timestamp()

    c.up(*service_names)
    c.up("testdrive", persistent=True)

    while time.time() < end_time:
        scenario = (
            Subsequent(c, rng) if args.scenario == "subsequent" else Concurrent(c, rng)
        )

        print("--- Scenario to run")
        scenario.print()

        print(f"--- Running scenario {args.repetitions} times")
        scenario.run(args.repetitions)
