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
from collections.abc import Callable
from textwrap import dedent
from typing import Any

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

    def __init__(self, name: str, references: str | None):
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
              URL '${testdrive.schema-registry-url}'
            );

            > CREATE CONNECTION IF NOT EXISTS kafka_conn
              TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

            > CREATE SOURCE IF NOT EXISTS upsert
              IN CLUSTER ${arg.single-replica-cluster}
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-dbzupsert-${testdrive.seed}')
        """
        )

    def create(self) -> str:
        return dedent(
            f"""
            > CREATE TABLE {self.name} FROM SOURCE upsert (REFERENCE "testdrive-dbzupsert-${{testdrive.seed}}")
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE DEBEZIUM
        """
        )

    def destroy(self) -> str:
        return f"DROP TABLE {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class MaterializedView(Object):
    def create(self) -> str:
        return (
            f"> CREATE MATERIALIZED VIEW {self.name} AS SELECT * FROM {self.references}"
        )

    def destroy(self) -> str:
        return f"> DROP MATERIALIZED VIEW {self.name}"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class DefaultIndex(Object):
    def create(self) -> str:
        return f"> CREATE DEFAULT INDEX on {self.references}"

    def destroy(self) -> str:
        return f"> DROP DEFAULT INDEX FROM {self.references}"

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

    def _impl(self, exe: Callable[[str], Any]) -> None:
        raise NotImplementedError

    def print(self) -> None:
        self._impl(lambda td: print(td))

    def run(self) -> None:
        self._impl(lambda td: self.c.testdrive(td))


class Concurrent(Scenario):
    # set timing of when to start
    def _impl(self, exe: Callable[[str], Any]) -> None:
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
        self.o1 = self.rng.choice(objects)("o1", None)
        self.o2 = self.rng.choice(objects)("o2", self.o1.name)
        self.o3 = self.rng.choice(objects)("o3", self.o1.name)
        self.first_run = True

    def _impl(self, exe: Callable[[str], Any]) -> None:
        if self.first_run:
            self.first_run = False
            self.o1.prepare()
            self.o2.prepare()
            self.o3.prepare()

        exe(self.o1.create())
        exe(self.o2.create())
        exe(self.o2.manipulate(self.rng.randrange(100)))
        exe(self.o2.destroy())
        exe(self.o3.create())
        exe(self.o3.manipulate(self.rng.randrange(100)))
        exe(self.o3.destroy())


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--seed", type=str, default=str(int(time.time())))
    parser.add_argument("--runtime", default=600, type=int, help="Runtime in seconds")
    parser.add_argument(
        "--scenario",
        default="subsequent",
        type=str,
        choices=["subsequent", "concurrent"],
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

    scenario = (
        Subsequent(c, rng) if args.scenario == "subsequent" else Concurrent(c, rng)
    )

    while time.time() < end_time:
        scenario.prepare()
        print("--- Scenario to run")
        scenario.print()
        print("--- Running scenario")
        scenario.run()
