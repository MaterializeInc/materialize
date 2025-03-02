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
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.parallel_workload.settings import Complexity, Scenario

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
    references: str

    def __init__(name: str, references: str):
        self.name = name
        self.references = references

    def prepare() -> str
        return ""

    def create(name: str, references: str) -> str:
        raise NotImplementedError

    def destroy() -> str:
        raise NotImplementedError

    def manipulate(kind: int) -> str:
        raise NotImplementedError

    def verify() -> str:
        raise NotImplementedError

class UpsertSource(Object):
    def prepare() -> str:
        return dedent("""
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
        """)

    def create() -> str:
        return dedent("""
            > CREATE TABLE upsert_tbl FROM SOURCE upsert (REFERENCE "testdrive-dbzupsert-${testdrive.seed}")
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE DEBEZIUM
        """)

    def destroy() -> str:
        raise NotImplementedError

    def manipulate(kind: int) -> str:
        raise NotImplementedError

    def verify() -> str:
        raise NotImplementedError

class MaterializedView(Object):
    def create() -> str:
        return f"> CREATE MATERIALIZED VIEW {self.name} AS SELECT * FROM {self.references}"

    def destroy() -> str:
        return f"> DROP MATERIALIZED VIEW {self.name}"

    def manipulate(kind: int) -> str:
        raise NotImplementedError

    def verify() -> str:
        raise NotImplementedError

class DefaultIndex(Objecet):
    def create(name: str, references: str) -> str:
        return f"> CREATE DEFAULT INDEX on {self.references}"

    def destroy() -> str:
        return f"> DROP DEFAULT INDEX FROM {self.references}"

    def manipulate(kind: int) -> str:
        raise NotImplementedError

    def verify() -> str:
        raise NotImplementedError

class Scenario:
    def print() -> str:
    def run() -> None:

class Concurrent(Scenario):
  # set timing of when to start
    def __init__():
    def run() -> None:
        thread1.create()
        sleep(...)
        thread2.create()
        threading.wait()
        thread1.manipulate()
        sleep(...)
        thread2.manipulate()
        thread1.destroy()
        sleep(...)
        thread2.destroy()

class Subsequent(Scenario):
    def run() -> None:
        object1.create()

        object2.create(references=object1)
        object2.manipulate()
        object2.destroy()
        object3.create(references=object1)
        object3.manipulate()
        object3.destroy()

        object1.destroy()


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--seed", type=str, default=str(int(time.time())))
    parser.add_argument("--runtime", default=600, type=int, help="Runtime in seconds")
    parser.add_argument(
        "--scenario",
        default="regression",
        type=str,
        choices=[elem.value for elem in Scenario] + ["random"],
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

    random.seed(args.seed)
    Scenario(args.scenario)
    Complexity(args.complexity)

    c.up(*service_names)
    c.up("testdrive", persistent=True)
