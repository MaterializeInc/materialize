# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import random
import sys
import time
from typing import Generator, List

from materialize.checks.actions import Action
from materialize.checks.all_checks import *  # noqa: F401 F403
from materialize.checks.checks import Check
from materialize.checks.executors import MzcomposeExecutor
from materialize.checks.scenarios import *  # noqa: F401 F403
from materialize.checks.scenarios import Scenario
from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Clusterd,
    SchemaRegistry,
    Materialized,
    Postgres,
    Kafka,
    Zookeeper,
)
from materialize.mzcompose.services import Testdrive as TestdriveService
from materialize.util import MzVersion
from materialize.version_list import VersionsFromGit
from materialize.checks.common import KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD
from time import sleep

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

SERVICES = [
    Postgres(),
    Zookeeper(),
    #Kafka(auto_create_topics=True),
    Kafka(auto_create_topics=False),
    SchemaRegistry(),
    Materialized(),
    TestdriveService(no_reset=True),
    Clusterd(name="clusterd1", options=["--scratch-directory=/mzdata/source_data"]),
]

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

def idfn(d, ctx):
    return d


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--upsert",
        action="store_true",
        help="Run upserts"
    )

    parser.add_argument("--seed", metavar="SEED", type=str, default=str(time.time()))

    args = parser.parse_args()

    print(f"--- Random seed is {args.seed}")
    random.seed(args.seed)

    c.up("testdrive", persistent=True)
    c.up("materialized", "zookeeper", "kafka", "schema-registry", "postgres")

        #$ kafka-ingest format=avro key-format=avro topic=upsert-insert key-schema=${keyschema} schema=${schema} repeat=10000
        #{"key1": "A${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

    c.testdrive(dedent(
        """
        $ set keyschema={
            "type": "record",
            "name": "Key",
            "fields": [
                {"name": "key1", "type": "string"}
            ]
          }

        $ set schema={
            "type" : "record",
            "name" : "test",
            "fields" : [
                {"name":"f1", "type":"string"}
            ]
          }

        $ kafka-create-topic topic=upsert-insert

        > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

        > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';

        > CREATE SOURCE upsert_insert
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-insert-${testdrive.seed}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

        > CREATE MATERIALIZED VIEW upsert_insert_view AS SELECT COUNT(DISTINCT key1 || ' ' || f1) FROM upsert_insert;
        """
    ))

    with open(f"user.avsc") as f:
        schema_str = f.read()
    
    with open(f"key.avsc") as f:
        key_schema_str = f.read()
    
    # docker port data-ingest-schema-registry-1 8081
    schema_registry_conf = {'url': "http://localhost:32874"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     idfn)
    
    key_avro_serializer = AvroSerializer(schema_registry_client,
                                         key_schema_str,
                                         idfn)
    
    ## docker port data-ingest-kafka-1 9092
    producer_conf = {'bootstrap.servers': "localhost:32873"}
    producer = Producer(producer_conf)
    
    producer.poll(0.0)
    topic = "testdrive-upsert-insert-2018493381"
    producer.produce(topic=topic,
                     key=key_avro_serializer({"key1": "A10000"}, SerializationContext(topic, MessageField.KEY)),
                    value=avro_serializer({"f1": "A10000"}, SerializationContext(topic, MessageField.VALUE)),
                    on_delivery=delivery_report)
    producer.flush()

    sleep(360000)

    c.down(destroy_volumes=True)
