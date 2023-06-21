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
from time import sleep
from typing import Generator, List

from materialize.checks.actions import Action
from materialize.checks.all_checks import *  # noqa: F401 F403
from materialize.checks.checks import Check
from materialize.checks.common import KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD
from materialize.checks.executors import MzcomposeExecutor
from materialize.checks.scenarios import *  # noqa: F401 F403
from materialize.checks.scenarios import Scenario
from materialize.mzcompose import Composition, Service, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Clusterd,
    Kafka,
    Materialized,
    Postgres,
    SchemaRegistry,
)
from materialize.mzcompose.services import Testdrive as TestdriveService
from materialize.mzcompose.services import Zookeeper
from materialize.util import MzVersion
from materialize.version_list import VersionsFromGit

SERVICES = [
    Postgres(),
    Zookeeper(),
    # Kafka(auto_create_topics=True),
    Kafka(auto_create_topics=False),
    SchemaRegistry(),
    Materialized(),
    TestdriveService(no_reset=True),
    Clusterd(name="clusterd1", options=["--scratch-directory=/mzdata/source_data"]),
    Service("data-ingest", {"mzbuild": "data-ingest"}),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--upsert", action="store_true", help="Run upserts")

    parser.add_argument("--seed", metavar="SEED", type=str, default=str(time.time()))

    args = parser.parse_args()

    print(f"--- Random seed is {args.seed}")
    random.seed(args.seed)

    c.up("testdrive", persistent=True)
    c.up("materialized", "zookeeper", "kafka", "schema-registry", "postgres")

    # $ kafka-ingest format=avro key-format=avro topic=upsert-insert key-schema=${keyschema} schema=${schema} repeat=10000
    # {"key1": "A${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

    c.testdrive(
        dedent(
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

        $ kafka-ingest format=avro key-format=avro topic=upsert-insert key-schema=${keyschema} schema=${schema} repeat=1
        {"key1": "A${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

        > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

        > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';

        > CREATE SOURCE upsert_insert
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-insert-${testdrive.seed}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

        > CREATE MATERIALIZED VIEW upsert_insert_view AS SELECT COUNT(DISTINCT key1 || ' ' || f1) FROM upsert_insert;
        """
        )
    )

    c.run("data-ingest")

    c.down(destroy_volumes=True)
