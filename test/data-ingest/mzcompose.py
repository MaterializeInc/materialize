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

from materialize.checks.all_checks import *  # noqa: F401 F403
from materialize.checks.scenarios import *  # noqa: F401 F403
from materialize.data_ingest import main
from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Clusterd,
    Kafka,
    Materialized,
    Postgres,
    SchemaRegistry,
    Zookeeper,
)

SERVICES = [
    Postgres(),
    Zookeeper(),
    Kafka(
        auto_create_topics=False,
        port="30123:30123",
        allow_host_ports=True,
        environment_extra=[
            "KAFKA_ADVERTISED_LISTENERS=HOST://localhost:30123,PLAINTEXT://kafka:9092",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        ],
    ),
    SchemaRegistry(),
    Materialized(),
    Clusterd(name="clusterd1", options=["--scratch-directory=/mzdata/source_data"]),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--upsert", action="store_true", help="Run upserts")

    parser.add_argument("--seed", metavar="SEED", type=str, default=str(time.time()))

    args = parser.parse_args()

    print(f"--- Random seed is {args.seed}")
    random.seed(args.seed)

    services = ("materialized", "zookeeper", "kafka", "schema-registry", "postgres")
    c.up(*services)

    ports = {s: c.default_port(s) for s in services}
    main(ports)
