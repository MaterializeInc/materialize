# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List

from materialize.mzcompose import Workflow, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

services = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Materialized(),
    Testdrive(),
]


def workflow_kafka_exactly_once(w: Workflow, args: List[str]) -> None:
    parser = WorkflowArgumentParser(w)
    parser.add_argument(
        "--seed",
        help="an alternate seed to use to avoid clashing with existing topics",
        type=int,
        default=1,
    )
    args = parser.parse_args(args)

    w.start_and_wait_for_tcp(
        services=["zookeeper", "kafka", "schema-registry", "materialized"]
    )
    w.run_service(
        service="testdrive-svc",
        command=f"--seed {args.seed} --kafka-option=group.id=group1 before-restart.td",
    )
    w.kill_services(services=["materialized"])
    w.start_services(services=["materialized"])
    w.wait_for_mz()
    w.run_service(
        service="testdrive-svc",
        command=f"--seed {args.seed} --no-reset --kafka-option=group.id=group2 after-restart.td",
    )
