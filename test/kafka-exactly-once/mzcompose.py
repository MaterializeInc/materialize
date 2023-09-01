# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.kafka import Kafka
from materialize.mzcompose.materialized import Materialized
from materialize.mzcompose.schema_registry import SchemaRegistry
from materialize.mzcompose.testdrive import Testdrive
from materialize.mzcompose.zookeeper import Zookeeper

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Materialized(),
    Testdrive(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--seed",
        help="an alternate seed to use to avoid clashing with existing topics",
        type=int,
        default=1,
    )
    args = parser.parse_args()

    c.up("zookeeper", "kafka", "schema-registry", "materialized")
    c.run(
        "testdrive",
        f"--seed={args.seed}",
        "--kafka-option=group.id=group1",
        "--no-reset",
        "before-restart.td",
    )
    c.kill("materialized")
    c.up("materialized")
    c.run(
        "testdrive",
        f"--seed={args.seed}",
        "--no-reset",
        "--kafka-option=group.id=group2",
        "after-restart.td",
    )
