# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    Postgres,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)
from materialize.platform_upgrade_test.checks import Check
from materialize.platform_upgrade_test.upgrade_scenarios import *  # noqa: F401 F403
from materialize.platform_upgrade_test.upgrade_scenarios import Scenario

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Postgres(),
    Materialized(),
    Testdrive(default_timeout="5s", no_reset=True, seed=1),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.silent = True

    parser.add_argument(
        "--scenario", metavar="SCENARIO", type=str, help="Scenario to run."
    )

    parser.add_argument("--check", metavar="CHECK", type=str, help="Check to run.")

    args = parser.parse_args()

    c.up("testdrive", persistent=True)

    #    c.start_and_wait_for_tcp(
    #        services=["zookeeper", "kafka", "schema-registry", "postgres"]
    #    )

    scenarios = (
        [globals()[args.scenario]] if args.scenario else Scenario.__subclasses__()
    )

    checks = [globals()[args.check]] if args.check else Check.__subclasses__()

    for scenario_class in scenarios:
        print(f"Testing upgrade scenario {scenario_class}")
        scenario = scenario_class(checks=checks)
        scenario.run(c)
