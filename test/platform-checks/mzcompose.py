# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum

from materialize.checks.all_checks import *  # noqa: F401 F403
from materialize.checks.checks import Check
from materialize.checks.executors import MzcomposeExecutor
from materialize.checks.scenarios import *  # noqa: F401 F403
from materialize.checks.scenarios import Scenario
from materialize.checks.scenarios_upgrade import *  # noqa: F401 F403
from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Computed,
    Debezium,
    Materialized,
    Postgres,
    Redpanda,
)
from materialize.mzcompose.services import Testdrive as TestdriveService

SERVICES = [
    Postgres(name="postgres-backend"),
    Postgres(name="postgres-source"),
    Redpanda(auto_create_topics=True),
    Debezium(),
    Computed(
        name="computed_1"
    ),  # Started by some Scenarios, defined here only for the teardown
    Materialized(),
    TestdriveService(default_timeout="300s", no_reset=True, seed=1),
]


class ExecutionMode(Enum):
    ALLTOGETHER = "alltogether"
    ONEATATIME = "oneatatime"

    def __str__(self) -> str:
        return self.value


def setup(c: Composition) -> None:
    c.up("testdrive", persistent=True)

    c.start_and_wait_for_tcp(
        services=["redpanda", "postgres-backend", "postgres-source", "debezium"]
    )
    for postgres in ["postgres-backend", "postgres-source"]:
        c.wait_for_postgres(service=postgres)

    c.sql(
        sql=f"""
       CREATE SCHEMA IF NOT EXISTS consensus;
       CREATE SCHEMA IF NOT EXISTS storage;
       CREATE SCHEMA IF NOT EXISTS adapter;
    """,
        service="postgres-backend",
        user="postgres",
        password="postgres",
    )


def teardown(c: Composition) -> None:
    c.rm(*[s.name for s in SERVICES], stop=True, destroy_volumes=True)
    c.rm_volumes("mzdata", "pgdata", "tmp", force=True)


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    # c.silent = True
    parser.add_argument(
        "--scenario", metavar="SCENARIO", type=str, help="Scenario to run."
    )

    parser.add_argument(
        "--check", metavar="CHECK", type=str, action="append", help="Check(s) to run."
    )

    parser.add_argument(
        "--execution-mode",
        type=ExecutionMode,
        choices=list(ExecutionMode),
        default=ExecutionMode.ALLTOGETHER,
    )

    args = parser.parse_args()

    scenarios = (
        [globals()[args.scenario]] if args.scenario else Scenario.__subclasses__()
    )

    checks = (
        [globals()[check] for check in args.check]
        if args.check
        else Check.__subclasses__()
    )

    executor = MzcomposeExecutor(composition=c)

    for scenario_class in scenarios:
        print(f"Testing scenario {scenario_class}...")
        if args.execution_mode is ExecutionMode.ALLTOGETHER:
            setup(c)
            scenario = scenario_class(checks=checks, executor=executor)
            scenario.run()
            teardown(c)
        elif args.execution_mode is ExecutionMode.ONEATATIME:
            for check in checks:
                print(f"Running individual check {check}, scenario {scenario_class}")
                setup(c)
                scenario = scenario_class(checks=[check], executor=executor)
                scenario.run()
                teardown(c)
        else:
            assert False
