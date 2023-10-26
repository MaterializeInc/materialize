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
from materialize.checks.executors import MzcomposeExecutor, MzcomposeExecutorParallel
from materialize.checks.scenarios import *  # noqa: F401 F403
from materialize.checks.scenarios import Scenario
from materialize.checks.scenarios_backup_restore import *  # noqa: F401 F403
from materialize.checks.scenarios_upgrade import *  # noqa: F401 F403
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.debezium import Debezium
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.testdrive import Testdrive as TestdriveService
from materialize.util import all_subclasses

SERVICES = [
    Cockroach(
        setup_materialize=True,
        # Workaround for #19810
        restart="on-failure:5",
    ),
    Minio(setup_materialize=True),
    Mc(),
    Postgres(),
    Redpanda(auto_create_topics=True),
    Debezium(redpanda=True),
    Clusterd(
        name="clusterd_compute_1"
    ),  # Started by some Scenarios, defined here only for the teardown
    Materialized(external_cockroach=True, external_minio=True, sanity_restart=False),
    TestdriveService(
        default_timeout="300s",
        no_reset=True,
        seed=1,
        entrypoint_extra=[
            "--var=replicas=1",
            f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
            f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
        ],
    ),
    Service(
        name="persistcli",
        config={"mzbuild": "jobs"},
    ),
]


class ExecutionMode(Enum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    ONEATATIME = "oneatatime"

    def __str__(self) -> str:
        return self.value


def setup(c: Composition) -> None:
    c.up("testdrive", persistent=True)
    c.up("cockroach")

    c.up("redpanda", "postgres", "debezium", "minio")
    c.up("mc", persistent=True)
    c.exec(
        "mc",
        "mc",
        "alias",
        "set",
        "persist",
        "http://minio:9000/",
        "minioadmin",
        "minioadmin",
    )

    c.exec("mc", "mc", "version", "enable", "persist/persist")


def teardown(c: Composition) -> None:
    c.rm(*[s.name for s in SERVICES], stop=True, destroy_volumes=True)
    c.rm_volumes("mzdata", "tmp", force=True)


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
        default=ExecutionMode.SEQUENTIAL,
    )

    parser.add_argument(
        "--seed",
        metavar="SEED",
        type=str,
        help="Seed for shuffling checks in sequential run.",
    )

    args = parser.parse_args()

    if args.scenario:
        assert args.scenario in globals(), f"scenario {args.scenario} does not exist"
        scenarios = [globals()[args.scenario]]
    else:
        scenarios = all_subclasses(Scenario)

    if args.check:
        all_checks = {check.__name__: check for check in all_subclasses(Check)}
        for check in args.check:
            assert check in all_checks, f"check {check} does not exist"
        checks = [all_checks[check] for check in args.check]
    else:
        checks = list(all_subclasses(Check))

    executor = MzcomposeExecutor(composition=c)
    for scenario_class in scenarios:
        assert issubclass(
            scenario_class, Scenario
        ), f"{scenario_class} is not a Scenario. Maybe you meant to specify a Check via --check ?"

        print(f"Testing scenario {scenario_class}...")

        executor_class = (
            MzcomposeExecutorParallel
            if args.execution_mode is ExecutionMode.PARALLEL
            else MzcomposeExecutor
        )
        executor = executor_class(composition=c)

        if args.execution_mode in [ExecutionMode.SEQUENTIAL, ExecutionMode.PARALLEL]:
            setup(c)
            scenario = scenario_class(checks=checks, executor=executor, seed=args.seed)
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
