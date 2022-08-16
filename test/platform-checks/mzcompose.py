# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum

from materialize.checks.aggregation import *  # noqa: F401 F403
from materialize.checks.alter_index import *  # noqa: F401 F403
from materialize.checks.boolean_type import *  # noqa: F401 F403
from materialize.checks.checks import Check
from materialize.checks.cluster import *  # noqa: F401 F403
from materialize.checks.commit import *  # noqa: F401 F403
from materialize.checks.constant_plan import *  # noqa: F401 F403
from materialize.checks.create_index import *  # noqa: F401 F403
from materialize.checks.create_table import *  # noqa: F401 F403
from materialize.checks.databases import *  # noqa: F401 F403
from materialize.checks.debezium import *  # noqa: F401 F403
from materialize.checks.delete import *  # noqa: F401 F403
from materialize.checks.drop_index import *  # noqa: F401 F403
from materialize.checks.drop_table import *  # noqa: F401 F403
from materialize.checks.error import *  # noqa: F401 F403
from materialize.checks.float_types import *  # noqa: F401 F403
from materialize.checks.having import *  # noqa: F401 F403
from materialize.checks.insert_select import *  # noqa: F401 F403
from materialize.checks.join_implementations import *  # noqa: F401 F403
from materialize.checks.join_types import *  # noqa: F401 F403
from materialize.checks.jsonb_type import *  # noqa: F401 F403
from materialize.checks.large_tables import *  # noqa: F401 F403
from materialize.checks.like import *  # noqa: F401 F403
from materialize.checks.materialized_views import *  # noqa: F401 F403
from materialize.checks.nested_types import *  # noqa: F401 F403
from materialize.checks.null_value import *  # noqa: F401 F403
from materialize.checks.numeric_types import *  # noqa: F401 F403
from materialize.checks.regex import *  # noqa: F401 F403
from materialize.checks.rename_index import *  # noqa: F401 F403
from materialize.checks.rename_source import *  # noqa: F401 F403
from materialize.checks.rename_table import *  # noqa: F401 F403
from materialize.checks.rename_view import *  # noqa: F401 F403
from materialize.checks.replica import *  # noqa: F401 F403
from materialize.checks.roles import *  # noqa: F401 F403
from materialize.checks.rollback import *  # noqa: F401 F403
from materialize.checks.scenarios import *  # noqa: F401 F403
from materialize.checks.scenarios import Scenario
from materialize.checks.sink import *  # noqa: F401 F403
from materialize.checks.temporal_types import *  # noqa: F401 F403
from materialize.checks.text_bytea_types import *  # noqa: F401 F403
from materialize.checks.threshold import *  # noqa: F401 F403
from materialize.checks.top_k import *  # noqa: F401 F403
from materialize.checks.update import *  # noqa: F401 F403
from materialize.checks.upsert import *  # noqa: F401 F403
from materialize.checks.users import *  # noqa: F401 F403
from materialize.checks.window_functions import *  # noqa: F401 F403
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
    Materialized(
        options=" ".join(
            [
                "--persist-consensus-url=postgresql://postgres:postgres@postgres-backend:5432?options=--search_path=consensus",
                "--storage-stash-url=postgresql://postgres:postgres@postgres-backend:5432?options=--search_path=storage",
                "--adapter-stash-url=postgresql://postgres:postgres@postgres-backend:5432?options=--search_path=adapter",
            ]
        )
    ),
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
        [globals()[c] for c in args.check] if args.check else Check.__subclasses__()
    )

    for scenario_class in scenarios:
        print(f"Testing scenario {scenario_class}...")
        if args.execution_mode is ExecutionMode.ALLTOGETHER:
            setup(c)
            scenario = scenario_class(checks=checks)
            scenario.run(c)
            teardown(c)
        elif args.execution_mode is ExecutionMode.ONEATATIME:
            for check in checks:
                print(f"Running individual check {check}, scenario {scenario_class}")
                setup(c)
                scenario = scenario_class(checks=[check])
                scenario.run(c)
                teardown(c)
        else:
            assert False
