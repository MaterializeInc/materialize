# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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
from materialize.checks.nested_types import *  # noqa: F401 F403
from materialize.checks.null_value import *  # noqa: F401 F403
from materialize.checks.numeric_types import *  # noqa: F401 F403
from materialize.checks.recorded_views import *  # noqa: F401 F403
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
from materialize.checks.temporal_types import *  # noqa: F401 F403
from materialize.checks.text_bytea_types import *  # noqa: F401 F403
from materialize.checks.threshold import *  # noqa: F401 F403
from materialize.checks.top_k import *  # noqa: F401 F403
from materialize.checks.update import *  # noqa: F401 F403
from materialize.checks.upsert import *  # noqa: F401 F403
from materialize.checks.users import *  # noqa: F401 F403
from materialize.checks.window_functions import *  # noqa: F401 F403
from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import Materialized, Postgres, Redpanda
from materialize.mzcompose.services import Testdrive as TestdriveService

SERVICES = [
    Postgres(name="postgres-stash"),
    Redpanda(),
    Materialized(
        options=" ".join(
            [
                "--persist-consensus-url=postgresql://postgres:postgres@postgres-stash:5432?options=--search_path=consensus",
                "--storage-stash-url=postgresql://postgres:postgres@postgres-stash:5432?options=--search_path=storage",
                "--adapter-stash-url=postgresql://postgres:postgres@postgres-stash:5432?options=--search_path=adapter",
            ]
        )
    ),
    TestdriveService(default_timeout="300s", no_reset=True, seed=1),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    # c.silent = True

    parser.add_argument(
        "--scenario", metavar="SCENARIO", type=str, help="Scenario to run."
    )

    parser.add_argument("--check", metavar="CHECK", type=str, help="Check to run.")

    args = parser.parse_args()

    c.up("testdrive", persistent=True)

    c.start_and_wait_for_tcp(services=["redpanda", "postgres-stash"])
    c.wait_for_postgres(service="postgres-stash")
    c.sql(
        sql=f"""
       CREATE SCHEMA IF NOT EXISTS consensus;
       CREATE SCHEMA IF NOT EXISTS storage;
       CREATE SCHEMA IF NOT EXISTS adapter;
    """,
        service="postgres-stash",
        user="postgres",
        password="postgres",
    )

    scenarios = (
        [globals()[args.scenario]] if args.scenario else Scenario.__subclasses__()
    )

    checks = [globals()[args.check]] if args.check else Check.__subclasses__()

    for scenario_class in scenarios:
        print(f"Testing upgrade scenario {scenario_class}")
        scenario = scenario_class(checks=checks)
        scenario.run(c)
