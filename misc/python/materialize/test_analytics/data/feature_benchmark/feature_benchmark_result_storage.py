# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from dataclasses import dataclass

from pg8000 import Cursor

from materialize import buildkite
from materialize.buildkite import BuildkiteEnvVar
from materialize.test_analytics.connection.test_analytics_connection import (
    execute_updates,
)


@dataclass
class FeatureBenchmarkResultEntry:
    scenario_name: str
    scenario_version: str
    wallclock: float | None
    messages: int | None
    memory: float | None


def insert_result(
    cursor: Cursor,
    framework_version: str,
    results: list[FeatureBenchmarkResultEntry],
    verbose: bool = False,
) -> None:
    step_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_STEP_ID)

    sql_statements = []

    for result_entry in results:
        # TODO: remove NULL castings when #27429 is resolved
        sql_statements.append(
            f"""
            INSERT INTO feature_benchmark_results
            (
                build_step_id,
                framework_version,
                scenario_name,
                scenario_version,
                wallclock,
                messages,
                memory
            )
            SELECT
                '{step_id}',
                '{framework_version}',
                '{result_entry.scenario_name}',
                '{result_entry.scenario_version}',
                {result_entry.wallclock or 'NULL::DOUBLE'},
                {result_entry.messages or 'NULL::INT'},
                {result_entry.memory or 'NULL::DOUBLE'}
            ;
            """
        )

    execute_updates(sql_statements, cursor, verbose)
