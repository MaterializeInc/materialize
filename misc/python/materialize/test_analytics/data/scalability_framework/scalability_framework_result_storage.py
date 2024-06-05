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
class ScalabilityFrameworkResultEntry:
    workload_name: str
    workload_version: str
    concurrency: int
    count: int
    tps: float


def insert_result(
    cursor: Cursor,
    framework_version: str,
    results: list[ScalabilityFrameworkResultEntry],
    verbose: bool = False,
) -> None:
    step_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_STEP_ID)

    sql_statements = []

    for result_entry in results:
        sql_statements.append(
            f"""
                INSERT INTO scalability_framework_result
                (
                    build_step_id,
                    framework_version,
                    workload_name,
                    workload_version,
                    concurrency,
                    count,
                    tps
                )
                SELECT
                    '{step_id}',
                    '{framework_version}',
                    '{result_entry.workload_name}',
                    '{result_entry.workload_version}',
                    {result_entry.concurrency},
                    {result_entry.count},
                    {result_entry.tps}
                ;
                """
        )

    execute_updates(sql_statements, cursor, verbose)
