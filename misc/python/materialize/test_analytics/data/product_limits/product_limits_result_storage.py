# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from dataclasses import dataclass

from materialize import buildkite
from materialize.buildkite import BuildkiteEnvVar
from materialize.test_analytics.data.base_data_storage import BaseDataStorage
from materialize.test_analytics.util.mz_sql_util import as_sanitized_literal


@dataclass
class ProductLimitsResultEntry:
    scenario_name: str
    scenario_version: str
    count: int
    wallclock: float
    explain_wallclock: float | None


class ProductLimitsResultStorage(BaseDataStorage):

    def add_result(
        self,
        framework_version: str,
        results: list[ProductLimitsResultEntry],
    ) -> None:
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)

        sql_statements = []

        for result_entry in results:
            # TODO: remove NULL castings when database-issues#8100 is resolved
            sql_statements.append(
                f"""
                INSERT INTO product_limits_result
                (
                    build_job_id,
                    framework_version,
                    scenario_name,
                    scenario_version,
                    count,
                    wallclock,
                    explain_wallclock
                )
                SELECT
                    {as_sanitized_literal(job_id)},
                    {as_sanitized_literal(framework_version)},
                    {as_sanitized_literal(result_entry.scenario_name)},
                    {as_sanitized_literal(result_entry.scenario_version)},
                    {result_entry.count},
                    {result_entry.wallclock},
                    {result_entry.explain_wallclock or 'NULL::DOUBLE'}
                ;
                """
            )

        self.database_connector.add_update_statements(sql_statements)
