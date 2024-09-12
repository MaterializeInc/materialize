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
class ScalabilityFrameworkResultEntry:
    workload_name: str
    workload_group: str
    workload_version: str
    concurrency: int
    count: int
    tps: float


class ScalabilityFrameworkResultStorage(BaseDataStorage):

    def add_result(
        self,
        framework_version: str,
        results: list[ScalabilityFrameworkResultEntry],
    ) -> None:
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)

        sql_statements = []

        for result_entry in results:
            sql_statements.append(
                f"""
                    INSERT INTO scalability_framework_result
                    (
                        build_job_id,
                        framework_version,
                        workload_name,
                        workload_group,
                        workload_version,
                        concurrency,
                        count,
                        tps
                    )
                    SELECT
                        {as_sanitized_literal(job_id)},
                        {as_sanitized_literal(framework_version)},
                        {as_sanitized_literal(result_entry.workload_name)},
                        {as_sanitized_literal(result_entry.workload_group)},
                        {as_sanitized_literal(result_entry.workload_version)},
                        {result_entry.concurrency},
                        {result_entry.count},
                        {result_entry.tps}
                    ;
                    """
            )

        self.database_connector.add_update_statements(sql_statements)
