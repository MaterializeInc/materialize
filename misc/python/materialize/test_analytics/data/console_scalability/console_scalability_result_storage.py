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
class ConsoleScalabilityResultEntry:
    workload: str
    phase: str
    viewers: int | None
    seconds: float
    catalog_server_size: str
    catalog_server_cores: float | None
    environmentd_cores: float | None
    scheduled_cores: float
    open_subscribes: int
    succeeded: bool


def _sql_value(value: float | int | None, null_type: str) -> str:
    return str(value) if value is not None else f"NULL::{null_type}"


class ConsoleScalabilityResultStorage(BaseDataStorage):

    def add_result(
        self,
        framework_version: str,
        results: list[ConsoleScalabilityResultEntry],
    ) -> None:
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)

        sql_statements = []

        for result_entry in results:
            # TODO: remove NULL castings when database-issues#8100 is resolved
            sql_statements.append(f"""
                INSERT INTO console_scalability_result
                (
                    build_job_id,
                    framework_version,
                    workload,
                    phase,
                    viewers,
                    seconds,
                    catalog_server_size,
                    catalog_server_cores,
                    environmentd_cores,
                    scheduled_cores,
                    open_subscribes,
                    succeeded
                )
                SELECT
                    {as_sanitized_literal(job_id)},
                    {as_sanitized_literal(framework_version)},
                    {as_sanitized_literal(result_entry.workload)},
                    {as_sanitized_literal(result_entry.phase)},
                    {_sql_value(result_entry.viewers, "INT")},
                    {result_entry.seconds},
                    {as_sanitized_literal(result_entry.catalog_server_size)},
                    {_sql_value(result_entry.catalog_server_cores, "DOUBLE")},
                    {_sql_value(result_entry.environmentd_cores, "DOUBLE")},
                    {result_entry.scheduled_cores},
                    {result_entry.open_subscribes},
                    {result_entry.succeeded}
                ;
                """)

        self.database_connector.add_update_statements(sql_statements)
