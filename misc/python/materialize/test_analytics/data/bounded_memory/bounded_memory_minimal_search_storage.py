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

BOUNDED_MEMORY_STATUS_PENDING = "PENDING"
BOUNDED_MEMORY_STATUS_SUCCESS = "SUCCESS"
BOUNDED_MEMORY_STATUS_FAILURE = "FAILURE"
BOUNDED_MEMORY_STATUS_CONFIGURED = "CONFIGURED"


@dataclass
class BoundedMemoryMinimalSearchEntry:
    scenario_name: str
    tested_memory_mz_in_gb: float
    tested_memory_clusterd_in_gb: float


class BoundedMemoryMinimalSearchStorage(BaseDataStorage):

    def add_entry(
        self,
        framework_version: str,
        entry: BoundedMemoryMinimalSearchEntry,
        minimization_target: str,
        flush: bool = True,
    ) -> None:
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)

        sql_statement = f"""
            INSERT INTO bounded_memory_config
            (
                build_job_id,
                framework_version,
                scenario_name,
                tested_memory_mz_in_gb,
                tested_memory_clusterd_in_gb,
                minimization_target,
                started_at,
                status
            )
            SELECT
                {as_sanitized_literal(job_id)},
                {as_sanitized_literal(framework_version)},
                {as_sanitized_literal(entry.scenario_name)},
                {entry.tested_memory_mz_in_gb},
                {entry.tested_memory_clusterd_in_gb},
                {as_sanitized_literal(minimization_target)},
                now(),
                '{BOUNDED_MEMORY_STATUS_PENDING}';
            """

        self.database_connector.add_update_statements([sql_statement])

        if flush:
            self.database_connector.submit_update_statements()

    def update_status(
        self,
        entry: BoundedMemoryMinimalSearchEntry,
        status: str,
        minimization_target: str,
        flush: bool = True,
    ) -> None:
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)

        sql_statement = f"""
            UPDATE bounded_memory_config
            SET status = {as_sanitized_literal(status)}
            WHERE build_job_id = {as_sanitized_literal(job_id)}
            AND scenario_name = {as_sanitized_literal(entry.scenario_name)}
            AND tested_memory_mz_in_gb = {entry.tested_memory_mz_in_gb}
            AND tested_memory_clusterd_in_gb = {entry.tested_memory_clusterd_in_gb}
            AND minimization_target = {as_sanitized_literal(minimization_target)}
            ;
            """

        self.database_connector.add_update_statements([sql_statement])

        if flush:
            self.database_connector.submit_update_statements()
