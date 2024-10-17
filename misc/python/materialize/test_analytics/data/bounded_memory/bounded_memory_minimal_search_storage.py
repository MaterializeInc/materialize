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
class BoundedMemoryMinimalSearchEntry:
    scenario_name: str
    configured_memory_mz_in_gb: float
    configured_memory_clusterd_in_gb: float
    found_memory_mz_in_gb: float
    found_memory_clusterd_in_gb: float


class BoundedMemoryMinimalSearchStorage(BaseDataStorage):

    def add_entry(
        self,
        framework_version: str,
        entry: BoundedMemoryMinimalSearchEntry,
    ) -> None:
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)

        sql_statement = f"""
            INSERT INTO bounded_memory_min_found_config
            (
                build_job_id,
                framework_version,
                scenario_name,
                configured_memory_mz_in_gb,
                configured_memory_clusterd_in_gb,
                found_memory_mz_in_gb,
                found_memory_clusterd_in_gb
            )
            SELECT
                {as_sanitized_literal(job_id)},
                {as_sanitized_literal(framework_version)},
                {as_sanitized_literal(entry.scenario_name)},
                {entry.configured_memory_mz_in_gb},
                {entry.configured_memory_clusterd_in_gb},
                {entry.found_memory_mz_in_gb},
                {entry.found_memory_clusterd_in_gb};
            """

        self.database_connector.add_update_statements([sql_statement])
