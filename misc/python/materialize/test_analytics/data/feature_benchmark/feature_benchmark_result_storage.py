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


@dataclass
class FeatureBenchmarkResultEntry:
    scenario_name: str
    scenario_group: str
    scenario_version: str
    scale: str
    wallclock: float | None
    messages: int | None
    memory_mz: float | None
    memory_clusterd: float | None


@dataclass
class FeatureBenchmarkDiscardedResultEntry(FeatureBenchmarkResultEntry):
    cycle: int


class FeatureBenchmarkResultStorage(BaseDataStorage):

    def add_result(
        self,
        framework_version: str,
        results: list[FeatureBenchmarkResultEntry],
    ) -> None:
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)

        sql_statements = []

        for result_entry in results:
            # TODO: remove NULL castings when #27429 is resolved
            sql_statements.append(
                f"""
                INSERT INTO feature_benchmark_result
                (
                    build_job_id,
                    framework_version,
                    scenario_name,
                    scenario_group,
                    scenario_version,
                    scale,
                    wallclock,
                    messages,
                    memory_mz,
                    memory_clusterd
                )
                SELECT
                    '{job_id}',
                    '{framework_version}',
                    '{result_entry.scenario_name}',
                    '{result_entry.scenario_group}',
                    '{result_entry.scenario_version}',
                    '{result_entry.scale}',
                    {result_entry.wallclock or 'NULL::DOUBLE'},
                    {result_entry.messages or 'NULL::INT'},
                    {result_entry.memory_mz or 'NULL::DOUBLE'},
                    {result_entry.memory_clusterd or 'NULL::DOUBLE'}
                ;
                """
            )

        self.database_connector.add_update_statements(sql_statements)

    def add_discarded_entries(
        self,
        discarded_entries: list[FeatureBenchmarkDiscardedResultEntry],
    ) -> None:
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)

        sql_statements = []

        for discarded_entry in discarded_entries:
            # TODO: remove NULL castings when #27429 is resolved

            # Do not store framework version, scenario version, and scale. If needed, they can be retrieved from the
            # result entries.
            sql_statements.append(
                f"""
                INSERT INTO feature_benchmark_discarded_result
                (
                    build_job_id,
                    scenario_name,
                    cycle,
                    wallclock,
                    messages,
                    memory_mz,
                    memory_clusterd
                )
                SELECT
                    '{job_id}',
                    '{discarded_entry.scenario_name}',
                    {discarded_entry.cycle},
                    {discarded_entry.wallclock or 'NULL::DOUBLE'},
                    {discarded_entry.messages or 'NULL::INT'},
                    {discarded_entry.memory_mz or 'NULL::DOUBLE'},
                    {discarded_entry.memory_clusterd or 'NULL::DOUBLE'}
                ;
                """
            )

        self.database_connector.add_update_statements(sql_statements)
