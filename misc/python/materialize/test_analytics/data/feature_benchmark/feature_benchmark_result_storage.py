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
from materialize.feature_benchmark.report import ReportMeasurement
from materialize.test_analytics.data.base_data_storage import BaseDataStorage
from materialize.test_analytics.util.mz_sql_util import as_sanitized_literal


@dataclass
class FeatureBenchmarkResultEntry:
    scenario_name: str
    scenario_group: str
    scenario_version: str
    cycle: int
    scale: str
    is_regression: bool
    wallclock: ReportMeasurement[float]
    memory_mz: ReportMeasurement[float]
    memory_clusterd: ReportMeasurement[float]


class FeatureBenchmarkResultStorage(BaseDataStorage):

    def add_result(
        self,
        framework_version: str,
        results: list[FeatureBenchmarkResultEntry],
    ) -> None:
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)

        sql_statements = []

        for result_entry in results:
            # TODO: remove NULL castings when database-issues#8100 is resolved
            sql_statements.append(
                f"""
                INSERT INTO feature_benchmark_result
                (
                    build_job_id,
                    framework_version,
                    scenario_name,
                    scenario_group,
                    scenario_version,
                    cycle,
                    scale,
                    is_regression,
                    wallclock,
                    messages,
                    memory_mz,
                    memory_clusterd,
                    wallclock_min,
                    wallclock_max,
                    wallclock_mean,
                    wallclock_variance
                )
                SELECT
                    {as_sanitized_literal(job_id)},
                    {as_sanitized_literal(framework_version)},
                    {as_sanitized_literal(result_entry.scenario_name)},
                    {as_sanitized_literal(result_entry.scenario_group)},
                    {as_sanitized_literal(result_entry.scenario_version)},
                    {result_entry.cycle},
                    {as_sanitized_literal(result_entry.scale)},
                    {result_entry.is_regression},
                    {result_entry.wallclock.result or 'NULL::DOUBLE'},
                    NULL::INT,  -- to be removed when we remove the "messages" column
                    {result_entry.memory_mz.result or 'NULL::DOUBLE'},
                    {result_entry.memory_clusterd.result or 'NULL::DOUBLE'},
                    {result_entry.wallclock.min or 'NULL::DOUBLE'},
                    {result_entry.wallclock.max or 'NULL::DOUBLE'},
                    {result_entry.wallclock.mean or 'NULL::DOUBLE'},
                    {result_entry.wallclock.variance or 'NULL::DOUBLE'}
                ;
                """
            )

        self.database_connector.add_update_statements(sql_statements)

    def add_discarded_entries(
        self,
        discarded_entries: list[FeatureBenchmarkResultEntry],
    ) -> None:
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)

        sql_statements = []

        for discarded_entry in discarded_entries:
            # TODO: remove NULL castings when database-issues#8100 is resolved

            # Do not store framework version, scenario version, and scale. If needed, they can be retrieved from the
            # result entries.
            sql_statements.append(
                f"""
                INSERT INTO feature_benchmark_discarded_result
                (
                    build_job_id,
                    scenario_name,
                    cycle,
                    is_regression,
                    wallclock,
                    messages,
                    memory_mz,
                    memory_clusterd,
                    wallclock_min,
                    wallclock_max,
                    wallclock_mean,
                    wallclock_variance
                )
                SELECT
                    {as_sanitized_literal(job_id)},
                    {as_sanitized_literal(discarded_entry.scenario_name)},
                    {discarded_entry.cycle},
                    {discarded_entry.is_regression},
                    {discarded_entry.wallclock.result or 'NULL::DOUBLE'},
                    NULL::INT,  -- to be removed when we remove the "messages" column
                    {discarded_entry.memory_mz.result or 'NULL::DOUBLE'},
                    {discarded_entry.memory_clusterd.result or 'NULL::DOUBLE'},
                    {discarded_entry.wallclock.min or 'NULL::DOUBLE'},
                    {discarded_entry.wallclock.max or 'NULL::DOUBLE'},
                    {discarded_entry.wallclock.mean or 'NULL::DOUBLE'},
                    {discarded_entry.wallclock.variance or 'NULL::DOUBLE'}
                ;
                """
            )

        self.database_connector.add_update_statements(sql_statements)
