# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from dataclasses import dataclass
from math import isfinite

from materialize import buildkite
from materialize.buildkite import BuildkiteEnvVar
from materialize.test_analytics.data.base_data_storage import BaseDataStorage
from materialize.test_analytics.util.mz_sql_util import as_sanitized_literal


@dataclass
class ParallelBenchmarkResultEntry:
    scenario_name: str
    scenario_version: str
    query: str
    load_phase_duration: int | None
    queries: int
    qps: float
    min: float
    max: float
    avg: float
    p50: float
    p95: float
    p99: float
    p99_9: float
    p99_99: float
    p99_999: float
    p99_9999: float
    p99_99999: float
    p99_999999: float
    std: float
    slope: float


class ParallelBenchmarkResultStorage(BaseDataStorage):

    def add_result(
        self,
        framework_version: str,
        results: list[ParallelBenchmarkResultEntry],
    ) -> None:
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)

        sql_statements = []

        for result_entry in results:
            # TODO: remove NULL castings when database-issues#8100 is resolved
            sql_statements.append(
                f"""
                INSERT INTO parallel_benchmark_result
                (
                    build_job_id,
                    framework_version,
                    scenario_name,
                    scenario_version,
                    query,
                    load_phase_duration,
                    queries,
                    qps,
                    min,
                    max,
                    avg,
                    p50,
                    p95,
                    p99,
                    p99_9,
                    p99_99,
                    p99_999,
                    p99_9999,
                    p99_99999,
                    p99_999999,
                    std,
                    slope
                )
                SELECT
                    {as_sanitized_literal(job_id)},
                    {as_sanitized_literal(framework_version)},
                    {as_sanitized_literal(result_entry.scenario_name)},
                    {as_sanitized_literal(result_entry.scenario_version)},
                    {as_sanitized_literal(result_entry.query)},
                    {result_entry.load_phase_duration or 'NULL::INT'},
                    {result_entry.queries},
                    {result_entry.qps},
                    {result_entry.min},
                    {result_entry.max},
                    {result_entry.avg},
                    {result_entry.p50},
                    {result_entry.p95},
                    {result_entry.p99},
                    {result_entry.p99_9},
                    {result_entry.p99_99},
                    {result_entry.p99_999},
                    {result_entry.p99_9999},
                    {result_entry.p99_99999},
                    {result_entry.p99_999999},
                    {result_entry.std if isfinite(result_entry.std) else 'NULL::FLOAT'},
                    {result_entry.slope}
                ;
                """
            )

        self.database_connector.add_update_statements(sql_statements)
