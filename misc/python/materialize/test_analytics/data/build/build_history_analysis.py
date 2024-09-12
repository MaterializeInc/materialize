# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize import buildkite
from materialize.buildkite import BuildkiteEnvVar
from materialize.buildkite_insights.data.build_history import (
    BuildHistory,
    BuildHistoryEntry,
)
from materialize.mz_version import MzVersion
from materialize.test_analytics.connector.test_analytics_connector import (
    DatabaseConnector,
)
from materialize.test_analytics.data.base_data_storage import BaseDataStorage
from materialize.test_analytics.util.mz_sql_util import as_sanitized_literal


class BuildHistoryAnalysis(BaseDataStorage):

    def get_recent_build_job_failures_on_main(
        self,
        pipeline: str,
        step_key: str,
        parallel_job_index: int | None,
    ) -> BuildHistory:
        shard_index_comparison = (
            f"shard_index = {parallel_job_index}"
            if parallel_job_index is not None
            else "shard_index IS NULL"
        )

        rows = self.query_data(
            f"""
                SELECT
                    build_number,
                    build_id,
                    build_job_id,
                    build_step_success
                FROM
                    mv_recent_build_job_success_on_main_v2
                WHERE
                    pipeline = {as_sanitized_literal(pipeline)} AND
                    build_step_key = {as_sanitized_literal(step_key)} AND
                    {shard_index_comparison}
                ORDER BY
                    build_number DESC
                """
        )

        result = []

        for row in rows:
            (
                predecessor_build_number,
                predecessor_build_id,
                predecessor_build_job_id,
                predecessor_build_step_success,
            ) = row
            url_to_job = buildkite.get_job_url_from_pipeline_and_build(
                pipeline, predecessor_build_number, predecessor_build_job_id
            )
            result.append(
                BuildHistoryEntry(
                    url_to_job=url_to_job, passed=predecessor_build_step_success
                )
            )

        return BuildHistory(
            pipeline=pipeline, branch="main", last_build_step_outcomes=result
        )
