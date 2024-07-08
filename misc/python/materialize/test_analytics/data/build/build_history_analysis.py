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


class BuildHistoryAnalysis(BaseDataStorage):

    def get_recent_build_job_failures(
        self,
        pipeline: str,
        branch: str,
        step_key: str,
        parallel_job_index: int | None,
        max_entries: int = 5,
        include_retries: bool = True,
    ) -> BuildHistory:
        shard_index_comparison = (
            f"shard_index = {parallel_job_index}"
            if parallel_job_index is not None
            else "shard_index IS NULL"
        )

        rows = self.query_data(
            f"""
                SELECT
                    build_job_id
                FROM
                    v_most_recent_build_job
                WHERE
                    pipeline = '{pipeline}'
                    AND branch = '{branch}'
                    AND build_step_key = '{step_key}'
                    AND {shard_index_comparison}
                """
        )

        assert len(rows) <= 1, f"Expected at most one row, got {len(rows)}"

        result = []

        if len(rows) == 1:
            build_job_id = rows[0][0]

            retry_filter = ""
            if not include_retries:
                retry_filter = "AND predecessor_is_latest_retry = TRUE"

            rows = self.query_data(
                f"""
                    SELECT
                        pipeline,
                        predecessor_build_number,
                        predecessor_build_id,
                        predecessor_build_job_id,
                        predecessor_build_step_success
                    FROM
                        mv_recent_build_job_success_on_main
                    WHERE
                        build_job_id = '{build_job_id}'
                        {retry_filter}
                    ORDER BY
                        predecessor_index ASC
                    LIMIT {max_entries}
                    """
            )

            for row in rows:
                (
                    pipeline,
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
            pipeline=pipeline, branch=branch, last_build_step_outcomes=result
        )
