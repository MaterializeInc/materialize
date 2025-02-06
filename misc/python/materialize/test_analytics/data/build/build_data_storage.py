# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from materialize import buildkite, git
from materialize.buildkite import BuildkiteEnvVar
from materialize.mz_version import MzVersion
from materialize.test_analytics.connector.test_analytics_connector import (
    DatabaseConnector,
)
from materialize.test_analytics.data.base_data_storage import BaseDataStorage
from materialize.test_analytics.util.mz_sql_util import as_sanitized_literal


class BuildDataStorage(BaseDataStorage):

    def __init__(self, database_connector: DatabaseConnector, data_version: int):
        super().__init__(database_connector)
        self.data_version = data_version

    def add_build(self) -> None:
        pipeline = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_PIPELINE_SLUG)
        build_number = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BUILD_NUMBER)
        build_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BUILD_ID)
        branch = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BRANCH)
        commit_hash = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_COMMIT)
        main_ancestor_commit_hash = git.get_common_ancestor_commit(
            remote=git.get_remote(), branch="main", fetch_branch=True
        )
        mz_version = MzVersion.parse_cargo()

        sql_statements = []
        sql_statements.append(
            f"""
            INSERT INTO build
            (
               pipeline,
               build_number,
               build_id,
               branch,
               commit_hash,
               main_ancestor_commit_hash,
               mz_version,
               date,
               data_version
            )
            SELECT
              {as_sanitized_literal(pipeline)},
              {build_number},
              {as_sanitized_literal(build_id)},
              {as_sanitized_literal(branch)},
              {as_sanitized_literal(commit_hash)},
              {as_sanitized_literal(main_ancestor_commit_hash)},
              {as_sanitized_literal(str(mz_version))},
              now(),
              {self.data_version}
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM build
                WHERE build_id = {as_sanitized_literal(build_id)}
            );
            """
        )

        self.database_connector.add_update_statements(sql_statements)

    def add_build_job(
        self,
        was_successful: bool,
        include_build: bool = True,
    ) -> None:
        if include_build:
            self.add_build()

        build_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BUILD_ID)
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)
        step_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_STEP_ID)
        step_key = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_STEP_KEY)
        # TODO: remove NULL casting when database-issues#8100 is resolved
        shard_index = buildkite.get_var(
            BuildkiteEnvVar.BUILDKITE_PARALLEL_JOB, "NULL::INT"
        )
        retry_count = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_RETRY_COUNT)
        agent_type = buildkite.get_var(
            BuildkiteEnvVar.BUILDKITE_AGENT_META_DATA_AWS_INSTANCE_TYPE
        ) or buildkite.get_var(BuildkiteEnvVar.BUILDKITE_AGENT_META_DATA_INSTANCE_TYPE)

        start_time_with_tz = os.getenv("STEP_START_TIMESTAMP_WITH_TZ")
        if buildkite.is_in_buildkite():
            assert (
                start_time_with_tz is not None
            ), "STEP_START_TIMESTAMP_WITH_TZ is not set"
            start_time_with_tz = (
                f"{as_sanitized_literal(start_time_with_tz)}::TIMESTAMPTZ"
            )
        else:
            start_time_with_tz = "NULL::TIMESTAMPTZ"

        sql_statements = []
        sql_statements.append(
            f"""
            INSERT INTO build_job
            (
                build_job_id,
                build_step_id,
                build_id,
                build_step_key,
                shard_index,
                retry_count,
                start_time,
                end_time,
                insert_date,
                is_latest_retry,
                success,
                agent_type
            )
            SELECT
              {as_sanitized_literal(job_id)},
              {as_sanitized_literal(step_id)},
              {as_sanitized_literal(build_id)},
              {as_sanitized_literal(step_key)},
              {shard_index},
              {retry_count},
              {start_time_with_tz},
              now(),
              now(),
              TRUE,
              {was_successful},
              {as_sanitized_literal(agent_type)}
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM build_job
                WHERE build_job_id = {as_sanitized_literal(job_id)}
            );
            """
        )

        sql_statements.append(
            f"""
            UPDATE build_job
            SET is_latest_retry = FALSE
            WHERE build_step_id = {as_sanitized_literal(step_id)}
            AND (shard_index = {shard_index} OR shard_index IS NULL)
            AND build_job_id <> {as_sanitized_literal(job_id)}
            ;
            """
        )

        self.database_connector.add_update_statements(sql_statements)

    def update_build_job_success(
        self,
        was_successful: bool,
    ) -> None:
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)

        sql_statements = []
        sql_statements.append(
            f"""
            UPDATE build_job
            SET success = {was_successful}
            WHERE build_job_id = {as_sanitized_literal(job_id)};
            """
        )

        self.database_connector.add_update_statements(sql_statements)

    def add_build_job_failure(
        self,
        part: str,
    ) -> None:
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)

        sql_statements = []
        sql_statements.append(
            f"""
            INSERT INTO build_job_failure
            (
                build_job_id,
                part
            )
            VALUES
            (
                {as_sanitized_literal(job_id)},
                {as_sanitized_literal(part)}
            )
            """
        )

        self.database_connector.add_update_statements(sql_statements)

    def get_part_priorities(self, timeout: int) -> dict[str, int]:
        branch = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BRANCH)
        build_step_key = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_STEP_KEY)
        with self.database_connector.create_cursor() as cur:
            cur.execute('SET cluster = "test_analytics"')
            cur.execute(f"SET statement_timeout = '{timeout}s'".encode("utf-8"))
            # 2 for failures in this PR
            # 1 for failed recently in CI
            cur.execute(
                f"""
            SELECT part, MAX(prio)
            FROM (
                SELECT part, 2 AS prio
                FROM mv_build_job_failed_on_branch
                WHERE branch = {as_sanitized_literal(branch)}
                  AND build_step_key = {as_sanitized_literal(build_step_key)}
              UNION
                SELECT part, 1 AS prio
                FROM mv_build_job_failed
                WHERE build_step_key = {as_sanitized_literal(build_step_key)}
            )
            GROUP BY part;
            """.encode(
                    "utf-8"
                )
            )
            results = cur.fetchall()
            return {part: prio for part, prio in results}
