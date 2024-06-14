# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize import buildkite
from materialize.buildkite import BuildkiteEnvVar
from materialize.mz_version import MzVersion
from materialize.test_analytics.data.base_data_storage import BaseDataStorage
from materialize.test_analytics.writer.test_analytics_writer import RawDatabaseWriter


class BuildDataStorage(BaseDataStorage):

    def __init__(self, writer: RawDatabaseWriter, data_version: int):
        super().__init__(writer)
        self.data_version = data_version

    def insert_build(self) -> None:
        pipeline = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_PIPELINE_SLUG)
        build_number = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BUILD_NUMBER)
        build_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BUILD_ID)
        branch = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BRANCH)
        commit_hash = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_COMMIT)
        mz_version = MzVersion.parse_cargo()
        build_url = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BUILD_URL)

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
               mz_version,
               date,
               build_url,
               data_version,
               remarks
            )
            SELECT
              '{pipeline}',
              {build_number},
              '{build_id}',
              '{branch}',
              '{commit_hash}',
              '{mz_version}',
              now(),
              '{build_url}',
              {self.data_version},
              NULL
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM build
                WHERE build_id = '{build_id}'
            );
            """
        )

        self.writer.execute_updates(sql_statements)

    def insert_build_step(
        self,
        was_successful: bool,
        include_insert_build: bool = True,
    ) -> None:
        if include_insert_build:
            self.insert_build()

        build_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BUILD_ID)
        build_url = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BUILD_URL)
        step_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_STEP_ID)
        step_key = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_STEP_KEY)
        # TODO: remove NULL casting when #27429 is resolved
        shard_index = buildkite.get_var(
            BuildkiteEnvVar.BUILDKITE_PARALLEL_JOB, "NULL::INT"
        )
        retry_count = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_RETRY_COUNT)
        build_step_url = f"{build_url}#{step_id}"
        aws_instance_type = buildkite.get_var(
            BuildkiteEnvVar.BUILDKITE_AGENT_META_DATA_AWS_INSTANCE_TYPE
        )

        sql_statements = []
        sql_statements.append(
            f"""
            INSERT INTO build_step
            (
                build_step_id,
                build_id,
                build_step_key,
                shard_index,
                retry_count,
                insert_date,
                url,
                is_latest_retry,
                success,
                aws_instance_type,
                remarks
            )
            SELECT
              '{step_id}',
              '{build_id}',
              '{step_key}',
              {shard_index},
              {retry_count},
              now(),
              '{build_step_url}',
              TRUE,
              {was_successful},
              '{aws_instance_type}',
              NULL
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM build_step
                WHERE build_step_id = '{step_id}'
            );
            """
        )

        sql_statements.append(
            f"""
            UPDATE build_step
            SET is_latest_retry = FALSE
            WHERE build_id = '{build_id}'
            AND build_step_key = '{step_key}'
            AND (shard_index = {shard_index} OR shard_index IS NULL)
            AND build_step_id <> '{step_id}'
            ;
            """
        )

        self.writer.execute_updates(sql_statements)

    def update_build_step_success(
        self,
        was_successful: bool,
    ) -> None:
        step_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_STEP_ID)

        sql_statements = []
        sql_statements.append(
            f"""
            UPDATE build_step
            SET success = {was_successful}
            WHERE build_step_id = '{step_id}';
            """
        )

        self.writer.execute_updates(sql_statements)
