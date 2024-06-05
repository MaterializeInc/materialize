# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from pg8000 import Cursor

from materialize import buildkite
from materialize.buildkite import BuildkiteEnvVar
from materialize.test_analytics.connection.test_analytics_connection import (
    execute_updates,
)


def insert_build(cursor: Cursor, verbose: bool = False) -> None:
    pipeline = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_PIPELINE_SLUG)
    build_number = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BUILD_NUMBER)
    build_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BUILD_ID)
    branch = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BRANCH)
    commit_hash = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_COMMIT)
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
           date,
           build_url,
           remarks
        )
        SELECT
          '{pipeline}',
          {build_number},
          '{build_id}',
          '{branch}',
          '{commit_hash}',
          now(),
          '{build_url}',
          NULL
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM build
            WHERE pipeline = '{pipeline}'
            AND build_number = {build_number}
        );
        """
    )

    execute_updates(sql_statements, cursor, verbose)


def insert_build_step(
    cursor: Cursor, was_successful: bool, verbose: bool = False
) -> None:
    pipeline = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_PIPELINE_SLUG)
    build_number = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BUILD_NUMBER)
    build_url = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BUILD_URL)
    step_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_STEP_ID)
    step_key = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_STEP_KEY)
    # TODO: remove NULL casting when #27429 is resolved
    shard_index = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_PARALLEL_JOB, "NULL::INT")
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
            pipeline,
            build_number,
            build_step_key,
            shard_index,
            retry_count,
            url,
            is_latest_retry,
            success,
            aws_instance_type,
            remarks
        )
        SELECT
          '{step_id}',
          '{pipeline}',
          {build_number},
          '{step_key}',
          {shard_index},
          {retry_count},
          '{build_step_url}',
          TRUE,
          {was_successful},
          '{aws_instance_type}',
          NULL
        ;
        """
    )

    sql_statements.append(
        f"""
        UPDATE build_step
        SET is_latest_retry = FALSE
        WHERE pipeline = '{pipeline}'
        AND build_number = {build_number}
        AND build_step_key = '{step_key}'
        AND (shard_index = {shard_index} OR shard_index IS NULL)
        AND build_step_id <> '{step_id}'
        ;
        """
    )

    execute_updates(sql_statements, cursor, verbose)
