# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.buildkite_insights.data.build_annotation import BuildAnnotation
from materialize.buildkite_insights.data.build_info import Build
from materialize.test_analytics.config.test_analytics_db_config import (
    create_test_analytics_config_with_credentials,
)
from materialize.test_analytics.test_analytics_db import TestAnalyticsDb
from materialize.test_analytics.util.mz_sql_util import as_sanitized_literal

ANY_PIPELINE_VALUE = "*"
ANY_BRANCH_VALUE = "*"


class TestAnalyticsDataSource:
    __test__ = False

    def __init__(
        self,
        test_analytics_hostname: str,
        test_analytics_username: str,
        test_analytics_app_password: str,
    ):
        test_analytics_db_config = create_test_analytics_config_with_credentials(
            test_analytics_hostname,
            test_analytics_username,
            test_analytics_app_password,
        )
        self.test_analytics_db = TestAnalyticsDb(test_analytics_db_config)

    def search_annotations(
        self,
        pipeline: str,
        branch: str | None,
        build_step_keys: list[str],
        not_newer_than_build_number: int | None,
        like_pattern: str,
        max_entries: int,
        only_failed_builds: bool,
    ) -> list[tuple[Build, BuildAnnotation]]:
        pipeline_clause = (
            f" AND bae.pipeline = {as_sanitized_literal(pipeline)}"
            if pipeline != ANY_PIPELINE_VALUE
            else ""
        )
        branch_clause = (
            f" AND bae.branch = {as_sanitized_literal(branch)}"
            if branch is not None
            else ""
        )
        failed_builds_clause = " AND bsu.has_failed_steps" if only_failed_builds else ""
        if len(build_step_keys) > 0:
            in_build_step_keys = ",".join(
                as_sanitized_literal(key) for key in build_step_keys
            )
            build_steps_keys_clause = (
                f" AND bae.build_step_key IN ({in_build_step_keys})"
            )
        else:
            build_steps_keys_clause = ""

        build_number_offset_clause = (
            f"AND bae.build_number <= {not_newer_than_build_number}"
            if not_newer_than_build_number is not None
            else ""
        )

        order_clause = (
            "bae.build_number DESC"
            if pipeline != ANY_PIPELINE_VALUE
            else "bae.build_date DESC"
        )

        result_rows = self.test_analytics_db.build_annotations.query_data(
            dedent(
                f"""
            SELECT
                bae.build_number,
                bae.pipeline,
                CASE WHEN bsu.has_failed_steps THEN 'FAILED' else 'PASSED' END AS state,
                bae.branch,
                bsu.build_url,
                bae.build_date,
                bae.test_suite,
                bae.content
            FROM v_build_annotation_error bae
            INNER JOIN v_build_success bsu
            ON bae.build_id = bsu.build_id
            WHERE bae.content ILIKE {as_sanitized_literal(like_pattern)}
            {pipeline_clause}
            {branch_clause}
            {failed_builds_clause}
            {build_steps_keys_clause}
            {build_number_offset_clause}
            ORDER BY
                {order_clause}
            LIMIT {max_entries}
            """
            ),
            verbose=False,
            statement_timeout="5s",
        )

        result = []

        for row in result_rows:
            build = Build(
                number=row[0],
                pipeline=row[1],
                state=row[2],
                branch=row[3],
                web_url=row[4],
                created_at=row[5],
            )
            annotation = BuildAnnotation(title=row[6], content=row[7])

            result.append((build, annotation))

        return result
