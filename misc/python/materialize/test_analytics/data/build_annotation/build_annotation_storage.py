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
class AnnotationErrorEntry:
    error_type: str
    message: str
    issue: str | None
    occurrence_count: int


@dataclass
class AnnotationEntry:
    test_suite: str
    test_retry_count: int
    is_failure: bool
    errors: list[AnnotationErrorEntry]


class BuildAnnotationStorage(BaseDataStorage):

    def add_annotation(
        self,
        annotation: AnnotationEntry,
    ) -> None:
        build_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BUILD_ID)
        job_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_JOB_ID)

        sql_statements = []

        sql_statements.append(
            f"""
            INSERT INTO build_annotation
            (
                build_id,
                build_job_id,
                test_suite,
                test_retry_count,
                is_failure,
                insert_date
            )
            SELECT
                {as_sanitized_literal(build_id)},
                {as_sanitized_literal(job_id)},
                {as_sanitized_literal(annotation.test_suite)},
                {annotation.test_retry_count},
                {annotation.is_failure},
                now()
            ;
                """
        )

        for error in annotation.errors:
            sql_statements.append(
                f"""
                INSERT INTO build_annotation_error
                (
                    build_job_id,
                    error_type,
                    content,
                    issue,
                    occurrence_count
                )
                SELECT
                    {as_sanitized_literal(job_id)},
                    {as_sanitized_literal(error.error_type)},
                    {as_sanitized_literal(error.message)},
                    {as_sanitized_literal(error.issue)},
                    {error.occurrence_count}
            ;
            """
            )

        self.database_connector.add_update_statements(sql_statements)
