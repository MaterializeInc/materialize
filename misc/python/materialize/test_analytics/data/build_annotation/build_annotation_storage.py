# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from dataclasses import dataclass

from pg8000 import Cursor

from materialize import buildkite
from materialize.buildkite import BuildkiteEnvVar
from materialize.test_analytics.connection.test_analytics_connection import (
    execute_updates,
)
from materialize.test_analytics.util import mz_sql_util


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


def insert_annotation(
    cursor: Cursor,
    annotation: AnnotationEntry,
    verbose: bool = False,
) -> None:
    if True:
        # disable uploading annotations for now because of planned schema changes
        return

    build_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BUILD_ID)
    step_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_STEP_ID)

    sql_statements = []

    sql_statements.append(
        f"""
        INSERT INTO build_annotation
        (
            build_id,
            build_step_id,
            test_suite,
            test_retry_count,
            is_failure,
            insert_date
        )
        SELECT
            '{build_id}',
            '{step_id}',
            '{mz_sql_util.sanitize_literal_value(annotation.test_suite)}',
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
                build_step_id,
                error_type,
                content,
                issue,
                occurrence_count
            )
            SELECT
                '{step_id}',
                '{error.error_type}',
                '{mz_sql_util.sanitize_literal_value(error.message)}',
                {mz_sql_util.as_literal_or_null(error.issue)},
                {error.occurrence_count}
        ;
        """
        )

    execute_updates(sql_statements, cursor, verbose)
