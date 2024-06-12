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


@dataclass
class AnnotationEntry:
    test_suite: str
    test_retry_count: int
    is_failure: bool
    count_known_errors: int
    count_unknown_errors: int
    markdown: str


def insert_annotation(
    cursor: Cursor,
    annotations: list[AnnotationEntry],
    verbose: bool = False,
) -> None:
    if True:
        # disable uploading annotations for now because of planned schema changes
        return

    build_id = buildkite.get_var(BuildkiteEnvVar.BUILDKITE_BUILD_ID)

    sql_statements = []

    for annotation in annotations:
        sql_statements.append(
            f"""
            INSERT INTO build_annotation
            (
                build_id,
                test_suite,
                test_retry_count,
                is_failure,
                markdown,
                count_known_errors,
                count_unknown_errors
            )
            SELECT
                '{build_id}',
                '{annotation.test_suite}',
                {annotation.test_retry_count},
                {annotation.is_failure},
                '{annotation.markdown}',
                {annotation.count_known_errors},
                {annotation.count_unknown_errors}
            ;
            """
        )

    execute_updates(sql_statements, cursor, verbose)
