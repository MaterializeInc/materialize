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
    type: str
    header: str | None
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
        annotation_header_value = (
            "NULL" if annotation.header is None else f"'{annotation.header}'"
        )

        sql_statements.append(
            f"""
            INSERT INTO build_annotation
            (
                build_id,
                type,
                header,
                markdown
            )
            SELECT
                '{build_id}',
                '{annotation.type}',
                {annotation_header_value},
                '{annotation.markdown}'
            ;
            """
        )

    execute_updates(sql_statements, cursor, verbose)
