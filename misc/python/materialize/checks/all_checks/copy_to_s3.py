# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check, externally_idempotent


@externally_idempotent(False)
class CopyToS3(Check):
    """Basic check on copy to s3"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $[version>=8000] postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_aws_connection = true
                ALTER SYSTEM SET enable_connection_validation_syntax = true

                > CREATE SECRET minio AS 'minioadmin'
                > CREATE CONNECTION aws_conn TO AWS (ENDPOINT 'http://minio:9000/', REGION 'minio', ACCESS KEY ID 'minioadmin', SECRET ACCESS KEY SECRET minio
                > COPY (SELECT 1000) TO 's3://copytos3/test' WITH (AWS CONNECTION = aws_conn, FORMAT = 'csv');
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                """,
                """
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            """
            )
        )
