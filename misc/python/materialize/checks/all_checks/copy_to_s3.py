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
                > CREATE SECRET minio AS '${arg.aws-secret-access-key}'
                > CREATE CONNECTION aws_conn1 TO AWS (ENDPOINT '${arg.aws-endpoint}', REGION 'us-east-1', ACCESS KEY ID '${arg.aws-access-key-id}', SECRET ACCESS KEY SECRET minio)
                > COPY (SELECT 1, 2, 3) TO 's3://copytos3/key1' WITH (AWS CONNECTION = aws_conn1, FORMAT = 'csv');
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE CONNECTION aws_conn2 TO AWS (ENDPOINT '${arg.aws-endpoint}', REGION 'us-east-1', ACCESS KEY ID '${arg.aws-access-key-id}', SECRET ACCESS KEY SECRET minio)
                > COPY (SELECT 11, 12, 13) TO 's3://copytos3/key11' WITH (AWS CONNECTION = aws_conn1, FORMAT = 'csv');
                > COPY (SELECT 11, 12, 13) TO 's3://copytos3/key12' WITH (AWS CONNECTION = aws_conn2, FORMAT = 'csv');
                """,
                """
                > CREATE CONNECTION aws_conn3 TO AWS (ENDPOINT '${arg.aws-endpoint}', REGION 'us-east-1', ACCESS KEY ID '${arg.aws-access-key-id}', SECRET ACCESS KEY SECRET minio)
                > COPY (SELECT 21, 22, 23) TO 's3://copytos3/key21' WITH (AWS CONNECTION = aws_conn1, FORMAT = 'csv');
                > COPY (SELECT 21, 22, 23) TO 's3://copytos3/key22' WITH (AWS CONNECTION = aws_conn2, FORMAT = 'csv');
                > COPY (SELECT 21, 22, 23) TO 's3://copytos3/key23' WITH (AWS CONNECTION = aws_conn3, FORMAT = 'csv');
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ s3-verify-data bucket=copytos3 key=key1
                1,2,3

                $ s3-verify-data bucket=copytos3 key=key11
                11,12,13

                $ s3-verify-data bucket=copytos3 key=key12
                11,12,13

                $ s3-verify-data bucket=copytos3 key=key21
                21,22,23

                $ s3-verify-data bucket=copytos3 key=key22
                21,22,23

                $ s3-verify-data bucket=copytos3 key=key23
                21,22,23
                """
            )
        )
