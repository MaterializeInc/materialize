# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from __future__ import annotations

from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check, externally_idempotent
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


@externally_idempotent(False)
class AwsConnection(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v0.80.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                f"""
                $[version>=8000] postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                ALTER SYSTEM SET enable_aws_connection = true
                ALTER SYSTEM SET enable_connection_validation_syntax = true

                > CREATE CONNECTION aws_assume_role
                  TO AWS (ASSUME ROLE ARN 'assume-role', ASSUME ROLE SESSION NAME 'session-name');

                > CREATE SECRET aws_secret_access_key as '...';

                > CREATE CONNECTION aws_credentials
                  TO AWS (ACCESS KEY ID = 'access_key', SECRET ACCESS KEY = SECRET aws_secret_access_key);
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > ALTER CONNECTION aws_assume_role SET (ASSUME ROLE ARN 'assume-role-2');
                """,
                """
                > ALTER CONNECTION aws_credentials SET (ACCESS KEY ID 'access_key_2');
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        # We just check that the connections are still safe to reference.
        # The error is inconsistent depending on the way the check is being run.
        return Testdrive(
            dedent(
                """
                ! VALIDATE CONNECTION aws_assume_role;
                regex:.*

                ! VALIDATE CONNECTION aws_credentials;
                regex:.*
                """
            )
        )
