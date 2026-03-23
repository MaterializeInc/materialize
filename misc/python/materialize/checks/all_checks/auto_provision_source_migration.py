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
from materialize.checks.checks import Check
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


class AutoProvisionSourceMigration(Check):
    """
    Verify that the migration in v26.16.0 -> v26.17.0 for the autoprovisionsource in mz_roles works correctly.

    Only run between v26.16.0 and v26.17.0. In mz_roles, the
    autoprovisionsource column does not exist in v26.16.0 but
    exists in v26.17.0.
    In v26.17.0:
    - Manually created roles always have autoprovisionsource = NULL.
    - Roles auto-provisioned via login should have autoprovisionsource = 'frontegg'.
    """

    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz(
            "v26.16.0"
        ) and self.base_version <= MzVersion.parse_mz("v26.17.0")

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(
                dedent(
                    """
                    $ postgres-execute connection=postgres://user%40materialize.com@${testdrive.materialize-sql-addr}
                    SELECT 1
                    """
                )
            ),
            Testdrive(
                dedent(
                    """
                    > CREATE ROLE manually_created_role
                    """
                )
            ),
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                >[version>=2601600&version<2601700] SELECT autoprovisionsource IS NULL FROM mz_roles WHERE name IN ('manually_created_role', 'user@materialize.com')
                true
                true

                >[version>=2601700] SELECT autoprovisionsource FROM mz_roles WHERE name = 'user@materialize.com'
                frontegg

                >[version>=2601700] SELECT autoprovisionsource IS NULL FROM mz_roles WHERE name = 'manually_created_role'
                true
                """
            )
        )
