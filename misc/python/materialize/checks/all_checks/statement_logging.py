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
from materialize.checks.checks import Check
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


class StatementLogging(Check):
    def _can_run(self, _e: Executor) -> bool:
        return self.base_version >= MzVersion(0, 69, 0)

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
                ALTER SYSTEM SET statement_logging_max_sample_rate TO 1.0
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > SET statement_logging_sample_rate TO 1.0
                > SELECT 'hello' /* Btv was here */;
                hello
                > SELECT mz_internal.mz_sleep(5);
                <null>
                """,
                """
                > SET statement_logging_sample_rate TO 1.0
                > SELECT 'goodbye' /* Btv was here */;
                goodbye
                > SELECT mz_internal.mz_sleep(5);
                <null>
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        if self.base_version == self.current_version:
            return Testdrive(
                dedent(
                    """
                    $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
                    ALTER SYSTEM SET enable_rbac_checks TO false
                    > SELECT sql, finished_status FROM mz_internal.mz_statement_execution_history mseh, mz_internal.mz_prepared_statement_history mpsh WHERE mseh.prepared_statement_id = mpsh.id AND sql LIKE '%/* Btv was here */' ORDER BY mseh.began_at;
                    "SELECT 'hello' /* Btv was here */" success
                    "SELECT 'goodbye' /* Btv was here */" success
                    $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
                    ALTER SYSTEM SET enable_rbac_checks TO true
                    """
                )
            )
        else:
            # Rows are expected to maybe disappear across versions
            return Testdrive(
                dedent(
                    """
                    $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
                    ALTER SYSTEM SET enable_rbac_checks TO false
                    > SELECT count(*) <= 2 FROM mz_internal.mz_statement_execution_history mseh, mz_internal.mz_prepared_statement_history mpsh WHERE mseh.prepared_statement_id = mpsh.id AND sql LIKE '%/* Btv was here */'
                    true
                    $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
                    ALTER SYSTEM SET enable_rbac_checks TO true
                    """
                )
            )
