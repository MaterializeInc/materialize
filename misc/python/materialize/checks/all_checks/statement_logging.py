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


class StatementLogging(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                # TODO(peek-seq): enable_frontend_peek_sequencing when it supports statement logging.
                """
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET statement_logging_max_sample_rate TO 1.0

                $[version>=16200] postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_frontend_peek_sequencing = false;
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
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="5s"
                """,
                """
                > SET statement_logging_sample_rate TO 1.0
                > SELECT 'goodbye' /* Btv was here */;
                goodbye
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="5s"
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        if self.base_version == self.current_version:
            return Testdrive(
                dedent(
                    """
                    > SELECT sql, finished_status FROM mz_internal.mz_statement_execution_history mseh, mz_internal.mz_prepared_statement_history mpsh, (SELECT DISTINCT sql, sql_hash, redacted_sql FROM mz_internal.mz_sql_text) mst WHERE mseh.prepared_statement_id = mpsh.id AND mst.sql_hash = mpsh.sql_hash AND sql LIKE '%/* Btv was here */' ORDER BY mseh.began_at;
                    "SELECT 'hello' /* Btv was here */" success
                    "SELECT 'goodbye' /* Btv was here */" success
                    """
                )
            )
        else:
            # Rows are expected to maybe disappear across versions
            return Testdrive(
                dedent(
                    """
                    > SELECT count(*) <= 2 FROM mz_internal.mz_statement_execution_history mseh, mz_internal.mz_prepared_statement_history mpsh, (SELECT DISTINCT sql, sql_hash, redacted_sql FROM mz_internal.mz_sql_text) mst WHERE mseh.prepared_statement_id = mpsh.id AND mpsh.sql_hash = mst.sql_hash AND sql LIKE '%/* Btv was here */'
                    true
                    """
                )
            )
