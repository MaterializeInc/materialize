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
from materialize.mz_version import MzVersion

CHECK_INTRODUCED_IN = MzVersion.parse_mz("v26.36.0-dev")

RELAX_CHECK = """
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_incomplete_view_column_lists = on
""".strip()

RESTORE_CHECK = """
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET unsafe_enable_incomplete_view_column_lists
""".strip()


class ViewColumnNameCountMismatch(Check):
    """A view whose column name list names fewer columns than its query, as
    accepted by versions predating the SQL-455 arity check, must keep loading
    during bootstrap after upgrade rather than crash-looping environmentd when
    its persisted `create_sql` is re-planned by the stricter planner."""

    def _create_objects(self) -> str:
        return dedent("""
            > CREATE TABLE col_count_table (a INT, b INT, c INT)
            > INSERT INTO col_count_table VALUES (1, 2, 3)

            > CREATE VIEW col_count_v (x) AS SELECT a, b, c FROM col_count_table
            > CREATE MATERIALIZED VIEW col_count_mv (x) AS SELECT a, b, c FROM col_count_table
            """).strip()

    def initialize(self) -> Testdrive:
        create = self._create_objects()
        if self.base_version >= CHECK_INTRODUCED_IN:
            body = f"{RELAX_CHECK}\n\n{create}\n\n{RESTORE_CHECK}"
        else:
            body = create
        return Testdrive(body)

    def manipulate(self) -> list[Testdrive]:
        check_usable = dedent("""
            > SELECT x, b, c FROM col_count_v
            1 2 3
            > SELECT x, b, c FROM col_count_mv
            1 2 3
            """).strip()
        return [Testdrive(check_usable), Testdrive(check_usable)]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT o.name, c.name, c.position
              FROM mz_columns c JOIN mz_objects o ON c.id = o.id
              WHERE o.name IN ('col_count_v', 'col_count_mv')
              ORDER BY o.name, c.position
            col_count_mv x 1
            col_count_mv b 2
            col_count_mv c 3
            col_count_v x 1
            col_count_v b 2
            col_count_v c 3

            > SELECT x, b, c FROM col_count_v
            1 2 3

            > SELECT x, b, c FROM col_count_mv
            1 2 3

            ![version>=2603600] CREATE VIEW col_count_reject (x) AS SELECT a, b FROM col_count_table
            contains: definition names 1 column, but view materialize.public.col_count_reject has 2 columns
            """))
