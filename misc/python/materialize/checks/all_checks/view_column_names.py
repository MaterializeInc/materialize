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


class ViewColumnNameCountMismatch(Check):
    """Severity guard for the `maybe_rename_columns` under-validation.

    `CREATE VIEW v (x) AS SELECT a, b` and its materialized-view form are meant
    to reject a column-name list whose length differs from the query arity.
    `maybe_rename_columns` only rejects lists that are too long, so a too-short
    list is silently accepted: the leading columns are renamed and the trailing
    columns keep their inferred names. The declared name `(x)` survives verbatim
    in the persisted `create_sql`.

    This check pins that behavior across restarts and upgrades. Today it passes:
    the invalid DDL is accepted, durably persisted, and its mixed schema
    re-derives identically on every boot.

    Its real purpose is to catch the landmine. Catalog bootstrap re-plans every
    persisted `create_sql` through the same planner, and a re-plan error there is
    a hard panic (no per-item tolerance). So the obvious fix, rejecting
    too-short lists at plan time, bricks environmentd on the next boot for any
    environment that already persisted such an object. If that fix ever lands
    without a catalog migration that repairs the offending `create_sql`, these
    upgrade scenarios stop coming up and surface the outage before production.
    """

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE col_count_table (a INT, b INT, c INT)
            > INSERT INTO col_count_table VALUES (1, 2, 3)

            # One name for three output columns: renames a->x, keeps b, c.
            > CREATE VIEW col_count_v0 (x) AS SELECT a, b, c FROM col_count_table
            > CREATE MATERIALIZED VIEW col_count_mv0 (x) AS SELECT a, b, c FROM col_count_table
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO col_count_table VALUES (4, 5, 6)

                # Two names for three output columns.
                > CREATE VIEW col_count_v1 (x, y) AS SELECT a, b, c FROM col_count_table
                > CREATE MATERIALIZED VIEW col_count_mv1 (x, y) AS SELECT a, b, c FROM col_count_table
                """,
                """
                > INSERT INTO col_count_table VALUES (7, 8, 9)

                # One name for two output columns.
                > CREATE VIEW col_count_v2 (p) AS SELECT a, b FROM col_count_table
                > CREATE MATERIALIZED VIEW col_count_mv2 (p) AS SELECT a, b FROM col_count_table
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            # The persisted schemas keep the silently-mismatched column names,
            # proving the invalid DDL survived every restart/upgrade unchanged.
            > SELECT o.name, c.name, c.position
                FROM mz_columns c
                JOIN mz_objects o ON c.id = o.id
                WHERE o.name LIKE 'col_count_%'
                ORDER BY o.name, c.position
            col_count_mv0 x 1
            col_count_mv0 b 2
            col_count_mv0 c 3
            col_count_mv1 x 1
            col_count_mv1 y 2
            col_count_mv1 c 3
            col_count_mv2 p 1
            col_count_mv2 b 2
            col_count_v0 x 1
            col_count_v0 b 2
            col_count_v0 c 3
            col_count_v1 x 1
            col_count_v1 y 2
            col_count_v1 c 3
            col_count_v2 p 1
            col_count_v2 b 2

            # The data is intact and readable under the mismatched schema.
            > SELECT x, b, c FROM col_count_v0 ORDER BY x
            1 2 3
            4 5 6
            7 8 9

            > SELECT x, b, c FROM col_count_mv0 ORDER BY x
            1 2 3
            4 5 6
            7 8 9
            """))
