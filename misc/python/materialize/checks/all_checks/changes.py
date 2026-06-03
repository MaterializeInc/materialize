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


class ChangesMaterializedView(Check):
    """Maintained materialized views over the CHANGES table function.

    Verifies the restart-exact-reproduction claim end to end: a maintained
    sliding-window changelog must survive restarts/upgrades with its window
    contents intact — pre-restart rows still present with their original
    `mz_timestamp`s (asserted via cross-phase timestamp ordering), no spurious
    snapshot rows (asserted via exact row multisets), and no correction churn.

    Every materialized view is created over a freshly created, empty table and
    all DML happens after the view exists, so the expected changelog contents
    are deterministic: exactly the changes made after creation, all within the
    generous one-day window for the duration of the test.
    """

    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v26.28.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_changes_table_function = true

                > CREATE TABLE changes_mv_t1 (a INT);

                > CREATE MATERIALIZED VIEW changes_mv_view1 AS
                  SELECT a, mz_diff, mz_timestamp
                  FROM CHANGES (changes_mv_t1 AS OF AT LEAST mz_now() - INTERVAL '1 day');

                > INSERT INTO changes_mv_t1 VALUES (11), (12);

                > DELETE FROM changes_mv_t1 WHERE a = 11;
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_changes_table_function = true

                > INSERT INTO changes_mv_t1 VALUES (13);

                > CREATE TABLE changes_mv_t2 (a INT);

                > CREATE MATERIALIZED VIEW changes_mv_view2 AS
                  SELECT a, mz_diff, mz_timestamp
                  FROM CHANGES (changes_mv_t2 AS OF AT LEAST mz_now() - INTERVAL '1 day');

                > INSERT INTO changes_mv_t2 VALUES (21), (22);

                > DELETE FROM changes_mv_t2 WHERE a = 21;
                """,
                """
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_changes_table_function = true

                > INSERT INTO changes_mv_t1 VALUES (14);

                > INSERT INTO changes_mv_t2 VALUES (23);

                > CREATE TABLE changes_mv_t3 (a INT);

                > CREATE MATERIALIZED VIEW changes_mv_view3 AS
                  SELECT a, mz_diff, mz_timestamp
                  FROM CHANGES (changes_mv_t3 AS OF AT LEAST mz_now() - INTERVAL '1 day');

                > INSERT INTO changes_mv_t3 VALUES (31), (32);

                > DELETE FROM changes_mv_t3 WHERE a = 31;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
                # Exact window contents: every change since creation, deletions
                # materialized as appends with mz_diff = -1. Spurious snapshot
                # rows or replayed changes would change these multisets.
                > SELECT a, mz_diff FROM changes_mv_view1 ORDER BY a, mz_diff;
                11 -1
                11 1
                12 1
                13 1
                14 1

                > SELECT a, mz_diff FROM changes_mv_view2 ORDER BY a, mz_diff;
                21 -1
                21 1
                22 1
                23 1

                > SELECT a, mz_diff FROM changes_mv_view3 ORDER BY a, mz_diff;
                31 -1
                31 1
                32 1

                # The net change per key equals the table's current contents.
                > SELECT a, sum(mz_diff) FROM changes_mv_view1 GROUP BY a ORDER BY a;
                11 0
                12 1
                13 1
                14 1

                # Original timestamps survive restarts: changes from earlier
                # phases keep strictly smaller timestamps than changes from
                # later phases. A restart that re-snapshotted the window would
                # collapse pre-restart rows onto a single post-restart
                # timestamp, violating this ordering.
                > SELECT (SELECT max(mz_timestamp) FROM changes_mv_view1 WHERE a IN (11, 12))
                       < (SELECT min(mz_timestamp) FROM changes_mv_view1 WHERE a = 13);
                true

                > SELECT (SELECT max(mz_timestamp) FROM changes_mv_view1 WHERE a = 13)
                       < (SELECT min(mz_timestamp) FROM changes_mv_view1 WHERE a = 14);
                true

                > SELECT (SELECT max(mz_timestamp) FROM changes_mv_view2 WHERE a IN (21, 22))
                       < (SELECT min(mz_timestamp) FROM changes_mv_view2 WHERE a = 23);
                true
           """))
