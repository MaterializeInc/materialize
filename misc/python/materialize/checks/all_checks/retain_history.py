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


class RetainHistory(Check):
    def _can_run(self, e: Executor) -> bool:
        return e.current_mz_version >= MzVersion.parse_mz("v0.81.0")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_logical_compaction_window = true

                > CREATE TABLE retain_history_table (key INT NOT NULL, value INT NOT NULL);
                > INSERT INTO retain_history_table VALUES (1, 100), (2, 200);

                > CREATE MATERIALIZED VIEW retain_history_mv1 WITH (RETAIN HISTORY FOR '30s') AS
                    SELECT * FROM retain_history_table;

                > SELECT count(*) FROM retain_history_mv1;
                2
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > UPDATE retain_history_table SET value = value + 10 WHERE key = 1;

                > CREATE MATERIALIZED VIEW retain_history_mv2 WITH (RETAIN HISTORY FOR '30s') AS
                    SELECT * FROM retain_history_table;
                """,
                """
                > CREATE MATERIALIZED VIEW retain_history_mv3 WITH (RETAIN HISTORY FOR '30s') AS
                    SELECT * FROM retain_history_mv2;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM retain_history_mv1;
                1 110
                2 200

                > SELECT * FROM retain_history_mv2;
                1 110
                2 200

                > SELECT * FROM retain_history_mv3;
                1 110
                2 200

                > SELECT replace(create_sql, '"', '') FROM (SHOW CREATE MATERIALIZED VIEW retain_history_mv1);
                "CREATE MATERIALIZED VIEW materialize.public.retain_history_mv1 IN CLUSTER quickstart WITH (RETAIN HISTORY = FOR '30s', REFRESH = ON COMMIT) AS SELECT * FROM materialize.public.retain_history_table"

                > SELECT replace(create_sql, '"', '') FROM (SHOW CREATE MATERIALIZED VIEW retain_history_mv2);
                "CREATE MATERIALIZED VIEW materialize.public.retain_history_mv2 IN CLUSTER quickstart WITH (RETAIN HISTORY = FOR '30s', REFRESH = ON COMMIT) AS SELECT * FROM materialize.public.retain_history_table"

                > SELECT replace(create_sql, '"', '') FROM (SHOW CREATE MATERIALIZED VIEW retain_history_mv3);
                "CREATE MATERIALIZED VIEW materialize.public.retain_history_mv3 IN CLUSTER quickstart WITH (RETAIN HISTORY = FOR '30s', REFRESH = ON COMMIT) AS SELECT * FROM materialize.public.retain_history_mv2"

                ? EXPLAIN OPTIMIZED PLAN AS TEXT FOR SELECT * FROM retain_history_mv1
                Explained Query:
                  ReadStorage materialize.public.retain_history_mv1
                """
            )
        )
