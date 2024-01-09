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
                > CREATE TABLE time (time_index INT, t TIMESTAMP);
                > INSERT INTO time VALUES (0, now());

                > CREATE TABLE retain_history_table (key INT, value INT);
                > INSERT INTO time VALUES (1, now());

                > INSERT INTO retain_history_table VALUES (1, 100), (2, 200), (3, 300);
                > INSERT INTO time VALUES (2, now());

                > CREATE MATERIALIZED VIEW retain_history_mv1 WITH (RETAIN HISTORY FOR '30s') AS
                    SELECT * FROM retain_history_table;

                > SELECT count(*) FROM retain_history_mv1;
                3
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > UPDATE retain_history_table SET value = value + 10 WHERE key = 1;
                > INSERT INTO time VALUES (3, now());

                > INSERT INTO retain_history_table VALUES (4, 400);
                > INSERT INTO time VALUES (4, now());

                > DELETE FROM retain_history_table WHERE key = 3;
                > INSERT INTO time VALUES (5, now());

                > CREATE MATERIALIZED VIEW retain_history_mv2 WITH (RETAIN HISTORY FOR '30s') AS
                    SELECT * FROM retain_history_table;
                """,
                """
                > CREATE MATERIALIZED VIEW retain_history_mv3 WITH (RETAIN HISTORY FOR '30s') AS
                    SELECT * FROM retain_history_mv2;

                > UPDATE retain_history_table SET value = value + 1;
                > INSERT INTO time VALUES (6, now());

                > INSERT INTO retain_history_table VALUES (5, 500);
                > INSERT INTO time VALUES (7, now());

                > DELETE FROM retain_history_table WHERE key = 4;
                > INSERT INTO time VALUES (8, now());
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        time_definitions = """
                $ set-from-sql var=time0
                SELECT t::STRING FROM time WHERE time_index = 0
                $ set-from-sql var=time1
                SELECT t::STRING FROM time WHERE time_index = 1
                $ set-from-sql var=time2
                SELECT t::STRING FROM time WHERE time_index = 2
                $ set-from-sql var=time3
                SELECT t::STRING FROM time WHERE time_index = 3
                $ set-from-sql var=time4
                SELECT t::STRING FROM time WHERE time_index = 4
                $ set-from-sql var=time5
                SELECT t::STRING FROM time WHERE time_index = 5
                $ set-from-sql var=time6
                SELECT t::STRING FROM time WHERE time_index = 6
                $ set-from-sql var=time7
                SELECT t::STRING FROM time WHERE time_index = 7
                $ set-from-sql var=time8
                SELECT t::STRING FROM time WHERE time_index = 8
                """

        content_validations = "\n".join(
            f"""
                ! SELECT * FROM {mv_name} AS OF '${{time0}}'::TIMESTAMP; -- time0
                contains: is not valid for all inputs

                > SELECT count(*) FROM {mv_name} AS OF '${{time1}}'::TIMESTAMP; -- time1
                0

                > SELECT * FROM {mv_name} AS OF '${{time2}}'::TIMESTAMP; -- time2
                1 100
                2 200
                3 300

                > SELECT * FROM {mv_name} AS OF '${{time2}}'::TIMESTAMP; -- time2
                1 100
                2 200
                3 300

                > SELECT * FROM {mv_name} AS OF '${{time3}}'::TIMESTAMP; -- time3
                1 110
                2 200
                3 300

                > SELECT * FROM {mv_name} AS OF '${{time4}}'::TIMESTAMP; -- time4
                1 110
                2 200
                3 300
                4 400

                > SELECT * FROM {mv_name} AS OF '${{time5}}'::TIMESTAMP; -- time5
                1 110
                2 200
                4 400

                > SELECT * FROM {mv_name} AS OF '${{time6}}'::TIMESTAMP; -- time6
                1 111
                2 201
                4 401

                > SELECT * FROM {mv_name} AS OF '${{time7}}'::TIMESTAMP; -- time7
                1 111
                2 201
                4 401
                5 500

                > SELECT * FROM {mv_name} AS OF '${{time8}}'::TIMESTAMP; -- time8
                1 111
                2 201
                5 500

                > SELECT * FROM {mv_name};
                1 111
                2 201
                5 500
                """
            for mv_name in [
                "retain_history_mv1",
                "retain_history_mv2",
                "retain_history_mv3",
            ]
        )

        definition_validations = """
                > SELECT create_sql FROM (SHOW CREATE MATERIALIZED VIEW retain_history_mv1);
                "CREATE MATERIALIZED VIEW \\"materialize\\".\\"public\\".\\"retain_history_mv1\\" IN CLUSTER \\"quickstart\\" WITH (RETAIN HISTORY = FOR '30s', REFRESH = ON COMMIT) AS SELECT * FROM \\"materialize\\".\\"public\\".\\"retain_history_table\\""

                > SELECT create_sql FROM (SHOW CREATE MATERIALIZED VIEW retain_history_mv2);
                "CREATE MATERIALIZED VIEW \\"materialize\\".\\"public\\".\\"retain_history_mv2\\" IN CLUSTER \\"quickstart\\" WITH (RETAIN HISTORY = FOR '30s', REFRESH = ON COMMIT) AS SELECT * FROM \\"materialize\\".\\"public\\".\\"retain_history_table\\""

                > SELECT create_sql FROM (SHOW CREATE MATERIALIZED VIEW retain_history_mv3);
                "CREATE MATERIALIZED VIEW \\"materialize\\".\\"public\\".\\"retain_history_mv3\\" IN CLUSTER \\"quickstart\\" WITH (RETAIN HISTORY = FOR '30s', REFRESH = ON COMMIT) AS SELECT * FROM \\"materialize\\".\\"public\\".\\"retain_history_mv2\\""
        """

        other_validations = """
                ? EXPLAIN OPTIMIZED PLAN AS TEXT FOR SELECT * FROM retain_history_mv1
                Explained Query:
                  ReadStorage materialize.public.retain_history_mv1
                """

        return Testdrive(
            dedent(
                f"""
                {time_definitions}

                {content_validations}

                {definition_validations}

                {other_validations}
                """
            )
        )
