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


class PreparedStatements(Check):
    """PREPARE / EXECUTE / DEALLOCATE. Prepared statements are session
    scoped, so each phase prepares its own and we verify the statements keep
    working against surviving data."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE prepared_stmt_table (f1 INT, f2 STRING)
            > INSERT INTO prepared_stmt_table VALUES (1, 'one')
            """))

    def manipulate(self) -> list[Testdrive]:
        # Prepared statements are session scoped, so exercising them here
        # would break when a disruption forces a reconnect mid-phase. The
        # phases only feed data, the PREPARE/EXECUTE/DEALLOCATE cycles run in
        # validate().
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO prepared_stmt_table VALUES (2, 'two')
                """,
                """
                > INSERT INTO prepared_stmt_table VALUES (3, 'three')
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > PREPARE prepared_stmt_select AS SELECT f1 + $1, f2 FROM prepared_stmt_table WHERE f1 <= $2
            > EXECUTE prepared_stmt_select(100, 2)
            101 one
            102 two

            > EXECUTE prepared_stmt_select(0, 3)
            1 one
            2 two
            3 three

            > DEALLOCATE prepared_stmt_select

            ! EXECUTE prepared_stmt_select(0, 3)
            contains: unknown prepared statement prepared_stmt_select

            # A deallocated name can be reused.
            > PREPARE prepared_stmt_select AS SELECT count(*) FROM prepared_stmt_table
            > EXECUTE prepared_stmt_select
            3

            # Writes through EXECUTE, undone afterwards to keep validate
            # idempotent.
            > PREPARE prepared_stmt_insert AS INSERT INTO prepared_stmt_table VALUES ($1, $2)
            > EXECUTE prepared_stmt_insert(100, 'hundred')
            > SELECT count(*) FROM prepared_stmt_table WHERE f1 = 100
            1
            > DELETE FROM prepared_stmt_table WHERE f1 = 100
            > DEALLOCATE ALL
            """))


class Cursors(Check):
    """SQL-level cursors over SELECT: DECLARE / FETCH / CLOSE.

    The table lives in its own schema: the transaction timedomain spans all
    collections in the queried schemas, so a table in `public` would make the
    FETCH wait on unrelated checks' collections (e.g. REFRESH AT ... MVs
    force the transaction timestamp up to their future refresh time).
    TODO: Reenable when CLU-171 is fixed: move the table back to the default
    schema.
    Manipulate only runs single retryable statements because in the parallel
    scenarios it executes concurrently with restarts and kills, where a
    failed statement inside an explicit transaction cannot be retried.
    """

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE SCHEMA cursors_schema
            > CREATE TABLE cursors_schema.cursors_table (f1 INT)
            > INSERT INTO cursors_schema.cursors_table VALUES (1), (2)
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO cursors_schema.cursors_table VALUES (3), (4)
                """,
                """
                > INSERT INTO cursors_schema.cursors_table VALUES (5), (6)
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            # Wait for readability outside of the transactions below, where
            # testdrive can retry.
            > SELECT count(*) FROM cursors_schema.cursors_table
            6

            > BEGIN
            > DECLARE cursors_c1 CURSOR FOR SELECT f1 FROM cursors_schema.cursors_table ORDER BY f1
            > FETCH 2 cursors_c1
            1
            2
            > FETCH ALL cursors_c1
            3
            4
            5
            6
            > CLOSE cursors_c1
            > COMMIT

            > BEGIN
            > DECLARE cursors_v1 CURSOR FOR SELECT f1 FROM cursors_schema.cursors_table ORDER BY f1 DESC
            > FETCH 3 cursors_v1
            6
            5
            4
            # Fetching more rows than remain returns the remainder.
            > FETCH 100 cursors_v1
            3
            2
            1
            > CLOSE cursors_v1
            ! FETCH 1 cursors_v1
            contains: cursor "cursors_v1" does not exist
            > ROLLBACK
            """))


class SessionState(Check):
    """Session-level state: SET / SHOW / RESET of session variables,
    TEMPORARY views, DISCARD, and the VALUES / TABLE productions."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE session_state_table (f1 INT)
            > INSERT INTO session_state_table VALUES (1), (2)
            """))

    def manipulate(self) -> list[Testdrive]:
        # Session variables and temporary views do not survive the reconnect
        # a disruption forces mid-phase, so all session-state exercising
        # happens in validate().
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO session_state_table VALUES (3)
                """,
                """
                > INSERT INTO session_state_table VALUES (4)
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > VALUES (1, 'a'), (2, 'b')
            1 a
            2 b

            > TABLE session_state_table
            1
            2
            3
            4

            > SET extra_float_digits = 0
            > SHOW extra_float_digits
            0
            > RESET extra_float_digits
            > SHOW extra_float_digits
            3

            > CREATE TEMPORARY VIEW session_state_temp_view AS SELECT max(f1) AS m FROM session_state_table
            > SELECT * FROM session_state_temp_view
            4
            > DISCARD TEMP
            ! SELECT * FROM session_state_temp_view
            contains: unknown catalog item 'session_state_temp_view'

            > SET extra_float_digits = 1
            > SHOW extra_float_digits
            1

            > CREATE TEMPORARY VIEW session_state_temp_view2 AS TABLE session_state_table
            > SELECT count(*) FROM session_state_temp_view2
            4

            # DISCARD ALL drops temporary objects.
            # TODO: Reenable when SQL-529 is fixed: assert that
            # extra_float_digits is back to its default, once DISCARD ALL
            # resets session variables on the extended protocol.
            > DISCARD ALL
            ! SELECT * FROM session_state_temp_view2
            contains: unknown catalog item 'session_state_temp_view2'
            > RESET extra_float_digits
            > SHOW extra_float_digits
            3
            """))


class InsertReturning(Check):
    """INSERT ... RETURNING."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE insert_returning_table (f1 INT, f2 STRING)
            > INSERT INTO insert_returning_table VALUES (1, 'one') RETURNING f1, f2
            1 one
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO insert_returning_table VALUES (2, 'two'), (3, 'three') RETURNING f1 * 10, upper(f2)
                20 TWO
                30 THREE
                """,
                """
                > INSERT INTO insert_returning_table SELECT f1 + 10, f2 FROM insert_returning_table WHERE f1 <= 3 RETURNING f1
                11
                12
                13
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT * FROM insert_returning_table
            1 one
            2 two
            3 three
            11 one
            12 two
            13 three
            """))
