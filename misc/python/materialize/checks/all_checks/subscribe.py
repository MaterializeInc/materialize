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

# NOTE: These checks keep their tables in dedicated schemas. A transaction's
# timedomain covers all collections in the queried schemas, so a table in
# `public` would tie cursor transactions to every other check's collections.
# Any object with a lagging or future frontier there (REFRESH AT ... MVs,
# rehydrating or replica-less clusters) then blocks the FETCH.
# TODO: Reenable when CLU-171 is fixed: move the tables back to the default
# schema so the cursor transactions run in the same timedomain as the other
# checks.
#
# Manipulate phases only run single, retryable statements: in the parallel
# scenarios they execute concurrently with restarts and kills, and a
# transient error inside an explicit transaction cannot be retried by
# testdrive statement by statement. The cursor interactions all live in
# validate().


class SubscribeCursor(Check):
    """SUBSCRIBE through DECLARE/FETCH cursors: snapshots, SNAPSHOT = false,
    PROGRESS, and UP TO."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE SCHEMA subscribe_cursor_schema
            > CREATE TABLE subscribe_cursor_schema.subscribe_table (key INT, value INT)
            > INSERT INTO subscribe_cursor_schema.subscribe_table VALUES (1, 10), (2, 20)
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO subscribe_cursor_schema.subscribe_table VALUES (3, 30), (4, 40)
                """,
                """
                > INSERT INTO subscribe_cursor_schema.subscribe_table VALUES (5, 50)
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            $ set-regex match=\\d{13,20} replacement=<TIMESTAMP>

            # Wait for readability outside of the transactions below, where
            # testdrive can retry.
            > SELECT count(*) FROM subscribe_cursor_schema.subscribe_table
            5

            > BEGIN
            > DECLARE subscribe_val1 CURSOR FOR SUBSCRIBE (SELECT key, value FROM subscribe_cursor_schema.subscribe_table)
            > FETCH 5 subscribe_val1
            <TIMESTAMP> 1 1 10
            <TIMESTAMP> 1 2 20
            <TIMESTAMP> 1 3 30
            <TIMESTAMP> 1 4 40
            <TIMESTAMP> 1 5 50
            > COMMIT

            # PROGRESS emits an initial progress row before the snapshot data.
            > BEGIN
            > DECLARE subscribe_val2 CURSOR FOR SUBSCRIBE (SELECT key FROM subscribe_cursor_schema.subscribe_table WHERE key <= 2) WITH (PROGRESS, SNAPSHOT)
            > FETCH 1 subscribe_val2
            <TIMESTAMP> true <null> <null>
            > FETCH 2 subscribe_val2
            <TIMESTAMP> false 1 1
            <TIMESTAMP> false 1 2
            > COMMIT

            # A subscribe without snapshot sees only data arriving after it
            # started. The scratch table keeps validate() idempotent and the
            # concurrent insert comes from a second connection.
            > CREATE TABLE subscribe_cursor_schema.subscribe_scratch (key INT, value INT)
            > INSERT INTO subscribe_cursor_schema.subscribe_scratch VALUES (1, 10)
            > BEGIN
            > DECLARE subscribe_val3 CURSOR FOR SUBSCRIBE subscribe_cursor_schema.subscribe_scratch WITH (SNAPSHOT = false)
            > FETCH ALL subscribe_val3 WITH (timeout = '0s')

            $ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
            INSERT INTO subscribe_cursor_schema.subscribe_scratch VALUES (2, 20)

            > FETCH 1 subscribe_val3
            <TIMESTAMP> 1 2 20
            > CLOSE subscribe_val3
            > COMMIT

            # A subscribe with UP TO in the near future emits the snapshot and
            # then completes once the frontier passes UP TO. The margin is
            # generous: the subscribe's AS OF is chosen at DECLARE time and
            # must stay below UP TO even on a slow, loaded system.
            $ set-from-sql var=subscribe-up-to
            SELECT (mz_now()::text::int8 + 30000)::text

            > BEGIN
            > DECLARE subscribe_val4 CURSOR FOR SUBSCRIBE (SELECT value FROM subscribe_cursor_schema.subscribe_scratch WHERE key = 1) UP TO ${subscribe-up-to}
            > FETCH ALL subscribe_val4 WITH (timeout = '300s')
            <TIMESTAMP> 1 10
            > FETCH ALL subscribe_val4 WITH (timeout = '300s')
            > COMMIT

            > DROP TABLE subscribe_cursor_schema.subscribe_scratch
            """))


class SubscribeEnvelopes(Check):
    """SUBSCRIBE output modifiers: ENVELOPE UPSERT, ENVELOPE DEBEZIUM, and
    WITHIN TIMESTAMP ORDER BY."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER SYSTEM SET enable_within_timestamp_order_by_in_subscribe = true;
            ALTER SYSTEM SET enable_envelope_debezium_in_subscribe = true;

            > CREATE SCHEMA subscribe_env_schema
            > CREATE TABLE subscribe_env_schema.subscribe_envelope_table (key INT, value INT)
            > INSERT INTO subscribe_env_schema.subscribe_envelope_table VALUES (1, 10), (2, 20)
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > UPDATE subscribe_env_schema.subscribe_envelope_table SET value = 11 WHERE key = 1
                > INSERT INTO subscribe_env_schema.subscribe_envelope_table VALUES (3, 30)
                """,
                """
                > DELETE FROM subscribe_env_schema.subscribe_envelope_table WHERE key = 2
                > INSERT INTO subscribe_env_schema.subscribe_envelope_table VALUES (4, 40)
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            $ set-regex match=\\d{13,20} replacement=<TIMESTAMP>

            > SELECT * FROM subscribe_env_schema.subscribe_envelope_table
            1 11
            3 30
            4 40

            > BEGIN
            > DECLARE subscribe_env_val1 CURSOR FOR SUBSCRIBE subscribe_env_schema.subscribe_envelope_table ENVELOPE UPSERT (KEY (key))
            > FETCH 3 subscribe_env_val1
            <TIMESTAMP> upsert 1 11
            <TIMESTAMP> upsert 3 30
            <TIMESTAMP> upsert 4 40
            > COMMIT

            > BEGIN
            > DECLARE subscribe_env_val2 CURSOR FOR SUBSCRIBE subscribe_env_schema.subscribe_envelope_table ENVELOPE DEBEZIUM (KEY (key))
            > FETCH 3 subscribe_env_val2
            <TIMESTAMP> insert 1 <null> 11
            <TIMESTAMP> insert 3 <null> 30
            <TIMESTAMP> insert 4 <null> 40
            > COMMIT

            # WITHIN TIMESTAMP ORDER BY makes the order of rows within one
            # timestamp deterministic, so single-row FETCHes are stable.
            > BEGIN
            > DECLARE subscribe_env_val3 CURSOR FOR SUBSCRIBE subscribe_env_schema.subscribe_envelope_table WITHIN TIMESTAMP ORDER BY value DESC, key, mz_diff
            > FETCH 1 subscribe_env_val3
            <TIMESTAMP> 1 4 40
            > FETCH 1 subscribe_env_val3
            <TIMESTAMP> 1 3 30
            > FETCH 1 subscribe_env_val3
            <TIMESTAMP> 1 1 11
            > COMMIT

            # Watch the upsert envelope react to live updates and deletes,
            # written from a second connection onto a scratch table so
            # validate() stays idempotent.
            > CREATE TABLE subscribe_env_schema.subscribe_env_scratch (key INT, value INT)
            > INSERT INTO subscribe_env_schema.subscribe_env_scratch VALUES (1, 10)

            > BEGIN
            > DECLARE subscribe_env_live1 CURSOR FOR SUBSCRIBE subscribe_env_schema.subscribe_env_scratch ENVELOPE UPSERT (KEY (key))
            > FETCH 1 subscribe_env_live1
            <TIMESTAMP> upsert 1 10

            $ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
            UPDATE subscribe_env_schema.subscribe_env_scratch SET value = 11 WHERE key = 1

            > FETCH 1 subscribe_env_live1
            <TIMESTAMP> upsert 1 11

            $ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
            DELETE FROM subscribe_env_schema.subscribe_env_scratch WHERE key = 1

            > FETCH 1 subscribe_env_live1
            <TIMESTAMP> delete 1 <null>
            > COMMIT

            # The debezium envelope additionally carries the previous value of
            # an updated key.
            > INSERT INTO subscribe_env_schema.subscribe_env_scratch VALUES (2, 20)
            > BEGIN
            > DECLARE subscribe_env_live2 CURSOR FOR SUBSCRIBE subscribe_env_schema.subscribe_env_scratch ENVELOPE DEBEZIUM (KEY (key))
            > FETCH 1 subscribe_env_live2
            <TIMESTAMP> insert 2 <null> 20

            $ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
            UPDATE subscribe_env_schema.subscribe_env_scratch SET value = 21 WHERE key = 2

            > FETCH 1 subscribe_env_live2
            <TIMESTAMP> upsert 2 20 21
            > COMMIT

            > DROP TABLE subscribe_env_schema.subscribe_env_scratch
            """))
