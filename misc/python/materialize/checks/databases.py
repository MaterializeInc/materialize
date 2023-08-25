# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent
from typing import List

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check


class CheckDatabaseCreate(Check):
    def schema(self) -> str:
        return "public"

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $[version>=5900] postgres-execute connection=postgres://mz_system@materialized:6877/materialize
                GRANT CREATEDB ON SYSTEM TO materialize

                $[version<5900] postgres-execute connection=postgres://mz_system@materialized:6877/materialize
                ALTER ROLE materialize CREATEDB

                > CREATE DATABASE to_be_created1;
                > SET DATABASE=to_be_created1;
                > CREATE TABLE t1 (f1 INTEGER);
                > INSERT INTO t1 VALUES (1);
                """,
                """
                $[version>=5900] postgres-execute connection=postgres://mz_system@materialized:6877/materialize
                GRANT CREATEDB ON SYSTEM TO materialize

                $[version<5900] postgres-execute connection=postgres://mz_system@materialized:6877/materialize
                ALTER ROLE materialize CREATEDB

                > CREATE DATABASE to_be_created2;
                > SET DATABASE=to_be_created2;
                > CREATE TABLE t1 (f1 INTEGER);
                > INSERT INTO t1 VALUES (2);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SHOW DATABASES LIKE 'to_be_created%';
                to_be_created1
                to_be_created2

                > SET DATABASE=to_be_created1;
                > SELECT * FROM t1;
                1

                > CREATE TABLE t2 (f1 INTEGER);
                > INSERT INTO t2 VALUES (1);
                > SELECT * FROM t2;
                1
                > DROP TABLE t2;

                > SET DATABASE=to_be_created2;
                > SELECT * FROM t1;
                2

                > CREATE TABLE t2 (f1 INTEGER);
                > INSERT INTO t2 VALUES (1);
                > SELECT * FROM t2;
                1
                > DROP TABLE t2;
                """
            )
        )


class CheckDatabaseDrop(Check):
    def schema(self) -> str:
        return "public"

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE DATABASE to_be_dropped;
                > SET DATABASE=to_be_dropped;
                > CREATE TABLE t1 (f1 INTEGER);
                """,
                """
                # When upgrading from old version without roles the database is
                # owned by default_role, thus we have to change the owner
                # before dropping it:
                $[version>=4700] postgres-execute connection=postgres://mz_system:materialize@materialized:6877
                ALTER DATABASE to_be_dropped OWNER TO materialize;

                > DROP DATABASE to_be_dropped CASCADE;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET DATABASE=to_be_dropped;

                ! SELECT * FROM t1;
                contains: unknown catalog item
                """
            )
        )
