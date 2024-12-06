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


class LoadGeneratorAsOfUpTo(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE SOURCE counter1 FROM LOAD GENERATOR COUNTER (AS OF 100, UP TO 200);

            > CREATE SOURCE auction1 FROM LOAD GENERATOR AUCTION (AS OF 100, UP TO 200);
            > CREATE TABLE accounts FROM SOURCE auction1 (REFERENCE accounts);
            > CREATE TABLE auctions FROM SOURCE auction1 (REFERENCE auctions);
            > CREATE TABLE bids FROM SOURCE auction1 (REFERENCE bids);
            > CREATE TABLE organizations FROM SOURCE auction1 (REFERENCE organizations);
            > CREATE TABLE users FROM SOURCE auction1 (REFERENCE users);
        """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
            > CREATE SOURCE counter2 FROM LOAD GENERATOR COUNTER (AS OF 1100, UP TO 1200);
                """,
                """
            > CREATE SOURCE counter3 FROM LOAD GENERATOR COUNTER (AS OF 11100, UP TO 11200);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT COUNT(*) FROM counter1;
                200
                > SELECT COUNT(*) FROM counter2;
                1200
                > SELECT COUNT(*) FROM counter3;
                11200
                > SELECT COUNT(*) FROM users;
                4076
            """
            )
        )
