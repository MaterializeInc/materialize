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


class LoadGeneratorAsOfUpTo(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE SOURCE counter1 FROM LOAD GENERATOR COUNTER (AS OF 100, UP TO 200);
            > CREATE TABLE counter1_tbl FROM SOURCE counter1;

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
            > CREATE TABLE counter2_tbl FROM SOURCE counter2;
                """,
                """
            > CREATE SOURCE counter3 FROM LOAD GENERATOR COUNTER (AS OF 11100, UP TO 11200);
            > CREATE TABLE counter3_tbl FROM SOURCE counter3;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT COUNT(*) FROM counter1_tbl;
                200
                > SELECT COUNT(*) FROM counter2_tbl;
                1200
                > SELECT COUNT(*) FROM counter3_tbl;
                11200
                > SELECT COUNT(*) FROM users;
                4076
            """
            )
        )


class LoadGeneratorMultiReplica(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v0.134.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            >[version>=13800] CREATE CLUSTER multi_cluster1 SIZE 'scale=1,workers=1', REPLICATION FACTOR 2;
            >[version<13800] CREATE CLUSTER multi_cluster1 SIZE 'scale=1,workers=1', REPLICATION FACTOR 1;
            >[version>=13800] CREATE CLUSTER multi_cluster2 SIZE 'scale=1,workers=1', REPLICATION FACTOR 2;
            >[version<13800] CREATE CLUSTER multi_cluster2 SIZE 'scale=1,workers=1', REPLICATION FACTOR 1;
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
            > CREATE SOURCE multi_counter1 IN CLUSTER multi_cluster1 FROM LOAD GENERATOR COUNTER (UP TO 10);
            > CREATE SOURCE multi_counter2 IN CLUSTER multi_cluster2 FROM LOAD GENERATOR COUNTER (UP TO 10);
            > CREATE TABLE multi_counter1_tbl FROM SOURCE multi_counter1;
            > CREATE TABLE multi_counter2_tbl FROM SOURCE multi_counter2;

            > ALTER CLUSTER multi_cluster1 SET (REPLICATION FACTOR 4);
                """,
                """
            > CREATE SOURCE multi_counter3 IN CLUSTER multi_cluster1 FROM LOAD GENERATOR COUNTER (UP TO 10);
            > CREATE SOURCE multi_counter4 IN CLUSTER multi_cluster2 FROM LOAD GENERATOR COUNTER (UP TO 10);
            > CREATE TABLE multi_counter3_tbl FROM SOURCE multi_counter3;
            > CREATE TABLE multi_counter4_tbl FROM SOURCE multi_counter4;
            > ALTER CLUSTER multi_cluster2 SET (REPLICATION FACTOR 4);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT COUNT(*) FROM multi_counter1_tbl;
                10
                > SELECT COUNT(*) FROM multi_counter2_tbl;
                10
                > SELECT COUNT(*) FROM multi_counter3_tbl;
                10
                > SELECT COUNT(*) FROM multi_counter4_tbl;
                10
            """
            )
        )
