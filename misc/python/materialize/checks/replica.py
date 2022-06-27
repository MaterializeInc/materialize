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


class CreateReplica(Check):
    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE TABLE create_replica_table (f1 INTEGER);
                > INSERT INTO create_replica_table VALUES (123);

                > CREATE CLUSTER create_replica REPLICAS ()

                > SET cluster=create_replica
                > CREATE DEFAULT INDEX ON create_replica_table;
                > CREATE MATERIALIZED VIEW create_replica_view AS SELECT SUM(f1) FROM create_replica_table;

                > CREATE CLUSTER REPLICA create_replica.replica1 SIZE '2-2'
                """,
                """
                > CREATE CLUSTER REPLICA create_replica.replica2 SIZE '2-2'
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET cluster=create_replica

                > SELECT * FROM create_replica_table;
                123
                > SELECT * FROM create_replica_view;
                123
           """
            )
        )


class DropReplica(Check):
    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE TABLE drop_replica_table (f1 INTEGER);
                > INSERT INTO drop_replica_table VALUES (1);

                > CREATE CLUSTER drop_replica REPLICAS ();

                > SET cluster=drop_replica
                > CREATE DEFAULT INDEX ON drop_replica_table;
                > CREATE MATERIALIZED VIEW drop_replica_view AS SELECT COUNT(f1) FROM drop_replica_table;

                > INSERT INTO drop_replica_table VALUES (2);
                > CREATE CLUSTER REPLICA drop_replica.replica1 SIZE '2-2';
                > INSERT INTO drop_replica_table VALUES (3);
                > CREATE CLUSTER REPLICA drop_replica.replica2 SIZE '2-2';
                > INSERT INTO drop_replica_table VALUES (4);
                """,
                """
                > INSERT INTO drop_replica_table VALUES (5);
                > DROP CLUSTER REPLICA drop_replica.replica2;
                > INSERT INTO drop_replica_table VALUES (6);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET cluster=drop_replica

                > SELECT * FROM drop_replica_table;
                1
                2
                3
                4
                5
                6

                > SELECT * FROM drop_replica_view;
                6
           """
            )
        )
