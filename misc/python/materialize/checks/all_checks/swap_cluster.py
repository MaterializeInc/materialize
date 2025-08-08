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


class SwapCluster(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE swap_cluster1_table (f1 INTEGER);
                > CREATE TABLE swap_cluster2_table (f1 INTEGER);
                > CREATE TABLE swap_cluster3_table (f1 INTEGER);
                > CREATE TABLE swap_cluster4_table (f1 INTEGER);

                > INSERT INTO swap_cluster1_table VALUES (123);
                > INSERT INTO swap_cluster2_table VALUES (234);
                > INSERT INTO swap_cluster3_table VALUES (345);
                > INSERT INTO swap_cluster4_table VALUES (456);

                > CREATE CLUSTER swap_cluster1 REPLICAS (replica1 (SIZE 'scale=2,workers=2'));
                > CREATE CLUSTER swap_cluster2 REPLICAS (replica1 (SIZE 'scale=2,workers=2'));
                > CREATE CLUSTER swap_cluster3 REPLICAS (replica1 (SIZE 'scale=2,workers=2'));
                > CREATE CLUSTER swap_cluster4 REPLICAS (replica1 (SIZE 'scale=2,workers=2'));

                > SET cluster=swap_cluster1
                > CREATE DEFAULT INDEX ON swap_cluster1_table;
                > CREATE MATERIALIZED VIEW swap_cluster1_view AS SELECT SUM(f1) FROM swap_cluster1_table;

                > SET cluster=swap_cluster2
                > CREATE DEFAULT INDEX ON swap_cluster2_table;
                > CREATE MATERIALIZED VIEW swap_cluster2_view AS SELECT SUM(f1) FROM swap_cluster2_table;

                > SET cluster=swap_cluster3
                > CREATE DEFAULT INDEX ON swap_cluster3_table;
                > CREATE MATERIALIZED VIEW swap_cluster3_view AS SELECT SUM(f1) FROM swap_cluster3_table;

                > SET cluster=swap_cluster4
                > CREATE DEFAULT INDEX ON swap_cluster4_table;
                > CREATE MATERIALIZED VIEW swap_cluster4_view AS SELECT SUM(f1) FROM swap_cluster4_table;
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > ALTER CLUSTER swap_cluster1 SWAP WITH swap_cluster2;
                """,
                """

                > ALTER CLUSTER swap_cluster3 SWAP WITH swap_cluster4;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET cluster=swap_cluster1

                > SET cluster=swap_cluster2

                > SET cluster=swap_cluster3

                > SET cluster=swap_cluster4

                > SET cluster=default

                > SELECT * FROM swap_cluster1_table;
                123

                > SELECT * FROM swap_cluster1_view;
                123

                > SELECT * FROM swap_cluster2_table;
                234

                > SELECT * FROM swap_cluster2_view;
                234

                > SELECT * FROM swap_cluster3_table;
                345

                > SELECT * FROM swap_cluster3_view;
                345

                > SELECT * FROM swap_cluster4_table;
                456

                > SELECT * FROM swap_cluster4_view;
                456
           """
            )
        )
