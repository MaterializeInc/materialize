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


class CreateCluster(Check):
    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE CLUSTER create_cluster1 REPLICAS (replica1 (SIZE '2-2'));
                """,
                """
                > CREATE CLUSTER create_cluster2 REPLICAS (replica1 (SIZE '2-2'));
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE create_cluster1_table (f1 INTEGER);
                > CREATE TABLE create_cluster2_table (f1 INTEGER);

                > INSERT INTO create_cluster1_table VALUES (123);
                > INSERT INTO create_cluster2_table VALUES (234);

                > SET cluster=create_cluster1
                > CREATE DEFAULT INDEX ON create_cluster1_table;
                > CREATE MATERIALIZED VIEW create_cluster1_view AS SELECT SUM(f1) FROM create_cluster1_table;

                > SELECT * FROM create_cluster1_table;
                123
                > SELECT * FROM create_cluster1_view;
                123

                > SET cluster=create_cluster2
                > CREATE DEFAULT INDEX ON create_cluster2_table;
                > CREATE MATERIALIZED VIEW create_cluster2_view AS SELECT SUM(f1) FROM create_cluster2_table;

                > SELECT * FROM create_cluster2_table;
                234
                > SELECT * FROM create_cluster2_view;
                234

                > DROP TABLE create_cluster1_table CASCADE;
                > DROP TABLE create_cluster2_table CASCADE;
           """
            )
        )


class DropCluster(Check):
    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE TABLE drop_cluster1_table (f1 INTEGER);
                > CREATE TABLE drop_cluster2_table (f1 INTEGER);

                > INSERT INTO drop_cluster1_table VALUES (123);
                > INSERT INTO drop_cluster2_table VALUES (234);

                > CREATE CLUSTER drop_cluster1 REPLICAS (replica1 (SIZE '2-2'));
                > CREATE CLUSTER drop_cluster2 REPLICAS (replica1 (SIZE '2-2'));

                > SET cluster=drop_cluster1
                > CREATE DEFAULT INDEX ON drop_cluster1_table;
                > CREATE MATERIALIZED VIEW drop_cluster1_view AS SELECT SUM(f1) FROM drop_cluster1_table;

                > SET cluster=drop_cluster2
                > CREATE DEFAULT INDEX ON drop_cluster2_table;
                > CREATE MATERIALIZED VIEW drop_cluster2_view AS SELECT SUM(f1) FROM drop_cluster2_table;

                > DROP CLUSTER drop_cluster1 CASCADE;
                """,
                """

                > DROP CLUSTER drop_cluster2 CASCADE;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET cluster=drop_cluster1

                > SET cluster=create_cluster2

                > SET cluster=default

                > SELECT * FROM drop_cluster1_table;
                123

                > SELECT * FROM drop_cluster1_view;
                123

                > SELECT * FROM drop_cluster2_table;
                234

                > SELECT * FROM drop_cluster2_view;
                234
           """
            )
        )
