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
from materialize.util import MzVersion


class CreateManagedCluster(Check):
    def _can_run(self) -> bool:
        return self.base_version >= MzVersion.parse("0.58.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $[version>=5500] postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_managed_clusters = true
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE CLUSTER create_managed_cluster1 SIZE '2-2', REPLICATION FACTOR 2;
                """,
                """
                > CREATE CLUSTER create_managed_cluster2 SIZE '2-2', REPLICATION FACTOR 2;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE create_managed_cluster1_table (f1 INTEGER);
                > CREATE TABLE create_managed_cluster2_table (f1 INTEGER);

                > INSERT INTO create_managed_cluster1_table VALUES (123);
                > INSERT INTO create_managed_cluster2_table VALUES (234);

                > SET cluster=create_managed_cluster1
                > CREATE DEFAULT INDEX ON create_managed_cluster1_table;
                > CREATE MATERIALIZED VIEW create_managed_cluster1_view AS SELECT SUM(f1) FROM create_managed_cluster1_table;

                > SELECT * FROM create_managed_cluster1_table;
                123
                > SELECT * FROM create_managed_cluster1_view;
                123

                > SET cluster=create_managed_cluster2
                > CREATE DEFAULT INDEX ON create_managed_cluster2_table;
                > CREATE MATERIALIZED VIEW create_managed_cluster2_view AS SELECT SUM(f1) FROM create_managed_cluster2_table;

                > SELECT * FROM create_managed_cluster2_table;
                234
                > SELECT * FROM create_managed_cluster2_view;
                234

                > DROP TABLE create_managed_cluster1_table CASCADE;
                > DROP TABLE create_managed_cluster2_table CASCADE;
           """
            )
        )


class DropManagedCluster(Check):
    def _can_run(self) -> bool:
        return self.base_version >= MzVersion.parse("0.58.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $[version>=5500] postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_managed_clusters = true
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE TABLE drop_managed_cluster1_table (f1 INTEGER);
                > CREATE TABLE drop_managed_cluster2_table (f1 INTEGER);

                > INSERT INTO drop_managed_cluster1_table VALUES (123);
                > INSERT INTO drop_managed_cluster2_table VALUES (234);

                > CREATE CLUSTER drop_managed_cluster1 SIZE '2-2', REPLICATION FACTOR 2;
                > CREATE CLUSTER drop_managed_cluster2 SIZE '2-2', REPLICATION FACTOR 2;

                > SET cluster=drop_managed_cluster1
                > CREATE DEFAULT INDEX ON drop_managed_cluster1_table;
                > CREATE MATERIALIZED VIEW drop_managed_cluster1_view AS SELECT SUM(f1) FROM drop_managed_cluster1_table;

                > SET cluster=drop_managed_cluster2
                > CREATE DEFAULT INDEX ON drop_managed_cluster2_table;
                > CREATE MATERIALIZED VIEW drop_managed_cluster2_view AS SELECT SUM(f1) FROM drop_managed_cluster2_table;

                > DROP CLUSTER drop_managed_cluster1 CASCADE;
                """,
                """

                > DROP CLUSTER drop_managed_cluster2 CASCADE;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET cluster=drop_managed_cluster1

                > SET cluster=drop_managed_cluster2

                > SET cluster=default

                > SELECT * FROM drop_managed_cluster1_table;
                123

                ! SELECT * FROM drop_managed_cluster1_view;
                contains: unknown catalog item 'drop_managed_cluster1_view'

                > SELECT * FROM drop_managed_cluster2_table;
                234

                ! SELECT * FROM drop_managed_cluster2_view;
                contains: unknown catalog item 'drop_managed_cluster2_view'
           """
            )
        )
