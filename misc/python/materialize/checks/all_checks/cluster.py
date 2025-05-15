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


class CreateCluster(Check):
    def manipulate(self) -> list[Testdrive]:
        # This list MUST be of length 2.
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT CREATECLUSTER ON SYSTEM TO materialize

                > CREATE CLUSTER create_cluster1 REPLICAS (replica1 (SIZE '2-2'));
                """,
                """
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT CREATECLUSTER ON SYSTEM TO materialize

                > CREATE CLUSTER create_cluster2 (SIZE '2-2', REPLICATION FACTOR 1);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ set-sql-timeout duration=240s

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

                ! SHOW CREATE CLUSTER create_cluster1;
                contains: SHOW CREATE for unmanaged clusters not yet supported

                > SHOW CREATE CLUSTER create_cluster2;
                create_cluster2 "CREATE CLUSTER \\"create_cluster2\\" (DISK = true, INTROSPECTION DEBUGGING = false, INTROSPECTION INTERVAL = INTERVAL '00:00:01', MANAGED = true, REPLICATION FACTOR = 1, SIZE = '2-2', SCHEDULE = MANUAL)"

                > DROP TABLE create_cluster1_table CASCADE;
                > DROP TABLE create_cluster2_table CASCADE;
           """
            )
        )


class AlterCluster(Check):
    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE CLUSTER alter_cluster1 REPLICAS (r1 (SIZE '2-2'));

                > CREATE TABLE alter_cluster1_table (f1 INTEGER);
                > INSERT INTO alter_cluster1_table VALUES (123);

                > SET cluster=alter_cluster1
                > CREATE DEFAULT INDEX ON alter_cluster1_table;
                > CREATE MATERIALIZED VIEW alter_cluster1_view AS SELECT SUM(f1) FROM alter_cluster1_table;
                """,
                """
                > ALTER CLUSTER alter_cluster1 SET (MANAGED);

                > ALTER CLUSTER alter_cluster1 SET (introspection debugging = TRUE, introspection interval = '45s');
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET cluster=default

                > SELECT * FROM alter_cluster1_table;
                123
                > SELECT * FROM alter_cluster1_view;
                123

                > SET cluster=alter_cluster1

                > SELECT * FROM alter_cluster1_table;
                123
                > SELECT * FROM alter_cluster1_view;
                123

                > SHOW CREATE CLUSTER alter_cluster1;
                alter_cluster1 "CREATE CLUSTER \\"alter_cluster1\\" (DISK = true, INTROSPECTION DEBUGGING = true, INTROSPECTION INTERVAL = INTERVAL '00:00:45', MANAGED = true, REPLICATION FACTOR = 1, SIZE = '2-2', SCHEDULE = MANUAL)"

                > SELECT name, introspection_debugging, introspection_interval FROM mz_catalog.mz_clusters WHERE name = 'alter_cluster1';
                alter_cluster1 true "00:00:45"
           """
            )
        )


class DropCluster(Check):
    def manipulate(self) -> list[Testdrive]:
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

                > SET cluster=drop_cluster2

                > SET cluster=default

                > SELECT * FROM drop_cluster1_table;
                123

                ! SELECT * FROM drop_cluster1_view;
                contains: unknown catalog item 'drop_cluster1_view'

                > SELECT * FROM drop_cluster2_table;
                234

                ! SELECT * FROM drop_cluster2_view;
                contains: unknown catalog item 'drop_cluster2_view'
           """
            )
        )
