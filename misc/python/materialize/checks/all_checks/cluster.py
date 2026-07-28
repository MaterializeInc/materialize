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
from materialize.checks.checks import Check, externally_idempotent
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


class CreateCluster(Check):
    def manipulate(self) -> list[Testdrive]:
        # This list MUST be of length 2.
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT CREATECLUSTER ON SYSTEM TO materialize

                > CREATE CLUSTER create_cluster1 REPLICAS (replica1 (SIZE 'scale=2,workers=2'));
                """,
                """
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT CREATECLUSTER ON SYSTEM TO materialize

                > CREATE CLUSTER create_cluster2 (SIZE 'scale=2,workers=2', REPLICATION FACTOR 1);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
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

                >[version<15500] SHOW CREATE CLUSTER create_cluster2;
                create_cluster2 "CREATE CLUSTER \\"create_cluster2\\" (DISK = true, INTROSPECTION DEBUGGING = false, INTROSPECTION INTERVAL = INTERVAL '00:00:01', MANAGED = true, REPLICATION FACTOR = 1, SIZE = 'scale=2,workers=2', SCHEDULE = MANUAL)"

                >[15500<=version<2603500] SHOW CREATE CLUSTER create_cluster2;
                create_cluster2 "CREATE CLUSTER \\"create_cluster2\\" (INTROSPECTION DEBUGGING = false, INTROSPECTION INTERVAL = INTERVAL '00:00:01', MANAGED = true, REPLICATION FACTOR = 1, SIZE = 'scale=2,workers=2', SCHEDULE = MANUAL)"

                >[version>=2603500] SHOW CREATE CLUSTER create_cluster2;
                create_cluster2 "CREATE CLUSTER \\"create_cluster2\\" (EXPERIMENTAL ARRANGEMENT COMPRESSION = false, INTROSPECTION DEBUGGING = false, INTROSPECTION INTERVAL = INTERVAL '00:00:01', MANAGED = true, REPLICATION FACTOR = 1, SIZE = 'scale=2,workers=2', SCHEDULE = MANUAL)"

                > DROP TABLE create_cluster1_table CASCADE;
                > DROP TABLE create_cluster2_table CASCADE;
           """))


class AlterCluster(Check):
    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE CLUSTER alter_cluster1 REPLICAS (r1 (SIZE 'scale=2,workers=2'));

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
        return Testdrive(dedent("""
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

                >[version<15500] SHOW CREATE CLUSTER alter_cluster1;
                alter_cluster1 "CREATE CLUSTER \\"alter_cluster1\\" (DISK = true, INTROSPECTION DEBUGGING = true, INTROSPECTION INTERVAL = INTERVAL '00:00:45', MANAGED = true, REPLICATION FACTOR = 1, SIZE = 'scale=2,workers=2', SCHEDULE = MANUAL)"

                >[15500<=version<2603500] SHOW CREATE CLUSTER alter_cluster1;
                alter_cluster1 "CREATE CLUSTER \\"alter_cluster1\\" (INTROSPECTION DEBUGGING = true, INTROSPECTION INTERVAL = INTERVAL '00:00:45', MANAGED = true, REPLICATION FACTOR = 1, SIZE = 'scale=2,workers=2', SCHEDULE = MANUAL)"

                >[version>=2603500] SHOW CREATE CLUSTER alter_cluster1;
                alter_cluster1 "CREATE CLUSTER \\"alter_cluster1\\" (EXPERIMENTAL ARRANGEMENT COMPRESSION = false, INTROSPECTION DEBUGGING = true, INTROSPECTION INTERVAL = INTERVAL '00:00:45', MANAGED = true, REPLICATION FACTOR = 1, SIZE = 'scale=2,workers=2', SCHEDULE = MANUAL)"

                > SELECT name, introspection_debugging, introspection_interval FROM mz_catalog.mz_clusters WHERE name = 'alter_cluster1';
                alter_cluster1 true "00:00:45"
           """))


@externally_idempotent(False)
class AlterClusterGracefulReconfiguration(Check):
    """Graceful reconfiguration (WAIT UNTIL READY) of a cluster hosting a
    single-replica Postgres source. Readiness must not wait for the source,
    which never hydrates on the target replicas, and the settled
    reconfiguration must survive restarts and upgrades. Regression coverage
    for SQL-530.
    """

    def _can_run(self, e: Executor) -> bool:
        # Graceful reconfiguration of a cluster hosting a single-replica
        # source wedges until its deadline rolls it back on versions without
        # the SQL-530 fix, so no phase may run on an older version. The fix
        # predates the v26.35.0-rc.1 cut, so every published v26.35 artifact
        # has it and the -dev gate includes the release candidates.
        return self.base_version >= MzVersion.parse_mz("v26.35.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
            ALTER SYSTEM SET enable_zero_downtime_cluster_reconfiguration = true

            $ postgres-execute connection=postgres://postgres:postgres@postgres
            CREATE USER graceful_reconfig WITH SUPERUSER PASSWORD 'postgres';
            ALTER USER graceful_reconfig WITH replication;
            DROP PUBLICATION IF EXISTS graceful_reconfig_pub;
            DROP TABLE IF EXISTS graceful_reconfig_table;
            CREATE TABLE graceful_reconfig_table (f1 INTEGER PRIMARY KEY);
            ALTER TABLE graceful_reconfig_table REPLICA IDENTITY FULL;
            INSERT INTO graceful_reconfig_table SELECT generate_series(1, 100);
            CREATE PUBLICATION graceful_reconfig_pub FOR TABLE graceful_reconfig_table;

            > CREATE CLUSTER graceful_reconfig_cluster (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

            > CREATE SECRET graceful_reconfig_pass AS 'postgres'

            > CREATE CONNECTION graceful_reconfig_conn FOR POSTGRES
              HOST 'postgres',
              DATABASE postgres,
              USER graceful_reconfig,
              PASSWORD SECRET graceful_reconfig_pass

            > CREATE SOURCE graceful_reconfig_source
              IN CLUSTER graceful_reconfig_cluster
              FROM POSTGRES CONNECTION graceful_reconfig_conn
              (PUBLICATION 'graceful_reconfig_pub')

            > CREATE TABLE graceful_reconfig_t FROM SOURCE graceful_reconfig_source (REFERENCE graceful_reconfig_table)

            > SELECT count(*) FROM graceful_reconfig_t
            100
        """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ set-sql-timeout duration=240s

                > ALTER CLUSTER graceful_reconfig_cluster SET (SIZE 'scale=1,workers=2') WITH (WAIT UNTIL READY (TIMEOUT '300s', ON TIMEOUT ROLLBACK))

                # A background ALTER returns immediately, so wait for the
                # realized config to cut over to the target.
                > SELECT size FROM mz_clusters WHERE name = 'graceful_reconfig_cluster'
                "scale=1,workers=2"

                # The source keeps ingesting on the promoted replica set.
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO graceful_reconfig_table SELECT generate_series(101, 200);

                > SELECT count(*) FROM graceful_reconfig_t
                200

                $ set-sql-timeout duration=default
                """,
                """
                $ set-sql-timeout duration=240s

                > ALTER CLUSTER graceful_reconfig_cluster SET (SIZE 'scale=2,workers=2') WITH (WAIT UNTIL READY (TIMEOUT '300s', ON TIMEOUT ROLLBACK))

                > SELECT size FROM mz_clusters WHERE name = 'graceful_reconfig_cluster'
                "scale=2,workers=2"

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO graceful_reconfig_table SELECT generate_series(201, 300);

                > SELECT count(*) FROM graceful_reconfig_t
                300

                $ set-sql-timeout duration=default
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            $ set-sql-timeout duration=240s

            > SELECT size FROM mz_clusters WHERE name = 'graceful_reconfig_cluster'
            "scale=2,workers=2"

            # The settled reconfiguration record is retained across restarts
            # and upgrades.
            > SELECT recon.status
              FROM mz_internal.mz_cluster_reconfigurations recon
              JOIN mz_clusters c ON c.id = recon.cluster_id
              WHERE c.name = 'graceful_reconfig_cluster'
            finalized

            > SELECT count(*) FROM graceful_reconfig_t
            300

            $ set-sql-timeout duration=default
        """))


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

                > CREATE CLUSTER drop_cluster1 REPLICAS (replica1 (SIZE 'scale=2,workers=2'));
                > CREATE CLUSTER drop_cluster2 REPLICAS (replica1 (SIZE 'scale=2,workers=2'));

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
        return Testdrive(dedent("""
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
           """))


class CreateClusterRF0(Check):
    def manipulate(self) -> list[Testdrive]:
        # This list MUST be of length 2.
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT CREATECLUSTER ON SYSTEM TO materialize

                > CREATE CLUSTER create_cluster_rf0_1 (SIZE 'scale=2,workers=2', REPLICATION FACTOR 0);
                """,
                """
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT CREATECLUSTER ON SYSTEM TO materialize

                > CREATE CLUSTER create_cluster_rf0_2 (SIZE 'scale=2,workers=2', REPLICATION FACTOR 0);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
                >[version<15500] SHOW CREATE CLUSTER create_cluster_rf0_1;
                create_cluster_rf0_1 "CREATE CLUSTER \\"create_cluster_rf0_1\\" (DISK = true, INTROSPECTION DEBUGGING = false, INTROSPECTION INTERVAL = INTERVAL '00:00:01', MANAGED = true, REPLICATION FACTOR = 0, SIZE = 'scale=2,workers=2', SCHEDULE = MANUAL)"

                >[15500<=version<2603500] SHOW CREATE CLUSTER create_cluster_rf0_1;
                create_cluster_rf0_1 "CREATE CLUSTER \\"create_cluster_rf0_1\\" (INTROSPECTION DEBUGGING = false, INTROSPECTION INTERVAL = INTERVAL '00:00:01', MANAGED = true, REPLICATION FACTOR = 0, SIZE = 'scale=2,workers=2', SCHEDULE = MANUAL)"

                >[version>=2603500] SHOW CREATE CLUSTER create_cluster_rf0_1;
                create_cluster_rf0_1 "CREATE CLUSTER \\"create_cluster_rf0_1\\" (EXPERIMENTAL ARRANGEMENT COMPRESSION = false, INTROSPECTION DEBUGGING = false, INTROSPECTION INTERVAL = INTERVAL '00:00:01', MANAGED = true, REPLICATION FACTOR = 0, SIZE = 'scale=2,workers=2', SCHEDULE = MANUAL)"

                >[version<15500] SHOW CREATE CLUSTER create_cluster_rf0_2;
                create_cluster_rf0_2 "CREATE CLUSTER \\"create_cluster_rf0_2\\" (DISK = true, INTROSPECTION DEBUGGING = false, INTROSPECTION INTERVAL = INTERVAL '00:00:01', MANAGED = true, REPLICATION FACTOR = 0, SIZE = 'scale=2,workers=2', SCHEDULE = MANUAL)"

                >[15500<=version<2603500] SHOW CREATE CLUSTER create_cluster_rf0_2;
                create_cluster_rf0_2 "CREATE CLUSTER \\"create_cluster_rf0_2\\" (INTROSPECTION DEBUGGING = false, INTROSPECTION INTERVAL = INTERVAL '00:00:01', MANAGED = true, REPLICATION FACTOR = 0, SIZE = 'scale=2,workers=2', SCHEDULE = MANUAL)"

                >[version>=2603500] SHOW CREATE CLUSTER create_cluster_rf0_2;
                create_cluster_rf0_2 "CREATE CLUSTER \\"create_cluster_rf0_2\\" (EXPERIMENTAL ARRANGEMENT COMPRESSION = false, INTROSPECTION DEBUGGING = false, INTROSPECTION INTERVAL = INTERVAL '00:00:01', MANAGED = true, REPLICATION FACTOR = 0, SIZE = 'scale=2,workers=2', SCHEDULE = MANUAL)"
           """))
