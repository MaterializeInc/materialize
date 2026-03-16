# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Native SQL Server source tests, functional.
"""

import glob
import pathlib
import random
import threading
import time
from textwrap import dedent

from materialize import MZ_ROOT, buildkite
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive

TLS_CONF_PATH = MZ_ROOT / "test" / "sql-server-cdc" / "tls-mssconfig.conf"

SERVICES = [
    Mz(app_password=""),
    Materialized(
        additional_system_parameter_defaults={
            "log_filter": "mz_storage::source::sql_server=trace,mz_storage::source::sql_server::replication=trace,mz_sql_server_util=debug,info"
        },
    ),
    Testdrive(),
    TestCerts(),
    SqlServer(
        volumes_extra=[
            "secrets:/var/opt/mssql/certs",
            f"{TLS_CONF_PATH}:/var/opt/mssql/mssql.conf",
        ]
    ),
]

VOLUMES = {"ms_scratch": {}, "ag-certs": {}}


#
# Test that SQL Server ingestion works
#
def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    def process(name: str) -> None:
        if name in ("default", "large-scale"):
            return
        with c.test_case(name):
            c.workflow(name, *parser.args)

    workflows_with_internal_sharding = ["cdc"]
    sharded_workflows = workflows_with_internal_sharding + buildkite.shard_list(
        sorted([w for w in c.workflows if w not in workflows_with_internal_sharding]),
        lambda w: w,
    )
    print(
        f"Workflows in shard with index {buildkite.get_parallelism_index()}: {sharded_workflows}"
    )
    c.test_parts(sharded_workflows, process)


def workflow_cdc(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "filter",
        nargs="*",
        default=["*.td"],
        help="limit to only the files matching filter",
    )
    args = parser.parse_args()

    matching_files: list[str] = []
    for filter in args.filter:
        matching_files.extend(
            glob.glob(filter, root_dir=MZ_ROOT / "test" / "sql-server-cdc")
        )
    matching_files = sorted(matching_files)
    sharded_files: list[str] = buildkite.shard_list(
        sorted(matching_files), lambda file: file
    )
    print(f"Filter: {args.filter} Files: {sharded_files}")

    # Start with a fresh state
    c.kill("sql-server")
    c.rm("sql-server")
    c.kill("materialized")
    c.rm("materialized")

    # must start test-certs, otherwise the certificates needed by sql-server may not be available
    # in the secrets volume when it starts up
    c.up("materialized", "test-certs", "sql-server", Service("testdrive", idle=True))
    seed = random.getrandbits(16)

    ssl_ca = c.exec(
        "sql-server", "cat", "/var/opt/mssql/certs/ca.crt", capture=True
    ).stdout
    alt_ssl_ca = c.exec(
        "sql-server", "cat", "/var/opt/mssql/certs/ca-selective.crt", capture=True
    ).stdout

    def run(file: pathlib.Path | str) -> None:
        c.run_testdrive_files(
            "--max-errors=1",
            f"--seed={seed}",
            f"--var=ssl-ca={ssl_ca}",
            f"--var=alt-ssl-ca={alt_ssl_ca}",
            f"--var=default-replica-size=scale={Materialized.Size.DEFAULT_SIZE},workers={Materialized.Size.DEFAULT_SIZE}",
            f"--var=default-sql-server-user={SqlServer.DEFAULT_USER}",
            f"--var=default-sql-server-password={SqlServer.DEFAULT_SA_PASSWORD}",
            "setup/setup.td",
            str(file),
        )

    c.test_parts(sharded_files, run)


def workflow_no_agent(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Ensures that MZ detects that the SQL Server Agent is not running at purification and produces
    an error to the user.
    """
    # Start with a fresh state
    c.kill("sql-server")
    c.rm("sql-server")
    c.kill("materialized")
    c.rm("materialized")

    try:
        with c.override(
            SqlServer(volumes_extra=["ms_scratch:/var/opt/mssql"]),
        ):
            c.up("materialized", "sql-server", Service("testdrive", idle=True))
            c.run_testdrive_files(
                "setup/setup.td",
                f"--var=default-sql-server-user={SqlServer.DEFAULT_USER}",
                f"--var=default-sql-server-password={SqlServer.DEFAULT_SA_PASSWORD}",
            )
            c.kill("sql-server")

            with c.override(
                SqlServer(
                    enable_agent=False, volumes_extra=["ms_scratch:/var/opt/mssql"]
                )
            ):
                c.up("sql-server")

                c.testdrive(
                    dedent(
                        f"""
                        > CREATE SECRET IF NOT EXISTS sql_server_pass AS '{SqlServer.DEFAULT_SA_PASSWORD}'

                        ! CREATE CONNECTION sql_server_conn TO SQL SERVER (
                            HOST 'sql-server',
                            PORT 1433,
                            DATABASE test,
                            USER '{SqlServer.DEFAULT_USER}',
                            PASSWORD = SECRET sql_server_pass);
                        contains:Invalid SQL Server system replication settings
                        """
                    )
                )
    finally:
        c.kill("sql-server")
        c.rm("sql-server")
        c.rm_volumes("ms_scratch")


def workflow_snapshot_consistency(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Tests the scenario where a new source creates a snapshot and transitions to replication
    while the upstream source table is seeing updates. This test validates that the source snapshot
    sees a consistent view of the table during snapshot and identifies the correct LSN to start
    replication.
    """
    # Start with a fresh state
    c.kill("sql-server")
    c.rm("sql-server")
    c.kill("materialized")
    c.rm("materialized")

    initial_rows = 100
    with c.override(SqlServer()):
        c.up("materialized", "sql-server", Service("testdrive", idle=True))

        # Setup MS SQL server and materialize
        c.run_testdrive_files(
            "setup/setup.td",
            f"--var=default-sql-server-user={SqlServer.DEFAULT_USER}",
            f"--var=default-sql-server-password={SqlServer.DEFAULT_SA_PASSWORD}",
        )
        c.testdrive(
            dedent(
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                CREATE TABLE t1 (id bigint, val bigint);
                EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;

                WITH nums AS (SELECT 1 as n UNION ALL SELECT n+1 FROM nums where n < {initial_rows}) INSERT INTO t1 SELECT n, n+1000 FROM nums;

                > CREATE SECRET IF NOT EXISTS mssql_pass AS '{SqlServer.DEFAULT_SA_PASSWORD}';
                > CREATE CONNECTION mssql_connection TO SQL SERVER (
                    HOST 'sql-server',
                    PORT 1433,
                    DATABASE test,
                    USER '{SqlServer.DEFAULT_USER}',
                    PASSWORD = SECRET mssql_pass);

                > DROP SOURCE IF EXISTS mssql_source CASCADE;
                """
            )
        )

    # Create a concurrent workload with 2 variaties of updates
    # - insert of new rows
    # - insert and delete of a row (the same row)
    update_id_offset = 10000
    update_val_offset = 100000
    insert_delete = lambda i: dedent(
        f"""
        INSERT INTO t1 VALUES (999999999,666666666), ({i + update_id_offset}, {i + update_val_offset});
        DELETE FROM t1 WHERE id = 999999999;
        """
    )

    # The number of update rows is based on local testing. While this doesn't guarantee that updates
    # to SQL server are ocurring throughout the snapshot -> replication transition of the source, there is
    # a generous overlap here. The possibility that they don't overlap should be exremely unlikely.
    update_rows = 1500
    upstream_updates = "\n".join([insert_delete(i) for i in range(update_rows)])

    def concurrent_updates(c: Composition) -> None:
        input = (
            dedent(
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                """
            )
            + upstream_updates
        )
        c.testdrive(args=["--no-reset"], input=input)

    driver_thread = threading.Thread(target=concurrent_updates, args=(c,))
    print("==== Starting concurrent updates")
    driver_thread.start()

    # create the subsource that will create a snapshot and start replicating
    c.testdrive(
        args=["--no-reset"],
        input=dedent(
            """
            > CREATE SOURCE mssql_source
              FROM SQL SERVER CONNECTION mssql_connection
              FOR TABLES (dbo.t1);
            """
        ),
    )

    # validate MZ sees the correct results once the conccurent load is complete
    driver_thread.join()
    print("==== Validate concurrent updates")
    c.testdrive(
        args=["--no-reset"],
        input=dedent(
            f"""
            > SELECT COUNT(*) >= {update_rows + initial_rows}, MIN(id), MAX(id) >= {update_rows + update_id_offset - 1} FROM t1;
            true 1 true
            """
        ),
    )


def workflow_large_scale(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    The goal is to test a large scale SQL Server instance and to make sure that we can successfully ingest data from it quickly.
    """
    with c.override(SqlServer()):
        c.kill("sql-server")
        c.rm("sql-server")

        c.up("materialized", "sql-server", Service("testdrive", idle=True))
        c.run_testdrive_files(
            "setup/setup.td",
            f"--var=default-sql-server-user={SqlServer.DEFAULT_USER}",
            f"--var=default-sql-server-password={SqlServer.DEFAULT_SA_PASSWORD}",
        )
        # Set up the SQL Server instance with the initial records, set up the
        # connection to the SQL Server instance in Materialize.
        c.testdrive(
            dedent(
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                DROP TABLE IF EXISTS products;
                CREATE TABLE products (id int NOT NULL, name varchar(255) DEFAULT NULL, merchant_id int NOT NULL, price int DEFAULT NULL, status int DEFAULT NULL, created_at datetime2 NULL, recordSizePayload VARCHAR(1024), PRIMARY KEY (id));
                EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'products', @role_name = 'SA', @supports_net_changes = 0;

                > CREATE SECRET IF NOT EXISTS mssql_pass AS '{SqlServer.DEFAULT_SA_PASSWORD}';
                > CREATE CONNECTION mssql_connection TO SQL SERVER (
                    HOST 'sql-server',
                    PORT 1433,
                    DATABASE test,
                    USER '{SqlServer.DEFAULT_USER}',
                    PASSWORD = SECRET mssql_pass);

                > DROP SOURCE IF EXISTS mssql_source CASCADE;
                """
            )
        )

        def make_inserts(c: Composition, start: int, batch_num: int):
            c.testdrive(
                args=["--no-reset"],
                input=dedent(
                    f"""
                    $ sql-server-connect name=sql-server
                    server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                    $ sql-server-execute name=sql-server
                    USE test;
                    WITH nums AS (SELECT TOP ({batch_num}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS i FROM sys.all_objects a CROSS JOIN sys.all_objects b) INSERT INTO products (id, name, merchant_id, price, status, created_at, recordSizePayload) SELECT {start} + i, 'name' + CAST({start} + i AS VARCHAR(20)), ({start} + i) % 1000, ({start} + i) % 1000, ({start} + i) % 10, CAST('2024-12-12' AS DATE), REPLICATE('x', 1024) FROM nums;
                """
                ),
            )

        num_rows = 10_000_000
        batch_size = 10_000
        for i in range(0, num_rows, batch_size):
            batch_num = min(batch_size, num_rows - i)
            make_inserts(c, i, batch_num)

        c.testdrive(
            args=["--no-reset"],
            input=dedent(
                f"""
                > CREATE SOURCE s1
                  FROM SQL SERVER CONNECTION mssql_connection;
                > CREATE TABLE products FROM SOURCE s1 (REFERENCE products);
                > SELECT COUNT(*) FROM products;
                {num_rows}
                """
            ),
        )

        make_inserts(c, num_rows, 1)

        c.testdrive(
            args=["--no-reset"],
            input=dedent(
                f"""
                > SELECT COUNT(*) FROM products;
                {num_rows + 1}
                """
            ),
        )


def workflow_readonly_intent(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Tests that READONLY INTENT routes connections to a secondary replica in an
    Availability Group, and that mz_internal.mz_sql_server_sources exposes the
    is_primary_replica flag.

    Sets up a 2-node SQL Server Availability Group (primary + secondary),
    creates Materialize sources against both, and verifies routing via the
    mz_sql_server_sources builtin table.
    """

    sa_pwd = SqlServer.DEFAULT_SA_PASSWORD

    # Start fresh — only kill services in the base SERVICES list.
    # sql-server-primary/secondary are created inside c.override() below.
    for svc in ("sql-server", "materialized"):
        c.kill(svc, signal="SIGKILL")
        c.rm(svc)

    with c.override(
        SqlServer(
            name="sql-server-primary",
            hostname="sql-server-primary",
            hadr_enabled=True,
            volumes_extra=["ag-certs:/ag-certs"],
        ),
        SqlServer(
            name="sql-server-secondary",
            hostname="sql-server-secondary",
            hadr_enabled=True,
            volumes_extra=["ag-certs:/ag-certs"],
        ),
        fail_on_new_service=False,
    ):
        c.up(
            "materialized",
            "sql-server-primary",
            "sql-server-secondary",
            Service("testdrive", idle=True),
        )

        # ── Setup AG via testdrive ──────────────────────────────────────────

        # Create certificates and master keys on primary.
        # Write certs to /tmp/ (SQL Server on Linux allows writes there).
        c.testdrive(
            args=["--no-reset"],
            input=dedent(
                f"""
                $ sql-server-connect name=primary
                server=tcp:sql-server-primary,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={sa_pwd}

                $ sql-server-execute name=primary split-lines=false
                USE master;
                CREATE MASTER KEY ENCRYPTION BY PASSWORD = '{sa_pwd}';
                CREATE LOGIN dbm_login WITH PASSWORD = '{sa_pwd}';
                CREATE USER dbm_user FOR LOGIN dbm_login;
                CREATE CERTIFICATE dbm_certificate WITH SUBJECT = 'dbm';
                BACKUP CERTIFICATE dbm_certificate TO FILE = '/tmp/dbm_certificate.cer' WITH PRIVATE KEY (FILE = '/tmp/dbm_certificate.pvk', ENCRYPTION BY PASSWORD = '{sa_pwd}');
                """
            ),
        )

        # Copy certificate files from primary to secondary via the shared volume.
        c.invoke(
            "exec",
            "--user",
            "root",
            "-T",
            "sql-server-primary",
            "bash",
            "-c",
            "cp /tmp/dbm_certificate.cer /tmp/dbm_certificate.pvk /ag-certs/ && chmod 644 /ag-certs/*",
        )
        c.invoke(
            "exec",
            "--user",
            "root",
            "-T",
            "sql-server-secondary",
            "bash",
            "-c",
            "cp /ag-certs/dbm_certificate.cer /ag-certs/dbm_certificate.pvk /tmp/ && chown mssql:root /tmp/dbm_certificate.*",
        )

        # Import certificates on secondary, create endpoints on both, create AG.
        c.testdrive(
            args=["--no-reset"],
            input=dedent(
                f"""
                $ sql-server-connect name=secondary
                server=tcp:sql-server-secondary,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={sa_pwd}

                $ sql-server-execute name=secondary split-lines=false
                USE master;
                CREATE MASTER KEY ENCRYPTION BY PASSWORD = '{sa_pwd}';
                CREATE LOGIN dbm_login WITH PASSWORD = '{sa_pwd}';
                CREATE USER dbm_user FOR LOGIN dbm_login;
                CREATE CERTIFICATE dbm_certificate AUTHORIZATION dbm_user FROM FILE = '/tmp/dbm_certificate.cer' WITH PRIVATE KEY (FILE = '/tmp/dbm_certificate.pvk', DECRYPTION BY PASSWORD = '{sa_pwd}');

                $ sql-server-connect name=primary
                server=tcp:sql-server-primary,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={sa_pwd}

                $ sql-server-execute name=primary split-lines=false
                CREATE ENDPOINT Hadr_endpoint STATE = STARTED AS TCP (LISTENER_PORT = 5022, LISTENER_IP = ALL) FOR DATABASE_MIRRORING (ROLE = ALL, AUTHENTICATION = CERTIFICATE dbm_certificate, ENCRYPTION = REQUIRED ALGORITHM AES);
                GRANT CONNECT ON ENDPOINT::Hadr_endpoint TO dbm_login;

                $ sql-server-execute name=secondary split-lines=false
                CREATE ENDPOINT Hadr_endpoint STATE = STARTED AS TCP (LISTENER_PORT = 5022, LISTENER_IP = ALL) FOR DATABASE_MIRRORING (ROLE = ALL, AUTHENTICATION = CERTIFICATE dbm_certificate, ENCRYPTION = REQUIRED ALGORITHM AES);
                GRANT CONNECT ON ENDPOINT::Hadr_endpoint TO dbm_login;

                $ sql-server-execute name=primary split-lines=false
                CREATE AVAILABILITY GROUP ag_test WITH (CLUSTER_TYPE = NONE) FOR REPLICA ON N'sql-server-primary' WITH (ENDPOINT_URL = N'TCP://sql-server-primary:5022', AVAILABILITY_MODE = SYNCHRONOUS_COMMIT, FAILOVER_MODE = MANUAL, SEEDING_MODE = AUTOMATIC, SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL)), N'sql-server-secondary' WITH (ENDPOINT_URL = N'TCP://sql-server-secondary:5022', AVAILABILITY_MODE = SYNCHRONOUS_COMMIT, FAILOVER_MODE = MANUAL, SEEDING_MODE = AUTOMATIC, SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL));

                $ sql-server-execute name=secondary split-lines=false
                ALTER AVAILABILITY GROUP ag_test JOIN WITH (CLUSTER_TYPE = NONE);
                ALTER AVAILABILITY GROUP ag_test GRANT CREATE ANY DATABASE;

                $ sql-server-execute name=primary split-lines=false
                CREATE DATABASE ag_test_db;
                ALTER DATABASE ag_test_db SET RECOVERY FULL;
                BACKUP DATABASE ag_test_db TO DISK = '/tmp/ag_test_db.bak';
                ALTER AVAILABILITY GROUP ag_test ADD DATABASE ag_test_db;
                """
            ),
        )

        # Wait for the database to sync to the secondary.
        for attempt in range(60):
            result = c.exec(
                "sql-server-secondary",
                "/opt/mssql-tools18/bin/sqlcmd",
                "-C",
                "-U",
                "SA",
                "-P",
                sa_pwd,
                "-Q",
                "SELECT COUNT(*) FROM sys.dm_hadr_database_replica_states WHERE is_local = 1 AND is_primary_replica = 0 AND synchronization_state_desc = 'SYNCHRONIZED';",
                capture=True,
            )
            if "1" in result.stdout.strip().split():
                print(f"    Database synchronized after {attempt} attempts.")
                break
            print(f"    Waiting for sync (attempt {attempt})...")
            time.sleep(3)
        else:
            raise RuntimeError(
                "Database did not synchronize to secondary within timeout"
            )

        # Enable CDC on the primary (required for Materialize sources).
        c.testdrive(
            args=["--no-reset"],
            input=dedent(
                f"""
                $ sql-server-connect name=primary
                server=tcp:sql-server-primary,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={sa_pwd}

                $ sql-server-execute name=primary
                USE ag_test_db;
                EXEC sys.sp_cdc_enable_db;
                ALTER DATABASE ag_test_db SET ALLOW_SNAPSHOT_ISOLATION ON;
                CREATE TABLE readonly_test (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL);
                EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'readonly_test', @role_name = 'SA', @supports_net_changes = 0;
                INSERT INTO readonly_test (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
                """
            ),
        )

        # Wait for data to replicate.
        time.sleep(5)

        # Set up a dummy ticker job on the primary so CDC keeps producing LSNs.
        c.testdrive(
            args=["--no-reset"],
            input=dedent(
                f"""
                $ sql-server-connect name=primary
                server=tcp:sql-server-primary,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={sa_pwd}

                $ sql-server-execute name=primary
                USE ag_test_db;
                CREATE TABLE dummy (t datetime);
                INSERT INTO dummy VALUES (CURRENT_TIMESTAMP);
                EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'dummy', @role_name = 'SA', @supports_net_changes = 0;

                $ sql-server-execute name=primary
                USE ag_test_db;

                $ sql-server-execute name=primary split-lines=false
                CREATE OR ALTER PROCEDURE dbo.DummyTicker AS BEGIN SET NOCOUNT ON; WHILE 1 = 1 BEGIN UPDATE dbo.dummy SET t = CURRENT_TIMESTAMP; WAITFOR DELAY '00:00:01'; END END;

                $ sql-server-execute name=primary
                USE msdb;
                EXEC sp_add_job @job_name = N'DummyTicker', @enabled = 1;
                EXEC sp_add_jobstep @job_name = N'DummyTicker', @step_name = N'run', @subsystem = N'TSQL', @database_name = N'ag_test_db', @command = N'EXEC dbo.DummyTicker;';
                EXEC sp_add_jobserver @job_name = N'DummyTicker';
                EXEC msdb.dbo.sp_start_job N'DummyTicker';
                """
            ),
        )

        # ── Materialize: verify mz_sql_server_sources exposes AG info ──────
        #
        # We create a source from the PRIMARY and verify is_primary_replica = true.
        # We cannot create a CDC source from the secondary because CDC system
        # checks (agent running, etc.) fail on read-only replicas. The existing
        # standalone test (12-readonly-intent.td) covers the READONLY INTENT
        # syntax with is_primary_replica = NULL (not in an AG).

        c.testdrive(
            args=["--no-reset"],
            input=dedent(
                f"""
                > CREATE SECRET ag_pass AS '{sa_pwd}'

                # Create a connection to the PRIMARY.
                > CREATE CONNECTION primary_conn TO SQL SERVER (
                    HOST 'sql-server-primary',
                    PORT 1433,
                    DATABASE ag_test_db,
                    USER '{SqlServer.DEFAULT_USER}',
                    PASSWORD = SECRET ag_pass
                  );

                > VALIDATE CONNECTION primary_conn;

                # Create a source from the primary.
                > CREATE SOURCE primary_source
                  FROM SQL SERVER CONNECTION primary_conn
                  FOR TABLES (dbo.readonly_test AS primary_readonly_test);

                # Verify the primary source reports is_primary_replica = true.
                > SELECT is_primary_replica FROM mz_internal.mz_sql_server_sources
                  WHERE id = (SELECT id FROM mz_sources WHERE name = 'primary_source');
                true

                # Verify we can read the data through the primary.
                > SELECT * FROM primary_readonly_test;
                1 alice
                2 bob
                3 charlie

                # Clean up.
                > DROP SOURCE primary_source CASCADE;
                > DROP CONNECTION primary_conn;
                > DROP SECRET ag_pass;
                """
            ),
        )
