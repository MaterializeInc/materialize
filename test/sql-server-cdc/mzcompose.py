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
import os
import pathlib
import random
import threading
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

VOLUMES = {"ms_scratch": {}}


#
# Test that SQL Server ingestion works
#
def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--sql-server-version",
        type=str,
        default=SqlServer.DEFAULT_VERSION,
        help="SQL Server image tag from mcr.microsoft.com/mssql/server, e.g. "
        "'2019-CU32-ubuntu-20.04'. Defaults to the locally-seeded mzbuild image.",
    )
    args, remaining_args = parser.parse_known_args()
    if args.sql_server_version is not None:
        print(f"Running with SQL Server version {args.sql_server_version}")

    def process(name: str) -> None:
        # `azure` runs against an external Azure SQL Database and requires
        # credentials from the environment, so it is not part of the default run.
        if name in ("default", "large-scale", "azure"):
            return
        with c.test_case(name):
            with c.override(
                SqlServer(
                    version=args.sql_server_version,
                    volumes_extra=[
                        "secrets:/var/opt/mssql/certs",
                        f"{TLS_CONF_PATH}:/var/opt/mssql/mssql.conf",
                    ],
                )
            ):
                # Pass through only the args the child workflow understands,
                # i.e. strip the `--sql-server-version` we just consumed.
                c.workflow(name, *remaining_args)

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


def workflow_azure(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Exercise the SQL Server source happy path against a real Azure SQL Database.

    Connection details are read from the environment and no local SQL Server
    container is started:

        AZURE_SQL_HOST      hostname of the Azure SQL Database server
        AZURE_SQL_PORT      port (default 1433)
        AZURE_SQL_DATABASE  database to ingest from
        AZURE_SQL_USER      login with db_owner on that database
        AZURE_SQL_PASSWORD  password for that login

    The password must not contain a single quote, semicolon, or closing brace,
    as it is interpolated into a testdrive SQL string and an ADO connection
    string.
    """
    parser.parse_args()

    def require_env(name: str, default: str | None = None) -> str:
        value = os.environ.get(name, default)
        if not value:
            raise ValueError(
                f"{name} must be set in the environment to run the `azure` workflow"
            )
        return value

    tvars = [
        f"--var=azure-sql-host={require_env('AZURE_SQL_HOST')}",
        f"--var=azure-sql-port={require_env('AZURE_SQL_PORT', '1433')}",
        f"--var=azure-sql-database={require_env('AZURE_SQL_DATABASE')}",
        f"--var=azure-sql-user={require_env('AZURE_SQL_USER')}",
        f"--var=azure-sql-password={require_env('AZURE_SQL_PASSWORD')}",
        # A distinct, complexity-compliant password for the provisioned
        # least-privilege user. Randomized per run so state left by a crashed run
        # cannot collide with the new one.
        f"--var=mz-least-priv-password=Mz!{random.getrandbits(48):012x}Az",
    ]

    c.up("materialized", Service("testdrive", idle=True))

    # Prepare the upstream database, including the ticker table the background
    # thread below drives.
    c.run_testdrive_files("--max-errors=1", *tvars, "azure/setup.td")

    # Azure SQL Database has no SQL Server Agent, so nothing advances the CDC max
    # LSN on its own. Drive a ticker from a background thread for the rest of the
    # workflow, otherwise CREATE SOURCE stalls at startup waiting for the LSN to
    # move. The ticker uses --no-reset so it never drops the Materialize objects
    # created by the main thread.
    ticker_thread = threading.Thread(
        target=lambda: c.run_testdrive_files(
            "--no-reset", "--max-errors=1", *tvars, "azure/ticker.td"
        ),
        daemon=True,
    )
    ticker_thread.start()
    try:
        # Happy path and least-privilege checks share one testdrive process so
        # the happy-path source survives into the restart check below.
        c.run_testdrive_files(
            "--no-reset",
            "--max-errors=1",
            *tvars,
            "azure/happy-path.td",
            "azure/privileges.td",
        )

        # Restart Materialize and confirm the source resumes. This is where the
        # engine-edition gating runs at runtime: the replication operator
        # re-checks the SQL Server Agent and the progress operator re-queries
        # restore history, both of which must be skipped for Azure SQL Database.
        c.kill("materialized")
        c.up("materialized", Service("testdrive", idle=True))
        c.run_testdrive_files(
            "--no-reset",
            "--max-errors=1",
            *tvars,
            "azure/verify-restart.td",
        )
    finally:
        c.run_testdrive_files(
            "--no-reset", "--max-errors=1", *tvars, "azure/ticker-stop.td"
        )
        ticker_thread.join()


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

                c.testdrive(dedent(f"""
                        > CREATE SECRET IF NOT EXISTS sql_server_pass AS '{SqlServer.DEFAULT_SA_PASSWORD}'

                        ! CREATE CONNECTION sql_server_conn TO SQL SERVER (
                            HOST 'sql-server',
                            PORT 1433,
                            DATABASE test,
                            USER '{SqlServer.DEFAULT_USER}',
                            PASSWORD = SECRET sql_server_pass);
                        contains:Invalid SQL Server system replication settings
                        """))
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
        c.testdrive(dedent(f"""
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
                """))

    # Create a concurrent workload with 2 variaties of updates
    # - insert of new rows
    # - insert and delete of a row (the same row)
    update_id_offset = 10000
    update_val_offset = 100000
    insert_delete = lambda i: dedent(f"""
        INSERT INTO t1 VALUES (999999999,666666666), ({i + update_id_offset}, {i + update_val_offset});
        DELETE FROM t1 WHERE id = 999999999;
        """)

    # The number of update rows is based on local testing. While this doesn't guarantee that updates
    # to SQL server are ocurring throughout the snapshot -> replication transition of the source, there is
    # a generous overlap here. The possibility that they don't overlap should be exremely unlikely.
    update_rows = 1500
    upstream_updates = "\n".join([insert_delete(i) for i in range(update_rows)])

    def concurrent_updates(c: Composition) -> None:
        input = dedent(f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                """) + upstream_updates
        c.testdrive(args=["--no-reset"], input=input)

    driver_thread = threading.Thread(target=concurrent_updates, args=(c,))
    print("==== Starting concurrent updates")
    driver_thread.start()

    # create the subsource that will create a snapshot and start replicating
    c.testdrive(
        args=["--no-reset"],
        input=dedent("""
            > CREATE SOURCE mssql_source
              FROM SQL SERVER CONNECTION mssql_connection
              FOR TABLES (dbo.t1);
            """),
    )

    # validate MZ sees the correct results once the conccurent load is complete
    driver_thread.join()
    print("==== Validate concurrent updates")
    c.testdrive(
        args=["--no-reset"],
        input=dedent(f"""
            > SELECT COUNT(*) >= {update_rows + initial_rows}, MIN(id), MAX(id) >= {update_rows + update_id_offset - 1} FROM t1;
            true 1 true
            """),
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
        c.testdrive(dedent(f"""
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
                """))

        def make_inserts(c: Composition, start: int, batch_num: int):
            c.testdrive(
                args=["--no-reset"],
                input=dedent(f"""
                    $ sql-server-connect name=sql-server
                    server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                    $ sql-server-execute name=sql-server
                    USE test;
                    WITH nums AS (SELECT TOP ({batch_num}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS i FROM sys.all_objects a CROSS JOIN sys.all_objects b) INSERT INTO products (id, name, merchant_id, price, status, created_at, recordSizePayload) SELECT {start} + i, 'name' + CAST({start} + i AS VARCHAR(20)), ({start} + i) % 1000, ({start} + i) % 1000, ({start} + i) % 10, CAST('2024-12-12' AS DATE), REPLICATE('x', 1024) FROM nums;
                """),
            )

        num_rows = 10_000_000
        batch_size = 10_000
        for i in range(0, num_rows, batch_size):
            batch_num = min(batch_size, num_rows - i)
            make_inserts(c, i, batch_num)

        c.testdrive(
            args=["--no-reset"],
            input=dedent(f"""
                > CREATE SOURCE s1
                  FROM SQL SERVER CONNECTION mssql_connection;
                > CREATE TABLE products FROM SOURCE s1 (REFERENCE products);
                > SELECT COUNT(*) FROM products;
                {num_rows}
                """),
        )

        make_inserts(c, num_rows, 1)

        c.testdrive(
            args=["--no-reset"],
            input=dedent(f"""
                > SELECT COUNT(*) FROM products;
                {num_rows + 1}
                """),
        )
