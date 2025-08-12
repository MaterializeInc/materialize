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


#
# Test that SQL Server ingestion works
#
def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    def process(name: str) -> None:
        if name == "default":
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
                $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                ALTER SYSTEM SET enable_sql_server_source = true;

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
        INSERT INTO t1 VALUES (999999999,666666666), ({i+update_id_offset}, {i+update_val_offset});
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
            > SELECT COUNT(*) >= {update_rows+initial_rows}, MIN(id), MAX(id) >= {update_rows + update_id_offset - 1} FROM t1;
            true 1 true
            """
        ),
    )
