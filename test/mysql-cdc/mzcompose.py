# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Materialized(
        additional_system_parameter_defaults={
            "log_filter": "mz_storage::source::mysql=trace,info"
        },
    ),
    MySql(),
    MySql(
        name="mysql-replica",
        additional_args=[
            "--gtid_mode=ON",
            "--enforce_gtid_consistency=ON",
            "--skip-replica-start",
            "--server-id=2",
        ],
    ),
    TestCerts(),
    Testdrive(default_timeout="60s"),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    for name in c.workflows:
        if name == "default":
            continue

        with c.test_case(name):
            c.workflow(name)


def workflow_cdc(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "filter",
        nargs="*",
        default=["*.td"],
        help="limit to only the files matching filter",
    )
    args = parser.parse_args()

    c.up("materialized", "mysql")

    # MySQL generates self-signed certificates for SSL connections on startup,
    # for both the server and client:
    # https://dev.mysql.com/doc/refman/8.3/en/creating-ssl-rsa-files-using-mysql.html
    # Grab the correct Server CA and Client Key and Cert from the MySQL container
    # (and strip the trailing null byte):
    ssl_ca = c.exec("mysql", "cat", "/var/lib/mysql/ca.pem", capture=True).stdout.split(
        "\x00", 1
    )[0]
    ssl_client_cert = c.exec(
        "mysql", "cat", "/var/lib/mysql/client-cert.pem", capture=True
    ).stdout.split("\x00", 1)[0]
    ssl_client_key = c.exec(
        "mysql", "cat", "/var/lib/mysql/client-key.pem", capture=True
    ).stdout.split("\x00", 1)[0]

    # Use the TestCert service to obtain a wrong CA and client cert/key:
    ssl_wrong_ca = c.run("test-certs", "cat", "/secrets/ca.crt", capture=True).stdout
    ssl_wrong_client_cert = c.run(
        "test-certs", "cat", "/secrets/certuser.crt", capture=True
    ).stdout
    ssl_wrong_client_key = c.run(
        "test-certs", "cat", "/secrets/certuser.key", capture=True
    ).stdout

    c.run_testdrive_files(
        f"--var=ssl-ca={ssl_ca}",
        f"--var=ssl-client-cert={ssl_client_cert}",
        f"--var=ssl-client-key={ssl_client_key}",
        f"--var=ssl-wrong-ca={ssl_wrong_ca}",
        f"--var=ssl-wrong-client-cert={ssl_wrong_client_cert}",
        f"--var=ssl-wrong-client-key={ssl_wrong_client_key}",
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        "--var=mysql-user-password=us3rp4ssw0rd",
        *args.filter,
    )


def workflow_replica_connection(c: Composition) -> None:
    c.up("materialized", "mysql", "mysql-replica")
    c.run_testdrive_files(
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        "override/10-replica-connection.td",
    )


def workflow_schema_change_restart(c: Composition) -> None:
    """
    Validates that a schema change done to a table after the MySQL source is created
    but before the snapshot is completed is detected after a restart.
    """
    c.up("materialized", "mysql")
    c.run_testdrive_files(
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        "schema-restart/before-restart.td",
    )

    with c.override(Testdrive(no_reset=True)):
        # Restart mz
        c.kill("materialized")
        c.up("materialized")

        c.run_testdrive_files(
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            "schema-restart/after-restart.td",
        )


def workflow_many_inserts(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Tests a scenario that caused a consistency issue in the past. We insert a large
    number of rows into a table, and then create a source for that table. We then
    insert more rows into the table while the source is snapshotting the table, and
    verify that the correct count of rows is captured by the source.

    In earlier incarnations of the MySQL source, the source would snapshot inside
    a transaction using REPEATABLE READ 'WITH CONSISTENT SNAPSHOT' semantics, which
    we assumed would produce a consistent read from the point that the transaction started at.
    However this test would fail because the snapshot would capture some of the
    rows inserted after the transaction started and they would not be correctly rewound.

    We replaced the 'WITH CONSISTENT SNAPSHOT' with a SELECT count(*) FROM {table}
    to trigger a consistent read inside the snapshot transaction to fix the issue.
    """
    c.up("materialized", "mysql")
    c.up("testdrive", persistent=True)

    n = 100000

    insertions = "\n".join(
        [
            f"""
            SET @i:=0;
            INSERT INTO t1 (f2) SELECT @i:=@i+1 FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT {round(n/1000)};
            """
            for i in range(0, 1000)
        ]
    )

    c.testdrive(
        dedent(
            f"""
            $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
            ALTER SYSTEM SET enable_mysql_source = true

            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

            > CREATE SECRET IF NOT EXISTS mysqlpass AS '{MySql.DEFAULT_ROOT_PASSWORD}'
            > CREATE CONNECTION IF NOT EXISTS mysql_conn TO MYSQL (HOST mysql, USER root, PASSWORD SECRET mysqlpass)

            $ mysql-execute name=mysql
            DROP DATABASE IF EXISTS public;
            CREATE DATABASE public;
            USE public;
            DROP TABLE IF EXISTS t1;
            CREATE TABLE t1 (pk SERIAL PRIMARY KEY, f2 BIGINT);

            {insertions}
            {insertions}

            > DROP SOURCE IF EXISTS s1;

            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

            > CREATE SOURCE s1
                FROM MYSQL CONNECTION mysql_conn
                FOR TABLES (public.t1);

            $ mysql-execute name=mysql
            USE public;
            {insertions}

            > SELECT count(*) FROM t1
            {n*3}
            """
        )
    )
