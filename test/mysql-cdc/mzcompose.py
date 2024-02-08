# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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
    MySql(
        additional_args=[
            "--log-bin=mysql-bin",
            "--gtid_mode=ON",
            "--enforce_gtid_consistency=ON",
            "--binlog-format=row",
            "--log-slave-updates",
            "--binlog-row-image=full",
            "--server-id=1",
        ],
    ),
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


def workflow_replica_connection(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.up("materialized", "mysql", "mysql-replica")
    c.run_testdrive_files(
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        "override/10-replica-connection.td",
    )


def workflow_schema_change_restart(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """
    Validates that a schema change done to a table after the MySQL source is created
    but before the snapshot is completed is detected after a restart.
    """
    with c.override(Testdrive(no_reset=True)):
        c.up("materialized", "mysql")
        c.run_testdrive_files(
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            "schema-restart/before-restart.td",
        )

        # Restart mz
        c.kill("materialized")
        c.up("materialized")

        c.run_testdrive_files(
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            "schema-restart/after-restart.td",
        )
