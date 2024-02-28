# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from materialize.mysql import (
    retrieve_invalid_ssl_context_for_mysql,
    retrieve_ssl_context_for_mysql,
)
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive

MYSQL_VERSIONS = ["5.7.44", "8.3.0"]


def create_mysql_replica(version: str) -> MySql:
    return MySql(
        name="mysql-replica",
        version=version,
        additional_args=[
            "--gtid_mode=ON",
            "--enforce_gtid_consistency=ON",
            "--skip-replica-start",
            "--server-id=2",
        ],
    )


SERVICES = [
    Materialized(
        additional_system_parameter_defaults={
            "log_filter": "mz_storage::source::mysql=trace,info"
        },
    ),
    MySql(),
    create_mysql_replica(MySql.DEFAULT_VERSION),
    TestCerts(),
    Testdrive(volumes_extra=["../mysql-cdc:/workdir/mysql-cdc"], default_timeout="60s"),
]


def workflow_default(c: Composition) -> None:
    for mysql_version in MYSQL_VERSIONS:
        print(f"--- Testing MySQL {mysql_version}")
        test_mysql_version(c, mysql_version)


def test_mysql_version(c: Composition, mysql_version: str) -> None:
    with c.override(
        MySql(version=mysql_version), create_mysql_replica(version=mysql_version)
    ):
        c.down(destroy_volumes=True)
        c.up("materialized", "mysql")

        valid_ssl_context = retrieve_ssl_context_for_mysql(c)
        wrong_ssl_context = retrieve_invalid_ssl_context_for_mysql(c)

        c.sources_and_sinks_ignored_from_validation.add("drop_table")

        files = ["mysql-cdc/*.td"]

        command = [
            f"--var=ssl-ca={valid_ssl_context.ca}",
            f"--var=ssl-client-cert={valid_ssl_context.client_cert}",
            f"--var=ssl-client-key={valid_ssl_context.client_key}",
            f"--var=ssl-wrong-ca={wrong_ssl_context.ca}",
            f"--var=ssl-wrong-client-cert={wrong_ssl_context.client_cert}",
            f"--var=ssl-wrong-client-key={wrong_ssl_context.client_key}",
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            "--var=mysql-user-password=us3rp4ssw0rd",
            f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
            f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
            *files,
        ]

        c.run_testdrive_files(*command)
