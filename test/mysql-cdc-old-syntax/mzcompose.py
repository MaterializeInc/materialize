# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Functional test for the native (non-Debezium) MySQL sources.
"""

from materialize.mysql_cdc import (
    workflow_cdc,  # noqa: F401
    workflow_replica_connection,  # noqa: F401
    workflow_schema_change_restart,  # noqa: F401
)
from materialize.mysql_cdc import (
    workflow_many_inserts as _workflow_many_inserts,
)
from materialize.mysql_util import (
    create_mysql,
    create_mysql_replica,
    get_targeted_mysql_version,
    retrieve_invalid_ssl_context_for_mysql,
    retrieve_ssl_context_for_mysql,
)
from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.metadata_store import (
    METADATA_STORE,
    CockroachOrPostgresMetadata,
)
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.source_table_migration import (
    verify_sources_after_source_table_migration,
)

SERVICES = [
    Mz(app_password=""),
    Materialized(
        external_blob_store=True,
        additional_system_parameter_defaults={
            "log_filter": "mz_storage::source::mysql=trace,info"
        },
        default_replication_factor=2,
    ),
    create_mysql(MySql.DEFAULT_VERSION),
    create_mysql_replica(MySql.DEFAULT_VERSION),
    TestCerts(),
    CockroachOrPostgresMetadata(),
    Minio(setup_materialize=True),
    Testdrive(default_timeout="60s"),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.run_all_workflows(
        exclude=["migration"],
        internally_sharded=["cdc"],
        args=parser.args,
    )


def workflow_many_inserts(c: Composition, parser: WorkflowArgumentParser) -> None:
    _workflow_many_inserts(
        c,
        parser,
        create_source_sql="""
            > CREATE SOURCE s1
                FROM MYSQL CONNECTION mysql_conn
                FOR TABLES (public.many_inserts);
            """,
    )


def workflow_migration(c: Composition, parser: WorkflowArgumentParser) -> None:
    sharded_files = c.glob_test_files(parser)

    mysql_version = get_targeted_mysql_version(parser)

    for file in sharded_files:

        mz_old = Materialized(
            name="materialized",
            external_metadata_store=True,
            external_blob_store=True,
            additional_system_parameter_defaults={
                "log_filter": "mz_storage::source::mysql=trace,info"
            },
            default_replication_factor=2,
        )

        mz_new = Materialized(
            name="materialized",
            external_metadata_store=True,
            external_blob_store=True,
            additional_system_parameter_defaults={
                "log_filter": "mz_storage::source::mysql=trace,info",
                "force_source_table_syntax": "true",
            },
            default_replication_factor=2,
        )

        with c.override(mz_old, create_mysql(mysql_version)):
            c.up("materialized", "mysql")

            print(f"Running {file} with mz_old")

            valid_ssl_context = retrieve_ssl_context_for_mysql(c)
            wrong_ssl_context = retrieve_invalid_ssl_context_for_mysql(c)

            c.sources_and_sinks_ignored_from_validation.add("drop_table")

            c.run_testdrive_files(
                f"--var=ssl-ca={valid_ssl_context.ca}",
                f"--var=ssl-client-cert={valid_ssl_context.client_cert}",
                f"--var=ssl-client-key={valid_ssl_context.client_key}",
                f"--var=ssl-wrong-ca={wrong_ssl_context.ca}",
                f"--var=ssl-wrong-client-cert={wrong_ssl_context.client_cert}",
                f"--var=ssl-wrong-client-key={wrong_ssl_context.client_key}",
                *MySql.default_testdrive_args(),
                "--var=mysql-user-password=us3rp4ssw0rd",
                *Materialized.default_testdrive_size_args(),
                "--no-reset",
                file,
            )

            c.kill("materialized", wait=True)

            with c.override(mz_new):
                c.up("materialized")

                print("Running mz_new")
                verify_sources_after_source_table_migration(c, file)

                c.kill("materialized", wait=True)
                c.kill("mysql", wait=True)
                c.kill(METADATA_STORE, wait=True)
                c.rm("materialized")
                c.rm(METADATA_STORE)
                c.rm("mysql")
                c.rm_volumes("mzdata")
