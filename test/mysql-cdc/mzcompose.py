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

from textwrap import dedent

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
)
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy

SERVICES = [
    Mz(app_password=""),
    Materialized(
        additional_system_parameter_defaults={
            "log_filter": "mz_storage::source::mysql=trace,info"
        },
        default_replication_factor=2,
    ),
    create_mysql(MySql.DEFAULT_VERSION),
    create_mysql_replica(MySql.DEFAULT_VERSION),
    TestCerts(),
    Toxiproxy(),
    Testdrive(default_timeout="60s"),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.run_all_workflows(
        exclude=["large-scale"],
        internally_sharded=["cdc"],
        args=parser.args,
    )


def workflow_many_inserts(c: Composition, parser: WorkflowArgumentParser) -> None:
    _workflow_many_inserts(
        c,
        parser,
        create_source_sql="""
            > CREATE SOURCE s1
                FROM MYSQL CONNECTION mysql_conn;
            > CREATE TABLE many_inserts FROM SOURCE s1 (REFERENCE public.many_inserts);
            """,
    )


def workflow_large_scale(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    The goal is to test a large scale MySQL instance and to make sure that we can successfully ingest data from it quickly.
    """
    mysql_version = get_targeted_mysql_version(parser)
    with c.override(create_mysql(mysql_version)):
        c.up("materialized", "mysql", Service("testdrive", idle=True))

        # Set up the MySQL server with the initial records, set up the connection to
        # the MySQL server in Materialize.
        c.testdrive(
            dedent(
                f"""
                $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                ALTER SYSTEM SET max_mysql_connections = 100

                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                > CREATE SECRET IF NOT EXISTS mysqlpass AS '{MySql.DEFAULT_ROOT_PASSWORD}'
                > CREATE CONNECTION IF NOT EXISTS mysql_conn TO MYSQL (HOST mysql, USER root, PASSWORD SECRET mysqlpass)

                $ mysql-execute name=mysql
                DROP DATABASE IF EXISTS public;
                CREATE DATABASE public;
                USE public;
                DROP TABLE IF EXISTS products;
                CREATE TABLE products (id int NOT NULL, name varchar(255) DEFAULT NULL, merchant_id int NOT NULL, price int DEFAULT NULL, status int DEFAULT NULL, created_at timestamp NULL DEFAULT CURRENT_TIMESTAMP(), recordSizePayload longtext, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
                ALTER TABLE products DISABLE KEYS;

                > DROP SOURCE IF EXISTS s1 CASCADE;
                """
            )
        )

    def make_inserts(c: Composition, start: int, batch_num: int):
        c.testdrive(
            args=["--no-reset"],
            input=dedent(
                f"""
            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
            $ mysql-execute name=mysql
            SET foreign_key_checks = 0;
            USE public;
            SET @i:={start};
            INSERT INTO products (id, name, merchant_id, price, status, created_at, recordSizePayload) SELECT @i:=@i+1, CONCAT("name", @i), @i % 1000, @i % 1000, @i % 10, '2024-12-12', repeat('x', 1000000) FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT {batch_num};
            """
            ),
        )

    num_rows = 100_000  # out of disk with 200_000 rows
    batch_size = 100
    for i in range(0, num_rows, batch_size):
        batch_num = min(batch_size, num_rows - i)
        make_inserts(c, i, batch_num)

    c.testdrive(
        args=["--no-reset"],
        input=dedent(
            f"""
            > CREATE SOURCE s1
                FROM MYSQL CONNECTION mysql_conn;
            > CREATE TABLE products FROM SOURCE s1 (REFERENCE public.products);
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


def workflow_source_timeouts(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Test source connect timeout using toxiproxy to drop network traffic.
    """
    mysql_version = get_targeted_mysql_version(parser)
    with c.override(
        Materialized(
            sanity_restart=False,
            additional_system_parameter_defaults={
                "log_filter": "mz_storage::source::mysql=trace,info"
            },
            default_replication_factor=2,
        ),
        Toxiproxy(),
        create_mysql(mysql_version),
    ):
        c.up("materialized", "mysql", "toxiproxy")
        c.run_testdrive_files(
            *MySql.default_testdrive_args(),
            "proxied/*.td",
        )
