# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Native Postgres source tests, functional.
"""

from textwrap import dedent

from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy
from materialize.pg_cdc import (
    kill_pg_and_mz,
    workflow_cdc,  # noqa: F401
    workflow_replication_disabled,  # noqa: F401
    workflow_replication_slots,  # noqa: F401
    workflow_silent_connection_drop,  # noqa: F401
    workflow_wal_level,  # noqa: F401
)
from materialize.postgres_util import (
    PostgresRecvlogical,
    create_postgres,
    get_targeted_pg_version,
)

SERVICES = [
    Mz(app_password=""),
    Materialized(
        volumes_extra=["secrets:/share/secrets"],
        additional_system_parameter_defaults={
            "log_filter": "mz_storage::source::postgres=trace,debug,info,warn,error"
        },
        default_replication_factor=2,
    ),
    Testdrive(),
    TestCerts(),
    Toxiproxy(),
    create_postgres(pg_version=None),
    PostgresRecvlogical(
        replication_slot_name="", publication_name=""
    ),  # Overriden below
]


# TODO: redesign ceased status database-issues#7687
# Test that how subsource statuses work across a variety of scenarios
# def workflow_statuses(c: Composition, parser: WorkflowArgumentParser) -> None:
#     c.up("materialized", "postgres", "toxiproxy")
#     c.run_testdrive_files("status/01-setup.td")

#     with c.override(Testdrive(no_reset=True)):
#         # Restart mz
#         c.kill("materialized")
#         c.up("materialized")

#         c.run_testdrive_files(
#             "status/02-after-mz-restart.td",
#             "status/03-toxiproxy-interrupt.td",
#             "status/04-drop-publication.td",
#         )


def workflow_large_scale(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    The goal is to test a large scale Postgres instance and to make sure that we can successfully ingest data from it quickly.
    """
    pg_version = get_targeted_pg_version(parser)
    with c.override(
        create_postgres(
            pg_version=pg_version, extra_command=["-c", "max_replication_slots=3"]
        )
    ):
        c.up("materialized", "postgres", Service("testdrive", idle=True))

        # Set up the Postgres server with the initial records, set up the connection to
        # the Postgres server in Materialize.
        c.testdrive(
            dedent(
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                ALTER USER postgres WITH replication;
                DROP SCHEMA IF EXISTS public CASCADE;
                DROP PUBLICATION IF EXISTS mz_source;
                CREATE SCHEMA public;

                > CREATE SECRET IF NOT EXISTS pgpass AS 'postgres'
                > CREATE CONNECTION IF NOT EXISTS pg TO POSTGRES (HOST postgres, DATABASE postgres, USER postgres, PASSWORD SECRET pgpass)

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                DROP TABLE IF EXISTS products;
                CREATE TABLE products (id int NOT NULL, name varchar(255) DEFAULT NULL, merchant_id int NOT NULL, price int DEFAULT NULL, status int DEFAULT NULL, created_at timestamp NULL, recordSizePayload text, PRIMARY KEY (id));
                ALTER TABLE products REPLICA IDENTITY FULL;
                CREATE PUBLICATION mz_source FOR ALL TABLES;

                > DROP SOURCE IF EXISTS s1 CASCADE;
                """
            )
        )

    def make_inserts(c: Composition, start: int, batch_num: int):
        c.testdrive(
            args=["--no-reset"],
            input=dedent(
                f"""
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO products (id, name, merchant_id, price, status, created_at, recordSizePayload) SELECT {start} + row_number() OVER (), 'name' || ({start} + row_number() OVER ()), ({start} + row_number() OVER ()) % 1000, ({start} + row_number() OVER ()) % 1000, ({start} + row_number() OVER ()) % 10, '2024-12-12'::DATE, repeat('x', 1000000) FROM generate_series(1, {batch_num});
            """
            ),
        )

    num_rows = 100_000  # out of memory with 200_000 rows
    batch_size = 10_000
    for i in range(0, num_rows, batch_size):
        batch_num = min(batch_size, num_rows - i)
        make_inserts(c, i, batch_num)

    c.testdrive(
        args=["--no-reset"],
        input=dedent(
            f"""
            > CREATE SOURCE s1
              FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
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


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.run_all_workflows(
        exclude=["large-scale", "migration"],
        internally_sharded=["cdc"],
        args=parser.args,
        between_workflows=kill_pg_and_mz,
    )
