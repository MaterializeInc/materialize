# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.mzcompose.composition import (
    Composition,
    Service,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.metabase import Metabase
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Materialized(),
    Testdrive(no_reset=True),
    Postgres(),
    Metabase(),
    Service(
        name="benchbase",
        config={
            "mzbuild": "benchbase",
            "init": True,
        },
    ),
]


def workflow_default(c: Composition) -> None:
    tables = [
        "customer",
        "district",
        "history",
        "item",
        "new_order",
        "oorder",
        "order_line",
        "stock",
        "warehouse",
    ]
    bench = "tpcc"
    config = "/benchbase/tpcc_sf_10.xml"

    c.up("materialized", "postgres")

    c.up("testdrive", persistent=True)

    c.testdrive(
        dedent(
            """
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            ALTER USER postgres WITH replication;
            DROP SCHEMA IF EXISTS public CASCADE;
            DROP PUBLICATION IF EXISTS mz_source;
            CREATE SCHEMA public;
            """
        ),
        persistent=True,
    )

    # Generate initial data.
    c.run(
        "benchbase",
        "-b",
        bench,
        "-c",
        config,
        "--create=true",
        "--load=false",
        "--execute=false",
    )

    replica_identity_full = "".join(
        f"ALTER TABLE {table} REPLICA IDENTITY FULL;" for table in tables
    )
    c.testdrive(
        dedent(
            f"""
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            {replica_identity_full}
            """
        ),
        persistent=True,
    )
    c.testdrive(
        dedent(
            """
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            CREATE PUBLICATION mz_source FOR ALL TABLES;
            > CREATE SECRET IF NOT EXISTS pgpass AS 'postgres'
            > DROP CONNECTION IF EXISTS pg CASCADE;
            > CREATE CONNECTION pg TO POSTGRES (
                HOST postgres,
                DATABASE postgres,
                USER postgres,
                PASSWORD SECRET pgpass
                );
            > CREATE SOURCE p
                FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
                FOR ALL TABLES;
            """
        ),
        persistent=True,
    )
    # IN CLUSTER single_worker_cluster

    # Generate initial data.
    c.run(
        "benchbase",
        "-b",
        bench,
        "-c",
        config,
        "--create=false",
        "--load=true",
        "--execute=false",
    )

    replica_identity_full = "".join(
        f"ALTER TABLE {table} REPLICA IDENTITY FULL;" for table in tables
    )
    c.testdrive(
        "".join(
            dedent(
                f"""
                > SELECT count(*) > 0 FROM {table};
                true
                """
            )
            for table in tables
        ),
        persistent=True,
    )

    # Generate initial data.
    c.run(
        "benchbase",
        "-b",
        bench,
        "-c",
        config,
        "--create=false",
        "--load=false",
        "--execute=true",
    )
