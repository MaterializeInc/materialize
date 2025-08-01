# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Verify how much storage is being used, based on mz_storage_usage and
mz_recent_storage_usage.
"""

import time
from dataclasses import dataclass
from textwrap import dedent

from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.testdrive import Testdrive

COLLECTION_INTERVAL_SECS = 5

PG_CDC_SETUP = dedent(
    """
    > CREATE SECRET pgpass AS 'postgres'
    > CREATE CONNECTION pg TO POSTGRES (
        HOST postgres,
        DATABASE postgres,
        USER postgres,
        PASSWORD SECRET pgpass
      )

    $ postgres-execute connection=postgres://postgres:postgres@postgres
    ALTER USER postgres WITH replication;
    DROP SCHEMA IF EXISTS public CASCADE;
    CREATE SCHEMA public;

    DROP PUBLICATION IF EXISTS mz_source;
    CREATE PUBLICATION mz_source FOR ALL TABLES;
    """
)

KAFKA_SETUP = dedent(
    """

    > CREATE CONNECTION IF NOT EXISTS kafka_conn
      TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

    > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
        URL '${testdrive.schema-registry-url}'
      );

    $ set key-schema={"type": "string"}
    $ set value-schema={"type": "record", "name": "r", "fields": [{"name": "a", "type": "string"}]}
    """
)

SERVICES = [
    Redpanda(),
    Postgres(),
    Materialized(
        environment_extra=[
            f"MZ_STORAGE_USAGE_COLLECTION_INTERVAL={COLLECTION_INTERVAL_SECS}s"
        ],
        additional_system_parameter_defaults={"persist_rollup_threshold": "20"},
    ),
    Testdrive(default_timeout="120s", no_reset=True),
]


@dataclass
class DatabaseObject:
    name: str
    testdrive: str
    expected_size: int


database_objects = [
    DatabaseObject(
        name="table_insert_unique_rows",
        testdrive=dedent(
            """
            > CREATE TABLE obj (f1 TEXT)
            > INSERT INTO obj SELECT generate_series::text || REPEAT('x', 1024) FROM generate_series(1, 1024)
            """
        ),
        expected_size=1024 * 1024,
    ),
    # Identical rows should cause a diff > 1 and not be stored individually
    DatabaseObject(
        name="table_insert_identical_rows",
        testdrive=dedent(
            """
            > CREATE TABLE obj (f1 TEXT)
            > INSERT INTO obj SELECT REPEAT('x', 1024 * 1024) FROM generate_series(1, 1024)
            """
        ),
        expected_size=1024 * 1024,
    ),
    # Deleted/updated rows should be garbage-collected
    # https://github.com/MaterializeInc/database-issues/issues/4313
    # DatabaseObject(
    #    name="table_delete",
    #    testdrive=dedent(
    #        f"""
    #        > CREATE TABLE obj (f1 TEXT)
    #        > INSERT INTO obj SELECT generate_series::text || REPEAT('x', 1024) FROM generate_series(1, 1024)
    #        > SELECT mz_unsafe.mz_sleep({COLLECTION_INTERVAL_SECS} + 1)
    #        <null>
    #        > DELETE FROM obj;
    #        """
    #    ),
    #    expected_size=???,
    # ),
    # DatabaseObject(
    #    name="upsert_update",
    #    testdrive=KAFKA_SETUP+ dedent(
    #        f"""
    #        $ kafka-create-topic topic=upsert-update
    #
    #        $ kafka-ingest format=avro topic=upsert-update key-format=avro key-schema=${{key-schema}} schema=${{value-schema}}
    #        "${{kafka-ingest.iteration}}" {{"a": "0"}}
    #
    #        > CREATE SOURCE obj
    #          FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-update-${{testdrive.seed}}')
    #
    #        > CREATE TABLE obj_tbl
    #          FROM SOURCE obj (REFERENCE "testdrive-upsert-update-${{testdrive.seed}}")
    #          FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
    #          ENVELOPE UPSERT
    #        """) + "\n".join([dedent(
    #            f"""
    #            $ kafka-ingest format=avro topic=upsert-update key-format=avro key-schema=${{key-schema}} schema=${{value-schema}} repeat=5000000
    #            "${{kafka-ingest.iteration}}" {{"a": "{i}"}}
    #            """
    #        ) for i in range(1,11)]) + dedent(
    #        """
    #        > SELECT COUNT(*) FROM obj_tbl WHERE a::integer = 10;
    #        5000000
    #        """
    #    ),
    #    expected_size=???,
    # ),
    DatabaseObject(
        name="materialized_view_constant",
        testdrive=dedent(
            """
            > CREATE MATERIALIZED VIEW obj AS SELECT generate_series::text , REPEAT('x', 1024) FROM generate_series(1, 1024)
            """
        ),
        # Dictionary encoding in Persist greatly reduces the size of repeated characters.
        expected_size=1024 * 10,
    ),
    # If a materialized view returns a small number of rows,
    # it should not require storage proportional to its input
    DatabaseObject(
        name="materialized_view_small_output",
        testdrive=dedent(
            """
            > CREATE TABLE t1 (f1 TEXT)
            > INSERT INTO t1 SELECT generate_series::text || REPEAT('x', 1024) FROM generate_series(1, 1024)

            > CREATE MATERIALIZED VIEW obj AS SELECT COUNT(*) FROM t1;
            """
        ),
        expected_size=4 * 1024,
    ),
    # The pg-cdc source is expected to be empty. The data is in the sub-source
    DatabaseObject(
        name="pg_cdc_source",
        testdrive=PG_CDC_SETUP
        + dedent(
            """
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            CREATE TABLE pg_table (f1 TEXT);
            INSERT INTO pg_table SELECT generate_series::text || REPEAT('x', 1024) FROM generate_series(1, 1024);
            ALTER TABLE pg_table REPLICA IDENTITY FULL;

            > CREATE SOURCE obj
              FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
            > CREATE TABLE pg_table FROM SOURCE obj (REFERENCE pg_table);
            """
        ),
        expected_size=4 * 1024,
    ),
    # The pg-cdc data is expected to be in the sub-source,
    # unaffected by the presence of other tables
    DatabaseObject(
        name="pg_cdc_subsource",
        testdrive=PG_CDC_SETUP
        + dedent(
            """
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            CREATE TABLE pg_table1 (f1 TEXT);
            INSERT INTO pg_table1 SELECT generate_series::text || REPEAT('x', 1024) FROM generate_series(1, 1024);
            ALTER TABLE pg_table1 REPLICA IDENTITY FULL;

            CREATE TABLE pg_table2 (f1 TEXT);
            INSERT INTO pg_table2 SELECT generate_series::text || REPEAT('x', 1024) FROM generate_series(1, 1024)
            ALTER TABLE pg_table2 REPLICA IDENTITY FULL;

            CREATE TABLE pg_table3 (f1 TEXT);
            INSERT INTO pg_table3 SELECT generate_series::text || REPEAT('x', 1024) FROM generate_series(1, 1024)
            ALTER TABLE pg_table3 REPLICA IDENTITY FULL;

            > CREATE SOURCE pg_source
              FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
            > CREATE TABLE obj FROM SOURCE pg_source (REFERENCE pg_table1);

            > SELECT COUNT(*) FROM obj;
            1024
            """
        ),
        expected_size=1024 * 1024,
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Create various database objects and confirm that their storage
    as reported in the mz_storage_usage table are as expected.
    """
    parser.add_argument("tests", nargs="*", default=None, help="run specified tests")
    args = parser.parse_args()

    c.up(
        "redpanda",
        "postgres",
        "materialized",
        Service("testdrive", idle=True),
    )

    for database_object in database_objects:
        if (
            args.tests is not None
            and len(args.tests) > 0
            and database_object.name not in args.tests
        ):
            continue

        print(f"Running scenario {database_object.name} ...")

        c.testdrive(
            dedent(
                """
                $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
                DROP SCHEMA IF EXISTS public CASCADE;
                CREATE SCHEMA public;
                GRANT ALL PRIVILEGES ON SCHEMA public TO materialize;
                """
            )
        )

        c.testdrive(database_object.testdrive)

        # Make sure the storage is fully accounted for
        print(
            f"Sleeping for {COLLECTION_INTERVAL_SECS + 1} seconds so that collection kicks in ..."
        )
        time.sleep(COLLECTION_INTERVAL_SECS + 1)

        c.testdrive(
            dedent(
                f"""
                $ set-regex match=\\d+ replacement=<SIZE>

                # Select the raw size as well, so if this errors in testdrive, its easier to debug.
                > SELECT size_bytes, size_bytes BETWEEN {database_object.expected_size//3} AND {database_object.expected_size*3}
                  FROM mz_storage_usage
                  WHERE collection_timestamp = ( SELECT MAX(collection_timestamp) FROM mz_storage_usage )
                  AND object_id = ( SELECT id FROM mz_objects WHERE name = 'obj' );
                <SIZE> true

                > SELECT size_bytes, size_bytes BETWEEN {database_object.expected_size//3} AND {database_object.expected_size*3}
                  FROM mz_recent_storage_usage
                  WHERE object_id = ( SELECT id FROM mz_objects WHERE name = 'obj' );
                <SIZE> true
                """
            )
        )
