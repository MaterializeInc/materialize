# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.cloudtest.application import MaterializeApplication
from materialize.cloudtest.wait import wait


def test_storaged_creation(mz: MaterializeApplication) -> None:
    """Test that creating multiple sources causes multiple storageds to be spawned."""
    mz.testdrive.run_string(
        dedent(
            """
            $ kafka-create-topic topic=test

            $ kafka-ingest format=bytes topic=test
            ABC

            > CREATE SOURCE source1
              FROM KAFKA BROKER '${testdrive.kafka-addr}'
              TOPIC 'testdrive-test-${testdrive.seed}'
              FORMAT BYTES
              ENVELOPE NONE;

            > CREATE SOURCE source2
              FROM KAFKA BROKER '${testdrive.kafka-addr}'
              TOPIC 'testdrive-test-${testdrive.seed}'
              FORMAT BYTES
              ENVELOPE NONE;
            """
        )
    )

    for source in ["source1", "source2"]:
        id = mz.environmentd.sql_query(
            f"SELECT id FROM mz_sources WHERE name = '{source}'"
        )[0][0]
        assert id is not None

        storaged = f"pod/storage-{id}-0"
        wait(condition="condition=Ready", resource=storaged)


def test_storaged_shutdown(mz: MaterializeApplication) -> None:
    """Test that dropping a source causes its respective storaged to shut down."""
    mz.testdrive.run_string(
        dedent(
            """
            $ kafka-create-topic topic=test

            $ kafka-ingest format=bytes topic=test
            ABC

            > CREATE SOURCE source1
              FROM KAFKA BROKER '${testdrive.kafka-addr}'
              TOPIC 'testdrive-test-${testdrive.seed}'
              FORMAT BYTES
              ENVELOPE NONE;

            # Those two objects do not currenly create storaged instances
            # > CREATE MATERIALIZED VIEW view1 AS SELECT COUNT(*) FROM source1;

            # > CREATE SINK sink1
            #  FROM view1
            #  INTO KAFKA BROKER '${testdrive.kafka-addr}'
            #  TOPIC 'testdrive-sink1-${testdrive.seed}'
            #  FORMAT JSON;
            """
        )
    )

    id = mz.environmentd.sql_query("SELECT id FROM mz_sources WHERE name = 'source1'")[
        0
    ][0]
    assert id is not None

    storaged = f"pod/storage-{id}-0"

    wait(condition="condition=Ready", resource=storaged)

    mz.environmentd.sql("DROP SOURCE source1")

    wait(condition="delete", resource=storaged)
