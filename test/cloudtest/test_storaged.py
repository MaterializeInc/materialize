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
from materialize.cloudtest.exists import exists, not_exists
from materialize.cloudtest.wait import wait


def test_storaged_creation(mz: MaterializeApplication) -> None:
    """Test that creating multiple sources causes multiple storageds to be spawned."""
    mz.testdrive.run(
        input=dedent(
            """
            $ kafka-create-topic topic=test

            $ kafka-ingest format=bytes topic=test
            ABC

            > CREATE CONNECTION IF NOT EXISTS kafka FOR KAFKA BROKER '${testdrive.kafka-addr}'

            > CREATE SOURCE source1
              FROM KAFKA CONNECTION kafka
              (TOPIC 'testdrive-test-${testdrive.seed}')
              FORMAT BYTES
              ENVELOPE NONE;

            > CREATE SOURCE source2
              FROM KAFKA CONNECTION kafka
              (TOPIC 'testdrive-test-${testdrive.seed}')
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

        mz.environmentd.sql(f"DROP SOURCE {source}")
        wait(condition="delete", resource=storaged)


def test_storaged_resizing(mz: MaterializeApplication) -> None:
    """Test that resizing a given source causes the storaged to be replaced."""
    mz.testdrive.run(
        input=dedent(
            """
            $ kafka-create-topic topic=test

            $ kafka-ingest format=bytes topic=test
            ABC

            > CREATE CONNECTION IF NOT EXISTS kafka FOR KAFKA BROKER '${testdrive.kafka-addr}'

            > CREATE SOURCE resize_storaged
              FROM KAFKA CONNECTION kafka
              (TOPIC 'testdrive-test-${testdrive.seed}')
              FORMAT BYTES
              ENVELOPE NONE;
            """
        )
    )
    id = mz.environmentd.sql_query(
        f"SELECT id FROM mz_sources WHERE name = 'resize_storaged'"
    )[0][0]
    assert id is not None
    storaged = f"pod/storage-{id}-0"

    wait(condition="condition=Ready", resource=storaged)

    mz.testdrive.run(
        input=dedent(
            """
            > ALTER SOURCE resize_storaged
              SET (SIZE '16');
            """
        ),
        no_reset=True,
    )

    wait(condition="condition=Ready", resource=storaged)

    # NB: We'd like to do the following, but jsonpath gives us no way to express it!
    # TODO: revisit or handroll a retry loop
    # wait(condition=f"jsonpath=metadata.labels.storage.environmentd.materialize.cloud/size=16", resource=storaged)

    mz.environmentd.sql("DROP SOURCE resize_storaged")
    wait(condition="delete", resource=storaged)


def test_storaged_shutdown(mz: MaterializeApplication) -> None:
    """Test that dropping a source causes its respective storaged to shut down."""
    mz.testdrive.run(
        input=dedent(
            """
            $ kafka-create-topic topic=test

            $ kafka-ingest format=bytes topic=test
            ABC

            > CREATE CONNECTION IF NOT EXISTS kafka FOR KAFKA BROKER '${testdrive.kafka-addr}'

            > CREATE SOURCE source1
              FROM KAFKA CONNECTION kafka
              (TOPIC 'testdrive-test-${testdrive.seed}')
              FORMAT BYTES
              ENVELOPE NONE;

            # Those two objects do not currently create storaged instances
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

    storaged_pod = f"pod/storage-{id}-0"
    storaged_svc = f"service/storage-{id}"

    wait(condition="condition=Ready", resource=storaged_pod)
    exists(storaged_svc)

    mz.environmentd.sql("DROP SOURCE source1")

    wait(condition="delete", resource=storaged_pod)
    not_exists(storaged_svc)


def test_sink_resizing(mz: MaterializeApplication) -> None:
    """Test that resizing a given sink causes the storaged to be replaced."""

    def get_num_workers(mz: MaterializeApplication) -> str:
        return mz.kubectl(
            "get",
            "pods",
            "--selector=environmentd.materialize.cloud/namespace=storage",
            "-o",
            r"jsonpath='{.items[*].metadata.labels.storage\.environmentd\.materialize\.cloud/size}'",
        )

    mz.testdrive.run(
        input=dedent(
            """
            > CREATE TABLE t1 (f1 int NOT NULL)

            > CREATE CONNECTION IF NOT EXISTS kafka FOR KAFKA BROKER '${testdrive.kafka-addr}'

            > CREATE CONNECTION IF NOT EXISTS csr_conn
              FOR CONFLUENT SCHEMA REGISTRY
              URL '${testdrive.schema-registry-url}'

            > INSERT INTO t1 VALUES (1)

            > CREATE SINK resize_sink FROM t1
              INTO KAFKA CONNECTION kafka (TOPIC 'testdrive-sink-resize-${testdrive.seed}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE DEBEZIUM
              WITH (SIZE = '2')

            $ kafka-verify-data format=avro sink=materialize.public.resize_sink sort-messages=true
            {"before": null, "after": {"row":{"f1": 1}}}
            """
        )
    )
    id = mz.environmentd.sql_query(
        f"SELECT id FROM mz_sinks WHERE name = 'resize_sink'"
    )[0][0]
    assert id is not None
    storaged = f"pod/storage-{id}-0"

    assert get_num_workers(mz) == "'2'"

    wait(condition="condition=Ready", resource=storaged)

    mz.testdrive.run(
        input=dedent(
            """
            > ALTER SINK resize_sink
              SET (SIZE '16');
            """
        ),
        no_reset=True,
    )

    wait(
        condition="condition=Ready",
        resource="pod",
        label="storage.environmentd.materialize.cloud/size=16",
    )

    assert get_num_workers(mz) == "'16'"

    mz.environmentd.sql("DROP SINK resize_sink")
    wait(condition="delete", resource=storaged)
