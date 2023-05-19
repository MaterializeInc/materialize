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


def test_disk_source(mz: MaterializeApplication) -> None:
    """Test that creating multiple sources causes multiple storage clusters to be spawned."""
    mz.testdrive.run(
        input=dedent(
            """
            $ kafka-create-topic topic=test

            $ kafka-ingest key-format=text format=text topic=test
            key1:val1
            key2:val2

            > CREATE CONNECTION IF NOT EXISTS kafka TO KAFKA (BROKER '${testdrive.kafka-addr}')

            > CREATE SOURCE source1
              FROM KAFKA CONNECTION kafka
              (TOPIC 'testdrive-test-${testdrive.seed}')
              KEY FORMAT TEXT
              VALUE FORMAT TEXT
              ENVELOPE UPSERT WITH (DISK);


            > SELECT * FROM source1;
            key           text
            ------------------
            key1          val1
            key2          val2

            $ kafka-ingest key-format=text format=text topic=test
            key1:val3

            > SELECT * FROM source1;
            key           text
            ------------------
            key1          val3
            key2          val2
            """
        )
    )
