# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.util.wait import wait


def test_wait(mz: MaterializeApplication) -> None:
    wait(
        condition="condition=Ready",
        resource="pod",
        label="cluster.environmentd.materialize.cloud/cluster-id=u1",
    )


def test_sql(mz: MaterializeApplication) -> None:
    mz.environmentd.sql("SELECT 1")

    one = mz.environmentd.sql_query("SELECT 1")[0][0]
    assert int(one) == 1


def test_testdrive(mz: MaterializeApplication) -> None:
    mz.testdrive.copy("test/testdrive", "/workdir")
    mz.testdrive.run("testdrive/testdrive.td")

    mz.testdrive.run(
        input=dedent(
            """
                $ kafka-create-topic topic=test

                $ kafka-ingest format=bytes topic=test
                ABC

                > CREATE TABLE t1 (f1 INTEGER);
                > CREATE DEFAULT INDEX ON t1;
                > INSERT INTO t1 VALUES (1);

                > CREATE CLUSTER c1 REPLICAS (r1 (SIZE 'scale=1,workers=1'), r2 (SIZE 'scale=2,workers=2'));
                > CREATE CLUSTER c2 SIZE 'scale=1,workers=1', REPLICATION FACTOR 2;
                > SET cluster=c1

                > CREATE CONNECTION kafka TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

                > CREATE SOURCE s1
                  IN CLUSTER c2
                  FROM KAFKA CONNECTION kafka
                  (TOPIC 'testdrive-test-${testdrive.seed}');

                > CREATE TABLE s1_tbl FROM SOURCE s1 (REFERENCE "testdrive-test-${testdrive.seed}")
                  FORMAT BYTES
                  ENVELOPE NONE;

                > CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) FROM t1;
                > SELECT * FROM v1;
                1

                > CREATE MATERIALIZED VIEW v2 AS SELECT COUNT(*) FROM s1_tbl;
                > SELECT * FROM v2;
                1

                > DROP CLUSTER c1 CASCADE;
                """
        )
    )
