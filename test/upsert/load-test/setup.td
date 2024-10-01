# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=topic1

> CREATE CLUSTER c1 REPLICAS (r1 (
        STORAGECTL ADDRESSES ['clusterd1:2100'],
        STORAGE ADDRESSES ['clusterd1:2103'],
        COMPUTECTL ADDRESSES ['clusterd1:2101'],
        COMPUTE ADDRESSES ['clusterd1:2102']
    ))

> CREATE CONNECTION IF NOT EXISTS kafka_conn
    FOR KAFKA BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT;

> CREATE SOURCE s1
    IN CLUSTER c1
    FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic1-${testdrive.seed}');

> CREATE TABLE s1_tbl FROM SOURCE s1 (REFERENCE "testdrive-topic1-${testdrive.seed}")
    KEY FORMAT TEXT VALUE FORMAT TEXT
    ENVELOPE UPSERT;

> CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) FROM s1_tbl;
