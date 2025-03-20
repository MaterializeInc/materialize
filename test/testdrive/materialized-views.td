# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Additional test for materialized views, on top of those in
# test/sqllogictest/materialized_views.slt



# Kafka source as a source for a materialized view

$ set materialized-views={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"}
        ]
    }

$ kafka-create-topic topic=materialized-views

$ kafka-ingest format=avro topic=materialized-views schema=${materialized-views}
{"f1": "123"}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE SOURCE s1
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-materialized-views-${testdrive.seed}')

> CREATE TABLE s1_tbl FROM SOURCE s1 (REFERENCE "testdrive-materialized-views-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

$ kafka-ingest format=avro topic=materialized-views schema=${materialized-views}
{"f1": "234"}

> SELECT COUNT(*) FROM s1_tbl;
2

> CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(f1::integer) AS c1 FROM s1_tbl;

$ kafka-ingest format=avro topic=materialized-views schema=${materialized-views}
{"f1": "345"}

> SELECT * FROM v1;
3

> CREATE SINK sink1
  IN CLUSTER ${arg.single-replica-cluster}
  FROM v1
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-materialized-views-sink-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

$ kafka-verify-topic sink=materialize.public.sink1 await-value-schema=true

> CREATE SOURCE sink1_check
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (
    TOPIC 'testdrive-materialized-views-sink-${testdrive.seed}'
  )

> CREATE TABLE sink1_check_tbl FROM SOURCE sink1_check (REFERENCE "testdrive-materialized-views-sink-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

$ kafka-ingest format=avro topic=materialized-views schema=${materialized-views}
{"f1": "456"}



> SELECT * FROM v1;
4

> SELECT MAX((after).c1) FROM sink1_check_tbl;
4

# Inject failure in the source

$ kafka-ingest format=avro topic=materialized-views schema=${materialized-views}
{"f1": "ABC"}

! SELECT * FROM v1;
contains: invalid input syntax for type integer
