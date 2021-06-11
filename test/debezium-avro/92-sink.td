# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test adding a primary key over an existing column
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE t1 (f1 INTEGER);
INSERT INTO t1 VALUES (123), (234);

$ schema-registry-wait-schema schema=postgres.public.t1-value

> CREATE MATERIALIZED SOURCE s1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.t1'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

> SELECT * FROM s1;
123
234

> CREATE SINK sink1
  FROM s1
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'sink1'
  WITH (exactly_once=true, consistency_topic='output-view-consistency-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 VALUES (345), (456);

> SELECT * FROM s1;
123
234
345
456

$ kafka-verify format=avro sink=materialize.public.sink1 sort-messages=true
{"before": null, "after": {"row": {"f1": "345" }}}
{"before": null, "after": {"row": {"f1": "456" }}}

$ schema-registry-wait-schema schema=materialize.public.sink1-value

> CREATE MATERIALIZED SOURCE s2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'materialize.public.sink1'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 VALUES (567), (678);

> SELECT * FROM s2;
123
234
