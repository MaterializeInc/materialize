# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (
    BROKER '${testdrive.kafka-addr}',
    SECURITY PROTOCOL PLAINTEXT
  )

> CREATE TABLE IF NOT EXISTS kafka_sink_from (a int)

> CREATE SINK compression_implicit FROM kafka_sink_from
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'kafka-sink')
  FORMAT JSON
  ENVELOPE DEBEZIUM

> CREATE TABLE expected_compression_implicit_create_sql (sql text)
>[version<11200] INSERT INTO expected_compression_implicit_create_sql
  VALUES ('CREATE SINK "materialize"."public"."compression_implicit" IN CLUSTER "quickstart" FROM "materialize"."public"."kafka_sink_from" INTO KAFKA CONNECTION "materialize"."public"."kafka_conn" (TOPIC = ''kafka-sink'', COMPRESSION TYPE = ''none'') FORMAT JSON ENVELOPE DEBEZIUM WITH (PARTITION STRATEGY = ''v0'')')
>[version>=11200] INSERT INTO expected_compression_implicit_create_sql
  VALUES ('CREATE SINK "materialize"."public"."compression_implicit" IN CLUSTER "quickstart" FROM "materialize"."public"."kafka_sink_from" INTO KAFKA CONNECTION "materialize"."public"."kafka_conn" (TOPIC = ''kafka-sink'') FORMAT JSON ENVELOPE DEBEZIUM WITH (PARTITION STRATEGY = ''v0'')')

> CREATE SINK compression_none_explicit FROM kafka_sink_from
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'kafka-sink', COMPRESSION TYPE = 'none')
  FORMAT JSON
  ENVELOPE DEBEZIUM

> CREATE SINK compression_lz4_explicit FROM kafka_sink_from
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'kafka-sink', COMPRESSION TYPE = 'lz4')
  FORMAT JSON
  ENVELOPE DEBEZIUM

> CREATE SINK compression_gzip_explicit FROM kafka_sink_from
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'kafka-sink', COMPRESSION TYPE = 'gzip')
  FORMAT JSON
  ENVELOPE DEBEZIUM
