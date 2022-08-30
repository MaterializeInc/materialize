# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test sinks that derive from table sources
#

> CREATE TABLE sink_table (f1 INTEGER);

> INSERT INTO sink_table VALUES (1);

> CREATE CONNECTION kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SINK sink_sink FROM sink_table
  INTO KAFKA CONNECTION kafka_conn
  TOPIC 'testdrive-exactly-once-sink-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn;

> INSERT INTO sink_table VALUES (2);
