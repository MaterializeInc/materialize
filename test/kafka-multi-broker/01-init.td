# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# This test initializes a source and pushes some data through kafka into it
#

$ set schema={"type": "record", "name": "schema", "fields": [ {"name": "f1", "type": "string" } ] }

$ kafka-create-topic topic=kafka-multi-broker replication-factor=2

$ kafka-ingest format=avro topic=kafka-multi-broker schema=${schema} timestamp=1
{"f1": "01-01" }

$ kafka-ingest format=avro topic=kafka-multi-broker schema=${schema} timestamp=2
{"f1": "01-02" }

$ kafka-ingest format=avro topic=kafka-multi-broker schema=${schema} timestamp=3
{"f1": "01-03" }

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}'

> CREATE SOURCE kafka_multi_broker
  FROM KAFKA CONNECTION kafka_conn TOPIC 'testdrive-kafka-multi-broker-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE NONE

$ kafka-ingest format=avro topic=kafka-multi-broker schema=${schema} timestamp=4
{"f1": "02-01" }

$ kafka-ingest format=avro topic=kafka-multi-broker schema=${schema} timestamp=5
{"f1": "02-02" }

$ kafka-ingest format=avro topic=kafka-multi-broker schema=${schema} timestamp=6
{"f1": "02-03" }

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SINK multi_broker_sink
  FROM kafka_multi_broker
  INTO KAFKA CONNECTION kafka_conn
  TOPIC 'testdrive-kafka-multi-broker-sink-${testdrive.seed}'
  WITH (replication_factor = 2, partition_count = 2)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
