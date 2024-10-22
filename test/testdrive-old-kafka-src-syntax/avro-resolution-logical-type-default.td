# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

#
# Attempt to resolve schemas with a logical type involving a default
#

$ set writer={"type": "record", "name": "row", "fields": [ ] }
$ set reader={"type": "record", "name": "row", "fields": [ {"name": "f1", "default": 0, "type": { "logicalType": "timestamp-micros", "type": "long" } } ] }

$ kafka-create-topic topic=resolution

$ kafka-ingest format=avro topic=resolution schema=${writer} timestamp=1
{}

$ kafka-ingest format=avro topic=resolution schema=${reader} timestamp=2
{"f1": 123 }

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE resolution
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-resolution-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

> SELECT f1 FROM resolution
"1970-01-01 00:00:00"
"1970-01-01 00:00:00.000123"
