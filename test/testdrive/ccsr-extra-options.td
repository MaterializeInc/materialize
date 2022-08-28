
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": [
    ]
  }
$ set key-schema={
    "type": "record",
    "name": "row",
    "fields": [
    ]
  }

$ kafka-create-topic topic=input

$ kafka-ingest format=avro key-format=avro topic=input schema=${schema} key-schema=${key-schema} publish=true
{}

> CREATE CONNECTION IF NOT EXISTS kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE input
  FROM KAFKA CONNECTION kafka_conn TOPIC 'testdrive-input-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

! CREATE SINK output FROM input
  INTO KAFKA CONNECTION kafka_conn TOPIC 'output1-view-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  WITH (
    gus = "gus"
  )
contains:unexpected parameters for CONFLUENT SCHEMA REGISTRY: gus

! CREATE SOURCE input_bad
  FROM KAFKA CONNECTION kafka_conn TOPIC 'testdrive-input-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  WITH (
    gus = "gus"
  )
  ENVELOPE UPSERT
contains:unexpected parameters for CONFLUENT SCHEMA REGISTRY: gus
