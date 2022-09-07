# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema={"type" : "record", "name" : "test", "fields": [ { "name": "f2", "type": "long" } ] }

$ kafka-create-topic topic=topic0

$ kafka-create-topic topic=topic1

$ kafka-ingest format=avro topic=topic0 schema=${schema} repeat=1
{"f2": 1}

$ kafka-ingest format=avro topic=topic1 schema=${schema} repeat=1
{"f2": 7}

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE source0
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-topic0-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

> CREATE SOURCE source1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-topic1-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

> CREATE SINK sink0 FROM source0
  INTO KAFKA CONNECTION kafka_conn TOPIC 'testdrive-sink-output-${testdrive.seed}'
  KEY (f2)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

> CREATE SINK sink1 FROM source1
  INTO KAFKA CONNECTION kafka_conn TOPIC 'testdrive-sink-output-${testdrive.seed}'
  KEY (f2)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn

$ set-regex match=\d{13} replacement=<TIMESTAMP>

$ kafka-verify format=avro sort-messages=true sink=materialize.public.sink1
{"f2": 1} {"before": null, "after": {"row": {"f2": 1}}, "transaction": {"id": "<TIMESTAMP>"}}
{"f2": 7} {"before": null, "after": {"row": {"f2": 7}}, "transaction": {"id": "<TIMESTAMP>"}}
