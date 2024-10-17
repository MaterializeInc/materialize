# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=1

$ set schema={"type" : "record", "name" : "test", "fields": [ { "name": "f2", "type": "long" } ] }

$ kafka-create-topic topic=topic0

$ kafka-create-topic topic=topic1

$ kafka-ingest format=avro topic=topic0 schema=${schema} repeat=1
{"f2": 1}

$ kafka-ingest format=avro topic=topic1 schema=${schema} repeat=1
{"f2": 7}

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CLUSTER source0_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE source0
  IN CLUSTER source0_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic0-${testdrive.seed}')

> CREATE TABLE source0_tbl FROM SOURCE source0 (REFERENCE "testdrive-topic0-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

> CREATE CLUSTER source1_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE source1
  IN CLUSTER source1_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic1-${testdrive.seed}')

> CREATE TABLE source1_tbl FROM SOURCE source1 (REFERENCE "testdrive-topic1-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

> CREATE CLUSTER sink0_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK sink0
  IN CLUSTER sink0_cluster
  FROM source0_tbl
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-output-${testdrive.seed}')
  KEY (f2)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> CREATE CLUSTER sink1_cluster SIZE '${arg.default-storage-size}';
> CREATE SINK sink1
  IN CLUSTER sink1_cluster
  FROM source1_tbl
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-sink-output-${testdrive.seed}')
  KEY (f2)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM



$ kafka-verify-data format=avro sort-messages=true sink=materialize.public.sink1
{"f2": 1} {"before": null, "after": {"row": {"f2": 1}}}
{"f2": 7} {"before": null, "after": {"row": {"f2": 7}}}
