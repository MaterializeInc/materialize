# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Insert 10K values prior to restart and check that we never re-ingest
# the entire topic as observed by the librdkafka counters
#

$ set count=10000

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "f1", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f2", "type":"string"}
        ]
    }

$ kafka-create-topic topic=kafka-counters

$ kafka-ingest format=avro topic=kafka-counters key-format=avro key-schema=${keyschema} schema=${schema} repeat=${count}
{"f1": "a${kafka-ingest.iteration}"} {"f2": "a${kafka-ingest.iteration}"}

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE kafka_counters
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-counters-${testdrive.seed}')

> CREATE TABLE kafka_counters_tbl FROM SOURCE kafka_counters (REFERENCE "testdrive-kafka-counters-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT
> COMMIT

> SELECT COUNT(*) FROM kafka_counters_tbl;
"${count}"
