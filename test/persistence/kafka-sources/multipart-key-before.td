# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Ingest a topic with a multi-part key
#

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "f1", "type": "string"},
        {"name": "f2", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f3", "type":"string"},
            {"name":"f4", "type":"string"}
        ]
    }

$ kafka-create-topic topic=multipart-key

# Ingest data where the first or the second part of the key has high cardinality
$ kafka-ingest format=avro topic=multipart-key key-format=avro key-schema=${keyschema} schema=${schema} repeat=10000
{"f1": "KEY1", "f2": "${kafka-ingest.iteration}"} {"f3": "KEY1", "f4": "${kafka-ingest.iteration}"}
{"f1": "${kafka-ingest.iteration}", "f2": "KEY2"} {"f3": "${kafka-ingest.iteration}", "f4": "KEY2"}

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE multipart_key
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-multipart-key-${testdrive.seed}');

> CREATE TABLE multipart_key_tbl FROM SOURCE multipart_key (REFERENCE "testdrive-multipart-key-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

> SELECT COUNT(*) FROM multipart_key_tbl;
20000
