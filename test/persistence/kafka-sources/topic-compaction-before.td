# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "f1", "type": "long"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f2", "type":"string"}
        ]
    }

$ kafka-create-topic topic=topic-compaction compaction=true

$ kafka-ingest format=avro topic=topic-compaction key-format=avro key-schema=${keyschema} schema=${schema} repeat=1000
{"f1": ${kafka-ingest.iteration}} {"f2": "A${kafka-ingest.iteration}"}
{"f1": ${kafka-ingest.iteration}} {"f2": "B${kafka-ingest.iteration}"}
{"f1": ${kafka-ingest.iteration}} {"f2": "C${kafka-ingest.iteration}"}

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE topic_compaction
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-topic-compaction-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

> SELECT COUNT(*) > 0 FROM topic_compaction;
true
