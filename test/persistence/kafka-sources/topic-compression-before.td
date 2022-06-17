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

$ kafka-create-topic topic=topic-compression compression=gzip

$ kafka-ingest format=avro topic=topic-compression key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=1000
{"f1": ${kafka-ingest.iteration}} {"f2": "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghij"}

> CREATE MATERIALIZED SOURCE topic_compression
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-topic-compression-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT;

> SELECT COUNT(*) FROM topic_compression;
1000
