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

$ set count=1000

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

$ kafka-create-topic topic=offset

$ kafka-ingest format=avro topic=offset key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=${count} timestamp=1
{"f1": "a${kafka-ingest.iteration}"} {"f2": "a${kafka-ingest.iteration}"}

$ kafka-ingest format=avro topic=offset key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=${count} timestamp=2
{"f1": "b${kafka-ingest.iteration}"} {"f2": "b${kafka-ingest.iteration}"}

$ kafka-ingest format=avro topic=offset key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=${count} timestamp=3
{"f1": "c${kafka-ingest.iteration}"} {"f2": "c${kafka-ingest.iteration}"}

> CREATE SOURCE start_offset
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-offset-${testdrive.seed}'
  WITH (start_offset = 100)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT

> CREATE SOURCE kafka_time_offset
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-offset-${testdrive.seed}'
  WITH (kafka_time_offset=2)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT

> SELECT COUNT(*) FROM start_offset;
2900

> SELECT COUNT(*) FROM kafka_time_offset;
2000
