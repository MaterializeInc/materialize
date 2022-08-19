# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Change the number of partitions in the topic just prior to restart
#

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
            {"name":"f2", "type":"long"}
        ]
    }

$ kafka-create-topic topic=partition-change partitions=5

$ kafka-ingest format=avro topic=partition-change key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=10000
{"f1": ${kafka-ingest.iteration}} {"f2": ${kafka-ingest.iteration}}

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE SOURCE partition_change
  FROM KAFKA CONNECTION kafka_conn
    WITH (TOPIC METADATA REFRESH INTERVAL MS = 100)
  TOPIC 'testdrive-partition-change-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT

> SELECT COUNT(*) > 0 FROM partition_change;
true

$ kafka-add-partitions topic=partition-change total-partitions=10

$ kafka-ingest format=avro topic=partition-change key-format=avro key-schema=${keyschema} schema=${schema} publish=true repeat=1000
{"f1": ${kafka-ingest.iteration}} {"f2": ${kafka-ingest.iteration}}
