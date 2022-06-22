# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"long"}
        ]
    }

$ kafka-create-topic topic=envelope-none partitions=2

# Make sure that no upsert semantics kicks in -- all the 15K records we insert must be processed independently

$ kafka-ingest format=avro topic=envelope-none schema=${schema} publish=true repeat=5000
{"f1": ${kafka-ingest.iteration}}

$ kafka-ingest format=avro topic=envelope-none key-format=avro key-schema=${schema} schema=${schema} publish=true repeat=5000
{"f1": ${kafka-ingest.iteration}} {"f1": ${kafka-ingest.iteration}}

> CREATE MATERIALIZED SOURCE envelope_none
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-envelope-none-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  INCLUDE PARTITION AS kafka_partition, OFFSET AS mz_offset
  ENVELOPE NONE

> SELECT COUNT(*) FROM envelope_none
10000

$ kafka-add-partitions topic=envelope-none total-partitions=4
