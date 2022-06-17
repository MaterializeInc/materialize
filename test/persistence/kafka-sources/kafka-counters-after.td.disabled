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

$ kafka-ingest format=avro topic=kafka-counters key-format=avro key-schema=${keyschema} schema=${schema} publish=true
{"f1": "-1"} {"f2": "-1"}


> SELECT COUNT(*) FROM kafka_counters;
10001

# We expect the counter to be at 1, that is no data was re-ingested
# from the Kafka topic post-restart.
> SELECT SUM(CAST(statistics->'topics'->'testdrive-kafka-counters-${testdrive.seed}'->'partitions'->'0'->'msgs' AS INT)) FROM mz_kafka_source_statistics;
1
