# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# A very simple smoke test that will be run against various Kafka versions
#

$ kafka-create-topic topic=input_csv

> CREATE MATERIALIZED SOURCE input_csv (first, second)
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input_csv-${testdrive.seed}'
  FORMAT CSV WITH 2 COLUMNS;

$ kafka-ingest format=bytes topic=input_csv
1,2
2,3

> SELECT * from input_csv;
first second mz_offset
----------------------
1     2      1
2     3      2

$ kafka-create-topic topic=input_proto

> CREATE MATERIALIZED SOURCE input_proto
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input_proto-${testdrive.seed}'
  FORMAT PROTOBUF MESSAGE '.Batch' USING SCHEMA '${testdrive.protobuf-descriptors}'

$ kafka-ingest format=protobuf topic=input_proto message=batch timestamp=1
{"id": "1", "interval_start": "2020-01-01_00:00:00", "interval_end": "2020-01-01_00:00:09", "records": []}
{"id": "2", "interval_start": "2020-01-01_00:00:10", "interval_end": "2020-01-01_00:00:19", "records": []}

> SELECT id, interval_start, interval_end, mz_offset from input_proto
id interval_start        interval_end      mz_offset
------------------------------------------------------------
1  2020-01-01_00:00:00 2020-01-01_00:00:09      1
2  2020-01-01_00:00:10 2020-01-01_00:00:19      2

> SELECT (rs) FROM input_proto, unnest(records) as rs
rs
---------


$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {"name": "a", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-create-topic topic=input_avro

$ kafka-ingest format=avro topic=input_avro schema=${schema} publish=true timestamp=1
{"before": null, "after": {"row": {"a": 123}}}

> CREATE MATERIALIZED SOURCE input_avro
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input_avro-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM

> SELECT * FROM input_avro
a
---
123
