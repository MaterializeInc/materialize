# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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
              {"name": "a", "type": "long"},
              {"name": "b", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-create-topic topic=data-consistency

$ kafka-ingest format=raw topic=data-consistency timestamp=1
dummy,1,0,0,0

$ kafka-create-topic topic=data partitions=2

$ kafka-ingest partition=0 format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 1, "b": 1}}

$ kafka-ingest partition=1 format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 3, "b": 1}}

> CREATE MATERIALIZED SOURCE data_byo
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
    WITH (consistency = 'testdrive-data-consistency-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

> CREATE MATERIALIZED SOURCE data_rt
    FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
    FORMAT AVRO USING SCHEMA '${schema}'
    ENVELOPE DEBEZIUM


> CREATE MATERIALIZED VIEW view_byo AS SELECT b, sum(a) FROM data_byo GROUP BY b

> CREATE MATERIALIZED VIEW view_rt AS SELECT b, sum(a) FROM data_rt GROUP BY b

! SELECT * FROM view_byo;
At least one input has no complete timestamps yet.

$ kafka-ingest format=raw topic=data-consistency timestamp=1
testdrive-data-${testdrive.seed},2,0,1,1

! SELECT * FROM view_byo;
At least one input has no complete timestamps yet.

$ kafka-ingest format=raw topic=data-consistency timestamp=1
testdrive-data-${testdrive.seed},2,1,1,1

> SELECT * FROM view_byo
b  sum
------
1  4

$ kafka-ingest format=raw topic=data-consistency timestamp=1
testdrive-data-${testdrive.seed},2,0,2,2

$ kafka-ingest partition=0 format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 2, "b": 1}}

$ kafka-ingest partition=1 format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 1, "b": 2}}

> SELECT * FROM view_byo
b  sum
------
1  4

$ kafka-ingest format=raw topic=data-consistency timestamp=1
testdrive-data-${testdrive.seed},2,1,2,2

> SELECT * FROM view_byo
b  sum
------
1  6
2  1

$ kafka-ingest format=raw topic=data-consistency timestamp=1
testdrive-data-${testdrive.seed},2,1,3,5

$ kafka-ingest partition=1 format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 1, "b": 3}}
{"before": null, "after": {"a": 1, "b": 3}}
{"before": null, "after": {"a": 1, "b": 3}}

> SELECT * FROM view_byo
b  sum
------
1  6
2  1


$ kafka-ingest format=raw topic=data-consistency timestamp=1
testdrive-data-${testdrive.seed},2,1,4,5
testdrive-data-${testdrive.seed},2,0,4,2

> SELECT * FROM view_byo
b  sum
------
1  6
2  1
3  3

> SELECT * FROM view_rt
b  sum
------
1  6
2  1
3  3

$ kafka-ingest format=raw topic=data-consistency timestamp=1
testdrive-data-${testdrive.seed},3,1,5,6
testdrive-data-${testdrive.seed},3,0,5,3

$ kafka-add-partitions topic=data total-partitions=3

$ kafka-ingest partition=0 format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 1, "b": 5}}

> SELECT * FROM view_byo
b  sum
------
1  6
2  1
3  3

$ kafka-ingest partition=1 format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 1, "b": 6}}

> SELECT * FROM view_byo
b  sum
------
1  6
2  1
3  3


$ kafka-ingest partition=2 format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 1, "b": 7}}

> SELECT * FROM view_byo
b  sum
------
1  6
2  1
3  3

$ kafka-ingest format=raw topic=data-consistency timestamp=1
testdrive-data-${testdrive.seed},3,2,5,1

> SELECT * FROM view_byo
b  sum
------
1  6
2  1
3  3
5  1
6  1
7  1

$ kafka-add-partitions topic=data total-partitions=4

$ kafka-ingest partition=2 format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 1, "b": 8}}

$ kafka-ingest partition=0 format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 1, "b": 9}}

$ kafka-ingest partition=1 format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 1, "b": 10}}

$ kafka-ingest partition=3 format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 1, "b": 11}}

$ kafka-ingest format=raw topic=data-consistency timestamp=1
testdrive-data-${testdrive.seed},3,1,6,7
testdrive-data-${testdrive.seed},3,0,6,4
testdrive-data-${testdrive.seed},3,2,6,2

> SELECT * FROM view_byo
b  sum
------
1  6
2  1
3  3
5  1
6  1
7  1

$ kafka-ingest format=raw topic=data-consistency timestamp=1
testdrive-data-${testdrive.seed},4,3,7,1

> SELECT * FROM view_byo
b  sum
------
1  6
2  1
3  3
5  1
6  1
7  1
8  1
9  1
10 1
11 1
