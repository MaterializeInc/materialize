# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=60s

$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": [
       {"name": "a", "type": "long"},
       {"name": "b", "type": "long"}
    ]
  }

$ kafka-create-topic topic=data partitions=2

$ kafka-create-topic topic=data2 partitions=2

> CREATE SOURCE data_empty
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data2-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'

> CREATE SOURCE data_rt
    FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
    WITH (topic_metadata_refresh_interval_ms=50)
    FORMAT AVRO USING SCHEMA '${schema}'

> CREATE MATERIALIZED VIEW view_rt AS SELECT b, sum(a) FROM data_rt GROUP BY b

> CREATE MATERIALIZED VIEW view_empty AS SELECT b, sum(a) FROM data_empty GROUP BY b

> SELECT * FROM view_empty;
b sum
-----

$ kafka-ingest partition=0 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 1}

$ kafka-ingest partition=1 format=avro topic=data schema=${schema} timestamp=1
{"a": 3, "b": 1}

$ kafka-ingest partition=0 format=avro topic=data schema=${schema} timestamp=1
{"a": 2, "b": 1}

$ kafka-ingest partition=1 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 2}

$ kafka-ingest partition=1 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 3}
{"a": 1, "b": 3}
{"a": 1, "b": 3}

> SELECT * FROM view_rt
b  sum
------
1  6
2  1
3  3

$ kafka-add-partitions topic=data total-partitions=3

$ kafka-ingest partition=0 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 5}

$ kafka-ingest partition=1 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 6}

$ kafka-ingest partition=2 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 7}

$ kafka-add-partitions topic=data total-partitions=4

$ kafka-ingest partition=2 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 8}

$ kafka-ingest partition=0 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 9}

$ kafka-ingest partition=1 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 10}

$ kafka-ingest partition=3 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 11}

> SELECT * FROM view_rt
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
