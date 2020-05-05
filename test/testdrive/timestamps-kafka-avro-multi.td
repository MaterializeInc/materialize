# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1minute

$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": [
       {"name": "a", "type": "long"},
       {"name": "b", "type": "long"}
    ]
  }

$ set consistency={
     "name": "materialize.byo.consistency",
     "type": "record",
     "fields": [
         {
           "name": "source",
           "type": "string"
         },
         {
           "name": "partition_count",
           "type": "int"
         },
         {
           "name": "partition_id",
           "type": ["int","string"]
         },
         {
            "name": "timestamp",
            "type": "long"
         },
         {
           "name": "offset",
           "type": "long"
         }
     ]
  }

$ kafka-create-topic topic=data-consistency

$ kafka-ingest format=avro topic=data-consistency timestamp=1  schema=${consistency}
{"source": "dummy", "partition_count": 1, "partition_id": 0, "timestamp": 0, "offset": 0}

$ kafka-create-topic topic=data partitions=2

$ kafka-create-topic topic=data2 partitions=2

$ kafka-ingest partition=0 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 1}

$ kafka-ingest partition=1 format=avro topic=data schema=${schema} timestamp=1
{"a": 3, "b": 1}

> CREATE MATERIALIZED SOURCE data_byo
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
    WITH (consistency = 'testdrive-data-consistency-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}'

> CREATE MATERIALIZED SOURCE data_empty
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data2-${testdrive.seed}'
    WITH (consistency = 'testdrive-data-consistency-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}'

> CREATE MATERIALIZED SOURCE data_rt
    FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
    FORMAT AVRO USING SCHEMA '${schema}'

> CREATE MATERIALIZED VIEW view_byo AS SELECT b, sum(a) FROM data_byo GROUP BY b

> CREATE MATERIALIZED VIEW view_rt AS SELECT b, sum(a) FROM data_rt GROUP BY b

> CREATE MATERIALIZED VIEW view_empty AS SELECT b, sum(a) FROM data_empty GROUP BY b


! SELECT * FROM view_byo;
At least one input has no complete timestamps yet.

! SELECT * FROM view_empty;
At least one input has no complete timestamps yet.

$ kafka-ingest format=avro topic=data-consistency schema=${consistency}
{"source": "testdrive-data2-${testdrive.seed}", "partition_count": 2 , "partition_id": 0, "timestamp": 1, "offset": 0}

! SELECT * FROM view_empty;
At least one input has no complete timestamps yet.

$ kafka-ingest format=avro topic=data-consistency  schema=${consistency}
{"source": "testdrive-data2-${testdrive.seed}", "partition_count": 2 , "partition_id": 1, "timestamp": 1, "offset": 0}


> SELECT * FROM view_empty;
b sum
-----



$ kafka-ingest format=avro topic=data-consistency  schema=${consistency}
{"source": "testdrive-data-${testdrive.seed}", "partition_count": 2 , "partition_id": 0, "timestamp": 1, "offset": 1}

! SELECT * FROM view_byo;
At least one input has no complete timestamps yet.

$ kafka-ingest format=avro topic=data-consistency  schema=${consistency}
{"source": "testdrive-data-${testdrive.seed}", "partition_count": 2 , "partition_id": 1, "timestamp": 1, "offset": 1}


> SELECT * FROM view_byo
b  sum
------
1  4

$ kafka-ingest format=avro topic=data-consistency  schema=${consistency}
{"source": "testdrive-data-${testdrive.seed}", "partition_count": 2 , "partition_id": 0, "timestamp": 2, "offset": 2}

$ kafka-ingest partition=0 format=avro topic=data schema=${schema} timestamp=1
{"a": 2, "b": 1}

$ kafka-ingest partition=1 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 2}

> SELECT * FROM view_byo
b  sum
------
1  4

$ kafka-ingest format=avro topic=data-consistency  schema=${consistency}
{"source": "testdrive-data-${testdrive.seed}", "partition_count": 2 , "partition_id": 1, "timestamp": 2, "offset": 2}

> SELECT * FROM view_byo
b  sum
------
1  6
2  1



$ kafka-ingest format=avro topic=data-consistency  schema=${consistency}
{"source": "testdrive-data-${testdrive.seed}", "partition_count": 2 , "partition_id": 1, "timestamp": 3, "offset": 5}

$ kafka-ingest partition=1 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 3}
{"a": 1, "b": 3}
{"a": 1, "b": 3}

> SELECT * FROM view_byo
b  sum
------
1  6
2  1


$ kafka-ingest format=avro topic=data-consistency  schema=${consistency}
{"source": "testdrive-data-${testdrive.seed}", "partition_count": 2 , "partition_id": 1, "timestamp": 4, "offset": 5}
{"source": "testdrive-data-${testdrive.seed}", "partition_count": 2 , "partition_id": 0, "timestamp": 4, "offset": 2}

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

$ kafka-ingest format=avro topic=data-consistency  schema=${consistency}
{"source": "testdrive-data-${testdrive.seed}", "partition_count": 3 , "partition_id": 1, "timestamp": 5, "offset": 6}
{"source": "testdrive-data-${testdrive.seed}", "partition_count": 3 , "partition_id": 0, "timestamp": 5, "offset": 3}

$ kafka-add-partitions topic=data total-partitions=3

$ kafka-ingest partition=0 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 5}

> SELECT * FROM view_byo
b  sum
------
1  6
2  1
3  3

$ kafka-ingest partition=1 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 6}

> SELECT * FROM view_byo
b  sum
------
1  6
2  1
3  3


$ kafka-ingest partition=2 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 7}

> SELECT * FROM view_byo
b  sum
------
1  6
2  1
3  3

$ kafka-ingest format=avro topic=data-consistency  schema=${consistency}
{"source": "testdrive-data-${testdrive.seed}", "partition_count": 3 , "partition_id": 2, "timestamp": 5, "offset": 1}

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
{"a": 1, "b": 8}

$ kafka-ingest partition=0 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 9}

$ kafka-ingest partition=1 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 10}

$ kafka-ingest partition=3 format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 11}

$ kafka-ingest format=avro topic=data-consistency  schema=${consistency}
{"source": "testdrive-data-${testdrive.seed}", "partition_count": 4 , "partition_id": 3, "timestamp": 7, "offset": 1}

> SELECT * FROM view_byo
b  sum
------
1  6
2  1
3  3
5  1
6  1
7  1

$ kafka-ingest format=avro topic=data-consistency  schema=${consistency}
{"source": "testdrive-data-${testdrive.seed}", "partition_count": 4 , "partition_id": 1, "timestamp": 7, "offset": 7}
{"source": "testdrive-data-${testdrive.seed}", "partition_count": 4 , "partition_id": 0, "timestamp": 7, "offset": 4}
{"source": "testdrive-data-${testdrive.seed}", "partition_count": 4, "partition_id": 2, "timestamp": 7, "offset": 2}

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
