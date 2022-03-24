# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test per partition kafka metrics on initial and dynamically added partitions
#

$ kafka-create-topic topic=input_csv partitions=2

> CREATE MATERIALIZED SOURCE input_csv (first, second)
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-input_csv-${testdrive.seed}' WITH (topic_metadata_refresh_interval_ms=5000)
  FORMAT CSV WITH 2 COLUMNS;

$ kafka-ingest format=bytes topic=input_csv partition=0
1,2
$ kafka-ingest format=bytes topic=input_csv partition=1
2,3

> SELECT * from input_csv;
first second mz_offset
----------------------
1     2      1
2     3      1

> SELECT labels->>'partition_id' AS partition_id, max(value) as value FROM mz_metrics WHERE metric='mz_kafka_partition_offset_max' group by partition_id;
partition_id value
------------------
0              1
1              1

$ kafka-add-partitions topic=input_csv total-partitions=4

$ kafka-ingest format=bytes topic=input_csv partition=2
3,4
4,5
$ kafka-ingest format=bytes topic=input_csv partition=3
5,6

> SELECT * from input_csv;
first second mz_offset
----------------------
1     2      1
2     3      1
3     4      1
4     5      2
5     6      1

> SELECT labels->>'partition_id' AS partition_id, max(value) as value FROM mz_metrics WHERE metric='mz_kafka_partition_offset_max' group by partition_id;
partition_id value
------------------
0             1
1             1
2             2
3             1
