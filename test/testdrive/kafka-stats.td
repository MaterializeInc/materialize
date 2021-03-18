# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the creation and removal of entries in mz_kafka_consumer_statistics

$ set schema={
    "type": "record",
    "name": "row",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "long"}
    ]
  }

$ kafka-create-topic topic=data

> CREATE SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (statistics_interval_ms = 1000)
  FORMAT AVRO USING SCHEMA '${schema}'

> CREATE MATERIALIZED VIEW test1 AS
  SELECT b, sum(a) FROM data GROUP BY b

> SELECT * FROM test1
b  sum
------

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 1}
{"a": 2, "b": 1}
{"a": 3, "b": 1}
{"a": 1, "b": 2}

> SELECT * FROM test1
b  sum
------
1  6
2  1

# It would be lovely to try and assert on the actual value for metrics here. Unfortunately,
# librdkafka is chatty and the metrics continue to change even when no data is being written. As
# such, we'll need to settle for a query that simply asserts the presence of a single source in
# the table:
> SELECT count(*) FROM mz_kafka_consumer_statistics
1

# and that each of the metrics should be non-zero:
> SELECT count(*) FROM mz_kafka_consumer_statistics where rx = 0
0

> SELECT count(*) FROM mz_kafka_consumer_statistics where rx_bytes = 0
0

> SELECT count(*) FROM mz_kafka_consumer_statistics where tx = 0
0

> SELECT count(*) FROM mz_kafka_consumer_statistics where tx_bytes = 0
0

# Drop the sources and verify that metrics have been removed
> DROP VIEW test1

> DROP SOURCE data

> SELECT count(*) FROM mz_kafka_consumer_statistics
0
