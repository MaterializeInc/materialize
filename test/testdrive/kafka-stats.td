# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the creation and removal of rows in mz_kafka_source_statistics.

# To start, there should be no entries.
> SELECT count(*) FROM mz_kafka_source_statistics
0

$ kafka-create-topic topic=data partitions=1
$ kafka-ingest topic=data format=bytes
one
two

> CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT TEXT

# Wait for the data to be ingested.
> SELECT text FROM data
one
two

> CREATE VIEW partition_statistics AS
  SELECT
    partition_statistics->'app_offset' AS app_offset,
    partition_statistics->'consumer_lag_stored' AS consumer_lag_stored
  FROM
      mz_sources s JOIN mz_kafka_source_statistics stats ON s.id = split_part(stats.source_id, '/', 1),
      jsonb_each(statistics->'topics') _ (topic, topic_statistics),
      jsonb_each(topic_statistics->'partitions') _ (partition, partition_statistics)
  WHERE s.name = 'data' AND partition = '0';

# Ensure that the statistics JSON blob indicates that the two rows have been
# read from Kafka and that there is no consumer lag.
> SELECT * FROM partition_statistics
app_offset  consumer_lag_stored
-------------------------------
2           0

$ kafka-ingest topic=data format=bytes
three

> SELECT text FROM data
one
two
three

# Ensure that the statistics update when a new row is ingested.
> SELECT * FROM partition_statistics
app_offset  consumer_lag_stored
-------------------------------
3           0

# Ensure that the statistics row is removed when the source is dropped.
> DROP SOURCE data
> SELECT count(*) FROM mz_kafka_source_statistics
0
