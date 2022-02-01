# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for `kafka_time_offset` configuration which resolves a `start_offset`
# during creation of the source.

#
# Errors
#

$ kafka-create-topic topic=t0

! CREATE MATERIALIZED SOURCE missing_topic
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'missing_topic'
  WITH (kafka_time_offset=1)
  FORMAT TEXT
contains:topic missing_topic does not exist

! CREATE MATERIALIZED SOURCE pick_one
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t0-${testdrive.seed}'
  WITH (kafka_time_offset=1, start_offset=1)
  FORMAT TEXT
contains:`start_offset` and `kafka_time_offset` cannot be set at the same time.

! CREATE MATERIALIZED SOURCE not_a_number
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t0-${testdrive.seed}'
  WITH (kafka_time_offset="not_a_number")
  FORMAT TEXT
contains:`kafka_time_offset` must be a number

#
# Append-Only
#

$ kafka-create-topic topic=t1 partitions=3

$ kafka-ingest format=bytes topic=t1 key-format=bytes key-terminator=: timestamp=1 partition=0
apple:apple
banana:banana

$ kafka-ingest format=bytes topic=t1 key-format=bytes key-terminator=: timestamp=2 partition=1
cherry:cherry
date:date
eggfruit:eggfruit

$ kafka-ingest format=bytes topic=t1 key-format=bytes key-terminator=: timestamp=3 partition=1
fig:fig

$ kafka-ingest format=bytes topic=t1 key-format=bytes key-terminator=: timestamp=4 partition=2
grape:grape

> CREATE MATERIALIZED SOURCE append_time_offset_0
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t1-${testdrive.seed}'
  WITH (kafka_time_offset=0)
  FORMAT TEXT

> CREATE MATERIALIZED SOURCE append_time_offset_1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t1-${testdrive.seed}'
  WITH (kafka_time_offset=1, topic_metadata_refresh_interval_ms=10)
  FORMAT TEXT

> CREATE MATERIALIZED SOURCE append_time_offset_2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t1-${testdrive.seed}'
  WITH (kafka_time_offset=2, topic_metadata_refresh_interval_ms=10)
  FORMAT TEXT

> CREATE MATERIALIZED SOURCE append_time_offset_3
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t1-${testdrive.seed}'
  WITH (kafka_time_offset=3, topic_metadata_refresh_interval_ms=10)
  FORMAT TEXT

> CREATE MATERIALIZED SOURCE append_time_offset_4
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t1-${testdrive.seed}'
  WITH (kafka_time_offset=4, topic_metadata_refresh_interval_ms=10)
  FORMAT TEXT

> CREATE MATERIALIZED SOURCE append_time_offset_5
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t1-${testdrive.seed}'
  WITH (kafka_time_offset=5, topic_metadata_refresh_interval_ms=10)
  FORMAT TEXT

> SELECT * FROM append_time_offset_0
text      mz_offset
-------------------
apple     1
banana    2
cherry    1
date      2
eggfruit  3
fig       4
grape     1

> SELECT * FROM append_time_offset_1
text      mz_offset
-------------------
apple     1
banana    2
cherry    1
date      2
eggfruit  3
fig       4
grape     1

> SELECT * FROM append_time_offset_2
text      mz_offset
-------------------
cherry    1
date      2
eggfruit  3
fig       4
grape     1

> SELECT * FROM append_time_offset_3
text      mz_offset
-------------------
fig       4
grape     1

> SELECT * FROM append_time_offset_4
text      mz_offset
-------------------
grape     1

> SELECT * FROM append_time_offset_5
text      mz_offset
-------------------

$ kafka-add-partitions topic=t1 total-partitions=4

$ kafka-ingest format=bytes topic=t1 key-format=bytes key-terminator=: timestamp=5 partition=3
hazelnut:hazelnut

$ set-sql-timeout duration=60s

> SELECT * FROM append_time_offset_5
text      mz_offset
-------------------
hazelnut  1

#
# Upsert
#

$ kafka-create-topic topic=t2 partitions=3

$ kafka-ingest format=bytes topic=t2 key-format=bytes key-terminator=: timestamp=1 partition=0
apple:apple
banana:banana

$ kafka-ingest format=bytes topic=t2 key-format=bytes key-terminator=: timestamp=1 partition=0
apple:

$ kafka-ingest format=bytes topic=t2 key-format=bytes key-terminator=: timestamp=2 partition=1
cherry:cherry
date:date
eggfruit:eggfruit

$ kafka-ingest format=bytes topic=t2 key-format=bytes key-terminator=: timestamp=3 partition=1
cherry:
fig:fig

$ kafka-ingest format=bytes topic=t2 key-format=bytes key-terminator=: timestamp=4 partition=2
grape:grape

> CREATE MATERIALIZED SOURCE upsert_time_offset_0
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t2-${testdrive.seed}'
  WITH (kafka_time_offset=0)
  FORMAT TEXT
  ENVELOPE UPSERT

> CREATE MATERIALIZED SOURCE upsert_time_offset_1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t2-${testdrive.seed}'
  WITH (kafka_time_offset=1)
  FORMAT TEXT
  ENVELOPE UPSERT

> CREATE MATERIALIZED SOURCE upsert_time_offset_2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t2-${testdrive.seed}'
  WITH (kafka_time_offset=2)
  FORMAT TEXT
  ENVELOPE UPSERT

> CREATE MATERIALIZED SOURCE upsert_time_offset_3
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t2-${testdrive.seed}'
  WITH (kafka_time_offset=3)
  FORMAT TEXT
  ENVELOPE UPSERT

> CREATE MATERIALIZED SOURCE upsert_time_offset_4
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t2-${testdrive.seed}'
  WITH (kafka_time_offset=4)
  FORMAT TEXT
  ENVELOPE UPSERT

> CREATE MATERIALIZED SOURCE upsert_time_offset_5
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t2-${testdrive.seed}'
  WITH (kafka_time_offset=5, topic_metadata_refresh_interval_ms=10)
  FORMAT TEXT
  ENVELOPE UPSERT

> SELECT * FROM upsert_time_offset_0
key0      text      mz_offset
-----------------------------
banana    banana    2
date      date      2
eggfruit  eggfruit  3
fig       fig       5
grape     grape     1

> SELECT * FROM upsert_time_offset_1
key0      text      mz_offset
-----------------------------
banana    banana    2
date      date      2
eggfruit  eggfruit  3
fig       fig       5
grape     grape     1

> SELECT * FROM upsert_time_offset_2
key0      text      mz_offset
-----------------------------
date      date      2
eggfruit  eggfruit  3
fig       fig       5
grape     grape     1

> SELECT * FROM upsert_time_offset_3
key0      text      mz_offset
-----------------------------
fig       fig       5
grape     grape     1

> SELECT * FROM upsert_time_offset_4
key0      text      mz_offset
-----------------------------
grape     grape     1

> SELECT * FROM upsert_time_offset_5
key0      text      mz_offset
-----------------------------

$ kafka-add-partitions topic=t2 total-partitions=4

$ kafka-ingest format=bytes topic=t2 key-format=bytes key-terminator=: timestamp=5 partition=3
hazelnut:hazelnut

# It takes a while for new partitions to be consumed...
$ set-sql-timeout duration=60s

> SELECT * FROM upsert_time_offset_5
key0      text      mz_offset
-----------------------------
hazelnut  hazelnut  1

#
# Relative timestamps
#
# These tests are mainly meant as smoke tests. We can't do good tests currently,
# because we cannot control system time in tests.
#

$ kafka-create-topic topic=t3 partitions=1

$ kafka-ingest format=bytes topic=t3 timestamp=1
apple

# Timestamp for June 2021
$ kafka-ingest format=bytes topic=t3 timestamp=1622666300000
banana

# Timestamp for June 2099
$ kafka-ingest format=bytes topic=t3 timestamp=4084108700000
cherry

> CREATE MATERIALIZED SOURCE relative_time_offset_30_years_ago
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t3-${testdrive.seed}'
  WITH (kafka_time_offset=-946100000000)
  FORMAT TEXT

> CREATE MATERIALIZED SOURCE relative_time_offset_today
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t3-${testdrive.seed}'
  WITH (kafka_time_offset=-1)
  FORMAT TEXT

> SELECT * FROM relative_time_offset_30_years_ago
text      mz_offset
-------------------
banana    2
cherry    3

> SELECT * FROM relative_time_offset_today
text      mz_offset
-------------------
cherry    3

# Make sure that we don't fetch any messages that we don't want to fetch

$ kafka-create-topic topic=t4 partitions=1

$ kafka-ingest format=bytes topic=t4 timestamp=1
apple
pie

# A time offset of -1 specifies that we want to start from the end of the topic
# (negative offsets are relative from the end).
> CREATE MATERIALIZED SOURCE verify_no_fetch
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-t4-${testdrive.seed}'
  WITH (kafka_time_offset = -1)
  FORMAT TEXT


> SELECT * FROM verify_no_fetch
text      mz_offset
-------------------

> SELECT SUM((statistics->'topics'->'testdrive-t4-${testdrive.seed}'->'partitions'->'0'->'msgs')::int) from mz_kafka_source_statistics;
0

$ kafka-ingest format=bytes topic=t4 timestamp=2
mud
pie

> SELECT * FROM verify_no_fetch
text      mz_offset
-------------------
mud       3
pie       4

> SELECT SUM((statistics->'topics'->'testdrive-t4-${testdrive.seed}'->'partitions'->'0'->'msgs')::int) from mz_kafka_source_statistics;
2
