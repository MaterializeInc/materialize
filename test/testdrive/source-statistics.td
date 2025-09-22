# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_statistics_collection_interval = 1000
ALTER SYSTEM SET storage_statistics_interval = 2000

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"},
            {"name":"f2", "type":"long"}
        ]
    }

$ kafka-create-topic topic=upsert partitions=1

# The length of `moosemooose` must be longer than `moose` to ensure a tombstone doesn't HAPPEN
# to have the same size.
$ kafka-ingest format=avro topic=upsert key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish"} {"f1": "fish", "f2": 1000}
{"key": "bird1"} {"f1":"goose", "f2": 1}
{"key": "birdmore"} {"f1":"geese", "f2": 2}
{"key": "mammal1"} {"f1": "moose", "f2": 1}
{"key": "bird1"}
{"key": "birdmore"} {"f1":"geese", "f2": 56}
{"key": "mammalmore"} {"f1": "moose", "f2": 42}
{"key": "mammal1"}
{"key": "mammalmore"} {"f1":"moosemoose", "f2": 2}

$ kafka-create-topic topic=metrics-test partitions=1
$ kafka-ingest topic=metrics-test format=bytes
jack,jill
goofus,gallant

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> BEGIN
> CREATE SOURCE metrics_test_source
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-metrics-test-${testdrive.seed}')

> CREATE TABLE metrics_test_source_tbl (a, b) FROM SOURCE metrics_test_source (REFERENCE "testdrive-metrics-test-${testdrive.seed}")
  FORMAT CSV WITH 2 COLUMNS
  INCLUDE OFFSET
> COMMIT

> SELECT * FROM metrics_test_source_tbl
jack   jill    0
goofus gallant 1

> SELECT
      s.name,
      bool_and(u.snapshot_committed),
      SUM(u.messages_received) >= 4,
      SUM(u.updates_staged) > 0,
      SUM(u.updates_committed) > 0,
      SUM(u.bytes_received) > 0,
      bool_and(u.rehydration_latency IS NOT NULL)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('metrics_test_source')
  GROUP BY s.name
  ORDER BY s.name
metrics_test_source true true true true true true

> DROP SOURCE metrics_test_source CASCADE

> BEGIN
> CREATE SOURCE upsert
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-upsert-${testdrive.seed}'
  )

> CREATE TABLE upsert_tbl FROM SOURCE upsert (REFERENCE "testdrive-upsert-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  INCLUDE OFFSET
  ENVELOPE UPSERT
> COMMIT

# Adding a select here so that the ingests after this
# triggers lookup from the upsert state
> SELECT key, f1, f2 FROM upsert_tbl
key           f1      f2
------------------------
fish          fish    1000
birdmore      geese   56
mammalmore    moosemoose   2

# Also test a source with multiple sub-sources.
# NOTE: We give this source a unique name because we want to query for it with
# a `SUBSCRIBE ... AS OF AT LEAST 0` below and want to avoid receiving results
# for sources created by previous tests.
> BEGIN
> CREATE SOURCE auction_house_in_source_statistics_td
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR AUCTION;

> CREATE TABLE accounts FROM SOURCE auction_house_in_source_statistics_td (REFERENCE accounts);
> CREATE TABLE auctions FROM SOURCE auction_house_in_source_statistics_td (REFERENCE auctions);
> CREATE TABLE bids FROM SOURCE auction_house_in_source_statistics_td (REFERENCE bids);
> CREATE TABLE organizations FROM SOURCE auction_house_in_source_statistics_td (REFERENCE organizations);
> CREATE TABLE users FROM SOURCE auction_house_in_source_statistics_td (REFERENCE users);
> COMMIT

> SELECT
      s.name,
      bool_and(u.snapshot_committed),
      SUM(u.messages_received) > 0,
      SUM(u.updates_staged) > 0,
      SUM(u.updates_committed) > 0,
      SUM(u.bytes_received) > 0,
      bool_and(u.rehydration_latency IS NOT NULL)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('accounts', 'auction_house_in_source_statistics_td', 'auctions', 'bids', 'organizations', 'users')
  GROUP BY s.name
  ORDER BY s.name
auction_house_in_source_statistics_td true true true true true true


# Assert that the codepath that ensures a 0-value as the first value in `mz_source_statistics` occurs, using a `SUBSCRIBE`.
# Sinks and subsources use the same codepath.
$ set-regex match=\d{13} replacement=<TIMESTAMP>
> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (
    SELECT u.messages_received
      FROM mz_sources s
      JOIN mz_internal.mz_source_statistics_with_history u ON s.id = u.id
      WHERE s.name = 'auction_house_in_source_statistics_td'
    )
  AS OF AT LEAST 0

> FETCH 1 c;
<TIMESTAMP> 1 0

> COMMIT

> DROP SOURCE auction_house_in_source_statistics_td CASCADE

# Test upsert

# Note that we always count 9 messages received (18 with source tables), but can see as low as 3 updates.
# This is because there are 3 active keys, as all the messages could be received in 1 second.
# There could also be up to 11 updates, as updates cause inserts and deletes
# (5 initial inserts, 2 deletes, and 2 updates). In total 3 records should be present in upsert state.
# We can't control this, so have to accept the range.
> SELECT
    s.name,
    SUM(u.messages_received) >= 18,
    SUM(u.bytes_received) > 0
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('upsert')
  GROUP BY s.name
  ORDER BY s.name
upsert true true

# Upsert stats are on the UPSERT sub source/table, not the main source.
> SELECT
    t.name,
    bool_and(u.snapshot_committed),
    SUM(u.updates_staged) BETWEEN 3 AND 11,
    SUM(u.updates_committed) BETWEEN 3 AND 11,
    SUM(u.bytes_indexed) > 0,
    SUM(u.records_indexed),
    bool_and(u.rehydration_latency IS NOT NULL)
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name IN ('upsert_tbl')
  GROUP BY t.name
  ORDER BY t.name
upsert_tbl true true true true 3 true

# While we can't control how batching works above, we can ensure that this new, later update
# causes 1 more messages to be received, which is 1 update, a delete.
# We use `set-from-sql` to assert this. We will also use this to validate that the
# `bytes_indexed` value goes down because of the delete.
$ set-from-sql var=updates-committed
SELECT
    (SUM(u.updates_committed) + 1)::text
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name IN ('upsert_tbl')

$ set-from-sql var=state-bytes
SELECT
    (SUM(u.bytes_indexed))::text
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name IN ('upsert_tbl')

$ kafka-ingest format=avro topic=upsert key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "mammalmore"}

> SELECT s.name,
    SUM(u.messages_received) >= 20,
    SUM(u.bytes_received) > 0
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('upsert')
  GROUP BY s.name
  ORDER BY s.name
upsert true true

> SELECT t.name,
    bool_and(u.snapshot_committed),
    SUM(u.updates_staged),
    SUM(u.updates_committed),
    SUM(u.bytes_indexed) < ${state-bytes},
    SUM(u.records_indexed)
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name IN ('upsert_tbl')
  GROUP BY t.name
  ORDER BY t.name
upsert_tbl true "${updates-committed}" "${updates-committed}" true 2

# check pre-aggregated view

> SELECT s.name,
    u.messages_received >= 20,
    u.bytes_received > 0
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('upsert')
upsert true true

> SELECT t.name,
    u.snapshot_committed,
    u.updates_staged,
    u.updates_committed,
    u.bytes_indexed < ${state-bytes},
    u.records_indexed,
    u.rehydration_latency IS NOT NULL
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name IN ('upsert_tbl')
upsert_tbl true "${updates-committed}" "${updates-committed}" true 2 true

> DROP SOURCE upsert CASCADE

# should be empty because the source was dropped
> SELECT count(*) FROM mz_internal.mz_source_statistics;
0
