# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that Materialize can create a sink using a Kafka user with restricted
# access to consumer groups and transactional IDs.

# ==> Set up. <==

$ kafka-create-topic topic=data
$ kafka-ingest topic=data format=bytes
banana

> CREATE SECRET kafka_password AS 'sekurity'

> CREATE CONNECTION kafka_bad_progress_topic TO KAFKA (
    BROKER 'kafka:9095',
    SASL MECHANISMS = 'PLAIN',
    SASL USERNAME = 'materialize_lockdown',
    SASL PASSWORD = SECRET kafka_password,
    SECURITY PROTOCOL SASL_PLAINTEXT
  );

> CREATE CONNECTION kafka_good_progress_topic TO KAFKA (
    BROKER 'kafka:9095',
    SASL MECHANISMS = 'PLAIN',
    SASL USERNAME = 'materialize_lockdown',
    SASL PASSWORD = SECRET kafka_password,
    SECURITY PROTOCOL SASL_PLAINTEXT,
    PROGRESS TOPIC = 'lockdown-progress'
  );

> CREATE TABLE t (column1 integer)
> INSERT INTO t VALUES (1), (2)
> CREATE MATERIALIZED VIEW mv AS SELECT DISTINCT column1 FROM t

# ==> Test. <==

# The default group ID prefix is not usable by the `materialize_lockdown`
# user.

> CREATE SOURCE broken
  FROM KAFKA CONNECTION kafka_good_progress_topic (
    TOPIC 'testdrive-data-${testdrive.seed}'
  )
  FORMAT TEXT

> SELECT EXISTS (
    SELECT 1
    FROM mz_sources
    JOIN mz_internal.mz_source_status_history ON mz_sources.id = mz_source_status_history.source_id
    WHERE name = 'broken'
    AND error ILIKE '%error when polling consumer for source%Group authorization failed%'
  )
true

> DROP SOURCE broken CASCADE

# The default group ID prefix *is* writeable by the `materialize_lockdown` user.
# Ensure that offsets are committed.

> CREATE SOURCE working_source
  FROM KAFKA CONNECTION kafka_good_progress_topic (
    TOPIC 'testdrive-data-${testdrive.seed}',
    GROUP ID PREFIX 'lockdown-'
  )
  FORMAT TEXT

> SELECT * FROM working_source
banana

$ set-from-sql var=conn-id
SELECT id FROM mz_connections WHERE name = 'kafka_good_progress_topic'

$ set-from-sql var=source-id
SELECT id FROM mz_sources WHERE name = 'working_source'

$ kafka-verify-commit topic=data partition=0 consumer-group-id=lockdown-materialize-${testdrive.materialize-environment-id}-${conn-id}-${source-id}
1

# A sink which uses a bad transactional ID should fail.
> CREATE SINK broken1 FROM mv
  INTO KAFKA CONNECTION kafka_bad_progress_topic (
    TOPIC 'testdrive-broken-${testdrive.seed}'
  )
  KEY (column1) FORMAT JSON ENVELOPE UPSERT
> SELECT EXISTS (
    SELECT 1
    FROM mz_sinks
    JOIN mz_internal.mz_sink_status_history ON mz_sinks.id = mz_sink_status_history.sink_id
    WHERE name = 'broken1'
    AND error ILIKE '%Transactional Id authorization failed%'
  )
true
> DROP SINK broken1

# A sink which uses a good transactional ID but a bad progress topic should
# fail.
> CREATE SINK broken2 FROM mv
  INTO KAFKA CONNECTION kafka_bad_progress_topic (
    TOPIC 'testdrive-broken-${testdrive.seed}',
    TRANSACTIONAL ID PREFIX 'lockdown'
  )
  KEY (column1) FORMAT JSON ENVELOPE UPSERT
> SELECT EXISTS (
    SELECT 1
    FROM mz_sinks
    JOIN mz_internal.mz_sink_status_history ON mz_sinks.id = mz_sink_status_history.sink_id
    WHERE name = 'broken2'
    AND error ILIKE '%kafka: error registering kafka progress topic for sink%Topic authorization failed%'
  )
true
> DROP SINK broken2

# A sink which uses a good transactional ID and progress topic but a bad data
# topic should fail.
> CREATE SINK broken3 FROM mv
  INTO KAFKA CONNECTION kafka_good_progress_topic (
    TOPIC 'testdrive-broken-${testdrive.seed}',
    TRANSACTIONAL ID PREFIX 'lockdown'
  )
  KEY (column1) FORMAT JSON ENVELOPE UPSERT
> SELECT EXISTS (
    SELECT 1
    FROM mz_sinks
    JOIN mz_internal.mz_sink_status_history ON mz_sinks.id = mz_sink_status_history.sink_id
    WHERE name = 'broken3'
    AND error ILIKE '%Error creating topic testdrive-broken-${testdrive.seed}%Topic authorization failed%'
  )
true
> DROP SINK broken3

# A sink which uses a good transactional ID, progress topic, and data topic
# but a bad group ID prefix will fail, but only after restart when the progress
# topic contains entries.

> CREATE CLUSTER c (SIZE = '1')

> CREATE SINK broken4 IN CLUSTER c FROM mv
  INTO KAFKA CONNECTION kafka_good_progress_topic (
    TOPIC 'lockdown-data1',
    TRANSACTIONAL ID PREFIX 'lockdown'
  )
  KEY (column1) FORMAT JSON ENVELOPE UPSERT

$ kafka-verify-data format=json key=false sink=materialize.public.broken4 sort-messages=true
{"column1": 1}
{"column1": 2}

# Resize the cluster on which the sink is running to force the sink to restart.
> ALTER CLUSTER c SET (SIZE = '2')

> SELECT EXISTS (
    SELECT 1
    FROM mz_sinks
    JOIN mz_internal.mz_sink_status_history ON mz_sinks.id = mz_sink_status_history.sink_id
    WHERE name = 'broken4'
    AND error ILIKE '%failed to fetch progress message%Group authorization failed%'
  )
true

> DROP SINK broken4

# A sink which uses a good transactional ID, progress topic, data topic, and
# group ID prefix should work.

> CREATE SINK working IN CLUSTER c FROM mv
  INTO KAFKA CONNECTION kafka_good_progress_topic (
    TOPIC 'lockdown-data2',
    TRANSACTIONAL ID PREFIX 'lockdown',
    PROGRESS GROUP ID PREFIX 'lockdown'
  )
  KEY (column1) FORMAT JSON ENVELOPE UPSERT

# Validate that the sink is actually emitting data. Success of the `CREATE SINK`
# command itself is not sufficient validation.
$ kafka-verify-data format=json key=false sink=materialize.public.working sort-messages=true
{"column1": 1}
{"column1": 2}

# Resize the cluster on which the sink is running to force the sink to restart.
> ALTER CLUSTER c SET (SIZE = '1')

# Ensure that the sink is emitting new messages.
> INSERT INTO t VALUES (3)
$ kafka-verify-data format=json key=false sink=materialize.public.working sort-messages=true
{"column1": 3}

# Ensure that the sink never entered the `stalled` status.
> SELECT DISTINCT status FROM mz_sinks
  JOIN mz_internal.mz_sink_status_history ON mz_sinks.id = mz_sink_status_history.sink_id
  WHERE mz_sinks.name = 'working'
starting
running
paused
