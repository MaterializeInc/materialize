# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=scale=1,workers=1

$ kafka-create-topic topic=status-history

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;
CREATE TABLE pg_table (a int);
ALTER TABLE pg_table REPLICA IDENTITY FULL;

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg_conn
  TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=p@ssw0rd

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE mysql_table (a int);

> CREATE SECRET mysqlpass AS 'p@ssw0rd';
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

# Test the contents of `mz_source_status_history` with various types of sources.

> CREATE CLUSTER sources SIZE '${arg.default-storage-size}', REPLICATION FACTOR 0

> CREATE SOURCE auction
  IN CLUSTER sources
  FROM LOAD GENERATOR AUCTION FOR ALL TABLES

> CREATE SOURCE kafka
  IN CLUSTER sources
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-status-history-${testdrive.seed}')
> CREATE TABLE kafka_tbl
  FROM SOURCE kafka (REFERENCE "testdrive-status-history-${testdrive.seed}")
  FORMAT TEXT

> CREATE SOURCE pg
  IN CLUSTER sources
  FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'mz_source')
> CREATE TABLE pg_tbl
  FROM SOURCE pg (REFERENCE "pg_table")

> CREATE SOURCE mysql
  IN CLUSTER sources
  FROM MYSQL CONNECTION mysql_conn
> CREATE TABLE mysql_tbl
  FROM SOURCE mysql (REFERENCE "public"."mysql_table")

> SELECT o.name, s.status, s.details, s.replica_id
  FROM mz_internal.mz_source_status_history s
  JOIN mz_objects o ON o.id = s.source_id
accounts       paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
auction        paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
auctions       paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
bids           paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
organizations  paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
users          paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
kafka          paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
kafka_tbl      paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
pg             paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
pg_tbl         paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
mysql          paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
mysql_tbl      paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>

> SELECT o.name, s.status FROM mz_internal.mz_source_statuses s JOIN mz_objects o USING (id)
accounts       paused
auction        paused
auctions       paused
bids           paused
organizations  paused
users          paused
kafka          paused
kafka_tbl      paused
pg             paused
pg_tbl         paused
mysql          paused
mysql_tbl      paused
auction_progress  running
kafka_progress    running
pg_progress       running
mysql_progress    running

> ALTER CLUSTER sources SET (REPLICATION FACTOR 1)

$ set-from-sql var=replica_id_1
SELECT r.id FROM mz_clusters c JOIN mz_cluster_replicas r ON c.id = r.cluster_id  WHERE c.name = 'sources'

> SELECT o.name, s.status, s.details, s.replica_id
  FROM mz_internal.mz_source_status_history s
  JOIN mz_objects o ON o.id = s.source_id
accounts       paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
auction        paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
auctions       paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
bids           paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
organizations  paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
users          paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
kafka          paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
kafka_tbl      paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
pg             paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
pg_tbl         paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
mysql          paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
mysql_tbl      paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
accounts       starting  <null>                                                                 ${replica_id_1}
auction        starting  <null>                                                                 ${replica_id_1}
auctions       starting  <null>                                                                 ${replica_id_1}
bids           starting  <null>                                                                 ${replica_id_1}
organizations  starting  <null>                                                                 ${replica_id_1}
users          starting  <null>                                                                 ${replica_id_1}
kafka          starting  <null>                                                                 ${replica_id_1}
kafka_tbl      starting  <null>                                                                 ${replica_id_1}
pg             starting  <null>                                                                 ${replica_id_1}
pg_tbl         starting  <null>                                                                 ${replica_id_1}
mysql          starting  <null>                                                                 ${replica_id_1}
mysql_tbl      starting  <null>                                                                 ${replica_id_1}
accounts       running   <null>                                                                 ${replica_id_1}
auction        running   <null>                                                                 ${replica_id_1}
auctions       running   <null>                                                                 ${replica_id_1}
bids           running   <null>                                                                 ${replica_id_1}
organizations  running   <null>                                                                 ${replica_id_1}
users          running   <null>                                                                 ${replica_id_1}
kafka          running   <null>                                                                 ${replica_id_1}
kafka_tbl      running   <null>                                                                 ${replica_id_1}
pg             running   <null>                                                                 ${replica_id_1}
pg_tbl         running   <null>                                                                 ${replica_id_1}
mysql          running   <null>                                                                 ${replica_id_1}
mysql_tbl      running   <null>                                                                 ${replica_id_1}

> SELECT o.name, s.status FROM mz_internal.mz_source_statuses s JOIN mz_objects o USING (id)
accounts       running
auction        running
auctions       running
bids           running
organizations  running
users          running
kafka          running
kafka_tbl      running
pg             running
pg_tbl         running
mysql          running
mysql_tbl      running
auction_progress  running
kafka_progress    running
pg_progress       running
mysql_progress    running

> ALTER CLUSTER sources SET (REPLICATION FACTOR 0)

> SELECT o.name, s.status, s.details, s.replica_id
  FROM mz_internal.mz_source_status_history s
  JOIN mz_objects o ON o.id = s.source_id
accounts       paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
auction        paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
auctions       paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
bids           paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
organizations  paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
users          paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
kafka          paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
kafka_tbl      paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
pg             paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
pg_tbl         paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
mysql          paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
mysql_tbl      paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
accounts       starting  <null>                                                                 ${replica_id_1}
auction        starting  <null>                                                                 ${replica_id_1}
auctions       starting  <null>                                                                 ${replica_id_1}
bids           starting  <null>                                                                 ${replica_id_1}
organizations  starting  <null>                                                                 ${replica_id_1}
users          starting  <null>                                                                 ${replica_id_1}
kafka          starting  <null>                                                                 ${replica_id_1}
kafka_tbl      starting  <null>                                                                 ${replica_id_1}
pg             starting  <null>                                                                 ${replica_id_1}
pg_tbl         starting  <null>                                                                 ${replica_id_1}
mysql          starting  <null>                                                                 ${replica_id_1}
mysql_tbl      starting  <null>                                                                 ${replica_id_1}
accounts       running   <null>                                                                 ${replica_id_1}
auction        running   <null>                                                                 ${replica_id_1}
auctions       running   <null>                                                                 ${replica_id_1}
bids           running   <null>                                                                 ${replica_id_1}
organizations  running   <null>                                                                 ${replica_id_1}
users          running   <null>                                                                 ${replica_id_1}
kafka          running   <null>                                                                 ${replica_id_1}
kafka_tbl      running   <null>                                                                 ${replica_id_1}
pg             running   <null>                                                                 ${replica_id_1}
pg_tbl         running   <null>                                                                 ${replica_id_1}
mysql          running   <null>                                                                 ${replica_id_1}
mysql_tbl      running   <null>                                                                 ${replica_id_1}
accounts       paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
auction        paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
auctions       paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
bids           paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
organizations  paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
users          paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
kafka          paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
kafka_tbl      paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
pg             paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
pg_tbl         paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
mysql          paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
mysql_tbl      paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}

> SELECT o.name, s.status FROM mz_internal.mz_source_statuses s JOIN mz_objects o USING (id)
accounts       paused
auction        paused
auctions       paused
bids           paused
organizations  paused
users          paused
kafka          paused
kafka_tbl      paused
pg             paused
pg_tbl         paused
mysql          paused
mysql_tbl      paused
auction_progress  running
kafka_progress    running
pg_progress       running
mysql_progress    running

> ALTER CLUSTER sources SET (REPLICATION FACTOR 1)

$ set-from-sql var=replica_id_2
SELECT r.id FROM mz_clusters c JOIN mz_cluster_replicas r ON c.id = r.cluster_id  WHERE c.name = 'sources'

> SELECT o.name, s.status, s.details, s.replica_id
  FROM mz_internal.mz_source_status_history s
  JOIN mz_objects o ON o.id = s.source_id
accounts       paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
auction        paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
auctions       paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
bids           paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
organizations  paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
users          paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
kafka          paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
kafka_tbl      paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
pg             paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
pg_tbl         paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
mysql          paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
mysql_tbl      paused    "{\"hints\":[\"There is currently no replica running this source\"]}"  <null>
accounts       starting  <null>                                                                 ${replica_id_1}
auction        starting  <null>                                                                 ${replica_id_1}
auctions       starting  <null>                                                                 ${replica_id_1}
bids           starting  <null>                                                                 ${replica_id_1}
organizations  starting  <null>                                                                 ${replica_id_1}
users          starting  <null>                                                                 ${replica_id_1}
kafka          starting  <null>                                                                 ${replica_id_1}
kafka_tbl      starting  <null>                                                                 ${replica_id_1}
pg             starting  <null>                                                                 ${replica_id_1}
pg_tbl         starting  <null>                                                                 ${replica_id_1}
mysql          starting  <null>                                                                 ${replica_id_1}
mysql_tbl      starting  <null>                                                                 ${replica_id_1}
accounts       running   <null>                                                                 ${replica_id_1}
auction        running   <null>                                                                 ${replica_id_1}
auctions       running   <null>                                                                 ${replica_id_1}
bids           running   <null>                                                                 ${replica_id_1}
organizations  running   <null>                                                                 ${replica_id_1}
users          running   <null>                                                                 ${replica_id_1}
kafka          running   <null>                                                                 ${replica_id_1}
kafka_tbl      running   <null>                                                                 ${replica_id_1}
pg             running   <null>                                                                 ${replica_id_1}
pg_tbl         running   <null>                                                                 ${replica_id_1}
mysql          running   <null>                                                                 ${replica_id_1}
mysql_tbl      running   <null>                                                                 ${replica_id_1}
accounts       paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
auction        paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
auctions       paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
bids           paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
organizations  paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
users          paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
kafka          paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
kafka_tbl      paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
pg             paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
pg_tbl         paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
mysql          paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
mysql_tbl      paused    "{\"hints\":[\"The replica running this source has been dropped\"]}"   ${replica_id_1}
accounts       starting  <null>                                                                 ${replica_id_2}
auction        starting  <null>                                                                 ${replica_id_2}
auctions       starting  <null>                                                                 ${replica_id_2}
bids           starting  <null>                                                                 ${replica_id_2}
organizations  starting  <null>                                                                 ${replica_id_2}
users          starting  <null>                                                                 ${replica_id_2}
kafka          starting  <null>                                                                 ${replica_id_2}
kafka_tbl      starting  <null>                                                                 ${replica_id_2}
pg             starting  <null>                                                                 ${replica_id_2}
pg_tbl         starting  <null>                                                                 ${replica_id_2}
mysql          starting  <null>                                                                 ${replica_id_2}
mysql_tbl      starting  <null>                                                                 ${replica_id_2}
accounts       running   <null>                                                                 ${replica_id_2}
auction        running   <null>                                                                 ${replica_id_2}
auctions       running   <null>                                                                 ${replica_id_2}
bids           running   <null>                                                                 ${replica_id_2}
organizations  running   <null>                                                                 ${replica_id_2}
users          running   <null>                                                                 ${replica_id_2}
kafka          running   <null>                                                                 ${replica_id_2}
kafka_tbl      running   <null>                                                                 ${replica_id_2}
pg             running   <null>                                                                 ${replica_id_2}
pg_tbl         running   <null>                                                                 ${replica_id_2}
mysql          running   <null>                                                                 ${replica_id_2}
mysql_tbl      running   <null>                                                                 ${replica_id_2}

> SELECT o.name, s.status FROM mz_internal.mz_source_statuses s JOIN mz_objects o USING (id)
accounts       running
auction        running
auctions       running
bids           running
organizations  running
users          running
kafka          running
kafka_tbl      running
pg             running
pg_tbl         running
mysql          running
mysql_tbl      running
auction_progress  running
kafka_progress    running
pg_progress       running
mysql_progress    running

# Ensure `dropped` status also shows up.

$ set-from-sql var=source_id
SELECT id FROM mz_sources WHERE name = 'kafka'

> DROP SOURCE kafka CASCADE
> SELECT true
  FROM mz_internal.mz_source_status_history
  WHERE source_id = '${source_id}' and status = 'dropped'
true

> DROP CLUSTER sources CASCADE


# Test the contents of `mz_sink_status_history`.
#
# Note that a sink sometimes enters a "stalled" state after creation because
# there is a race between creating the progress collection and reading from it.
# The sink then restarts and comes back up healthy. We have to account for that
# sequence in events in the history queries below.

> CREATE CLUSTER sinks SIZE '${arg.default-storage-size}', REPLICATION FACTOR 0

> CREATE TABLE t (a int)
> CREATE SINK kafka1
  IN CLUSTER sinks
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka1-sink-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
> CREATE SINK kafka2
  IN CLUSTER sinks
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka2-sink-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> SELECT o.name, s.status, s.details, s.replica_id
  FROM mz_internal.mz_sink_status_history s
  JOIN mz_objects o ON o.id = s.sink_id
kafka1  paused  "{\"hints\":[\"There is currently no replica running this sink\"]}"  <null>
kafka2  paused  "{\"hints\":[\"There is currently no replica running this sink\"]}"  <null>

> SELECT o.name, s.status FROM mz_internal.mz_sink_statuses s JOIN mz_objects o USING (id)
kafka1  paused
kafka2  paused

> ALTER CLUSTER sinks SET (REPLICATION FACTOR 1)

$ set-from-sql var=replica_id_1
SELECT r.id FROM mz_clusters c JOIN mz_cluster_replicas r ON c.id = r.cluster_id  WHERE c.name = 'sinks'

> SELECT o.name, s.status, s.details, s.replica_id
  FROM mz_internal.mz_sink_status_history s
  JOIN mz_sinks o ON o.id = s.sink_id
  WHERE o.name = 'kafka1'
  ORDER BY occurred_at DESC
  LIMIT 2
kafka1  starting  <null>  ${replica_id_1}
kafka1  running   <null>  ${replica_id_1}

> SELECT o.name, s.status, s.details, s.replica_id
  FROM mz_internal.mz_sink_status_history s
  JOIN mz_sinks o ON o.id = s.sink_id
  WHERE o.name = 'kafka2'
  ORDER BY occurred_at DESC
  LIMIT 2
kafka2  starting  <null>  ${replica_id_1}
kafka2  running   <null>  ${replica_id_1}

> SELECT o.name, s.status FROM mz_internal.mz_sink_statuses s JOIN mz_sinks o USING (id)
kafka1  running
kafka2  running

> ALTER CLUSTER sinks SET (REPLICATION FACTOR 0)

> SELECT o.name, s.status, s.details, s.replica_id
  FROM mz_internal.mz_sink_status_history s
  JOIN mz_sinks o ON o.id = s.sink_id
  WHERE o.name = 'kafka1'
  ORDER BY occurred_at DESC
  LIMIT 1
kafka1  paused    "{\"hints\":[\"The replica running this sink has been dropped\"]}"   ${replica_id_1}

> SELECT o.name, s.status, s.details, s.replica_id
  FROM mz_internal.mz_sink_status_history s
  JOIN mz_sinks o ON o.id = s.sink_id
  WHERE o.name = 'kafka2'
  ORDER BY occurred_at DESC
  LIMIT 1
kafka2  paused    "{\"hints\":[\"The replica running this sink has been dropped\"]}"   ${replica_id_1}

> SELECT o.name, s.status FROM mz_internal.mz_sink_statuses s JOIN mz_objects o USING (id)
kafka1  paused
kafka2  paused

> ALTER CLUSTER sinks SET (REPLICATION FACTOR 1)

$ set-from-sql var=replica_id_2
SELECT r.id FROM mz_clusters c JOIN mz_cluster_replicas r ON c.id = r.cluster_id  WHERE c.name = 'sinks'

> SELECT o.name, s.status, s.details, s.replica_id
  FROM mz_internal.mz_sink_status_history s
  JOIN mz_sinks o ON o.id = s.sink_id
  WHERE o.name = 'kafka1'
  ORDER BY occurred_at DESC
  LIMIT 2
kafka1  starting  <null>  ${replica_id_2}
kafka1  running   <null>  ${replica_id_2}

> SELECT o.name, s.status, s.details, s.replica_id
  FROM mz_internal.mz_sink_status_history s
  JOIN mz_sinks o ON o.id = s.sink_id
  WHERE o.name = 'kafka2'
  ORDER BY occurred_at DESC
  LIMIT 2
kafka2  starting  <null>  ${replica_id_2}
kafka2  running   <null>  ${replica_id_2}

> SELECT o.name, s.status FROM mz_internal.mz_sink_statuses s JOIN mz_objects o USING (id)
kafka1  running
kafka2  running

# Ensure `dropped` status also shows up.

$ set-from-sql var=sink_id
SELECT id FROM mz_sinks WHERE name = 'kafka1'

> DROP SINK kafka1
> SELECT true
  FROM mz_internal.mz_sink_status_history
  WHERE sink_id = '${sink_id}' and status = 'dropped'
true

> DROP CLUSTER sinks CASCADE
