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
ALTER SYSTEM SET kafka_socket_timeout = 5000
ALTER SYSTEM SET kafka_transaction_timeout = 5100

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE TABLE t (a text, b text)
> CREATE MATERIALIZED VIEW simple_view AS SELECT * from t;

> CREATE SINK simple_view_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM simple_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'topic-${testdrive.seed}')
  KEY (a)
  FORMAT JSON
  ENVELOPE DEBEZIUM

> INSERT INTO t VALUES ('key1', 'value')

# NOTE: These queries are slow to succeed because the default metrics scraping
# interval is 60 seconds.
$ set-sql-timeout duration=2minutes

> SELECT s.name, SUM(u.messages_staged), SUM(u.messages_committed), SUM(u.bytes_staged) > 0, SUM(bytes_staged) = SUM(bytes_committed)
  FROM mz_sinks s
  JOIN mz_internal.mz_sink_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('simple_view_sink')
  GROUP BY s.name
  ORDER BY s.name
simple_view_sink 1 1 true true

> INSERT INTO t VALUES ('key1', 'value')
> SELECT s.name, SUM(u.messages_staged), SUM(u.messages_committed), SUM(u.bytes_staged) > 0, SUM(bytes_staged) = SUM(bytes_committed)
  FROM mz_sinks s
  JOIN mz_internal.mz_sink_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('simple_view_sink')
  GROUP BY s.name
  ORDER BY s.name
simple_view_sink 2 2 true true

# check the aggregated view as well
> SELECT s.name, u.messages_staged, u.messages_committed, u.bytes_staged > 0, bytes_staged = bytes_committed
  FROM mz_sinks s
  JOIN mz_internal.mz_sink_statistics u ON s.id = u.id
  WHERE s.name IN ('simple_view_sink')
simple_view_sink 2 2 true true

> ALTER CONNECTION kafka_conn SET (BROKER 'asdf') WITH (VALIDATE = false);

# Ensure we shut down first, so the insert later comes after we have restarted.
> SELECT count(*) > 0 FROM mz_internal.mz_sink_status_history
  JOIN mz_sinks ON mz_sink_status_history.sink_id = mz_sinks.id
  WHERE status = 'stalled'
  AND name = 'simple_view_sink'
true

> ALTER CONNECTION kafka_conn SET (BROKER '${testdrive.kafka-addr}') WITH (VALIDATE = true);

> SELECT status FROM mz_internal.mz_sink_statuses
  WHERE name = 'simple_view_sink'
running

> INSERT INTO t VALUES ('key1', 'value')
> SELECT s.name, u.messages_staged, u.messages_committed, u.bytes_staged > 0, bytes_staged = bytes_committed
  FROM mz_sinks s
  JOIN mz_internal.mz_sink_statistics u ON s.id = u.id
  WHERE s.name IN ('simple_view_sink')
simple_view_sink 3 3 true true

# The librdkafka metrics are labeled by sink ID only, while every worker renders
# the sink and only one runs the producer. They must survive both the idle
# workers shutting down and the sink being re-rendered in the same process.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET compute_prometheus_introspection_scrape_interval = '1s'

> CREATE CLUSTER rdkafka_four_workers SIZE 'scale=1,workers=4'
> CREATE CLUSTER rdkafka_one_worker SIZE 'scale=1,workers=1'

> CREATE SINK four_worker_sink
  IN CLUSTER rdkafka_four_workers
  FROM simple_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'topic-four-worker-${testdrive.seed}')
  KEY (a)
  FORMAT JSON
  ENVELOPE DEBEZIUM

> CREATE SINK one_worker_sink
  IN CLUSTER rdkafka_one_worker
  FROM simple_view
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'topic-one-worker-${testdrive.seed}')
  KEY (a)
  FORMAT JSON
  ENVELOPE DEBEZIUM

$ set-from-sql var=four-worker-sink-id
SELECT id FROM mz_sinks WHERE name = 'four_worker_sink'

$ set-from-sql var=one-worker-sink-id
SELECT id FROM mz_sinks WHERE name = 'one_worker_sink'

> SET cluster = rdkafka_four_workers

> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_sink_rdkafka_txmsgs' AND labels->'sink_id' = '${four-worker-sink-id}'
1

> SET cluster = rdkafka_one_worker

> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_sink_rdkafka_txmsgs' AND labels->'sink_id' = '${one-worker-sink-id}'
1

# Re-rendering builds the new dataflow before the old one is torn down.
> ALTER SINK one_worker_sink SET FROM t

# Wait out several scrape intervals so the check below cannot pass on a
# snapshot taken before the old dataflow finished shutting down.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

> SELECT count(*) FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_sink_rdkafka_txmsgs' AND labels->'sink_id' = '${one-worker-sink-id}'
1
