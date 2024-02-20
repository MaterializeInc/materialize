# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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
