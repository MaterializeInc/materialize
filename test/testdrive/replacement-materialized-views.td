# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for replacement materialized views.

$ set-arg-default single-replica-cluster=quickstart

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)
> CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  )

> CREATE TABLE t (a int NOT NULL, b int NOT NULL)
> INSERT INTO t VALUES (1, 2), (3, 4), (5, 6)
> CREATE MATERIALIZED VIEW mv AS SELECT a, b FROM t

> SELECT * FROM mv
1 2
3 4
5 6

# Create downstream objects that depend on the materialized view and that will
# observe its replacement.

> CREATE INDEX mv_idx ON mv (a)
> CREATE MATERIALIZED VIEW mv_downstream AS SELECT a + b AS sum FROM mv
> CREATE SINK mv_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM mv
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-mv-replacement-sink-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

> SELECT * FROM mv_downstream
3
7
11

$ kafka-verify-data format=avro sort-messages=true sink=materialize.public.mv_sink
{"before": null, "after": {"row": {"a": 1, "b": 2}}}
{"before": null, "after": {"row": {"a": 3, "b": 4}}}
{"before": null, "after": {"row": {"a": 5, "b": 6}}}

# Create and reply a replacement that changes the MV's output.

> CREATE REPLACEMENT MATERIALIZED VIEW mv_rp FOR mv AS SELECT a * 10 AS a, b FROM t

> SELECT * FROM mv
1 2
3 4
5 6

> SELECT * FROM mv_rp
10 2
30 4
50 6

> ALTER MATERIALIZED VIEW mv APPLY REPLACEMENT mv_rp

# Verify that all downstream objects still work and contain the correct data.

> SELECT * FROM mv
10 2
30 4
50 6

> SELECT * FROM mv_downstream
12
34
56

$ kafka-verify-data format=avro sort-messages=true sink=materialize.public.mv_sink
{"before": null, "after": {"row": {"a": 10, "b": 2}}}
{"before": null, "after": {"row": {"a": 30, "b": 4}}}
{"before": null, "after": {"row": {"a": 50, "b": 6}}}
{"before": {"row": {"a": 1, "b": 2}}, "after": null}
{"before": {"row": {"a": 3, "b": 4}}, "after": null}
{"before": {"row": {"a": 5, "b": 6}}, "after": null}

> INSERT INTO t VALUES (7, 8)

> SELECT * FROM mv
10 2
30 4
50 6
70 8

> SELECT * FROM mv_downstream
12
34
56
78

$ kafka-verify-data format=avro sort-messages=true sink=materialize.public.mv_sink
{"before": null, "after": {"row": {"a": 70, "b": 8}}}
