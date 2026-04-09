# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that a sink can read from multiple collections (tables, MVs) and
# produce the union of their rows.

$ set-sql-timeout duration=60s
$ set-arg-default single-replica-cluster=quickstart

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

# ---- Setup: two tables with identical schemas ----

> CREATE TABLE t1 (a int, b text);
> INSERT INTO t1 VALUES (1, 'from_t1'), (2, 'from_t1');

> CREATE TABLE t2 (a int, b text);
> INSERT INTO t2 VALUES (3, 'from_t2'), (4, 'from_t2');

# ---- Test: multi-source Kafka sink (Debezium) ----

> CREATE SINK multi_src_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t1, t2
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-multi-src-sink-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM

$ kafka-verify-data format=json sink=materialize.public.multi_src_sink sort-messages=true key=false
{"before": null, "after": {"a": 1, "b": "from_t1"}}
{"before": null, "after": {"a": 2, "b": "from_t1"}}
{"before": null, "after": {"a": 3, "b": "from_t2"}}
{"before": null, "after": {"a": 4, "b": "from_t2"}}

# ---- Test: multi-source with materialized views ----

> CREATE MATERIALIZED VIEW mv1 AS SELECT a, b FROM t1 WHERE a = 1;
> CREATE MATERIALIZED VIEW mv2 AS SELECT a, b FROM t2 WHERE a = 3;

> CREATE SINK multi_src_mv_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM mv1, mv2
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-multi-src-mv-sink-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM

$ kafka-verify-data format=json sink=materialize.public.multi_src_mv_sink sort-messages=true key=false
{"before": null, "after": {"a": 1, "b": "from_t1"}}
{"before": null, "after": {"a": 3, "b": "from_t2"}}

# ---- Test: single-source still works (backwards compatibility) ----

> CREATE SINK single_src_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t1
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-single-src-sink-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM

$ kafka-verify-data format=json sink=materialize.public.single_src_sink sort-messages=true key=false
{"before": null, "after": {"a": 1, "b": "from_t1"}}
{"before": null, "after": {"a": 2, "b": "from_t1"}}

# ---- Test: three sources ----

> CREATE TABLE t3 (a int, b text);
> INSERT INTO t3 VALUES (5, 'from_t3');

> CREATE SINK three_src_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t1, t2, t3
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-three-src-sink-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM

$ kafka-verify-data format=json sink=materialize.public.three_src_sink sort-messages=true key=false
{"before": null, "after": {"a": 1, "b": "from_t1"}}
{"before": null, "after": {"a": 2, "b": "from_t1"}}
{"before": null, "after": {"a": 3, "b": "from_t2"}}
{"before": null, "after": {"a": 4, "b": "from_t2"}}
{"before": null, "after": {"a": 5, "b": "from_t3"}}

# ---- Test: multi-source upsert sink requires NOT ENFORCED ----

! CREATE SINK bad_upsert_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t1, t2
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-bad-upsert-sink-${testdrive.seed}')
  KEY (a)
  FORMAT JSON
  ENVELOPE UPSERT
contains:upsert key could not be validated as unique

> CREATE SINK upsert_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t1, t2
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-upsert-sink-${testdrive.seed}')
  KEY (a) NOT ENFORCED
  FORMAT JSON
  ENVELOPE UPSERT

$ kafka-verify-data format=json sink=materialize.public.upsert_sink sort-messages=true key=true
{"a": 1} {"a": 1, "b": "from_t1"}
{"a": 2} {"a": 2, "b": "from_t1"}
{"a": 3} {"a": 3, "b": "from_t2"}
{"a": 4} {"a": 4, "b": "from_t2"}

# ---- Test: inserts after sink creation propagate ----

> INSERT INTO t1 VALUES (10, 'new_t1');
> INSERT INTO t2 VALUES (20, 'new_t2');

$ kafka-verify-data format=json sink=materialize.public.multi_src_sink sort-messages=true key=false
{"before": null, "after": {"a": 10, "b": "new_t1"}}
{"before": null, "after": {"a": 20, "b": "new_t2"}}

# ---- Test: deletes after sink creation propagate ----

> DELETE FROM t1 WHERE a = 1;

$ kafka-verify-data format=json sink=materialize.public.multi_src_sink sort-messages=true key=false
{"before": {"a": 1, "b": "from_t1"}, "after": null}

# ---- Test: ALTER SINK from single-source to multi-source ----

> CREATE TABLE alt1 (x int, y text);
> CREATE TABLE alt2 (x int, y text);
> INSERT INTO alt1 VALUES (100, 'alt1');

> CREATE SINK alter_test_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM alt1
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-alter-test-sink-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM

$ kafka-verify-data format=json sink=materialize.public.alter_test_sink sort-messages=true key=false
{"before": null, "after": {"x": 100, "y": "alt1"}}

> ALTER SINK alter_test_sink SET FROM alt1, alt2;

# Wait for the new dataflow to start, then insert into the second table.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=4s

> INSERT INTO alt2 VALUES (200, 'alt2');

$ kafka-verify-data format=json sink=materialize.public.alter_test_sink sort-messages=true key=false
{"before": null, "after": {"x": 200, "y": "alt2"}}

# ---- Test: ALTER SINK from multi-source to different multi-source ----

> CREATE TABLE alt3 (x int, y text);
> INSERT INTO alt3 VALUES (300, 'alt3');

> ALTER SINK alter_test_sink SET FROM alt2, alt3;

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=4s

> INSERT INTO alt3 VALUES (400, 'alt3_new');

$ kafka-verify-data format=json sink=materialize.public.alter_test_sink sort-messages=true key=false
{"before": null, "after": {"x": 400, "y": "alt3_new"}}

# ---- Error: schema mismatch between sources ----

> CREATE TABLE t_different (x text, y text, z text);

! CREATE SINK bad_multi_src_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t1, t_different
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-bad-multi-src-sink-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM
contains:all FROM items must have identical schemas

# ---- Error: cannot sink from a view ----

> CREATE VIEW v_plain AS SELECT a, b FROM t1;

! CREATE SINK bad_view_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM t1, v_plain
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-bad-view-sink-${testdrive.seed}')
  FORMAT JSON
  ENVELOPE DEBEZIUM
contains:view v_plain cannot be exported as a sink

# ---- Verify the sink shows up in SHOW SINKS ----

> SELECT name FROM (SHOW SINKS) WHERE name LIKE 'multi_src%' OR name LIKE 'three_src%' ORDER BY name
multi_src_mv_sink
multi_src_sink
three_src_sink
