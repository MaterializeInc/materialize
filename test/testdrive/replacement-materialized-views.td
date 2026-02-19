# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for replacement materialized views.
#
# This is a testdrive, rather than an SLT, because applying a replacement
# happens asynchronously in the dataflow layer. After the ALTER MV command
# returns, queries against the MV might still return the old data for a while,
# even with strict serializable isolation. So we need retries.

$ set-arg-default single-replica-cluster=quickstart


# Setup

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)
> CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  )

> CREATE TABLE t (a int NOT NULL, b int NOT NULL)
> INSERT INTO t VALUES (1, 2), (3, 4), (5, 6)
> CREATE CLUSTER other REPLICAS (r1 (SIZE 'scale=1,workers=1'), r2 (SIZE 'scale=2,workers=2'))


# Test: basic replacement workflow

> CREATE MATERIALIZED VIEW mv AS SELECT a, b FROM t

> SELECT * FROM mv
1 2
3 4
5 6

> CREATE REPLACEMENT MATERIALIZED VIEW rp FOR mv AS SELECT a + b as a, b FROM t

> SELECT * FROM mv
1 2
3 4
5 6

> SELECT * FROM rp
3  2
7  4
11 6

# Ensure that querying the replacement view uses the definition, not the (replacement) materialized view.
? EXPLAIN OPTIMIZED PLAN WITH (humanized expressions) AS VERBOSE TEXT FOR SELECT * FROM rp
Explained Query:
  Project (#2, #1{b})
    Map ((#0{a} + #1{b}))
      ReadStorage materialize.public.t

Source materialize.public.t

Target cluster: quickstart

> SHOW MATERIALIZED VIEWS
name cluster    comment
-----------------------
mv   quickstart ""
rp   quickstart ""

> ALTER MATERIALIZED VIEW mv APPLY REPLACEMENT rp

> SHOW MATERIALIZED VIEWS
name cluster    comment
-----------------------
mv   quickstart ""

> SELECT * FROM mv
3  2
7  4
11 6


# Test: aborted replacement workflow

> CREATE REPLACEMENT MATERIALIZED VIEW rp FOR mv AS SELECT -a as a, b FROM t

> SELECT * FROM mv
3  2
7  4
11 6

> SHOW MATERIALIZED VIEWS
name cluster    comment
-----------------------
mv   quickstart ""
rp   quickstart ""

> DROP MATERIALIZED VIEW rp

> SHOW MATERIALIZED VIEWS
name cluster    comment
-----------------------
mv   quickstart ""

> SELECT * FROM mv
3  2
7  4
11 6


# Test: replacement can be created in another cluster

> CREATE REPLACEMENT MATERIALIZED VIEW rp FOR mv IN CLUSTER other AS SELECT a, b FROM t;

> SHOW MATERIALIZED VIEWS
name cluster    comment
-----------------------
mv   quickstart ""
rp   other      ""

> ALTER MATERIALIZED VIEW mv APPLY REPLACEMENT rp

> SHOW MATERIALIZED VIEWS
name cluster comment
--------------------
mv   other   ""


> DROP MATERIALIZED VIEW mv CASCADE


# Test: replacement with downstream objects

> CREATE MATERIALIZED VIEW mv AS SELECT a, b FROM t

> SELECT * FROM mv
1 2
3 4
5 6

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
