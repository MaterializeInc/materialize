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
ALTER SYSTEM SET enable_expressions_in_limit_syntax TO true;

# Test monotonic top-k processing with k > 1.

$ set non-dbz-schema={
    "type": "record",
    "name": "cpx",
    "fields": [
      {"name": "a", "type": [ "null", "long" ]},
      {"name": "b", "type": "long"}
    ]
  }

$ kafka-create-topic topic=non-dbz-data

$ kafka-ingest format=avro topic=non-dbz-data schema=${non-dbz-schema} timestamp=1
{"a": {"long": 1}, "b": 1}
{"a": {"long": 1}, "b": 2}
{"a": {"long": 1}, "b": 3}
{"a": {"long": 1}, "b": 4}
{"a": {"long": 1}, "b": 5}
{"a": {"long": 2}, "b": 1000}
{"a": {"long": 2}, "b": 1001}
{"a": {"long": 2}, "b": 1002}
{"a": {"long": 2}, "b": 1003}
{"a": {"long": 2}, "b": 1004}
{"a": {"long": 3}, "b": 2000}
{"a": {"long": 3}, "b": 2000}
{"a": {"long": 4}, "b": 3001}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE non_dbz_data
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-non-dbz-data-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  ENVELOPE NONE

# Create a monotonic topk plan that has both a limit and a group to test that thinning works as expected
> SELECT * FROM (SELECT DISTINCT a FROM non_dbz_data) grp, LATERAL (SELECT b FROM non_dbz_data WHERE a = grp.a ORDER BY b LIMIT 2);
a b
---------
1 1
1 2
2 1000
2 1001
3 2000
3 2000
4 3001

# Create a topk plan that has a limit expression that will initially not fail, but
# then eventually fail after the addition of offending data to a monotonic input.
# We observe that a non-monotonic top-k plan can recover from this condition after
# the addition of even more data, but a monotonic top-k plan cannot.
> CREATE VIEW v_basic AS
  SELECT *
  FROM
    (SELECT sum(a) AS s FROM non_dbz_data GROUP BY a) grp,
    LATERAL (
      SELECT b FROM non_dbz_data
      WHERE (grp.s IS NULL AND a IS NULL) OR a = grp.s
      ORDER BY b LIMIT 6 / abs(grp.s+2)
    );

> CREATE VIEW v_monotonic AS
  SELECT *
  FROM
    (SELECT DISTINCT a::numeric FROM non_dbz_data) grp,
    LATERAL (
      SELECT b FROM non_dbz_data
      WHERE (grp.a IS NULL AND a IS NULL) OR a = grp.a
      ORDER BY b LIMIT 6 / abs(grp.a+2)
    );

> CREATE DEFAULT INDEX ON v_basic;

> CREATE DEFAULT INDEX ON v_monotonic;

> SELECT * FROM v_basic ORDER BY s;
s b
----
4 3001

> SELECT * FROM v_monotonic ORDER BY a;
a b
----
1 1
1 2
2 1000
2 1001
3 2000
4 3001

$ kafka-ingest format=avro topic=non-dbz-data schema=${non-dbz-schema} timestamp=2
{"a": {"long": 5}, "b": 4001}
{"a": {"long": 5}, "b": 4002}
{"a": null, "b": 0}
{"a": null, "b": 1}

> SELECT * FROM v_basic ORDER BY s;
s b
----
4 3001
5 4001
<null> 0
<null> 1

> SELECT * FROM v_monotonic ORDER BY a;
a b
----
1 1
1 2
2 1000
2 1001
3 2000
4 3001
5 4001
<null> 0
<null> 1

$ kafka-ingest format=avro topic=non-dbz-data schema=${non-dbz-schema} timestamp=3
{"a": {"long": -1}, "b": -1}
{"a": {"long": -1}, "b": -2}
{"a": {"long": -2}, "b": -1001}

! SELECT * FROM v_basic ORDER BY s;
contains:division by zero

! SELECT * FROM v_monotonic ORDER BY a;
contains:division by zero

$ kafka-ingest format=avro topic=non-dbz-data schema=${non-dbz-schema} timestamp=4
{"a": {"long": -1}, "b": -3}
{"a": {"long": -2}, "b": -1002}
{"a": {"long": -3}, "b": -2001}

> SELECT * FROM v_basic ORDER BY s;
s b
----
-3 -2001
-3 -2001
4 3001
5 4001
<null> 0
<null> 1

! SELECT * FROM v_monotonic ORDER BY a;
contains:division by zero

> DROP VIEW v_basic;

> DROP VIEW v_monotonic;

# The following tests repeat the scenario in database-issues#5442.
$ set other-non-dbz-schema={
    "type": "record",
    "name": "cpx",
    "fields": [
      {"name": "a", "type": "long" },
      {"name": "b", "type": "long"}
    ]
  }

$ kafka-create-topic topic=other-non-dbz-data

$ kafka-ingest format=avro topic=other-non-dbz-data schema=${other-non-dbz-schema} timestamp=1
{"a": 1, "b": 42}
{"a": 2, "b": 42}
{"a": 3, "b": 42}
{"a": 4, "b": 42}
{"a": 5, "b": 42}
{"a": 6, "b": 42}
{"a": 7, "b": 42}
{"a": 8, "b": 42}
{"a": 9, "b": 42}
{"a": 10, "b": 42}
{"a": 11, "b": 42}
{"a": 12, "b": 42}
{"a": 13, "b": 42}
{"a": 14, "b": 42}
{"a": 15, "b": 42}
{"a": 16, "b": 42}
{"a": 17, "b": 42}
{"a": 18, "b": 42}
{"a": 19, "b": 42}
{"a": 20, "b": 42}
{"a": 21, "b": 42}
{"a": 22, "b": 42}
{"a": 23, "b": 42}
{"a": 24, "b": 42}
{"a": 25, "b": 42}
{"a": 26, "b": 42}
{"a": 27, "b": 42}
{"a": 28, "b": 42}
{"a": 29, "b": 42}
{"a": 30, "b": 42}
{"a": 31, "b": 42}
{"a": 32, "b": 42}
{"a": 33, "b": 42}
{"a": 34, "b": 42}
{"a": 35, "b": 42}
{"a": 36, "b": 42}
{"a": 37, "b": 42}
{"a": 38, "b": 42}
{"a": 39, "b": 42}

> CREATE SOURCE other_non_dbz_data
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-other-non-dbz-data-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${other-non-dbz-schema}'
  ENVELOPE NONE

> SELECT sum(a) FROM (SELECT a FROM other_non_dbz_data ORDER BY b LIMIT 37);
sum
----
703

> CREATE VIEW v_other AS
  SELECT a FROM other_non_dbz_data ORDER BY b LIMIT 37;

> CREATE DEFAULT INDEX ON v_other;

> SELECT * FROM v_other;
a
----
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37

# Check arrangements, seeing new arrangements can mean a significant increase
# in memory consumptions and should be understood before adapting the values.
> SET cluster_replica = r1

>[version>=15000] SELECT mdod.dataflow_name, mdod.name
  FROM mz_introspection.mz_arrangement_sharing mash
  JOIN mz_introspection.mz_dataflow_operator_dataflows mdod ON mash.operator_id = mdod.id
  JOIN mz_introspection.mz_compute_exports USING (dataflow_id)
  WHERE export_id LIKE 'u%'
"Dataflow: materialize.public.v_other_primary_idx" "ArrangeBy[[Column(0, \"a\")]]"
"Dataflow: materialize.public.v_other_primary_idx" "ArrangeBy[[Column(0, \"a\")]]-errors"
"Dataflow: materialize.public.v_other_primary_idx" "Arranged TopK input"
"Dataflow: materialize.public.v_other_primary_idx" "Reduced TopK input"
