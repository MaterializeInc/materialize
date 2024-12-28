# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

#
# Tests in support of https://github.com/MaterializeInc/materialize/pull/6471
# "optimize topk when limit=1 and input is monotonic (append-only)"
#

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET min_timestamp_interval = '100ms'

#
# Make sure that the general pattern of all queries in this file will use TopK and not some other future operator or optimization
#

# Remove references to internal table identifiers and "materialize.public" strings
$ set-regex match=(\s\(u\d+\)|materialize\.public\.) replacement=

? EXPLAIN SELECT (SELECT 'a' LIMIT 1);
Explained Query (fast path):
  Constant
    - ("a")

Target cluster: mz_catalog_server

$ set schema={"type": "record", "name": "schema", "fields": [ {"name": "f1", "type": ["int", "null"]} , {"name": "f2", "type": ["int", "null"]}] }

$ kafka-create-topic topic=top1

$ kafka-ingest format=avro topic=top1 schema=${schema} timestamp=1

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE t1
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-top1-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
  WITH (TIMESTAMP INTERVAL '100ms')

#
# Over constants
#

> SELECT (SELECT 'a' LIMIT 1);
a

> SELECT (SELECT 'a' ORDER BY 1 LIMIT 1);
a

> SELECT (SELECT 'a' GROUP BY 1 LIMIT 1);
a

> SELECT (SELECT 'a' ORDER BY 'a' LIMIT 1);
a

> SELECT (SELECT 'a' GROUP BY 'a' LIMIT 1);
a

#
# And now some actual materialized views
#

> CREATE MATERIALIZED VIEW limit_only AS SELECT (SELECT f1 FROM t1 LIMIT 1);

> CREATE MATERIALIZED VIEW group_by_limit AS SELECT (SELECT f1 FROM t1 GROUP BY f1 LIMIT 1);

> CREATE MATERIALIZED VIEW order_by_limit AS SELECT (SELECT f1 FROM t1 ORDER BY f1 LIMIT 1);

> CREATE MATERIALIZED VIEW order_by_desc_limit AS SELECT (SELECT f1 FROM t1 ORDER BY f1 DESC LIMIT 1);

> CREATE MATERIALIZED VIEW group_by_in_top_1 AS SELECT (select f2 FROM t1 AS inner WHERE inner.f1 = outer.f1 GROUP BY f2 LIMIT 1) FROM t1 AS outer;

> CREATE MATERIALIZED VIEW group_by_order_by_in_top_1 AS SELECT (select f2 FROM t1 AS inner WHERE inner.f1 = outer.f1 ORDER BY f2 DESC LIMIT 1) FROM t1 AS outer;

#
# Over an empty source
#

> SELECT * from limit_only;
<null>

> SELECT * from group_by_limit;
<null>

> SELECT * FROM order_by_limit;
<null>

> SELECT * from order_by_desc_limit;
<null>

> SELECT * FROM group_by_in_top_1;

> SELECT * FROM group_by_order_by_in_top_1;

#
# Over a source with a single record
#

$ kafka-ingest format=avro topic=top1 schema=${schema} timestamp=1
{"f1": {"int": 123}, "f2": {"int": -123} }

> SELECT * from limit_only;
123

> SELECT * from group_by_limit;
123

> SELECT * FROM order_by_limit;
123

> SELECT * from order_by_desc_limit;
123

> SELECT * FROM group_by_in_top_1;
-123

> SELECT * FROM group_by_order_by_in_top_1;
-123

#
# A second record arrives, causes the ORDER BY DESC view to change output
#

$ kafka-ingest format=avro topic=top1 schema=${schema} timestamp=2
{"f1": {"int": 234}, "f2": {"int" : -234} }

> SELECT * from limit_only;
123

> SELECT * from group_by_limit;
123

> SELECT * FROM order_by_limit;
123

> SELECT * from order_by_desc_limit;
234

> SELECT * FROM group_by_in_top_1;
-123
-234

> SELECT * FROM group_by_order_by_in_top_1;
-123
-234

#
# The third record causes all other views to change outputs
#

$ kafka-ingest format=avro topic=top1 schema=${schema} timestamp=3
{"f1": {"int": 0}, "f2": {"int": 0} }

> SELECT * from limit_only;
0

> SELECT * from group_by_limit;
0

> SELECT * FROM order_by_limit;
0

> SELECT * from order_by_desc_limit;
234

> SELECT * FROM group_by_in_top_1;
0
-123
-234

> SELECT * FROM group_by_order_by_in_top_1;
0
-123
-234

#
# Insert some more rows, mostly for the benefit of the "in_top_1" views
#

$ kafka-ingest format=avro topic=top1 schema=${schema} timestamp=4
{"f1": {"int": 0}, "f2": {"int": 0}}
{"f1": {"int": 234}, "f2": {"int": 0}}
{"f1": {"int": 123}, "f2": {"int": -234} }

> SELECT * from limit_only;
0

> SELECT * from group_by_limit;
0

> SELECT * FROM order_by_limit;
0

> SELECT * from order_by_desc_limit;
234

> SELECT * FROM group_by_in_top_1;
0
0
-234
-234
-234
-234

> SELECT * FROM group_by_order_by_in_top_1;
-123
-123
0
0
0
0

#
# And finally, insert some NULL values
#

$ kafka-ingest format=avro topic=top1 schema=${schema} timestamp=5
{"f1": null, "f2": null}
{"f1": {"int":0}, "f2": null}
{"f1": null, "f2": {"int": 0}}
{"f1": null, "f2": {"int": -234}}

> SELECT * from limit_only;
0

> SELECT * from group_by_limit;
0

> SELECT * FROM order_by_limit;
0

> SELECT * from order_by_desc_limit;
<null>

> SELECT * FROM group_by_in_top_1;
-234
-234
0
0
0
-234
-234
<null>
<null>
<null>

> SELECT * FROM group_by_order_by_in_top_1;
-123
-123
0
0
<null>
<null>
<null>
<null>
<null>
<null>

# Check arrangements, seeing new arrangements can mean a significant increase
# in memory consumptions and should be understood before adapting the values.
> SET cluster_replica = r1

> SELECT mdod.dataflow_name, mdod.name
  FROM mz_introspection.mz_arrangement_sharing mash
  JOIN mz_introspection.mz_dataflow_operator_dataflows mdod ON mash.operator_id = mdod.id
  JOIN mz_introspection.mz_compute_exports USING (dataflow_id)
  WHERE export_id LIKE 'u%'
"Dataflow: group_by_in_top_1" ArrangeBy[[Column(0)]]
"Dataflow: group_by_in_top_1" ArrangeBy[[Column(0)]]
"Dataflow: group_by_in_top_1" ArrangeBy[[Column(0)]]
"Dataflow: group_by_in_top_1" "Arranged DistinctBy"
"Dataflow: group_by_in_top_1" "Arranged DistinctBy"
"Dataflow: group_by_in_top_1" "Arranged MonotonicTop1 partial [val: empty]"
"Dataflow: group_by_in_top_1" DistinctBy
"Dataflow: group_by_in_top_1" DistinctBy
"Dataflow: group_by_in_top_1" DistinctByErrorCheck
"Dataflow: group_by_in_top_1" DistinctByErrorCheck
"Dataflow: group_by_in_top_1" MonotonicTop1
"Dataflow: group_by_limit" "Arranged DistinctBy"
"Dataflow: group_by_limit" "Arranged MonotonicTop1 partial [val: empty]"
"Dataflow: group_by_limit" DistinctBy
"Dataflow: group_by_limit" DistinctByErrorCheck
"Dataflow: group_by_limit" MonotonicTop1
"Dataflow: group_by_order_by_in_top_1" ArrangeBy[[Column(0)]]
"Dataflow: group_by_order_by_in_top_1" ArrangeBy[[Column(0)]]
"Dataflow: group_by_order_by_in_top_1" ArrangeBy[[Column(0)]]
"Dataflow: group_by_order_by_in_top_1" "Arranged DistinctBy"
"Dataflow: group_by_order_by_in_top_1" "Arranged MonotonicTop1 partial [val: empty]"
"Dataflow: group_by_order_by_in_top_1" DistinctBy
"Dataflow: group_by_order_by_in_top_1" DistinctByErrorCheck
"Dataflow: group_by_order_by_in_top_1" MonotonicTop1
"Dataflow: limit_only" "Arranged MonotonicTop1 partial [val: empty]"
"Dataflow: limit_only" MonotonicTop1
"Dataflow: order_by_desc_limit" "Arranged MonotonicTop1 partial [val: empty]"
"Dataflow: order_by_desc_limit" MonotonicTop1
"Dataflow: order_by_limit" "Arranged MonotonicTop1 partial [val: empty]"
"Dataflow: order_by_limit" MonotonicTop1
