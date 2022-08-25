# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Tests in support of https://github.com/MaterializeInc/materialize/pull/6471
# "optimize topk when limit=1 and input is monotonic (append-only)"
#

#
# Make sure that the general pattern of all queries in this file will use TopK and not some other future operator or optimization
#

# Remove both newlines, references to internal table identifiers, and "materialize.public" strings, all with a single regexp
$ set-regex match=(\s\(u\d+\)|\n|materialize\.public\.) replacement=
> EXPLAIN SELECT (SELECT 'a' LIMIT 1);
"%0 =| Constant (\"a\")"

$ set schema={"type": "record", "name": "schema", "fields": [ {"name": "f1", "type": ["int", "null"]} , {"name": "f2", "type": ["int", "null"]}] }

$ kafka-create-topic topic=top1

$ kafka-ingest format=avro topic=top1 schema=${schema} publish=true timestamp=1

> CREATE SOURCE t1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-top1-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE NONE
  WITH (TIMESTAMP GRANULARITY '100ms')

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

$ kafka-ingest format=avro topic=top1 schema=${schema} publish=true timestamp=1
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

$ kafka-ingest format=avro topic=top1 schema=${schema} publish=true timestamp=2
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

$ kafka-ingest format=avro topic=top1 schema=${schema} publish=true timestamp=3
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

$ kafka-ingest format=avro topic=top1 schema=${schema} publish=true timestamp=4
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

$ kafka-ingest format=avro topic=top1 schema=${schema} publish=true timestamp=5
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
