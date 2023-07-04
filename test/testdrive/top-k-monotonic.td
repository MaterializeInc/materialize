# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test monotonic top-k processing with k > 1.

$ set non-dbz-schema={
    "type": "record",
    "name": "cpx",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "long"}
    ]
  }

$ kafka-create-topic topic=non-dbz-data

$ kafka-ingest format=avro topic=non-dbz-data schema=${non-dbz-schema} timestamp=1
{"a": 1, "b": 1}
{"a": 1, "b": 2}
{"a": 1, "b": 3}
{"a": 1, "b": 4}
{"a": 1, "b": 5}
{"a": 2, "b": 1000}
{"a": 2, "b": 1001}
{"a": 2, "b": 1002}
{"a": 2, "b": 1003}
{"a": 2, "b": 1004}
{"a": 3, "b": 2000}
{"a": 3, "b": 2000}
{"a": 4, "b": 3001}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE SOURCE non_dbz_data
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

# The following tests repeat the scenario in #18445.
$ kafka-create-topic topic=other-non-dbz-data

$ kafka-ingest format=avro topic=other-non-dbz-data schema=${non-dbz-schema} timestamp=1
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
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-other-non-dbz-data-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
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
