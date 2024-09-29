# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Remove references to internal table identifiers and "materialize.public" strings
$ set-regex match=(\s\(u\d+\)|materialize\.public\.) replacement=

> CREATE TABLE t1(f1 INT, f2 INT);
> CREATE INDEX i1 ON t1(f1);
> CREATE TABLE t2(f1 INT, f2 INT);
> CREATE INDEX i2 ON t2(f1);
> INSERT INTO t1 VALUES (1, 2);
> INSERT INTO t2 SELECT s, s FROM generate_series(0, 2000) AS s;

> select count(*) as count from t2;
count
----
2001

> CREATE VIEW delta_join (t1_f1, t1_f2, t2_f1, t2_f2) AS SELECT * FROM t1, t2 WHERE t1.f1 = t2.f1;

# This should cause a delta join to be executed.
# But this test is currently broken: https://github.com/MaterializeInc/database-issues/issues/4772
> SELECT COUNT(*) FROM delta_join;
count
----
1

? EXPLAIN WITH(join implementations) SELECT * FROM delta_join;
Explained Query:
  Project (#0, #1, #0, #3)
    Filter (#0) IS NOT NULL
      Join on=(#0 = #2) type=differential
        implementation
          %0:t1[#0]KA » %1:t2[#0]KA
        ArrangeBy keys=[[#0]]
          ReadIndex on=t1 i1=[differential join]
        ArrangeBy keys=[[#0]]
          ReadIndex on=t2 i2=[differential join]

Used Indexes:
  - i1 (differential join)
  - i2 (differential join)

Target cluster: quickstart

> SELECT count(*) AS count FROM delta_join;
count
----
1

# The purpose of this test is to check that only the first delta path sees updates
# at start-up time. According to the plan above, only t1's path will see them, so
# the delta path for t2 won't see the 2000 rows in t2. 100 is used as an arbitrary
# threshold since the actual number of messages sent depends on the number of
# workers.

# In case the environment has other replicas
> SET cluster_replica = r1

> SELECT
    sent
  FROM
    mz_introspection.mz_dataflow_channels AS channels
    LEFT JOIN mz_introspection.mz_message_counts AS counts
        ON channels.id = counts.channel_id
  WHERE
    id IN
        (SELECT id
         FROM mz_introspection.mz_dataflow_addresses
         WHERE address[1] =
             (SELECT id
              FROM mz_introspection.mz_dataflows
              WHERE name LIKE '%.delta_join%'))
    AND sent > 100
  ORDER BY sent;
sent
----
