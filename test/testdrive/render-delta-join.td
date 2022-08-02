# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Remove both newlines, references to internal table identifiers, and "materialize.public" strings, all with a single regexp
$ set-regex match=(\s\(u\d+\)|\n|materialize\.public\.) replacement=

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

# This causes a delta join to be executed.
> SELECT COUNT(*) FROM delta_join;
count
----
1

> EXPLAIN VIEW delta_join;
"%0 =| Get t1| ArrangeBy (#0)%1 =| Get t2| ArrangeBy (#0)%2 =| Join %0 %1 (= #0 #2)| | implementation = DeltaQuery| |   delta %0 %1.(#0)| |   delta %1 %0.(#0)| Filter (#0) IS NOT NULL| Project (#0, #1, #0, #3)"

> SELECT count(*) AS count FROM delta_join;
count
----
1

# The purpose of this test is to check that only the first delta path sees updates
# at start-up time. According to the plan above, only t1's path will see them, so
# the delta path for t2 won't see the 2000 rows in t2. 100 is used as an arbitrary
# threshold since the actual number of messages sent depends on the number of
# workers.
> SELECT
    sum(sent) as sent
  FROM
    mz_catalog.mz_dataflow_channels AS channels
    LEFT JOIN mz_catalog.mz_message_counts AS counts
        ON channels.id = counts.channel AND channels.worker = counts.source_worker
  WHERE id IN
        (SELECT id
         FROM mz_catalog.mz_dataflow_addresses
         WHERE address[1] =
             (SELECT DISTINCT address[1]
              FROM mz_catalog.mz_dataflow_addresses
              WHERE id =
                  (SELECT DISTINCT id
                   FROM mz_catalog.mz_dataflow_names
                   WHERE name LIKE '%.delta_join%')))
  GROUP BY id, source_node, target_node, source_port, target_port
  HAVING sum(sent) > 100
  ORDER BY sent;
sent
----
