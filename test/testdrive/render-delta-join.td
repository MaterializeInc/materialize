# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Remove both newlines, references to internal table identifiers, and "materialize.public" strings, all with a single regexp

$ set-regex match=(\s\(u\d+\)|\n|materialize\.public\.) replacement=

> create table t1(f1 int, f2 int);
> create index i1 on t1(f1);
> create table t2(f1 int, f2 int);
> create index i2 on t2(f1);
> insert into t1 values (1, 2);
> insert into t2 select s, s from generate_series(0, 2000) as s;

> select count(*) as count from t2;
count
----
2001

> create materialized view delta_join as select * from t1, t2 where t1.f1 = t2.f1;

> explain view delta_join;
"%0 =| Get t1| ArrangeBy (#0)%1 =| Get t2| ArrangeBy (#0)%2 =| Join %0 %1 (= #0 #2)| | implementation = DeltaQuery| |   delta %0 %1.(#0)| |   delta %1 %0.(#0)| | demand = (#0, #1, #3)| Filter !(isnull(#0))| Project (#0, #1, #0, #3)"

> select count(*) as count from delta_join;
count
----
1

# check that no dataflow channel reports more than a single message
> SELECT
    sum(sent) as sent
  FROM
    mz_catalog.mz_dataflow_channels AS channels
    LEFT JOIN mz_catalog.mz_message_counts AS counts
        ON channels.id = counts.channel AND channels.worker = counts.source_worker
  WHERE id IN
        (SELECT id
         FROM mz_catalog.mz_dataflow_operator_addresses
         WHERE address[1] =
             (SELECT DISTINCT address[1]
              FROM mz_catalog.mz_dataflow_operator_addresses
              WHERE id =
                  (SELECT DISTINCT id
                   FROM mz_catalog.mz_dataflow_names
                   WHERE name LIKE '%.delta_join%')))
  GROUP BY id, source_node, target_node, source_port, target_port
  HAVING sum(sent) > 0
  ORDER BY sent;
sent
----
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1

> insert into t2 select s, s from generate_series(2001, 4000) as s;
> select count(*) as count from t2;
count
----
4001

> SELECT
    sum(sent) as sent
  FROM
    mz_catalog.mz_dataflow_channels AS channels
    LEFT JOIN mz_catalog.mz_message_counts AS counts
        ON channels.id = counts.channel AND channels.worker = counts.source_worker
  WHERE id IN
        (SELECT id
         FROM mz_catalog.mz_dataflow_operator_addresses
         WHERE address[1] =
             (SELECT DISTINCT address[1]
              FROM mz_catalog.mz_dataflow_operator_addresses
              WHERE id =
                  (SELECT DISTINCT id
                   FROM mz_catalog.mz_dataflow_names
                   WHERE name LIKE '%.delta_join%')))
  GROUP BY id, source_node, target_node, source_port, target_port
  HAVING sum(sent) > 0
  ORDER BY sent;
sent
----
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
1
2
2
2
2
2
2000
2000
2000
