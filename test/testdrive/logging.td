# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test only verifies that the log relations are published, not that they
# have any specific output.

> CREATE MATERIALIZED VIEW count_operates AS SELECT count(*) FROM mz_dataflow_operators;
> SELECT count(*) FROM count_operates;
1

> CREATE MATERIALIZED VIEW count_channels AS SELECT count(*) FROM mz_dataflow_channels;
> SELECT count(*) FROM count_channels;
1

> CREATE MATERIALIZED VIEW count_elapsed AS SELECT count(*) FROM mz_scheduling_elapsed;
> SELECT count(*) FROM count_elapsed;
1

> CREATE MATERIALIZED VIEW count_histogram AS SELECT count(*) FROM mz_scheduling_histogram;
> SELECT count(*) FROM count_histogram;
1

> CREATE MATERIALIZED VIEW count_arrangement AS SELECT count(*) FROM mz_arrangement_sizes;
> SELECT count(*) FROM count_arrangement;
1

> CREATE MATERIALIZED VIEW count_materializations AS SELECT count(*) FROM mz_materializations;
> SELECT count(*) FROM count_materializations;
1

> CREATE MATERIALIZED VIEW count_frontiers AS SELECT count(*) FROM mz_materialization_frontiers;
> SELECT count(*) FROM count_frontiers;
1

> CREATE MATERIALIZED VIEW count_peeks AS SELECT count(*) FROM mz_peek_active;
> SELECT count(*) FROM count_peeks;
1

> CREATE MATERIALIZED VIEW count_peek_durations AS SELECT count(*) FROM mz_peek_durations;
> SELECT count(*) FROM count_peek_durations;
1

> CREATE MATERIALIZED VIEW count_messages AS SELECT count(*) FROM mz_message_counts;
> SELECT count(*) FROM count_messages;
1

! DROP SCHEMA mz_catalog
contains:cannot drop schema mz_catalog because it is required by the database system

! DROP VIEW mz_peek_durations
contains:cannot drop item mz_catalog.mz_peek_durations because it is required by the database system

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_scheduling_elapsed'
  ORDER BY position
id        name        position    type
----------------------------------------
s5027.1   id          1           bigint
s5027.1   worker      2           bigint
s5027.1   elapsed_ns  3           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_scheduling_histogram'
  ORDER BY position
id        name        position    type
----------------------------------------
s5028.1   id          1           bigint
s5028.1   worker      2           bigint
s5028.1   duration_ns 3           bigint
s5028.1   count       4           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_scheduling_parks'
  ORDER BY position
id        name        position    type
----------------------------------------
s5029.1   worker      1           bigint
s5029.1   slept_for   2           bigint
s5029.1   requested   3           bigint
s5029.1   count       4           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_message_counts'
  ORDER BY position
id        name            position    type
--------------------------------------------
s5030.1   channel         1           bigint
s5030.1   source_worker   2           bigint
s5030.1   target_worker   3           bigint
s5030.1   sent            4           bigint
s5030.1   received        5           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_dataflow_operator_reachability'
  ORDER BY position
id        name        position    type
----------------------------------------
s5031.1   address     1           list
s5031.1   port        2           bigint
s5031.1   worker      3           bigint
s5031.1   update_type 4           text
s5031.1   timestamp   5           bigint
s5031.1   count       6           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_arrangement_sizes'
  ORDER BY position
id        name        position    type
----------------------------------------
s5032.1   operator    1           bigint
s5032.1   worker      2           bigint
s5032.1   records     3           bigint
s5032.1   batches     4           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_arrangement_sharing'
  ORDER BY position
id        name        position    type
----------------------------------------
s5033.1   operator    1           bigint
s5033.1   worker      2           bigint
s5033.1   count       3           bigint
