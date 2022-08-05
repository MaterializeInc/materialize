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

$ set-regex match=s\d+ replacement=SID

> CREATE VIEW count_operates AS SELECT count(*) FROM mz_dataflow_operators;
> SELECT count(*) FROM count_operates;
1

> CREATE VIEW count_channels AS SELECT count(*) FROM mz_dataflow_channels;
> SELECT count(*) FROM count_channels;
1

> CREATE VIEW count_elapsed AS SELECT count(*) FROM mz_scheduling_elapsed;
> SELECT count(*) FROM count_elapsed;
1

> CREATE VIEW count_histogram AS SELECT count(*) FROM mz_scheduling_histogram;
> SELECT count(*) FROM count_histogram;
1

> CREATE VIEW count_arrangement AS SELECT count(*) FROM mz_arrangement_sizes;
> SELECT count(*) FROM count_arrangement;
1

> CREATE VIEW count_materializations AS SELECT count(*) FROM mz_materializations;
> SELECT count(*) FROM count_materializations;
1

> CREATE VIEW count_frontiers AS SELECT count(*) FROM mz_materialization_frontiers;
> SELECT count(*) FROM count_frontiers;
1

> CREATE VIEW count_source_frontiers AS SELECT count(*) FROM mz_materialization_source_frontiers;
> SELECT count(*) FROM count_source_frontiers;
1

> CREATE VIEW count_frontier_delays AS SELECT count(*) FROM mz_worker_materialization_delays;
> SELECT count(*) FROM count_frontier_delays;
1

> CREATE VIEW count_peeks AS SELECT count(*) FROM mz_peek_active;
> SELECT count(*) FROM count_peeks;
1

> CREATE VIEW count_peek_durations AS SELECT count(*) FROM mz_peek_durations;
> SELECT count(*) FROM count_peek_durations;
1

> CREATE VIEW count_messages AS SELECT count(*) FROM mz_message_counts;
> SELECT count(*) FROM count_messages;
1

! DROP SCHEMA mz_catalog
contains:cannot drop schema mz_catalog because it is required by the database system

! DROP VIEW mz_peek_durations
contains:cannot drop item mz_catalog.mz_peek_durations because it is required by the database system

! DROP SOURCE mz_worker_materialization_delays
contains:cannot drop item mz_catalog.mz_worker_materialization_delays because it is required by the database system

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_scheduling_elapsed'
  ORDER BY position
id      name        position    type
--------------------------------------
SID   id          1           bigint
SID   worker      2           bigint
SID   elapsed_ns  3           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_scheduling_histogram'
  ORDER BY position
id      name        position    type
--------------------------------------
SID   id          1           bigint
SID   worker      2           bigint
SID   duration_ns 3           bigint
SID   count       4           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_scheduling_parks'
  ORDER BY position
id      name        position    type
--------------------------------------
SID   worker      1           bigint
SID   slept_for   2           bigint
SID   requested   3           bigint
SID   count       4           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_message_counts'
  ORDER BY position
id      name            position    type
------------------------------------------
SID   channel         1           bigint
SID   source_worker   2           bigint
SID   target_worker   3           bigint
SID   sent            4           bigint
SID   received        5           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_dataflow_operator_reachability'
  ORDER BY position
id      name        position    type
--------------------------------------
SID   address     1           list
SID   port        2           bigint
SID   worker      3           bigint
SID   update_type 4           text
SID   timestamp   5           bigint
SID   count       6           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_arrangement_sizes'
  ORDER BY position
id      name        position    type
--------------------------------------
SID   operator    1           bigint
SID   worker      2           bigint
SID   records     3           bigint
SID   batches     4           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_arrangement_sharing'
  ORDER BY position
id      name        position    type
--------------------------------------
SID   operator    1           bigint
SID   worker      2           bigint
SID   count       3           bigint

> SELECT mz_columns.id, mz_columns.name, position, mz_columns.type
  FROM mz_sources JOIN mz_columns USING (id)
  WHERE mz_sources.name = 'mz_worker_materialization_delays'
  ORDER BY position
id      name        position    type
--------------------------------------
SID   global_id   1           text
SID   source      2           text
SID   worker      3           bigint
SID   delay_ns    4           bigint
SID   count       5           bigint

> SELECT mz_columns.id, mz_columns.name, position, mz_columns.type
  FROM mz_sources JOIN mz_columns USING (id)
  WHERE mz_sources.name = 'mz_worker_materialization_source_frontiers'
  ORDER BY position
id      name        position    type
--------------------------------------
SID   global_id   1           text
SID   source      2           text
SID   worker      3           bigint
SID   time        4           bigint
