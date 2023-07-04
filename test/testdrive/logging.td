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

# In case the environment has other replicas
> SET cluster_replica = r1

$ set-regex match=s\d+ replacement=SID

> SELECT count(*) FROM (SELECT count(*) FROM mz_internal.mz_dataflow_addresses);
1

> SELECT count(*) FROM (SELECT count(*) FROM mz_internal.mz_dataflow_channel_operators);
1

> SELECT count(*) FROM (SELECT count(*) FROM mz_internal.mz_dataflow_channels);
1

> SELECT count(*) FROM (SELECT count(*) FROM mz_internal.mz_dataflow_operator_dataflows);
1

> SELECT count(*) FROM (SELECT count(*) FROM mz_internal.mz_dataflow_operator_parents);
1

> SELECT count(*) FROM (SELECT count(*) FROM mz_internal.mz_dataflow_operator_reachability);
1

> SELECT count(*) FROM (SELECT count(*) FROM mz_internal.mz_dataflow_operators);
1

> SELECT count(*) FROM (SELECT count(*) FROM mz_internal.mz_dataflows);
1

> SELECT count(*) FROM (SELECT count (*) FROM mz_internal.mz_scheduling_elapsed);
1

> SELECT count(*) FROM (SELECT count (*) FROM mz_internal.mz_scheduling_parks_histogram);
1

> SELECT count(*) FROM (SELECT count (*) FROM mz_internal.mz_compute_operator_durations_histogram);
1

> SELECT count(*) FROM (SELECT count (*) FROM mz_internal.mz_arrangement_sharing);
1

> SELECT count(*) FROM (SELECT count (*) FROM mz_internal.mz_arrangement_sizes);
1

> SELECT count(*) FROM (SELECT count (*) FROM mz_internal.mz_compute_exports);
1

> SELECT count(*) FROM (SELECT count (*) FROM mz_internal.mz_compute_frontiers);
1

> SELECT count(*) FROM (SELECT count (*) FROM mz_internal.mz_compute_import_frontiers);
1

> SELECT count(*) FROM (SELECT count (*) FROM mz_internal.mz_compute_delays_histogram);
1

> SELECT count(*) FROM (SELECT count (*) FROM mz_internal.mz_compute_dependencies);
1

> SELECT count(*) FROM (SELECT count (*) FROM mz_internal.mz_active_peeks);
1

> SELECT count(*) FROM (SELECT count (*) FROM mz_internal.mz_peek_durations_histogram);
1

> SELECT count(*) FROM (SELECT count (*) FROM mz_internal.mz_records_per_dataflow);
1

> SELECT count(*) FROM (SELECT count (*) FROM mz_internal.mz_records_per_dataflow_operator);
1

> SELECT count(*) FROM (SELECT count (*) FROM mz_internal.mz_message_counts);
1

! DROP SCHEMA mz_internal
contains:cannot drop schema mz_internal because it is required by the database system

! DROP VIEW mz_internal.mz_compute_frontiers
contains:cannot drop view mz_internal.mz_compute_frontiers because it is required by the database system

! DROP SOURCE mz_internal.mz_compute_delays_histogram_raw
contains:cannot drop source mz_internal.mz_compute_delays_histogram_raw because it is required by the database system

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_peek_durations_histogram_per_worker'
  ORDER BY position
id      name        position    type
--------------------------------------
SID   worker_id   1           uint8
SID   duration_ns 2           uint8
SID   count       3           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_scheduling_elapsed_per_worker'
  ORDER BY position
id      name        position    type
--------------------------------------
SID   id          1           uint8
SID   worker_id   2           uint8
SID   elapsed_ns  3           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_compute_operator_durations_histogram_per_worker'
  ORDER BY position
id      name        position    type
--------------------------------------
SID   id          1           uint8
SID   worker_id   2           uint8
SID   duration_ns 3           uint8
SID   count       4           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_scheduling_parks_histogram_per_worker'
  ORDER BY position
id      name           position    type
-----------------------------------------
SID   worker_id      1           uint8
SID   slept_for_ns   2           uint8
SID   requested_ns   3           uint8
SID   count          4           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_message_counts_per_worker'
  ORDER BY position
id      name              position    type
--------------------------------------------
SID   channel_id        1           uint8
SID   from_worker_id    2           uint8
SID   to_worker_id      3           uint8
SID   sent              4           bigint
SID   received          5           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_dataflow_operator_reachability_per_worker'
  ORDER BY position
id      name        position    type
--------------------------------------
SID   address     1           list
SID   port        2           uint8
SID   worker_id   3           uint8
SID   update_type 4           text
SID   time        5           mz_timestamp
SID   count       6           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_arrangement_sizes_per_worker'
  ORDER BY position
id      name        position    type
--------------------------------------
SID   operator_id 1           uint8
SID   worker_id   2           uint8
SID   records     3           bigint
SID   batches     4           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_arrangement_sharing_per_worker'
  ORDER BY position
id      name        position    type
--------------------------------------
SID   operator_id 1           uint8
SID   worker_id   2           uint8
SID   count       3           bigint

> SELECT mz_columns.id, mz_columns.name, position, type
  FROM mz_views JOIN mz_columns USING (id)
  WHERE mz_views.name = 'mz_compute_delays_histogram_per_worker'
  ORDER BY position
id      name        position    type
--------------------------------------
SID   export_id   1           text
SID   import_id   2           text
SID   worker_id   3           uint8
SID   delay_ns    4           uint8
SID   count       5           bigint

> SELECT mz_columns.id, mz_columns.name, position, mz_columns.type
  FROM mz_sources JOIN mz_columns USING (id)
  WHERE mz_sources.name = 'mz_compute_import_frontiers_per_worker'
  ORDER BY position
id      name        position    type
--------------------------------------
SID   export_id   1           text
SID   import_id   2           text
SID   worker_id   3           uint8
SID   time        4           mz_timestamp
