# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test for a subset of the information returned by introspection sources.
# The test focuses on computing answers that are independent of particular
# timing measurement data, even if these stable answers may take some time
# to appear in the results of queries to introspection sources.

# Note that we count on the retry behavior of testdrive in this test
# since introspection sources may take some time to catch up.

> CREATE TABLE t (a int)

> CREATE MATERIALIZED VIEW mv AS SELECT * FROM t

> SELECT count_all > 0
  FROM (SELECT SUM(count) as count_all
        FROM
            mz_materialized_views AS views,
            mz_worker_materialization_delays AS delays
        WHERE
            views.name = 'mv' AND
            views.id = delays.dataflow)
true

> CREATE VIEW vv AS SELECT * FROM t

> SELECT COUNT(*)
  FROM (SELECT DISTINCT delays.dataflow, delays.source
        FROM
            mz_worker_materialization_delays AS delays)
1

> CREATE DEFAULT INDEX ON vv

> SELECT COUNT(*)
  FROM (SELECT DISTINCT delays.dataflow, delays.source
        FROM
            mz_worker_materialization_delays AS delays)
2

> SELECT count_all > 0
  FROM (SELECT SUM(count) as count_all
        FROM
            mz_views AS views,
            mz_indexes AS indexes,
            mz_worker_materialization_delays AS delays
        WHERE
            views.name = 'vv' AND
            views.id = indexes.on_id AND
            indexes.id = delays.dataflow)
true

> DROP INDEX vv_primary_idx

> SELECT COUNT(*)
  FROM (SELECT DISTINCT delays.dataflow, delays.source
        FROM
            mz_worker_materialization_delays AS delays)
1

> DROP MATERIALIZED VIEW mv

> SELECT COUNT(*)
  FROM (SELECT DISTINCT delays.dataflow, delays.source
        FROM
            mz_worker_materialization_delays AS delays)
0
