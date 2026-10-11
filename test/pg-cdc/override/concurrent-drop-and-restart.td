# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test for a race condition where dropping a source concurrently with
# the upstream postgres connection dying can cause a panic in the remap
# operator.
#
# Root cause: FrontierUpper is reported from the timely dataflow capability
# graph, which can be AHEAD of what persist has actually committed. The
# controller uses this optimistic upper to compute step_back(upper) and
# advances its dependency read hold on the remap shard accordingly. If
# the source crashes before the persist write commits, remap_since has
# already been advanced to step_back(optimistic_upper) = T, but persist's
# actual data shard upper is still T (not T+1). On SuspendAndRestart,
# as_of = max(step_back(T), remap_since=T) = T = remap_upper, violating
# the invariant as_of < upper.

> CREATE SECRET pgpass AS 'postgres'

> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

> CREATE CLUSTER cdc_cluster SIZE '${arg.default-replica-size}'

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE t1 (pk INTEGER PRIMARY KEY, f2 TEXT);
ALTER TABLE t1 REPLICA IDENTITY FULL;
INSERT INTO t1 SELECT generate_series(1, 1000), 'data';

> BEGIN
> CREATE SOURCE mz_source
  IN CLUSTER cdc_cluster
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1)
> COMMIT

# Wait for initial snapshot to complete and data to be visible.
> SELECT count(*) FROM t1
1000

# Keep inserting data so the source is actively writing when we kill PG.
# This maximizes the chance that the dataflow capability advances (and
# FrontierUpper is reported) before the persist write commits, creating
# the window where the controller's dep hold on remap is ahead of
# persist's actual upper.
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 SELECT generate_series(1001, 40000), 'more_data';

> SELECT count(*) FROM t1
40000
