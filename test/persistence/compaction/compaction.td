# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Smoke tests that verify compaction occurs as expected.
#

$ set-sql-timeout duration=30s

> CREATE MATERIALIZED VIEW most_recent_mz_metrics AS
  SELECT * FROM
  (SELECT DISTINCT metric FROM mz_metrics) grp,
  LATERAL (
    SELECT value FROM mz_metrics
    WHERE metric = grp.metric
    ORDER BY time DESC LIMIT 1)

# At the beginning there are no trace batches.
> SELECT value FROM most_recent_mz_metrics where metric = 'mz_persist_trace_blob_count'
value
-----
0

> CREATE TABLE compaction (f1 INTEGER);

> INSERT INTO compaction VALUES (1);

# The insert should make it into trace, and form 1 trace batch.
> SELECT value FROM most_recent_mz_metrics where metric = 'mz_persist_trace_blob_count'
value
-----
1

# Read after write to coerce different writes to happen at different timestamps.
> SELECT * FROM compaction
f1
----
1

> INSERT INTO compaction VALUES (2);

> SELECT * FROM compaction
f1
----
1
2

# The previous insert should get compacted into one batch with the one before it.
> SELECT value FROM most_recent_mz_metrics where metric = 'mz_persist_trace_blob_count'
value
-----
1

> INSERT INTO compaction VALUES (3);

> SELECT * FROM compaction
f1
----
1
2
3

# The most recent insert forms a new batch because it is at a lower compaction level.
> SELECT value FROM most_recent_mz_metrics where metric = 'mz_persist_trace_blob_count'
value
-----
2

> INSERT INTO compaction VALUES (4);

> SELECT * FROM compaction
f1
----
1
2
3
4

# The previous insert should compact together with the previous batch, which then compacts recursively
# up, leaving just one batch.
> SELECT value FROM most_recent_mz_metrics where metric = 'mz_persist_trace_blob_count'
value
-----
1

> INSERT INTO compaction VALUES (5);

> SELECT * FROM compaction
f1
----
1
2
3
4
5

# The previous insert forms a new batch.
> SELECT value FROM most_recent_mz_metrics where metric = 'mz_persist_trace_blob_count'
value
-----
2

> INSERT INTO compaction VALUES (6);

> SELECT * FROM compaction
f1
----
1
2
3
4
5
6

# The previous insert gets merged together with the one just before it.
> SELECT value FROM most_recent_mz_metrics where metric = 'mz_persist_trace_blob_count'
value
-----
2

> INSERT INTO compaction VALUES (7);

> SELECT * FROM compaction
f1
----
1
2
3
4
5
6
7

# The previous insert forms a new batch.
> SELECT value FROM most_recent_mz_metrics where metric = 'mz_persist_trace_blob_count'
value
-----
3

> INSERT INTO compaction VALUES (8);

> SELECT * FROM compaction
f1
----
1
2
3
4
5
6
7
8

# TODO: This suddenly started flaking fairly frequently with nothing related in
# persist seemingly changing. Reenable when we figure out what's going on.
#
# # All of the previously inserted batches get compacted together into one batch.
# > SELECT value FROM most_recent_mz_metrics where metric = 'mz_persist_trace_blob_count'
# value
# -----
# 1
