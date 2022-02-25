# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify that compaction frontiers advance when there are multiple
# materialized views on some source.

> CREATE TABLE t1 (f1 INTEGER, f2 INTEGER);
> CREATE DEFAULT INDEX on t1

> CREATE MATERIALIZED VIEW v1 AS SELECT * FROM t1;

> CREATE MATERIALIZED VIEW v2 AS SELECT * FROM t1;

# Sleep enough for the frontier to advance

> SELECT mz_internal.mz_sleep(2);
<null>

# Check that we are not allowed to go back to a time before
# the frontier

! SELECT * FROM t1 AS OF NOW() - INTERVAL '1 second';
contains:is not valid for all inputs

! SELECT * FROM v1 AS OF NOW() - INTERVAL '1 second';
contains:is not valid for all inputs

! SELECT * FROM v2 AS OF NOW() - INTERVAL '1 second';
contains:is not valid for all inputs
