# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for https://github.com/MaterializeInc/materialize/issues/13790

> CREATE TABLE t (a int)

> CREATE MATERIALIZED VIEW mv AS SELECT * FROM t

# Note: We rely on testdrive's retry behavior here, as it takes some time for
# the logging to catch up.

> SELECT time > 0
  FROM
    mz_materialized_views AS views,
    mz_materialization_frontiers AS frontiers
  WHERE
    views.name = 'mv' AND
    views.id = frontiers.global_id
true
