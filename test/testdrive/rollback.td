# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify that ROLLBACK allows the since to advance. After the 2s sleep,
# the SELECT AS OF should fail because the since should have advanced. If
# the SELECT AS OF succeeds, it is because the old timestamp is still
# (incorrectly) available.

> CREATE TABLE t1 (f1 INTEGER);
> CREATE DEFAULT INDEX ON t1

> BEGIN

> SELECT * FROM t1;

> ROLLBACK;

> SELECT mz_internal.mz_sleep(2);
<null>

! SELECT * FROM t1 AS OF NOW() - INTERVAL '1 second';
contains:is not valid for all inputs
