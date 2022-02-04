# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify that transaction-stable functions, like "now", don't change during a transaction.

> CREATE TABLE now_inc (a TIMESTAMP)

> INSERT INTO now_inc VALUES (now())

# Sleep 2ms to ensure now() has increased, due to its ms resolution.
> SELECT mz_internal.mz_sleep(0.002)
<null>

# These execute in a single txn, so should be the same, and should
# produce 3 identical rows.
> BEGIN
> INSERT INTO now_inc VALUES (now()), (now())
> INSERT INTO now_inc VALUES (now())
> COMMIT

> SELECT mz_internal.mz_sleep(0.002)
<null>

> INSERT INTO now_inc VALUES (now())

> SELECT count(*) FROM now_inc GROUP BY a ORDER BY a
1
3
1

> CREATE TABLE dec (d DECIMAL)

# Verify that mz_logical_timestamp cannot be used in INSERTs (until
# we decide what it should do in an INSERT). Previously it could, but
# would produce different times even in a transaction (hence the test
# in this file). Although this error message is misleading, we don't
# expect users to do this, so it's ok for now.
! INSERT INTO dec VALUES (mz_logical_timestamp())
contains:calls to mz_logical_timestamp in write statements are not supported
