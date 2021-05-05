# Copyright Materialize, Inc. All rights reserved.
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
