# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# With storaged down, the upper of t1 will not advance. However, the global timestamp will advance.
# In strict serializable mode we may select a timestamp that is ahead of on of the table's upper and hang forever.
> SET transaction_isolation = serializable

# Increased from the default because of CI flakiness.
$ set-sql-timeout duration=180s

> SELECT COUNT(*) FROM t1;
0

> SET transaction_isolation = 'strict serializable'
