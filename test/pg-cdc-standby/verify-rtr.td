# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Validate real-time-recency queries work with CDC data from a physical standby.
# This ensures RTR semantics are preserved when decoding from a standby replica.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET allow_real_time_recency = true

> SET TRANSACTION_ISOLATION = 'STRICT SERIALIZABLE'
> SET REAL_TIME_RECENCY TO TRUE

# Query the CDC table with RTR enabled to verify it works without throwing
> SELECT count(*) FROM t
1250

> SELECT v FROM t WHERE id = 1000
updated

> SELECT min(id), max(id) FROM t
251 1500

# Query the small table with RTR enabled
> SELECT * FROM small ORDER BY id
2 B
3 c
4 d

# Disable RTR and reset isolation level
> SET REAL_TIME_RECENCY TO FALSE
> SET TRANSACTION_ISOLATION = 'READ COMMITTED'
