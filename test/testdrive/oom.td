# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that we generate errors instead of crashing due to out of memory issues

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_result_size = '1MB'

# In environmentd each Row requires 24 bytes + number of bytes to encode. But we'll also de-dupe
# repeated values by incrementing the diff.

> SELECT 1::int4 FROM generate_series(1, 10000), repeat('a', 100);
10000 values hashing to b223cca8b360eae4e49568512e2de29f

! SELECT * FROM generate_series(1, 10000), repeat('a', 100);
contains:result exceeds max size of 1.0 MiB

> CREATE TABLE t1 (a int4, b text)
> INSERT INTO t1 SELECT * FROM generate_series(1, 10000), repeat('a', 100);

! SELECT * FROM t1
contains:result exceeds max size of 1.0 MiB

! INSERT INTO t1 SELECT * FROM t1;
contains:result exceeds max size of 1.0 MiB

> INSERT INTO t1 SELECT * FROM generate_series(1, 100), repeat('a', 100);

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE t1;

# No output should be produced. Instead an error .. notice?
! FETCH 1 c;
contains:result exceeds max size of 1.0 MiB

> ROLLBACK;

# Constants with less than or equal to 10,000 rows will be evaluated in environmentd. Anything in excess of this will
# be sent to computed to be executed. Therefore, we need to set the number of rows high enough such that it will be evaluated by
# computed to test the computed side of things.

> SELECT generate_series::int4 FROM generate_series(1, 4);
1
2
3
4

! SELECT * FROM generate_series(1, 10001), repeat('a', 100)
contains:result exceeds max size of 1.0 MiB

> SELECT 1::int4 FROM generate_series(1, 10001)
10001 values hashing to 7e844fba503f0b3f02daa3de7c80938e

> CREATE TABLE t2 (a int4, b text)

! INSERT INTO t2 SELECT * FROM generate_series(1, 10001), repeat('a', 100);
contains:result exceeds max size of 1.0 MiB

> INSERT INTO t2 SELECT * FROM generate_series(1, 5000), repeat('a', 100);
> INSERT INTO t2 SELECT * FROM generate_series(5001, 10000), repeat('a', 100);

! SELECT * FROM t2
contains:result exceeds max size of 1.0 MiB

! INSERT INTO t2 SELECT * FROM t2;
contains:result exceeds max size of 1.0 MiB

# Regression test: subscribe should not fail when cumulative bytes across
# multiple FETCHes exceed max_result_size, as long as each individual batch
# is within the limit. Previously, stashed_result_size in
# PartitionedComputeState was never reset when batches were shipped, causing
# a monotonically growing counter to eventually trip the limit.

$ postgres-connect name=sub url=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
$ postgres-connect name=writer url=postgres://materialize:materialize@${testdrive.materialize-sql-addr}

> CREATE TABLE t_subscribe_cumulative (a int4, b text);

# Seed ~400KB of data (3000 rows * ~130 bytes each in wire format).
$ postgres-execute connection=writer
INSERT INTO t_subscribe_cumulative SELECT generate_series, repeat('a', 100) FROM generate_series(1, 3000);

# Start a subscribe on the separate connection.
$ postgres-execute connection=sub
BEGIN;
DECLARE c CURSOR FOR SUBSCRIBE t_subscribe_cumulative;
FETCH ALL c WITH (timeout = '5s');

# Second batch, cumulative ~800KB.
$ postgres-execute connection=writer
INSERT INTO t_subscribe_cumulative SELECT generate_series + 100000, repeat('a', 100) FROM generate_series(1, 3000);

$ postgres-execute connection=sub
FETCH ALL c WITH (timeout = '5s');

# Third batch, cumulative ~1.2MB — exceeds max_result_size of 1MB.
# Before the fix, this FETCH would fail with "total result exceeds max size".
$ postgres-execute connection=writer
INSERT INTO t_subscribe_cumulative SELECT generate_series + 200000, repeat('a', 100) FROM generate_series(1, 3000);

$ postgres-execute connection=sub
FETCH ALL c WITH (timeout = '5s');

# Fourth batch, cumulative ~1.6MB — well past the 1MB limit.
$ postgres-execute connection=writer
INSERT INTO t_subscribe_cumulative SELECT generate_series + 300000, repeat('a', 100) FROM generate_series(1, 3000);

$ postgres-execute connection=sub
FETCH ALL c WITH (timeout = '5s');

$ postgres-execute connection=sub
ROLLBACK;

> DROP TABLE t_subscribe_cumulative;

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET max_result_size

! SELECT csv_extract(9223372036854775807, '');
contains:attempt to create relation with too many columns, 9223372036854775807 max: 8192
