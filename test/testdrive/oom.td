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

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET max_result_size

! SELECT csv_extract(9223372036854775807, '');
contains:attempt to create relation with too many columns, 9223372036854775807 max: 8192
