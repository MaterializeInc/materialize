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
ALTER SYSTEM SET max_result_size = 128

# Each inline row takes 24 bytes of memory, 23 for the inline array and 1 for the length.

> SELECT 1::int4 FROM generate_series(1, 5);
1
1
1
1
1

! SELECT 1::int4 FROM generate_series(1, 6);
contains:result exceeds max size of 128 B

> CREATE TABLE t1 (a int4)

> INSERT INTO t1 SELECT 1::int4 FROM generate_series(1, 5);

> INSERT INTO t1 VALUES (6);

! SELECT * FROM t1
contains:result exceeds max size of 128 B

! INSERT INTO t1 SELECT * FROM t1;
contains:result exceeds max size of 128 B

> INSERT INTO t1 SELECT * FROM generate_series(1, 100);

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE t1;

# No output should be produced. Instead an error .. notice?
! FETCH 1 c;
contains:result exceeds max size of 128 B

> ROLLBACK;

# Constants with less than or equal to 10,000 rows will be evaluated in environmentd. Anything in excess of this will
# be sent to computed to be executed. Therefore, we need to set the size high enough such that it will be evaluated by
# computed to test the computed side of things.
$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_result_size = 240000;

> SELECT generate_series::int4 FROM generate_series(1, 4);
1
2
3
4

! SELECT generate_series::int4 FROM generate_series(1, 10001)
contains:result exceeds max size of 240.0 KB

! SELECT 1::int4 FROM generate_series(1, 10001)
contains:result exceeds max size of 240.0 KB

> CREATE TABLE t2 (a int4)

! INSERT INTO t2 SELECT generate_series::int4 FROM generate_series(1, 10001);
contains:result exceeds max size of 240.0 KB

> INSERT INTO t2 SELECT generate_series::int4 FROM generate_series(1, 10000);

> INSERT INTO t2 VALUES (10000);

! SELECT * FROM t2
contains:result exceeds max size of 240.0 KB

! INSERT INTO t2 SELECT * FROM t2;
contains:result exceeds max size of 240.0 KB

# Rows keep 23 bytes inline, after that the row is spilled to the heap. int4 takes 5 bytes,
# 4 for the int and 1 for the tag. A row of 5 int4's will spill to the heap, but any less will
# be kept inline. A row of 5 int4's should then take 25 + 24 = 49 bytes.
#
# Large numbers are used to avoid this test being defeated
# by the optimization in https://github.com/MaterializeInc/materialize/pull/21016

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_result_size = 49

> SELECT 17000000, 17000001, 17000002, 17000003;
17000000 17000001 17000002 17000003

> SELECT 17000000, 17000001, 17000002, 17000003, 17000004;
17000000 17000001 17000002 17000003 17000004

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_result_size = 48

! SELECT 17000000, 17000001, 17000002, 17000003, 17000004;
contains:result exceeds max size of 48 B

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET max_result_size

! SELECT csv_extract(9223372036854775807, '');
contains:attempt to create relation with too many columns, 9223372036854775807 max: 8192
