# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that we generate errors instead of crashing due to out of memory issues

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-sql-addr-internal}

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_result_size = 20

> SELECT 1::int4 FROM generate_series(1, 4) as a;
1
1
1
1

! SELECT 1::int4 FROM generate_series(1, 5) as a;
contains:result exceeds max size of 20 bytes

> CREATE TABLE t1 (a int4)

> INSERT INTO t1 SELECT 1::int4 FROM generate_series(1, 4) as a;

> INSERT INTO t1 VALUES (5);

! SELECT * FROM t1
contains:result exceeds max size of 20 bytes

! INSERT INTO t1 SELECT * FROM t1;
contains:result exceeds max size of 20 bytes

# Constants with less than or equal to 10,000 rows will be evaluated in environmentd. Anything in excess of this will
# be sent to computed to be executed. Therefore, we need to set the size high enough such that it will be evaluated by
# computed to test the computed side of things.
$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_result_size = 50000;

# int4 takes 5 bytes, 4 for the data and 1 for the tag.

> SELECT generate_series::int4 FROM generate_series(1, 4);
1
2
3
4

! SELECT generate_series::int4 FROM generate_series(1, 10001)
contains:result exceeds max size of 50000 bytes

! SELECT 1::int4 FROM generate_series(1, 10001)
contains:result exceeds max size of 50000 bytes

> CREATE TABLE t2 (a int4)

! INSERT INTO t2 SELECT generate_series::int4 FROM generate_series(1, 10001);
contains:result exceeds max size of 50000 bytes

> INSERT INTO t2 SELECT generate_series::int4 FROM generate_series(1, 10000);

> INSERT INTO t2 VALUES (10000);

! SELECT * FROM t2
contains:result exceeds max size of 50000 bytes

! INSERT INTO t2 SELECT * FROM t2;
contains:result exceeds max size of 50000 bytes

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET max_result_size
