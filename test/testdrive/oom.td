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

# int4 takes 5 bytes, 4 for the data and 1 for the tag.

> SELECT 1::int4 FROM generate_series(1, 4) as a;
1
1
1
1

! SELECT 1::int4 FROM generate_series(1, 5) as a;
contains:result exceeds max size of 20 bytes

> CREATE TABLE t (a int4)

> INSERT INTO t SELECT 1::int4 FROM generate_series(1, 4) as a;

> SELECT * FROM t
1
1
1
1

> INSERT INTO t VALUES (1);

! SELECT * FROM t
contains:result exceeds max size of 20 bytes

! INSERT INTO t SELECT * FROM t;
contains:result exceeds max size of 20 bytes

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET max_result_size
