# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT COUNT(*) FROM t1;
500000
> SELECT COUNT(*) FROM t2;
500000

# We can encounter different errors here:
# - encounter an alter table error if the NOT NULL constraint dropped
# - a NULL decoding failure if the NOT NULL constraint is re-instated
#   after being dropped and MZ reboots.
! SELECT * FROM alter_fail_drop_constraint;
contains:Source error

! SELECT * FROM alter_fail_drop_col;
contains:has been altered

# Ensure non-definite errors are cleared.
> SELECT COUNT(*) = 0 FROM mz_internal.mz_source_statuses WHERE error LIKE '%Connection refused%';
true

! SELECT * FROM t0
contains:unknown catalog item 't0'
