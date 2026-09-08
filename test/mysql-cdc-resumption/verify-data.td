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

# The PK tables snapshot as per-worker PK ranges. The distinct-count and the
# exact checksums catch duplicated or dropped ranges that COUNT(*) alone can
# mask when a duplicate offsets a loss.
# t1pk: even pks deleted, leaving odd pks 1..499999 with f2 = 3 * pk.
> SELECT COUNT(*), COUNT(DISTINCT pk) FROM t1pk;
250000 250000
> SELECT SUM(f2) FROM t1pk;
187500000000
# t2pk: odd f2 deleted, leaving f2 = 2, 4, .., 250000.
> SELECT COUNT(*), COUNT(DISTINCT pk) FROM t2pk;
125000 125000
> SELECT SUM(f2) FROM t2pk;
15625125000

# We can encounter different errors here:
# - encounter an alter table error if the NOT NULL constraint dropped
# - a NULL decoding failure if the NOT NULL constraint is re-instated
#   after being dropped and MZ reboots.
! SELECT * FROM alter_fail_drop_constraint;
contains:Source error

! SELECT * FROM alter_fail_drop_col;
contains:incompatible schema change

# Ensure non-definite errors are cleared.
> SELECT COUNT(*) = 0 FROM mz_internal.mz_source_statuses WHERE error LIKE '%Connection refused%';
true

! SELECT * FROM t0
contains:unknown catalog item 't0'
