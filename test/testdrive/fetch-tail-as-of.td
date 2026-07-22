# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that the AS OF is observed for SUBSCRIBE
#

$ set-regex match=\d{13,} replacement=<TIMESTAMP>

> CREATE TABLE t1 (f1 INTEGER);
> CREATE DEFAULT INDEX ON t1

> INSERT INTO t1 VALUES (123);

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE t1 AS OF -1;

! FETCH 1 c;
contains:AS OF or UP TO should be castable to a (non-null) mz_timestamp value

> COMMIT

# Since FETCH isn't idempotent and can't be correctly retried by testdrive
# (although testdrive will try), wait until SELECT fails with this error
# (which will happen after t has been compacted) and then we should be
# able to see the same failure with FETCH.
# (The error msg's details are tested in `test_subscribe_basic`.)
! SELECT * FROM t1 AS OF 0
contains:imestamp

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE t1 AS OF 0;

! FETCH 1 c;
contains:imestamp

> COMMIT

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE t1 AS OF AT LEAST 0;

> FETCH 1 c;
<TIMESTAMP> 1 123

> COMMIT

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE t1 AS OF 18446744073709551615;

# No rows expected
> FETCH 1 c WITH (timeout = '1s');

> COMMIT

# Regression test for CLU-169. `AS OF MAX UP TO MAX` describes an empty range
# (`as_of` equals the exclusive `up_to`), so the subscribe is guaranteed empty.
# Optimization used to trip the `is_single_time` invariant assertion, which
# expected an empty `until` whenever `as_of` was `MAX`, and surfaced as an
# internal optimizer error. The empty subscribe is now short-circuited without
# installing a dataflow.
> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE t1 AS OF 18446744073709551615 UP TO 18446744073709551615;

# No rows expected.
> FETCH ALL c WITH (timeout = '5s');

> COMMIT

# The short-circuited empty subscribe still honors `WITH (PROGRESS)`: it emits
# the initial progress row at the snapshot timestamp before completing, matching
# a subscribe that installs a dataflow.
> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE t1 WITH (PROGRESS = TRUE) AS OF 18446744073709551615 UP TO 18446744073709551615;

> FETCH ALL c WITH (timeout = '5s');
<TIMESTAMP> true <null> <null>

> COMMIT
