# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that the AS OF is observed for TAIL
#

$ set-regex match=\d{13} replacement=<TIMESTAMP>

> CREATE TABLE t1 (f1 INTEGER);
> CREATE DEFAULT INDEX ON t1

> INSERT INTO t1 VALUES (123);

> BEGIN

> DECLARE c CURSOR FOR TAIL t1 AS OF -1;

! FETCH 1 c;
contains:out of range integral type conversion attempted

> COMMIT

# Since FETCH isn't idempotent and can't be correctly retried by testdrive
# (although testdrive will try), wait until SELECT fails with this error
# (which will happen after t has been compacted) and then we should be
# able to see the same failure with FETCH.
! SELECT * FROM t1 AS OF 0
contains:Timestamp (0) is not valid for all inputs

> BEGIN

> DECLARE c CURSOR FOR TAIL t1 AS OF 0;

! FETCH 1 c;
contains:Timestamp (0) is not valid for all inputs

> COMMIT

> BEGIN

> DECLARE c CURSOR FOR TAIL t1 AS OF NOW();

> FETCH 1 c;
<TIMESTAMP> 1 123

> COMMIT

> BEGIN

> DECLARE c CURSOR FOR TAIL t1 AS OF 18446744073709551615;

# No rows expected
> FETCH 1 c WITH (timeout = '1s');
