# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=125ms

$ set-regex match=(\s\(u\d+\)|materialize\.public\.|\s\d\d:\d\d:\d\d\.\d\d\d\d\d\d) replacement=

> CREATE TABLE t1 (col_null INTEGER, col_not_null INTEGER NOT NULL);

? EXPLAIN (TIMING true) SELECT * FROM t1;
%0 =
| Get t1

Decorrelation time:
Optimization time:

? EXPLAIN (TIMING false, TIMING true) SELECT * FROM t1;
%0 =
| Get t1

Decorrelation time:
Optimization time:

? EXPLAIN (TIMING false) SELECT * FROM t1;
%0 =
| Get t1

? EXPLAIN (TIMING true, TIMING false) SELECT * FROM t1;
%0 =
| Get t1

? EXPLAIN (TIMING true) RAW PLAN FOR SELECT * FROM t1;
%0 =
| Get t1

? EXPLAIN (TIMING true) DECORRELATED PLAN FOR SELECT * FROM t1;
%0 = Let l0 =
| Constant ()

%1 =
| Get t1

%2 =
| Join %0 %1
| | implementation = Unimplemented

Decorrelation time:
