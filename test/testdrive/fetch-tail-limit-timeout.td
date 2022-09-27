# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that specifying a limit for FETCH works with timeout
#

$ set-regex match=\d{13} replacement=<TIMESTAMP>

> CREATE TABLE v1 (f1 INTEGER);

> INSERT INTO v1 VALUES (123);

> SELECT * FROM v1;
123

> BEGIN

> DECLARE c CURSOR FOR SELECT * FROM v1;

> FETCH 1 c WITH (timeout = '1d');
123

> COMMIT

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE v1;

> FETCH 1 c WITH (timeout = '1d');
<TIMESTAMP> 1 123

> COMMIT

> INSERT INTO v1 VALUES (234);

> INSERT INTO v1 VALUES (345);

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE v1;

> FETCH 3 c WITH (timeout = '1d');
<TIMESTAMP> 1 123
<TIMESTAMP> 1 234
<TIMESTAMP> 1 345

> COMMIT

> BEGIN

> DECLARE c CURSOR FOR SELECT * FROM v1;

> FETCH 3 c WITH (timeout = '1d');
123
234
345
