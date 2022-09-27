# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Check that the timeout value is indeed respected and sleeping happens
# if no rows are available to satify the FETCH immediately
#

#
# FETCH + SUBSCRIBE - sleep happens
#

> CREATE TABLE t1 (f1 INTEGER);

> CREATE TABLE ts_log (ts TIMESTAMP);

> INSERT INTO ts_log VALUES (NOW());

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE t1;

> FETCH 1 c WITH (timeout='1.1s');

> COMMIT;

> INSERT INTO ts_log VALUES (NOW());

> SELECT MAX(ts) - MIN(ts) > interval '1 second' FROM ts_log;
true

> DROP TABLE ts_log;

#
# FETCH + SELECT - timeout not observed, sleep does not happen
#

> BEGIN

> DECLARE c CURSOR FOR SELECT * FROM t1;

> FETCH 1 c WITH (timeout='1d');
