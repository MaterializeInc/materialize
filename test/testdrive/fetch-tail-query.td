# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that non-views can be used in TAIL.

$ set-regex match=\d{13,20} replacement=<TIMESTAMP>

> CREATE TABLE t1 (f1 INTEGER);

> INSERT INTO t1 VALUES (123);

> BEGIN

> DECLARE c CURSOR FOR TAIL (SELECT * FROM t1)

> FETCH ALL c
<TIMESTAMP> 1 123

> COMMIT

> TAIL (WITH a(x) AS (SELECT 'a') SELECT generate_series(1, 2), x FROM a)
<TIMESTAMP> 1 1 a
<TIMESTAMP> 1 2 a

> CREATE MATERIALIZED VIEW v1 AS SELECT count(*) FROM t1

> CREATE VIEW v2 AS SELECT 3

> BEGIN

> DECLARE c CURSOR FOR TAIL (SELECT * FROM v1, v2)

> FETCH ALL c
<TIMESTAMP> 1 1 3

> COMMIT

# Verify TAIL behavior when the query includes an ORDER BY and a LIMIT.

# Check that the initial output of the TAIL is correct.
> BEGIN
> DECLARE c CURSOR FOR TAIL (SELECT * FROM t1 ORDER BY 1 DESC LIMIT 1)
> FETCH ALL c
<TIMESTAMP> 1 123

# Insert a value from another connection that is not a new maximum.
$ postgres-connect name=alt url=postgres://materialize:materialize@${testdrive.materialized-addr}
$ postgres-execute connection=alt
INSERT INTO t1 VALUES (100)

# Verify that the TAIL does not emit any updates.
> FETCH ALL c WITH (timeout = '100ms')

# Insert a value from another connection that *is* a new maximum.
$ postgres-execute connection=alt
INSERT INTO t1 VALUES (124)

# Verify that the TAIL emits the new maximum.
> FETCH ALL c
<TIMESTAMP> -1 123
<TIMESTAMP> 1 124
