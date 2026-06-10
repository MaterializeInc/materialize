# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Regression test for https://github.com/MaterializeInc/database-issues/issues/1845
#
# panic with self-join
#

$ set-sql-timeout duration=125ms

> CREATE TABLE t1 (f1 INTEGER);

> CREATE TABLE t2 (f1 INTEGER, f2 INTEGER);

> CREATE INDEX i2 ON t2(f1);

> SELECT * FROM t1 WHERE f1 IN (SELECT a1 . f2 FROM t2 AS a1, t2 AS a2 WHERE a2.f1 = a1.f1);
