# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE TABLE t1 (f1 INTEGER, f2 INTEGER);

> CREATE INDEX i1 ON t1 (f2);

> CREATE VIEW v1 AS SELECT * FROM t1;

$ postgres-connect name=conn1 url=postgres://materialize:materialize@${testdrive.materialized-sql-addr}

$ postgres-connect name=conn2 url=postgres://materialize:materialize@${testdrive.materialized-sql-addr}

$ postgres-execute connection=conn1
BEGIN;
SELECT * FROM v1;

$ postgres-execute connection=conn2
BEGIN;
SELECT * FROM v1;

$ postgres-execute connection=conn1
SELECT * FROM v1;

$ postgres-execute connection=conn2
SELECT * FROM v1;

$ postgres-execute connection=conn1
SELECT * FROM v1;

$ postgres-execute connection=conn2
SELECT * FROM v1;

$ postgres-execute connection=conn1
SELECT * FROM v1;

$ postgres-execute connection=conn2
SELECT * FROM v1;
