# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test concurrent DDL transactions

> CREATE TABLE foo (a INT)
> INSERT INTO foo VALUES (42)
> BEGIN
> SELECT * FROM foo
42

$ postgres-connect name=ddl url=postgres://materialize:materialize@${testdrive.materialized-addr}

$ postgres-execute connection=ddl
DROP TABLE foo

! SELECT * FROM foo
contains:unknown catalog item 'foo'

> ROLLBACK


> CREATE TABLE foo (a INT)
> INSERT INTO foo VALUES (42)
> BEGIN
> SELECT * FROM foo
42

$ postgres-connect name=ddl url=postgres://materialize:materialize@${testdrive.materialized-addr}

$ postgres-execute connection=ddl
DROP TABLE foo

> COMMIT
