# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

! CREATE CLUSTER "." REPLICAS (r1 (SIZE '1'));
contains: invalid identifier

! CREATE CLUSTER ".." REPLICAS (r1 (SIZE '1'));
contains: invalid identifier

! CREATE DATABASE "."
contains: invalid identifier

! CREATE DATABASE ".."
contains: invalid identifier

! CREATE SCHEMA "."
contains: invalid identifier

! CREATE SCHEMA ".."
contains: invalid identifier

> CREATE SCHEMA s1000
! ALTER SCHEMA s1000 RENAME TO "."
contains: invalid identifier
> DROP SCHEMA s1000

! CREATE TABLE "." (x INT)
contains: invalid identifier

! CREATE TABLE ".." (x INT)
contains: invalid identifier

! CREATE TABLE "public"."." (x INT)
contains: invalid identifier

> CREATE TABLE "t.a.b" (x INT)
! ALTER TABLE "t.a.b" RENAME TO ".";
contains: invalid identifier

> CREATE SCHEMA "a"
> CREATE TABLE "a"."b" (x INT)
! CREATE INDEX "." ON "a"."b"(x);
contains: invalid identifier
> DROP TABLE "a"."b"
! ALTER SCHEMA "a" RENAME TO "."
contains: invalid identifier
> DROP SCHEMA "a"

! CREATE TABLE t1 ("." INT)
contains: invalid identifier

! CREATE TABLE t2 (".." INT)
contains: invalid identifier

> CREATE TABLE t3 ("x.y" INT)
> DROP TABLE t3

> CREATE TABLE t4 (x INT)
! ALTER TABLE t4 RENAME TO "."
contains: invalid identifier

> CREATE TABLE t5 (x INT)
! CREATE VIEW "." AS SELECT * FROM t5
contains: invalid identifier
> DROP TABLE t5 CASCADE

> CREATE TABLE t6 (x INT)
! CREATE VIEW ".." AS SELECT * FROM t6
contains: invalid identifier
> DROP TABLE t6 CASCADE

> CREATE TABLE t7 (x INT)
! CREATE VIEW v7 AS SELECT x AS "." FROM t7
contains: invalid identifier
> DROP TABLE t7 CASCADE

# only supported by Postgres ("\002E" is unicode for the dot)
! CREATE TABLE U&"\002E" (x int);
contains: found operator "&"

! CREATE VIEW "." AS SELECT 1
contains: invalid identifier

> CREATE VIEW v2 AS SELECT 1
! ALTER VIEW v2 RENAME TO "."
contains: invalid identifier
> DROP VIEW v2

! CREATE MATERIALIZED VIEW "." AS SELECT 1
contains: invalid identifier

> CREATE CLUSTER temp_cluster REPLICAS (r1 (SIZE '1'));

! CREATE SOURCE "." IN CLUSTER temp_cluster FROM WEBHOOK BODY FORMAT TEXT;
contains: invalid identifier

! CREATE SOURCE ".." IN CLUSTER temp_cluster FROM WEBHOOK BODY FORMAT TEXT;
contains: invalid identifier

> CREATE SOURCE "s.a.b" IN CLUSTER temp_cluster FROM WEBHOOK BODY FORMAT TEXT;
! ALTER SOURCE "s.a.b" RENAME TO ".";
contains: invalid identifier

> DROP CLUSTER temp_cluster CASCADE

! CREATE TYPE "." AS LIST (ELEMENT TYPE = int4);
contains: invalid identifier

! CREATE SECRET "." AS 'a';
contains: invalid identifier

> CREATE SECRET secret1 AS '.';
> DROP SECRET secret1

! SELECT 1 AS "."
contains: invalid identifier

! SELECT 2 AS ".."
contains: invalid identifier

! SELECT 1 "."
contains: invalid identifier

# Bad error messages due to complex code in the SQL parser that is hard to fix.
! SELECT * FROM (SELECT 1) AS "."
contains:Expected joined table, found number
! SELECT * FROM (SELECT 1 AS ".") x
contains:Expected joined table, found number
