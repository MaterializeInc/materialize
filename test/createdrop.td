# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# Test basic create and drop functionality

> CREATE MATERIALIZED VIEW v AS SELECT 42 AS a

> CREATE MATERIALIZED VIEW v2 AS SELECT 0 AS x

> CREATE INDEX i ON v(a)

# Test that creating objects of the same name does not work

! CREATE MATERIALIZED VIEW i AS SELECT 1.5 AS c
catalog item i already exists

! CREATE INDEX v ON v2(x)
catalog item v already exists

# Test that objects do not get dropped if the drop command does not specify the correct type
! DROP SOURCE v
v is not of type SOURCE

! DROP SOURCE IF EXISTS v
v is not of type SOURCE

! DROP INDEX v
v is not of type INDEX

! DROP INDEX IF EXISTS v
v is not of type INDEX

! DROP SOURCE i
i is not of type SOURCE

! DROP SOURCE IF EXISTS i
i is not of type SOURCE

! DROP VIEW i
i is not of type VIEW

! DROP VIEW IF EXISTS i
i is not of type VIEW

# Delete objects

> DROP INDEX i

> DROP VIEW v

# test that the names of the deleted objects can be used to create objects of other types
> CREATE MATERIALIZED VIEW i AS SELECT 84 AS b

> CREATE INDEX v ON i(b)

# Test that if exists works when the referred objects do not exist

> DROP INDEX IF EXISTS v

> DROP VIEW IF EXISTS i

# Test that if exists works when the referred objects do not exist

> DROP INDEX IF EXISTS v

> DROP VIEW IF EXISTS v
