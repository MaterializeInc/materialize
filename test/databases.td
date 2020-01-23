# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

> SHOW SCHEMAS
public

> SHOW EXTENDED SCHEMAS
Schema
----
public
mz_catalog
pg_catalog

> CREATE SCHEMA s

> SHOW SCHEMAS
Schema
----
public
s

# TODO(benesch): fix DROP SCHEMA.
! DROP SCHEMA s
catalog item 's' does not exist

> SHOW DATABASES
Database
----
materialize

> CREATE DATABASE d

> SHOW DATABASES
Database
----
d
materialize

> SHOW SCHEMAS FROM d
Schema
----
public

> CREATE SCHEMA d.s

> SHOW SCHEMAS FROM d
Schema
----
public
s

# TODO(benesch): implement DROP DATABASE.
! DROP DATABASE d
unsupported SQL statement
