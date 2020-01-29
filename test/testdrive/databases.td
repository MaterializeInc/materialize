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

! CREATE SCHEMA s
schema 's' already exists

> DROP SCHEMA s

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

! CREATE DATABASE d
database 'd' already exists

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

> DROP DATABASE d

> SHOW DATABASES
Database
----
materialize


! CREATE VIEW noexist.ignored AS SELECT 1
unknown schema 'noexist'

! CREATE VIEW materialize.noexist.ignored AS SELECT 1
unknown schema 'noexist'

! CREATE VIEW noexist.ignored.ignored AS SELECT 1
unknown database 'noexist'
