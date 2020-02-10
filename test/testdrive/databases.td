# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# What schemas do we have by default?
> SHOW SCHEMAS
public
> SHOW EXTENDED SCHEMAS
Schema
----
public
mz_catalog
pg_catalog

# Creating a schema should be reflected in the output of SHOW SCHEMAS.
> CREATE SCHEMA s
> SHOW SCHEMAS
Schema
----
public
s

# Creating a schema with a name that already exists should fail.
! CREATE SCHEMA s
schema 's' already exists

# Dropping a schema with a view should only succeed with CASCADE.
> CREATE VIEW s.v AS SELECT 1
! DROP SCHEMA s
schema 'materialize.s' cannot be dropped without CASCADE while it contains objects
> DROP SCHEMA s CASCADE

# Dropping a schema with no objects should succeed without CASCADE.
> CREATE SCHEMA s
> CREATE VIEW s.v AS SELECT 1
> DROP VIEW s.v
> DROP SCHEMA s

# What databases do we have by default?
> SHOW DATABASES
Database
----
materialize

# Creating a database should be reflected in the output of SHOW DATABASES.
> CREATE DATABASE d
> SHOW DATABASES
Database
----
d
materialize

# Creating a database with a name that already exists should fail.
! CREATE DATABASE d
database 'd' already exists

# The new database should have a default public schema.
> SHOW SCHEMAS FROM d
Schema
----
public

# New schemas in the database should appear in the output of SHOW SCHEMAS FROM.
> CREATE SCHEMA d.s
> SHOW SCHEMAS FROM d
Schema
----
public
s

# New views in the database should work.
> CREATE MATERIALIZED VIEW d.public.v1 AS SELECT 1
> SHOW VIEWS FROM d.public
View
----
v1

# Setting the session database should update name resolution appropriately.
> SET DATABASE = d
> SHOW DATABASE
d
> SHOW SCHEMAS
public
s
> CREATE MATERIALIZED VIEW v2 AS SELECT 2
> SHOW VIEWS
v1
v2
> SELECT * FROM v1 UNION ALL SELECT * FROM v2
1
2

# DROP DATABASE should not support RESTRICT or CASCADE.
! DROP DATABASE d RESTRICT
Expected end of statement, found: RESTRICT
! DROP DATABASE d RESTRICT
Expected end of statement, found: RESTRICT

# DROP DATABASE should succeed even when there are objects in the database.
> DROP DATABASE d
> SHOW DATABASES
Database
----
materialize

# The session database should remain set to the dropped database, but future
# queries that depend on the session database should fail with sensible error
# messages.
> SHOW DATABASE
d
! SELECT * FROM v
unknown database 'd'

# But queries that do not depend on the session database should work fine.
> CREATE VIEW materialize.public.v AS SELECT 1
> CREATE DATABASE d

# Dropping the public schema is okay, but dropping the catalog schemas is not.
> DROP SCHEMA public
! DROP SCHEMA mz_catalog
cannot drop schema mz_catalog because it is required by the database system
! DROP SCHEMA pg_catalog
cannot drop schema pg_catalog because it is required by the database system

# Schema names that start with "mz_" or "pg_" are reserved for future use by the
# system.
! CREATE SCHEMA mz_foo
unacceptable schema name 'mz_foo'
! CREATE SCHEMA pg_bar
unacceptable schema name 'pg_bar'

# The search path is currently hardcoded.
> SHOW search_path
"mz_catalog, pg_catalog, public"
! SET search_path = foo
parameter search_path is read only

# Creating views in non-existent databases should fail.
! CREATE VIEW noexist.ignored AS SELECT 1
unknown schema 'noexist'
! CREATE VIEW materialize.noexist.ignored AS SELECT 1
unknown schema 'noexist'
! CREATE VIEW noexist.ignored.ignored AS SELECT 1
unknown database 'noexist'
