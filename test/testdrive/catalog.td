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
SCHEMAS
-------
public
mz_temp
mz_catalog
pg_catalog

# Creating a schema should be reflected in the output of SHOW SCHEMAS.
> CREATE SCHEMA s
> SHOW SCHEMAS
SCHEMAS
-------
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
database
----
materialize

> SELECT id, database FROM mz_catalog.mz_databases
id          database
-----------------------
-1          AMBIENT
1           materialize

# Creating a database should be reflected in the output of SHOW DATABASES.
> CREATE DATABASE d
> SHOW DATABASES
database
----
d
materialize

> SELECT id, database FROM mz_catalog.mz_databases WHERE id != -1
id          database
-----------------------
1           materialize
2           d

# SHOW DATABASES should filter its output according to the provided LIKE or
# WHERE clause.
> SHOW DATABASES LIKE 'foo'
> SHOW DATABASES LIKE 'd'
d
> SHOW DATABASES LIKE 'mat%'
materialize
> SHOW DATABASES WHERE (SELECT database = database)
materialize
d
> SHOW DATABASES WHERE (id = (SELECT max(id) FROM mz_databases))
d
! SHOW DATABASES WHERE 7
WHERE clause error: no overload for bool AND i32

# Creating a database with a name that already exists should fail.
! CREATE DATABASE d
database 'd' already exists

# The new database should have a default public schema.
> SHOW SCHEMAS FROM d
SCHEMAS
-------
public

# New schemas in the database should appear in the output of SHOW SCHEMAS FROM.
> CREATE SCHEMA d.s
> SHOW SCHEMAS FROM d
SCHEMAS
-------
public
s

# SHOW SCHEMAS should filter its output based on the provided LIKE or WHERE
# clause.
> SHOW SCHEMAS LIKE 'pub%'
public
> SHOW SCHEMAS LIKE 'private'
> SHOW SCHEMAS WHERE schema = 'public'
public

# New views in the database should work.
> CREATE MATERIALIZED VIEW d.public.v1 AS SELECT 1
> SHOW VIEWS FROM d.public
VIEWS
-----
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
# SHOW DATABASES should work, even if the current database has been dropped.
> SHOW DATABASES
database
----
materialize
> SELECT id, database FROM mz_catalog.mz_databases
id          database
-----------------------
-1          AMBIENT
1           materialize

# The session database should remain set to the dropped database, but future
# queries that depend on the session database should fail with sensible error
# messages.
> SHOW DATABASE
d
! SELECT * FROM v
unknown catalog item 'v'

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
"mz_catalog, pg_catalog, public, mz_temp"
! SET search_path = foo
parameter search_path is read only

# Creating views in non-existent databases should fail.
! CREATE VIEW noexist.ignored AS SELECT 1
unknown schema 'noexist'
! CREATE VIEW materialize.noexist.ignored AS SELECT 1
unknown schema 'noexist'
! CREATE VIEW noexist.ignored.ignored AS SELECT 1
unknown database 'noexist'

# As should showing views.
! SHOW VIEWS FROM noexist
unknown schema 'noexist'
! SHOW VIEWS FROM noexist_db.noexist_schema
unknown database 'noexist_db'

# Dropping database with cross-schema dependencies is ok.
> CREATE DATABASE d1;
> CREATE SCHEMA d1.s1;
> CREATE VIEW d1.s1.t as select 1;
> CREATE VIEW d1.public.tt as select * from d1.s1.t;
> DROP DATABASE d1;

# Dropping database with cross-database dependencies is ok and drops the
# dependent views.
> CREATE DATABASE d1;
> CREATE VIEW d1.public.t as select 1;
> CREATE DATABASE d2;
> CREATE VIEW d2.public.t AS SELECT * FROM d1.public.t;
> DROP DATABASE d1;
> SHOW DATABASES
database
----
d
d2
materialize
> SELECT id, database FROM mz_catalog.mz_databases
id          database
-----------------------
-1          AMBIENT
1           materialize
2           d
4           d2

> SHOW VIEWS FROM d2.public;
 VIEWS
-------

# Check default sources, tables, and views in mz_catalog.

> SHOW SOURCES FROM mz_catalog
mz_arrangement_sharing
mz_arrangement_sizes
mz_dataflow_channels
mz_dataflow_operator_addresses
mz_dataflow_operators
mz_materialization_dependencies
mz_materializations
mz_peek_active
mz_peek_durations
mz_scheduling_elapsed
mz_scheduling_histogram
mz_scheduling_parks
mz_worker_materialization_frontiers

> SHOW FULL SOURCES FROM mz_catalog
SOURCES                              TYPE   MATERIALIZED
--------------------------------------------------------
mz_arrangement_sharing               SYSTEM true
mz_arrangement_sizes                 SYSTEM true
mz_dataflow_channels                 SYSTEM true
mz_dataflow_operator_addresses       SYSTEM true
mz_dataflow_operators                SYSTEM true
mz_materialization_dependencies      SYSTEM true
mz_materializations                  SYSTEM true
mz_peek_active                       SYSTEM true
mz_peek_durations                    SYSTEM true
mz_scheduling_elapsed                SYSTEM true
mz_scheduling_histogram              SYSTEM true
mz_scheduling_parks                  SYSTEM true
mz_worker_materialization_frontiers  SYSTEM true

> SHOW TABLES FROM mz_catalog
mz_avro_ocf_sinks
mz_kafka_sinks
mz_view_foreign_keys
mz_view_keys
mz_databases
mz_schemas
mz_columns
mz_indexes
mz_tables
mz_sources
mz_sinks
mz_views

> SHOW FULL TABLES FROM mz_catalog
TABLES                TYPE
----------------------------
mz_avro_ocf_sinks     SYSTEM
mz_kafka_sinks        SYSTEM
mz_view_foreign_keys  SYSTEM
mz_view_keys          SYSTEM
mz_databases          SYSTEM
mz_schemas            SYSTEM
mz_columns            SYSTEM
mz_indexes            SYSTEM
mz_tables             SYSTEM
mz_sinks              SYSTEM
mz_sources            SYSTEM
mz_views              SYSTEM

! SHOW EXTENDED TABLES
SHOW EXTENDED TABLES not yet supported

# `SHOW TABLES` and `mz_tables` should agree.
> SELECT COUNT(*) FROM mz_tables WHERE global_id LIKE 's%'
12

# There is one entry in mz_indexes for each field_number/expression of the index.
> SELECT COUNT(global_id) FROM mz_indexes WHERE global_id LIKE 's%'
75

> SHOW VIEWS FROM mz_catalog
mz_addresses_with_unit_length
mz_catalog_names
mz_dataflow_names
mz_dataflow_operator_dataflows
mz_materialization_frontiers
mz_objects
mz_perf_arrangement_records
mz_perf_dependency_frontiers
mz_perf_peek_durations_aggregates
mz_perf_peek_durations_bucket
mz_perf_peek_durations_core
mz_records_per_dataflow
mz_records_per_dataflow_global
mz_records_per_dataflow_operator
mz_relations

> SHOW FULL VIEWS FROM mz_catalog
VIEWS                             TYPE   MATERIALIZED
-------------------------------------------------------
mz_addresses_with_unit_length     SYSTEM false
mz_dataflow_names                 SYSTEM false
mz_dataflow_operator_dataflows    SYSTEM false
mz_materialization_frontiers      SYSTEM false
mz_objects                        SYSTEM false
mz_perf_arrangement_records       SYSTEM false
mz_perf_dependency_frontiers      SYSTEM false
mz_perf_peek_durations_aggregates SYSTEM false
mz_perf_peek_durations_bucket     SYSTEM false
mz_perf_peek_durations_core       SYSTEM false
mz_records_per_dataflow           SYSTEM false
mz_records_per_dataflow_global    SYSTEM false
mz_records_per_dataflow_operator  SYSTEM false
mz_relations                      SYSTEM false
mz_catalog_names                  SYSTEM false

> SHOW MATERIALIZED SOURCES FROM mz_catalog LIKE '%peek%';
mz_peek_active
mz_peek_durations

> SHOW VIEWS FROM mz_catalog LIKE '%peek%';
mz_perf_peek_durations_aggregates
mz_perf_peek_durations_bucket
mz_perf_peek_durations_core
