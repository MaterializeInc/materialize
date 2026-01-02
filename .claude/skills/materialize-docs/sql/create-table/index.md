---
audience: developer
canonical_url: https://materialize.com/docs/sql/create-table/
complexity: intermediate
description: '`CREATE TABLE` creates a table that is persisted in durable storage.'
doc_type: reference
keywords:
- 'Private Preview:'
- CREATE VIEWS
- Private Preview
- CREATE TABLE
- 'Note:'
product_area: Indexes
status: experimental
title: CREATE TABLE
---

# CREATE TABLE

## Purpose
`CREATE TABLE` creates a table that is persisted in durable storage.

If you need to understand the syntax and options for this command, you're in the right place.


`CREATE TABLE` creates a table that is persisted in durable storage.


`CREATE TABLE` defines a table that is persisted in durable storage.

In Materialize, you can create:
- Read-write tables. With read-write tables, users can read ([`SELECT`]) and
  write to the tables ([`INSERT`], [`UPDATE`], [`DELETE`]).

-  ***Private Preview***. Read-only tables from [PostgreSQL sources (new
  syntax)](/sql/create-source/postgres-v2/). Users cannot be write ([`INSERT`],
  [`UPDATE`], [`DELETE`]) to these tables. These tables are populated by [data
  ingestion from a source](/ingest-data/postgres/). <!-- Unresolved shortcode: {{% include-example file="examples/create_table/ex... -->


Tables in Materialize are similar to tables in standard relational databases:
they consist of rows and columns where the columns are fixed when the table is
created.

Tables can be joined with other tables, materialized views, views, and
subsources; and you can create views/materialized views/indexes on tables.


[//]: # "TODO(morsapaes) Bring back When to use a table? once there's more
clarity around best practices."

## Syntax

This section covers syntax.

#### Read-write table

### Read-write table

<!-- Unresolved shortcode: {{% include-example file="examples/create_table/ex... -->

<!-- Unresolved shortcode: {{% include-example file="examples/create_table/ex... -->

#### PostgreSQL source table

### PostgreSQL source table

> **Private Preview:** This feature is in private preview.

> **Note:** 
<!-- Unresolved shortcode: {{% include-example file="examples/create_table/ex... -->


<!-- Unresolved shortcode: {{% include-example file="examples/create_table/ex... -->

<!-- Unresolved shortcode: {{% include-example file="examples/create_table/ex... -->


## Read-write tables

This section covers read-write tables.

### Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/sql/identifiers/#naming-restrictions).

### Known limitations

Tables do not currently support:

- Primary keys
- Unique constraints
- Check constraints

See also the known limitations for [`INSERT`](/sql/insert#known-limitations),
[`UPDATE`](/sql/update#known-limitations), and [`DELETE`](/sql/delete#known-limitations).

## PostgreSQL source tables

> **Private Preview:** This feature is in private preview.

> **Note:** 
<!-- Unresolved shortcode: {{% include-example file="examples/create_table/ex... -->


### Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/sql/identifiers/#naming-restrictions).

<a name="supported-db-source-types"></a>

### Read-only tables

Source-populated tables are **read-only** tables. Users **cannot** perform write
operations
([`INSERT`](/sql/insert/)/[`UPDATE`](/sql/update/)/[`DELETE`](/sql/delete/)) on
these tables.


### Source-populated tables and snapshotting

Creating the tables from sources starts the [snapshotting](/ingest-data/#snapshotting) process. Snapshotting syncs the
currently available data into Materialize. Because the initial snapshot is
persisted in the storage layer atomically (i.e., at the same ingestion
timestamp), you are not able to query the table until snapshotting is complete.

> **Note:** 

During the snapshotting, the data ingestion for
the existing tables for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.


### Supported data types

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->

### Handling table schema changes

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->

#### Incompatible schema changes

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->

### Upstream table truncation restrictions

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->

### Inherited tables

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all types used in the table definition.
- `USAGE` privileges on the schemas that all types in the statement are
  contained in.


## Examples

This section covers examples.

### Create a table (User-populated)

<!-- Unresolved shortcode: {{% include-example file="examples/create_table/ex... -->

Once a user-populated table is created, you can perform CRUD
(Create/Read/Update/Write) operations on it.

<!-- Unresolved shortcode: {{% include-example file="examples/create_table/ex... -->

<!-- Unresolved shortcode: {{% include-example file="examples/create_table/ex... -->

### Create a table (PostgreSQL source)

> **Private Preview:** This feature is in private preview.

> **Note:** 

<!-- Unresolved shortcode: {{% include-example file="examples/create_table/ex... -->

The example assumes you have configured your upstream PostgreSQL 11+ (i.e.,
enabled logical replication, created the publication for the various tables and
replication user, and updated the network configuration).

For details about configuring your upstream system, see the [PostgreSQL
integration guides](/ingest-data/postgres/#supported-versions-and-services).


<!-- Unresolved shortcode: {{% include-example file="examples/create_table/ex... -->

Source-populated tables are **read-only** tables. Users **cannot** perform write
operations
([`INSERT`](/sql/insert/)/[`UPDATE`](/sql/update/)/[`DELETE`](/sql/delete/)) on
these tables.


<!-- Unresolved shortcode: {{% include-example file="examples/create_table/ex... -->


## Related pages

- [`INSERT`]
- [`DROP TABLE`](/sql/drop-table)

[`INSERT`]: /sql/insert/
[`SELECT`]: /sql/select/
[`UPDATE`]: /sql/update/
[`DELETE`]: /sql/delete/