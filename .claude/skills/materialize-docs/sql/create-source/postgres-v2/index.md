---
audience: developer
canonical_url: https://materialize.com/docs/sql/create-source/postgres-v2/
complexity: advanced
description: Creates a new source from PostgreSQL 11+.
doc_type: reference
keywords:
- CREATE MULTIPLE
- 'Private Preview:'
- CREATE SOURCE
- a single
- CREATE TABLE
- 'Tip:'
- 'replication

  slot'
- CREATE TABLES
- 'CREATE SOURCE: PostgreSQL (New Syntax)'
product_area: Sources
status: experimental
title: 'CREATE SOURCE: PostgreSQL (New Syntax)'
---

# CREATE SOURCE: PostgreSQL (New Syntax)

## Purpose
Creates a new source from PostgreSQL 11+.

If you need to understand the syntax and options for this command, you're in the right place.


Creates a new source from PostgreSQL 11+.


> **Private Preview:** This feature is in private preview.


<!-- Unresolved shortcode: {{% create-source-intro external_source="PostgreSQ... -->

## Prerequisites

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->

## Syntax

<!-- Unresolved shortcode: {{% include-example file="examples/create_source/e... -->

<!-- Unresolved shortcode: {{% include-example file="examples/create_source/e... -->

## Details

This section covers details.

### Ingesting data

After a source is created, you can create tables from the source, referencing
the tables in the publication, to start ingesting data. You can create multiple
tables that reference the same table in the publication.

See [`CREATE TABLE FROM SOURCE`](/sql/create-table/) for details.

#### Handling table schema changes

The use of the `CREATE SOURCE` with the new [`CREATE TABLE FROM
SOURCE`](/sql/create-table/) allows for the handling of certain upstream DDL
changes without downtime.

See [`CREATE TABLE FROM
SOURCE`](/sql/create-table/#handling-table-schema-changes) for details.

#### Supported types

With the new syntax, after a PostgreSQL source is created, you [`CREATE TABLE
FROM SOURCE`](/sql/create-table/) to create a corresponding table in
Matererialize and start ingesting data.

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->

For more information, including strategies for handling unsupported types,
see [`CREATE TABLE FROM SOURCE`](/sql/create-table/).

#### Upstream table truncation restrictions

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->

For additional considerations, see also [`CREATE TABLE`](/sql/create-table/).

### Publication membership

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->

### PostgreSQL replication slots

When you define a source, Materialize will automatically create a **replication
slot** in the upstream PostgreSQL database (see [PostgreSQL replication
slots](#postgresql-replication-slots)). Each source ingests the raw replication
stream data for all tables in the specified publication using **a single**
replication slot. This allows you to minimize the performance impact on the
upstream database as well as reuse the same source across multiple
materializations.

The name of the replication slot created by Materialize is prefixed with
`materialize_`. In Materialize, you can query the
`mz_internal.mz_postgres_sources` to find the replication slots created:

```mzsql
SELECT id, replication_slot FROM mz_internal.mz_postgres_sources;
```text

```text
    id   |             replication_slot
---------+----------------------------------------------
  u8     | materialize_7f8a72d0bf2a4b6e9ebc4e61ba769b71
```


> **Tip:** 

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->


## Examples

This section covers examples.

### Prerequisites

<!-- Unresolved shortcode: {{% include-from-yaml data="postgres_source_detail... -->


### Create a source {#create-source-example}

<!-- Unresolved shortcode: {{% include-example file="examples/create_source/e... -->

<!-- Unresolved shortcode: {{% include-example file="examples/create_source/e... -->

<!-- Unresolved shortcode: {{% include-example file="examples/create_source/e... -->

## Related pages

- [`CREATE TABLE`](/sql/create-table/)
- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [PostgreSQL integration guides](/ingest-data/postgres/#integration-guides)

[`enum`]: https://www.postgresql.org/docs/current/datatype-enum.html
[`money`]: https://www.postgresql.org/docs/current/datatype-money.html