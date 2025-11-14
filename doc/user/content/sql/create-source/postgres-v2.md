---
title: "CREATE SOURCE: PostgreSQL (New Syntax)"
description: "Creates a new source from PostgreSQL 11+."
menu:
  main:
    parent: 'create-source'
    name: "PostgreSQL (New Syntax)"
    identifier: 'create-source-postgresql-v2'
    weight: 22
---

{{< private-preview />}}

{{< source-versioning-disambiguation is_new=true
other_ref="[old reference page](/sql/create-source/postgres/)" include_blurb=true >}}

{{% create-source-intro external_source="PostgreSQL" version="11+"
create_table="/sql/create-table/" %}}

## Prerequisites

{{< include-md file="shared-content/postgres-source-prereq.md" >}}

## Syntax

{{% include-example file="examples/create_source/example_postgres_source"
 example="syntax" %}}

{{% include-example file="examples/create_source/example_postgres_source"
 example="syntax-options" %}}

## Details

### Ingesting data

After a source is created, you can create tables from the source, referencing
the tables in the publication, to start ingesting data. You can create multiple
tables that reference the same table in the publication.

See [`CREATE TABLE FROM SOURCE`](/sql/create-table/) for details.

#### Handling table schema changes

The use of the `CREATE SOURCE` with the new [`CREATE TABLE FROM
SOURCE`](/sql/create-table/) allows for the handling of upstream DDL changes without downtime.

See [`CREATE TABLE FROM SOURCE`](/sql/create-table/) for details.

#### Supported types

With the new syntax, after a PostgreSQL source is created, you [`CREATE TABLE
FROM SOURCE`](/sql/create-table/) to create a corresponding table in
Matererialize and start ingesting data.

{{< include-md file="shared-content/postgres-supported-types.md" >}}

For more information, including strategies for handling unsupported types,
see [`CREATE TABLE FROM SOURCE`](/sql/create-table/).

#### Upstream table truncation restrictions

{{< include-md file="shared-content/postgres-truncation-restriction.md" >}}

For additional considerations, see also [`CREATE TABLE`](/sql/create-table/).

### Publication membership

{{< include-md file="shared-content/postgres-publication-membership.md" >}}

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
```

```
    id   |             replication_slot
---------+----------------------------------------------
  u8     | materialize_7f8a72d0bf2a4b6e9ebc4e61ba769b71
```


{{< note >}}
The schema metadata is captured when the source is
initially created and is validated against the upstream schema upon restart.
If you create new tables upstream after creating a PostgreSQL source and want to
replicate them to Materialize, the source must be dropped and recreated.
{{</ >}}

{{< tip >}}

- {{< include-md file="shared-content/postgres-wal.md" >}}

{{< include-md file="shared-content/postgres-remove-unused-replication-slots.md" >}}

{{</ tip >}}

## Examples

### Prerequisites

{{< include-md file="shared-content/postgres-source-prereq.md" >}}


### Create a source {#create-source-example}

{{% include-example file="examples/create_source/example_postgres_source"
 example="example-prereq" %}}

{{% include-example file="examples/create_source/example_postgres_source"
 example="create-source" %}}

{{% include-example file="examples/create_source/example_postgres_source"
 example="create-table" %}}

## Related pages

- [`CREATE TABLE`](/sql/create-table/)
- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [PostgreSQL integration guides](/ingest-data/postgres/#integration-guides)

[`enum`]: https://www.postgresql.org/docs/current/datatype-enum.html
[`money`]: https://www.postgresql.org/docs/current/datatype-money.html
