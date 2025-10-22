---
title: "CREATE SOURCE: PostgreSQL"
description: "Creates a new source from PostgreSQL 11+."
menu:
  main:
    parent: 'create-source'
    name: "PostgreSQL"
    identifier: 'create-source-postgresql'
---

{{< source-versioning-disambiguation is_new=true
other_ref="[old reference page](/sql/create-source-v1/postgres/)" include_blurb=true >}}

{{% create-source-intro external_source="PostgreSQL" version="11+"
create_table="/sql/create-table/postgres/" %}}

## Prerequisites

{{< include-md file="shared-content/postgres-source-prereq.md" >}}

## Syntax

{{% include-example file="examples/create_source/example_postgres_source"
 example="syntax" %}}

{{% include-example file="examples/create_source/example_postgres_source"
 example="syntax-options" %}}

## Details

### Change data capture

Materialize uses PostgreSQL's native replication protocol to continually ingest
changes resulting from `INSERT`, `UPDATE` and `DELETE` operations in the
upstream database — a process also known as _change data capture_.

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

### Ingesting data

After a source is created, you can create tables from the source, referencing
the tables in the publication, to start ingesting data. See [Create a table
(PostgreSQL source)](/sql/create-table/postgres/) for
details. You can create multiple tables that reference the same table in the
publication.

## Known limitations

{{% include-md file="shared-content/postgres-known-limitations.md" %}}

## Examples

### Prerequisites

{{< include-md file="shared-content/postgres-source-prereq.md" >}}

### Create a source {#create-source-example}

{{% include-example file="examples/create_source/example_postgres_source"
 example="create-source" %}}

{{% include-example file="examples/create_source/example_postgres_source"
 example="create-table" %}}

#### Adding subsources

When adding tables to a PostgreSQL source, Materialize opens a temporary
replication slot to snapshot the new table's current states. After
completing the snapshot, the table will be kept up-to-date, like all other
tables in the publication.

#### Dropping subsources

Dropping a table in PostgreSQL prevents Materialize from ingesting any data from
it, in addition to dropping any state that Materialize previously had for the
table.

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)
- [PostgreSQL integration guides](/ingest-data/postgres/#integration-guides)

[`enum`]: https://www.postgresql.org/docs/current/datatype-enum.html
[`money`]: https://www.postgresql.org/docs/current/datatype-money.html
