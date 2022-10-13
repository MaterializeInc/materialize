---
title: "CREATE SOURCE: PostgreSQL"
description: "Connecting Materialize to a PostgreSQL database"
pagerank: 10
menu:
  main:
    parent: 'create-source'
    identifier: cs_postgres
    name: PostgreSQL
    weight: 20
aliases:
  - /sql/create-source/postgresql
---

{{< beta />}}

{{% create-source/intro %}}
To connect to a Postgres instance, you first need to [create a connection](#creating-a-connection) that specifies access and authentication parameters. Once created, a connection is **reusable** across multiple `CREATE SOURCE` statements.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-postgres.svg" >}}

Field | Use
------|-----
_src_name_  | The name for the source.
**IF NOT EXISTS**  | Do nothing (except issuing a notice) if a source with the same name already exists. _Default._
**CONNECTION** _connection_name_ | The name of the Postgres connection to use in the source. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection/#postgres) documentation page.
**PUBLICATION** _publication_name_ | Postgres [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) (the replication data set containing the tables to be streamed to Materialize).
**FOR ALL TABLES** | Creates subsources for all tables in the publication.
**FOR TABLES** _table_name_ | Creates subsources for specific tables in the publication.

### `WITH` options

Field                                | Value     | Description
-------------------------------------|-----------|-------------------------------------
`SIZE`                               | `text`    | **Required.** The [size](../#sizing-a-source) for the source. Accepts values: `3xsmall`, `2xsmall`, `xsmall`, `small`, `medium`, `large`.

## Features

### Change data capture

This source uses PostgreSQL's native replication protocol to continually ingest changes resulting from `INSERT`, `UPDATE` and `DELETE` operations in the upstream database (also know as _change data capture_).

For this reason, the upstream database must be configured to support logical replication. To get logical replication set up, follow the step-by-step instructions in the [Change Data Capture (Postgres) guide](/integrations/cdc-postgres/#direct-postgres-source).

#### Creating a source

To avoid creating multiple replication slots upstream and minimize the required bandwidth, Materialize ingests the raw replication stream data for **all** tables included in a specific publication. When you define a Postgres source:

```sql
CREATE SOURCE mz_source
  FROM POSTGRES
    CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR ALL TABLES
  WITH (SIZE '3xsmall');
```

Materialize will automatically create a **subsource** for each original table in the publication:

```sql
SHOW SOURCES;
```

```nofmt
         name         |   type    |  size
----------------------+-----------+---------
 table_1              | subsource | 3xsmall
 table_2              | subsource | 3xsmall
 mz_source            | postgres  | 3xsmall
```

It's important to note that the schema metadata is captured when the source is initially created, and is validated against the upstream schema upon restart. If you wish to add additional tables to the original publication and use them in Materialize, the source must be dropped and recreated.

##### Postgres schemas

`CREATE SOURCE` will attempt to create each upstream table in the **current** schema. This may lead to naming collisions if, for example, you are replicating `schema1.table_1` and `schema2.table_1`. Use the `FOR TABLES` clause to provide aliases for each upstream table, in such cases, or to specify an alternative destination schema in Materialize.

```sql
CREATE SOURCE mz_source
  FROM POSTGRES
    CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR TABLES ( schema1.table_1 AS s1_table_1,
               schema2_table_1 AS s2_table_1 )
  WITH (SIZE = '3xsmall');
```
#### Creating materialized views

As soon as you define a Postgres source, Materialize will:

1. Create a replication slot in the upstream Postgres database (see [Postgres replication slots](#postgres-replication-slots)). The name of the replication slots created by Materialize is prefixed with `materialize_` for easy identification.

1. Perform an initial, snapshot-based sync of the tables in the publication before it starts ingesting change events.

1. Incrementally update any materialized or indexed views that depend on the source as change events stream in, as a result of `INSERT`, `UPDATE` and `DELETE` operations in the original Postgres database.

##### Postgres replication slots

Each source ingests the raw replication stream data for all tables in the specified publication using **a single** replication slot. This allows you to minimize the performance impact on the upstream database, as well as reuse the same source across multiple materializations.

{{< warning >}}
Make sure to delete any replication slots if you stop using Materialize, or if either the Materialize or Postgres instances crash.
{{< /warning >}}

If you delete all objects that depend on a source without also dropping the source, the upstream replication slot will linger and continue to accumulate data so that the source can resume in the future. To avoid unbounded disk space usage, make sure to use [`DROP SOURCE`](/sql/drop-source/) or manually delete the replication slot.

For PostgreSQL 13+, it is recommended that you set a reasonable value for [`max_slot_wal_keep_size`](https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE) to limit the amount of storage used by replication slots.

## Known limitations

##### Schema changes

Materialize does not support changes to schemas for existing publications, and will set the source into an error state if a breaking DDL change is detected upstream. To handle schema changes, you need to drop the existing sources and then recreate them after creating new publications for the updated schemas.

##### Supported types

Sources can only be created from publications that use [data types](/sql/types/) supported by Materialize. Attempts to create sources from publications which contain tables with unsupported data types (like `enum` {{% gh 12689 %}}) will fail with an error.

##### Truncation

Tables replicated into Materialize should not be truncated. If a table is truncated while replicated, the whole source becomes inaccessible and will not produce any data until it is recreated.

## Examples

### Creating a connection

A connection describes how to connect and authenticate to an external system you want Materialize to read data from.

Once created, a connection is **reusable** across multiple `CREATE SOURCE` statements. For more details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection/#postgres) documentation page.

```sql
CREATE SECRET pgpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION pg_connection
  FOR POSTGRES
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    USER 'postgres',
    PASSWORD SECRET pgpass,
    SSL MODE 'require',
    DATABASE 'postgres';
```

### Creating a source

```sql
CREATE SOURCE mz_source
  FROM POSTGRES
    CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR ALL TABLES
  WITH (SIZE = 'xsmall');
```

### Sizing a source

To provision a specific amount of CPU and memory to a source on creation, use the `SIZE` option:

```sql
CREATE SOURCE mz_source
  FROM POSTGRES
    CONNECTION pg_connection (PUBLICATION 'mz_source')
  WITH (SIZE = 'xsmall');
```

To resize the source after creation:

```sql
ALTER SOURCE mz_source SET (SIZE = 'large');
```

The smallest source size (`3xsmall`) is a resonable default to get started. For more details on sizing sources, check the [`CREATE SOURCE`](../) documentation page.

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)
- [Change Data Capture (Postgres) guide](/integrations/cdc-postgres/#direct-postgres-source)
