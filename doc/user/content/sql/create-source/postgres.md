---
title: "CREATE SOURCE: PostgreSQL"
description: "Connecting Materialize to a PostgreSQL database"
menu:
  main:
    parent: 'create-source'
    name: Postgres
    weight: 20
aliases:
  - /sql/create-source/postgresql
---

{{< beta />}}

{{< version-added v0.8.0 />}}

{{% create-source/intro %}}
This page describes how to connect Materialize to a PostgreSQL (10+) database to create and efficiently maintain real-time materialized views on top of a replication stream.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-postgres.svg" >}}

Field | Use
------|-----
**MATERIALIZED** | Materializes the source's data, which retains all data in memory and makes sources directly selectable. For more information, see [Key Concepts &mdash; Materialized sources](/overview/key-concepts/#materialized-sources).
_src_name_  | The name for the source.
**IF NOT EXISTS**  | Do nothing (except issuing a notice) if a source with the same name already exists. _Default._
**CONNECTION** _connection_info_ | Postgres connection parameters. See the Postgres documentation on [supported connection parameters](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS) for details.
**PUBLICATION** _publication_name_ | Postgres [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) (the replication data set containing the tables to be streamed to Materialize).

### `WITH` options

The following option is valid within the `WITH` clause.

Field | Value type | Description
------|------------|------------
`timestamp_frequency_ms`  |  `int` |  Default: `1000`. Sets the timestamping frequency in `ms`. Reflects how frequently the source advances its timestamp. This measure reflects how stale data in views will be. Lower values result in more-up-to-date views but may reduce throughput.

## Features

### Change data capture

This source uses PostgreSQL's native replication protocol to continually ingest changes resulting from `INSERT`, `UPDATE` and `DELETE` operations in the upstream database (also know as _change data capture_).

For this reason, the upstream database must be configured to support logical replication. To get logical replication set up, follow the step-by-step instructions in the [Change Data Capture (Postgres) guide](/integrations/cdc-postgres/#direct-postgres-source).

#### Creating a source

To avoid creating multiple replication slots upstream and minimize the required bandwidth, Materialize ingests the raw replication stream data for **all** tables included in a specific publication. This means that, when you define a source:

```sql
CREATE SOURCE mz_source
FROM POSTGRES
  CONNECTION 'host=example.com port=5432 user=host dbname=postgres sslmode=require'
  PUBLICATION 'mz_source';
```

, its schema looks like:

```sql
SHOW COLUMNS FROM mz_source;

   name   | nullable |  type
----------+----------+---------
 oid      | f        | integer
 row_data | f        | list
```

where each row of every upstream table is represented as a single row with two columns:

| Column | Description |
|--------|-------------|
| `oid`  | A unique identifier for the tables included in the publication. |
| `row_data` | A text-encoded, variable length `list`. The number of text elements in a list is always equal to the number of columns in the upstream table. |

It's important to note that the schema metadata is captured when the source is initially created, and is validated against the upstream schema upon restart. If you wish to add additional tables to the original publication and use them in Materialize, the source must be dropped and recreated.

#### Creating replication views

From here, you can break down the source into views that reproduce the publication's original tables based on the `oid` identifier and convert the text elements in `row_data` to the original data types:

_Create views for specific tables included in the Postgres publication_

```sql
CREATE VIEWS FROM SOURCE mz_source (table1, table2);
```

_Create views for all tables_

```sql
CREATE VIEWS FROM SOURCE mz_source;
```

Under the hood, Materialize parses this statement into view definitions for each table that can be used as a base for your materialized view.

##### Postgres schemas

`CREATE VIEWS` will attempt to create each upstream table in the same schema as Postgres. For example, if the publication contains tables `public.foo` and `otherschema.foo`, `CREATE VIEWS` is the equivalent of:

```sql
CREATE VIEW public.foo;

CREATE VIEW otherschema.foo;
```

For `CREATE VIEWS` to succeed, either all upstream schemas included in the publication must exist in Materialize as well, or you must explicitly specify the downstream schemas and rename the resulting views:

```sql
CREATE VIEWS FROM SOURCE mz_source
(public.foo AS foo, otherschema.foo AS foo2);
```

#### Creating materialized views

To produce correct results, Postgres sources can only be materialized _once_. As soon as you define a materialized view, Materialize:

1. Creates a replication slot in the upstream Postgres database (see [Postgres replication slots](#postgres-replication-slots)). The name of the replication slots created by Materialize is prefixed with `materialize_` for easy identification.

1. Performs an initial, snapshot-based sync of the tables in the publication before it starts ingesting change events.

   **Note:** During this phase, **disk space** consumption may increase proportionally to the size of the upstream database before returning to a steady state. To profile disk usage, see [Troubleshooting](/ops/troubleshooting/#how-much-disk-space-is-materialize-using).

1. Incrementally updates the view as new change events stream in as a result of `INSERT`, `UPDATE` and `DELETE` operations in the upstream Postgres database.

##### Postgres replication slots

Each Materialize replication slot can be used to source data for a single materialized view. You can create multiple non-materialized views for the same replication slot using the [`CREATE VIEWS`](/sql/create-views) statement.

{{< warning >}}
Make sure to delete any replication slots if you stop using Materialize, or if either the Materialize or Postgres instances crash.
{{< /warning >}}

If you stop Materialize or delete the materialized view without also dropping the source, the upstream replication slot will linger and continue to accumulate data so that the source can resume in the future. To avoid unbounded disk space usage, make sure to use [`DROP SOURCE`](/sql/drop-source/) or manually delete the replication slot (in case you have deleted the Materialize instance).

For PostgreSQL 13+, it is recommended that you set a reasonable value for [`max_slot_wal_keep_size`](https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE) to limit the amount of storage used by replication slots.

## Known limitations

 **Schema changes:** Materialize does not support changes to schemas for existing publications, and will set the source into an error state if a breaking DDL change is detected upstream. To handle schema changes, you need to drop the existing sources and then recreate them after creating new publications for the updated schemas.
- **Supported data types:** Sources can only be created from publications that use [data types](/sql/types/) supported by Materialize. Attempts to create sources from publications which contain unsupported data types will fail with an error.
- **Truncation:** Tables replicated into Materialize should not be truncated. If a table is truncated while replicated, the whole source becomes inaccessible and will not produce any data until it is recreated.

## Related pages

- [Change Data Capture (Postgres) guide](/integrations/cdc-postgres/#direct-postgres-source)
- [`CREATE VIEWS`](../../create-views)
