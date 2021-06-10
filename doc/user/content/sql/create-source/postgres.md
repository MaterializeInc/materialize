---
title: "CREATE SOURCE: PostgreSQL"
description: "Learn how to connect Materialize to a PostgreSQL database."
menu:
  main:
    parent: 'create-source'
aliases:
  - /sql/create-source/postgresql
---

{{% create-source/intro %}}
This document details how to connect Materialize to a Postgres database. Before you create the source in Materialize, you must perform [some prerequisite steps](#postgresql-source-details) in Postgres.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-postgres.svg" >}}

Field | Use
------|-----
**MATERIALIZED** | Materializes the source's data, which retains all data in memory and makes sources directly selectable. **Currently required for all Postgres sources.** For more information, see [Materialized source details](#materialized-source-details).
_src_name_  | The name for the source.
**IF NOT EXISTS**  | Do nothing (except issuing a notice) if a source with the same name already exists. _Default._
**HOST** _connection_info_ | Postgres host. See the Postgres documentation on [supported correction parameters](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS) for details.
**PUBLICATION** _publication_name_ | Postgres publication.

## Details

### Materialized source details

Materializing a source keeps data it receives in an in-memory
[index](/overview/api-components/#indexes), the presence of which makes the
source directly queryable. In contrast, non-materialized sources cannot process
queries directly; to access the data the source receives, you need to create
[materialized views](/sql/create-materialized-view) that `SELECT` from the
source.

For a mental model, materializing the source is approximately equivalent to
creating a non-materialized source, and then creating a materialized view from
all of the source's columns:

```sql
CREATE SOURCE src ...;
CREATE MATERIALIZED VIEW src_view AS SELECT * FROM src;
```

The actual implementation of materialized sources differs, though, by letting
you refer to the source's name directly in queries.

For more details about the impact of materializing sources (and implicitly
creating an index), see [`CREATE INDEX`: Details &mdash; Memory
footprint](/sql/create-index/#memory-footprint).

### PostgreSQL source details

Materialize makes use of PostgreSQL's native replication capabilities to create a continuously updated replica of the desired Postgres tables.

Before creating the source in Materialize, you must:

1. Set up your Postgres database to allow logical replication.
2. Ensure that the user for your Materialize connection has `REPLICATION` privileges.
3. Create a Postgres [publication](https://www.postgresql.org/docs/10/logical-replication-publication.html), or replication data set, containing the tables to be streamed to Materialize. Since Postgres sources are materialized (kept in memory) in their entirety, we strongly recommend that you limit publications only to the data you need to query.

Once you create a materialized source from the publication, the source will contain the raw data stream of replication updates. You can then break the stream out into views that represent the publication's original tables with [`CREATE VIEWS`](/sql/create-views/). You can treat these tables as you would any other source and create other views or materialized views from them.

#### Postgres schemas

`CREATE VIEWS` will attempt to create each upstream table in the same schema as Postgres. For example, if the publication contains tables` "public"."foo"` and `"otherschema"."foo"`, `CREATE VIEWS` is the equivalent of

```
CREATE VIEW "public"."foo";
CREATE VIEW "otherschema"."foo";
```

Therefore, in order for `CREATE VIEWS` to succeed, all upstream schemas included in the publication must exist in Materialize as well, or you must explicitly specify the downstream schemas and rename the resulting tables. For example:

```sql
CREATE VIEWS FROM "mz_source"
("public"."foo" AS "foo", "otherschema"."foo" AS "foo2");
```
#### Postgres replication slots

If you stop or delete Materialize without first dropping the Postgres source, the Postgres replication slot isn't deleted and will continue to accumulate data. In such cases, you should manually delete the Materialize replication slot to recover memory and avoid degraded performance in the upstream database. Materialize replication slot names always begin with `materialize_` for easy identification.

#### Restrictions on Postgres sources

- Materialize does not support changes to schemas for existing publications. You will need to drop the existing sources and then recreate them after creating new publications for the updated schemas.
- Sources can only be created from publications that use [data types](/sql/types/) supported by Materialize. Attempts to create sources from publications which contain unsupported data types will fail with an error.
- Materialize only supports [TOASTED](https://www.postgresql.org/docs/current/storage-toast.html) values for append-only tables. Practically speaking, you can include rows with TOASTED values as long as they are never updated or deleted, or you can disable TOAST on the original Postgres table. If a TOASTED column is updated, the source will enter an errored state that renders all per-table views inaccessible.

  To disable TOAST on a column, use the following command in Postgres:
  ```
  ALTER TABLE table_name ALTER COLUMN column_name
  SET STORAGE PLAIN;
  ```
- Tables replicated into Materialize should not be truncated. If a table is truncated while replicated, the whole source becomes inaccessible and will not produce any data until it is re-created.

## Examples

### Setting up PostgreSQL

Before you create a Postgres source in Materialize, you must complete the following prerequisite steps in Postgres.

1. Ensure the database allows logical replicationz ( in the configuration). For most configurations, it should suffice to set `wal_level = logical` in `postgresql.conf`, but additional steps may be required for Amazon RDS. See the [Amazon Relational Database Service Documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html#PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication) for details.

2. Assign the user `REPLICATION` privileges:
    ```sql
    ALTER ROLE "user" WITH REPLICATION;
    ```
3. Set replica identity to full:
    ```sql
    ALTER TABLE foo
    REPLICA IDENTITY FULL;
    ```
4. Create a publication containing all the tables you wish to query in Materialize:

    *For all tables in Postgres:*
    ```sql
    CREATE PUBLICATION mz_source FOR ALL TABLES;
    ```

    *For specific tables:*
    ```sql
    CREATE PUBLICATION mz_source FOR TABLE table1, table2;
    ```

### Creating a source

Once you have set up Postgres, you can create the source in Materialize.

```sql
CREATE MATERIALIZED SOURCE "mz_source" FROM POSTGRES
HOST 'host=postgres port=5432 user=host sslmode=disable dbname=postgres'
PUBLICATION 'mz_source';
```

This creates a source that...

- Connects to a Postgres server
- Contains raw data from all of the tables that went into the publication
- Needs to broken out into more usable views that reproduce the original Postgres tables

### Creating views

Once you have created the Postgres source, you need to create views that represent the upstream publication's original tables.

*Create views for all tables included in the Postgres publication*

```sql
CREATE VIEWS FROM SOURCE "mz_source";
SHOW FULL VIEWS;
```

*Create views for specific upstream tables*

```sql
CREATE VIEWS FROM SOURCE "mz_source" ("a", "b");
SHOW FULL VIEWS;
```
## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEWS`](../../create-views)
- [`SELECT`](../../select)
