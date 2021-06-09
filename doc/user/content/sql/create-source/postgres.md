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
This document details how to connect Materialize to a Postgres database.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-postgres.svg" >}}

Field | Use
------|-----
**MATERIALIZED** | Materializes the source's data, which retains all data in memory and makes sources directly selectable. **Currently required for all Postgres sources.** For more information, see [Materialized source details](#materialized-source-details).
_src_name_  | The name for the source.
**IF NOT EXISTS**  | Do nothing (except issuing a notice) if a source with the same name already exists. _Default._
**HOST** | Postgres host. See [Host options](#host-options) for details.
**PUBLICATION** _publication_name_ | Postgres publication.

### Host options

Field | Type | Value
------|------|------
_host_  | Host for Postgres connection.
_port_  | Port for Postgres connection.
_user_  | User for Postgres connection.
_password_  | Password for Postgres connection.
_dbname_ | Postgres database name.
_sslmode_ | Optionally specify TLS mode. Valid options are `disable`, `prefer`, `require`, `verify-ca`, and `verify-full`.
_sslcert_ | SSL certificate.
_sslkey_  |  SSL key.
_sslrootcert_  | SSL root certificate.

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

- Set up your Postgres database to allow logical replication.
- Ensure that the user for your Materialize connection has `REPLICATION` privileges.
- Create a Postgres publication containing the data to be streamed to Materialize. Since Postgres sources are materialized (kept in memory) in their entirety, we strongly recommend that you limit publications only to the data you need to query.

The materialized source created from the publication contains the raw data stream of replication updates, which you can then split into the familiar Postgres table view with [`CREATE VIEWS`](/sql/create-views/).

Note that if you stop or delete Materialize without first dropping the Postgres source, the Postgres replication slot isn't deleted and will continue to accumulate data. In such cases, you may want to manually delete the Materialize replication slot in order to recover memory. Materialize replication slot names always begin with `materialize_` for easy identification.

#### Restrictions on Postgres sources

- Materialize does not support changes to schemas for existing publications. You will need to drop the existing sources and then recreate them after creating new publications for the updated schemas.
- Sources can only be created from publications that use [data types](/sql/types/) supported by Materialize.
- Materialize only supports [TOASTED](https://www.postgresql.org/docs/9.5/storage-toast.html) values for append-only tables. Practically speaking, you can include rows with TOASTED values as long as they are never updated or deleted, or you can disable TOAST on the original Postgres table.

## Example

### Setting up PostgreSQL

Before you create a Postgres source in Materialize, you must complete some prerequisite steps in ostgres.

1. Allow logical replication by updating `postgresql.conf` with the line:
    ```sql
    wal_level = logical
    ```
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
    ```sql
    CREATE PUBLICATION mz_source FOR ALL TABLES;
    ```

### Creating a source

```sql
CREATE MATERIALIZED SOURCE "mz_source" FROM POSTGRES
HOST 'host=postgres port=5432 user=host sslmode=disable dbname=postgres'
PUBLICATION 'mz_source';
```

This creates a source that...

- Connects to a Postgres server
- Is raw data composed of all of the tables that went into the publication
- Needs to broken out into more usable views

### Creating views

```sql
CREATE VIEWS FROM SOURCE "mz_source";
SHOW FULL VIEWS;
```

This creates views that represent the replication stream broken out into the publication's composite tables. You can treat these tables as you would any other source and create materialized views from them.

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEWS`](../../create-views)
- [`SELECT`](../../select)
