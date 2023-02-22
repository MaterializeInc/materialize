---
title: "CREATE SOURCE: PostgreSQL"
description: "Connecting Materialize to a PostgreSQL database"
pagerank: 40
menu:
  main:
    parent: 'create-source'
    identifier: cs_postgres
    name: PostgreSQL
    weight: 20
aliases:
  - /sql/create-source/postgresql
---

{{< warning >}}
Before creating a PostgreSQL source, you must set up logical replication in the upstream database. For step-by-step instructions, see the [PostgreSQL CDC guide](/integrations/cdc-postgres/#direct-postgres-source).
{{< /warning >}}

{{% create-source/intro %}}
To connect to a PostgreSQL instance, you first need to [create a connection](#creating-a-connection) that specifies access and authentication parameters. Once created, a connection is **reusable** across multiple `CREATE SOURCE` statements.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-postgres.svg" >}}

Field | Use
------|-----
_src_name_  | The name for the source.
**IF NOT EXISTS**  | Do nothing (except issuing a notice) if a source with the same name already exists. _Default._
**IN CLUSTER** _cluster_name_ | The [cluster](/sql/create-cluster) to maintain this source. If not specified, the `SIZE` option must be specified.
**CONNECTION** _connection_name_ | The name of the PostgreSQL connection to use in the source. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection/#postgresql) documentation page.
**FOR ALL TABLES** | Create subsources for all tables in the publication.
**FOR TABLES (** _table_list_ **)** | Create subsources for specific tables in the publication.
**EXPOSE PROGRESS AS** _progress_subsource_name_ | Name this source's progress collection `progress_subsource_name`; if this is not specified, Materialize names the progress collection `<src_name>_progress`. For details about the progress collection, see [Progress collection](#progress-collection).

### `CONNECTION` options

Field                                | Value     | Description
-------------------------------------|-----------|-------------------------------------
`PUBLICATION`                        | `text`    | **Required.** The PostgreSQL [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) (the replication data set containing the tables to be streamed to Materialize).
`TEXT COLUMNS`                       | A list of names | Decode data as `text` for specific columns that contain PostgreSQL types that are unsupported in Materialize.

### `WITH` options

Field                                | Value     | Description
-------------------------------------|-----------|-------------------------------------
`SIZE`                               | `text`    | The [size](../#sizing-a-source) for the source. Accepts values: `3xsmall`, `2xsmall`, `xsmall`, `small`, `medium`, `large`, `xlarge`. Required if the `IN CLUSTER` option is not specified.

## Features

### Change data capture

This source uses PostgreSQL's native replication protocol to continually ingest changes resulting from `INSERT`, `UPDATE` and `DELETE` operations in the upstream database — a process also known as _change data capture_.

For this reason, you must configure the upstream PostgreSQL database to support logical replication before creating a source in Materialize. Follow the step-by-step instructions in the [PostgreSQL CDC guide](/integrations/cdc-postgres/#direct-postgres-source) to get logical replication set up.

#### Creating a source

To avoid creating multiple replication slots in the upstream PostgreSQL database and minimize the required bandwidth, Materialize ingests the raw replication stream data for either all tables (`FOR ALL TABLES`) or a specified subset of tables (`FOR TABLES`) included in a specific publication.

```sql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR ALL TABLES
  WITH (SIZE = '3xsmall');
```

When you define a source, Materialize will automatically:

1. Create a **replication slot** in the upstream PostgreSQL database (see [PostgreSQL replication slots](#postgresql-replication-slots)).

    The name of the replication slot created by Materialize is prefixed with `materialize_` for easy identification, and can be looked up in `mz_internal.mz_postgres_sources`.

    ```sql
    SELECT * FROM mz_internal.mz_postgres_sources;
    ```

    ```
       id   |             replication_slot
    --------+----------------------------------------------
     u8     | materialize_7f8a72d0bf2a4b6e9ebc4e61ba769b71
    ```
1. Create a **subsource** for each original table in the publication.

    ```sql
    SHOW SOURCES;
    ```

    ```nofmt
             name         |   type    |  size
    ----------------------+-----------+---------
     table_1              | subsource |
     table_2              | subsource |
     mz_source            | postgres  | 3xsmall
    ```

    And perform an initial, snapshot-based sync of the tables in the publication before it starts ingesting change events.

1. Incrementally update any materialized or indexed views that depend on the source as change events stream in, as a result of `INSERT`, `UPDATE` and `DELETE` operations in the upstream PostgreSQL database.

It's important to note that the schema metadata is captured when the source is initially created, and is validated against the upstream schema upon restart. If you wish to add additional tables to the original publication and use them in Materialize, the source must be dropped and recreated.

##### PostgreSQL replication slots

Each source ingests the raw replication stream data for all tables in the specified publication using **a single** replication slot. This allows you to minimize the performance impact on the upstream database, as well as reuse the same source across multiple materializations.

{{< warning >}}
Make sure to delete any replication slots if you stop using Materialize, or if either the Materialize or PostgreSQL instances crash. To look up the name of the replication slot created for each source, use `mz_internal.mz_postgres_sources`.
{{< /warning >}}

If you delete all objects that depend on a source without also dropping the source, the upstream replication slot will linger and continue to accumulate data so that the source can resume in the future. To avoid unbounded disk space usage, make sure to use [`DROP SOURCE`](/sql/drop-source/) or manually delete the replication slot.

For PostgreSQL 13+, it is recommended that you set a reasonable value for [`max_slot_wal_keep_size`](https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE) to limit the amount of storage used by replication slots.

##### PostgreSQL schemas

`CREATE SOURCE` will attempt to create each upstream table in the **current** schema. This may lead to naming collisions if, for example, you are replicating `schema1.table_1` and `schema2.table_1`. Use the `FOR TABLES` clause to provide aliases for each upstream table, in such cases, or to specify an alternative destination schema in Materialize.

```sql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR TABLES (schema1.table_1 AS s1_table_1, schema2_table_1 AS s2_table_1)
  WITH (SIZE = '3xsmall');
```

### Progress collection

Each source exposes its progress as a separate progress collection. You can
choose a name for this collection using **EXPOSE PROGRESS AS**
_progress_subsource_name_ or Materialize will automatically name the collection
`<source_name>_progress`. You can find the collection's name using [`SHOW
SOURCES`](/sql/show-sources).

The progress collection schema depends on your source type. For Postgres
sources, we return the last `lsn` ([`uint8`](/sql/types/uint)) we have consumed
from your Postgres server's replication stream.

As long as as the LSN continues to change, Materialize is consuming data.

## Known limitations

##### Schema changes

Materialize does not support changes to schemas for existing publications, and will set the source into an error state if a breaking DDL change is detected upstream. To handle schema changes, you need to drop the existing sources and then recreate them after creating new publications for the updated schemas.

##### Supported types

Replicating tables that contain [data types](/sql/types/) unsupported in Materialize is possible via the `TEXT COLUMNS` option. The specified columns will be treated as `text`, and will thus not offer the expected PostgreSQL type features. For example:

* [`enum`]: the implicit ordering of the original PostgreSQL `enum` type is not preserved, as Materialize will sort values as `text`.
* [`money`]: the resulting `text` value cannot be cast back to e.g. `numeric`, since PostgreSQL adds typical currency formatting to the output.

##### Truncation

Tables replicated into Materialize should not be truncated. If a table is truncated while replicated, the whole source becomes inaccessible and will not produce any data until it is recreated.
Instead, remove all rows from a table using an unqualified `DELETE`.

```sql
DELETE FROM t;
```

## Examples

{{< warning >}}
Before creating a PostgreSQL source, you must set up logical replication in the upstream database. For step-by-step instructions, see the [PostgreSQL CDC guide](/integrations/cdc-postgres/#direct-postgres-source).
{{< /warning >}}

### Creating a connection

A connection describes how to connect and authenticate to an external system you want Materialize to read data from.

Once created, a connection is **reusable** across multiple `CREATE SOURCE` statements. For more details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection/#postgresql) documentation page.

```sql
CREATE SECRET pgpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    USER 'postgres',
    PASSWORD SECRET pgpass,
    SSL MODE 'require',
    DATABASE 'postgres'
);
```

If your PostgreSQL server is not exposed to the public internet, you can [tunnel the connection](/sql/create-connection/#network-security-connections) through an AWS PrivateLink service or an SSH bastion host.

{{< tabs tabID="1" >}}
{{< tab "AWS PrivateLink">}}

```sql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);
```

```sql
CREATE SECRET pgpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    USER 'postgres',
    PASSWORD SECRET pgpass,
    AWS PRIVATELINK privatelink_svc,
    DATABASE 'postgres'
);
```

For step-by-step instructions on creating AWS PrivateLink connections and configuring an AWS PrivateLink service to accept connections from Materialize, check [this guide](/ops/network-security/privatelink/).
{{< /tab >}}
{{< tab "SSH tunnel">}}
```sql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize',
);
```

```sql
CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    SSH TUNNEL ssh_connection,
    DATABASE 'postgres'
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring an SSH bastion server to accept connections from Materialize, check [this guide](/ops/network-security/ssh-tunnel/).
{{< /tab >}}
{{< /tabs >}}

### Creating a source {#create-source-example}

_Create subsources for all tables included in the PostgreSQL publication_

```sql
CREATE SOURCE mz_source
    FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
    FOR ALL TABLES
    WITH (SIZE = '3xsmall');
```

_Create subsources for specific tables included in the PostgreSQL publication_

```sql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR TABLES (table_1, table_2 AS alias_table_2)
  WITH (SIZE = '3xsmall');
```

#### Handling unsupported types

If the publication contains tables that use [data types](/sql/types/) unsupported by Materialize, use the `TEXT COLUMNS` option to decode data as `text` for the affected columns.

```sql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (
    PUBLICATION 'mz_source',
    TEXT COLUMNS (table.column_of_unsupported_type)
  ) FOR ALL TABLES
  WITH (SIZE = '3xsmall');
```

### Sizing a source

To provision a specific amount of CPU and memory to a source on creation, use the `SIZE` option:

```sql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  WITH (SIZE = '3xsmall');
```

To resize the source after creation:

```sql
ALTER SOURCE mz_source SET (SIZE = 'large');
```

The smallest source size (`3xsmall`) is a resonable default to get started. For more details on sizing sources, check the [`CREATE SOURCE`](../#sizing-a-source) documentation page.

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)
- [PostgreSQL CDC guide](/integrations/cdc-postgres/#direct-postgres-source)

[`enum`]: https://www.postgresql.org/docs/current/datatype-enum.html
[`money`]: https://www.postgresql.org/docs/current/datatype-money.html
