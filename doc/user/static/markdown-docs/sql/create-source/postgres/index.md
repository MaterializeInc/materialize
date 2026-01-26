# CREATE SOURCE: PostgreSQL (Legacy Syntax)

Connecting Materialize to a PostgreSQL database for Change Data Capture (CDC).



> **Disambiguation:** This page reflects the legacy syntax, which requires downtime to handle upstream DDL changes. For the new syntax which can handle adding or dropping columns to the upstream tables without downtime, see the [new reference page](/sql/create-source/postgres-v2).


[`CREATE SOURCE`](/sql/create-source/) connects Materialize to an external system you want to read data from, and provides details about how to decode and interpret that data.


Materialize supports PostgreSQL (11+) as a data source. To connect to a
PostgreSQL instance, you first need to [create a connection](#creating-a-connection)
that specifies access and authentication parameters.
Once created, a connection is **reusable** across multiple `CREATE SOURCE`
statements.



> **Warning:** Before creating a PostgreSQL source, you must set up logical replication in the
> upstream database. For step-by-step instructions, see the integration guide for
> your PostgreSQL service: [AlloyDB](/ingest-data/postgres-alloydb/),
> [Amazon RDS](/ingest-data/postgres-amazon-rds/),
> [Amazon Aurora](/ingest-data/postgres-amazon-aurora/),
> [Azure DB](/ingest-data/postgres-azure-db/),
> [Google Cloud SQL](/ingest-data/postgres-google-cloud-sql/),
> [Self-hosted](/ingest-data/postgres-self-hosted/).
>


> **Note:** Connections using AWS PrivateLink is for Materialize Cloud only.
>



## Syntax



```mzsql
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM POSTGRES CONNECTION <connection_name> (
  PUBLICATION '<publication_name>'
  [, TEXT COLUMNS ( <col1> [, ...] ) ]
  [, EXCLUDE COLUMNS ( <col1> [, ...] ) ]
)
<FOR ALL TABLES | FOR SCHEMAS ( <schema1> [, ...] ) | FOR TABLES ( <table1> [AS <subsrc_name>] [, ...] )>
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]

```

| Syntax element | Description |
| --- | --- |
| `<src_name>` | The name for the source.  |
| **IF NOT EXISTS** | Optional. If specified, do not throw an error if a source with the same name already exists. Instead, issue a notice and skip the source creation.  |
| **IN CLUSTER** `<cluster_name>` | Optional. The [cluster](/sql/create-cluster) to maintain this source.  |
| **CONNECTION** `<connection_name>` | The name of the PostgreSQL connection to use in the source. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection/#postgresql) documentation page.  |
| **PUBLICATION** `'<publication_name>'` | The PostgreSQL [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) (the replication data set containing the tables to be streamed to Materialize).  |
| **TEXT COLUMNS** ( `<col1>` [, ...] ) | Optional. Decode data as `text` for specific columns that contain PostgreSQL types that are unsupported in Materialize.  |
| **EXCLUDE COLUMNS** ( `<col1>` [, ...] ) | Optional. Exclude specific columns that cannot be decoded or should not be included in the subsources created in Materialize.  |
| **FOR** `<table_schema_specification>` | Specifies which tables to create subsources for. The following `<table_schema_specification>`s are supported:  \| Option \| Description \| \|--------\|-------------\| \| `ALL TABLES` \| Create subsources for all tables in the publication. \| \| `SCHEMAS ( <schema1> [, ...] )` \| Create subsources for specific schemas in the publication. \| \| `TABLES ( <table1> [AS <subsrc_name>] [, ...] )` \| Create subsources for specific tables in the publication. \|  |
| **EXPOSE PROGRESS AS** `<progress_subsource_name>` | Optional. The name of the progress collection for the source. If this is not specified, the progress collection will be named `<src_name>_progress`. For more information, see [Monitoring source progress](#monitoring-source-progress).  |
| **WITH** (`<with_option>` [, ...]) | Optional. The following `<with_option>`s are supported:  \| Option \| Description \| \|--------\|-------------\| \| `RETAIN HISTORY FOR <retention_period>` \| ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`. \|  |


## Features

### Change data capture

This source uses PostgreSQL's native replication protocol to continually ingest
changes resulting from `INSERT`, `UPDATE` and `DELETE` operations in the
upstream database — a process also known as _change data capture_.

For this reason, you must configure the upstream PostgreSQL database to support
logical replication before creating a source in Materialize. For step-by-step
instructions, see the integration guide for your PostgreSQL service:
[AlloyDB](/ingest-data/postgres-alloydb/),
[Amazon RDS](/ingest-data/postgres-amazon-rds/),
[Amazon Aurora](/ingest-data/postgres-amazon-aurora/),
[Azure DB](/ingest-data/postgres-azure-db/),
[Google Cloud SQL](/ingest-data/postgres-google-cloud-sql/),
[Self-hosted](/ingest-data/postgres-self-hosted/).

#### Creating a source

To avoid creating multiple replication slots in the upstream PostgreSQL database
and minimize the required bandwidth, Materialize ingests the raw replication
stream data for some specific set of tables in your publication.

```mzsql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR ALL TABLES;
```

When you define a source, Materialize will automatically:

1. Create a **replication slot** in the upstream PostgreSQL database (see
   [PostgreSQL replication slots](#postgresql-replication-slots)).

    The name of the replication slot created by Materialize is prefixed with
    `materialize_` for easy identification, and can be looked up in
    `mz_internal.mz_postgres_sources`.

    ```mzsql
    SELECT id, replication_slot FROM mz_internal.mz_postgres_sources;
    ```

    ```
       id   |             replication_slot
    --------+----------------------------------------------
     u8     | materialize_7f8a72d0bf2a4b6e9ebc4e61ba769b71
    ```
1. Create a **subsource** for each original table in the publication.

    ```mzsql
    SHOW SOURCES;
    ```

    ```nofmt
             name         |   type
    ----------------------+-----------
     mz_source            | postgres
     mz_source_progress   | progress
     table_1              | subsource
     table_2              | subsource
    ```

    And perform an initial, snapshot-based sync of the tables in the publication
    before it starts ingesting change events.

1. Incrementally update any materialized or indexed views that depend on the
source as change events stream in, as a result of `INSERT`, `UPDATE` and
`DELETE` operations in the upstream PostgreSQL database.

##### PostgreSQL replication slots

Each source ingests the raw replication stream data for all tables in the
specified publication using **a single** replication slot. This allows you to
minimize the performance impact on the upstream database, as well as reuse the
same source across multiple materializations.

> **Tip:** <ul>
> <li>
> <p>For PostgreSQL 13+, set a reasonable value
> for <a href="https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE" ><code>max_slot_wal_keep_size</code></a>
> to limit the amount of storage used by replication slots.</p>
> </li>
> <li>
> <p>If you stop using Materialize, or if either the Materialize instance or
> the PostgreSQL instance crash, delete any replication slots. You can query
> the <code>mz_internal.mz_postgres_sources</code> table to look up the name of the
> replication slot created for each source.</p>
> </li>
> <li>
> <p>If you delete all objects that depend on a source without also dropping
> the source, the upstream replication slot remains and will continue to
> accumulate data so that the source can resume in the future. To avoid
> unbounded disk space usage, make sure to use <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> or manually delete the replication slot.</p>
> </li>
> </ul>
>
>
>


##### PostgreSQL schemas

`CREATE SOURCE` will attempt to create each upstream table in the same schema as
the source. This may lead to naming collisions if, for example, you are
replicating `schema1.table_1` and `schema2.table_1`. Use the `FOR TABLES`
clause to provide aliases for each upstream table, in such cases, or to specify
an alternative destination schema in Materialize.

```mzsql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR TABLES (schema1.table_1 AS s1_table_1, schema2_table_1 AS s2_table_1);
```

### Monitoring source progress

By default, PostgreSQL sources expose progress metadata as a subsource that you
can use to monitor source **ingestion progress**. The name of the progress
subsource can be specified when creating a source using the `EXPOSE PROGRESS
AS` clause; otherwise, it will be named `<src_name>_progress`.

The following metadata is available for each source as a progress subsource:

Field          | Type                                     | Meaning
---------------|------------------------------------------|--------
`lsn`          | [`uint8`](/sql/types/uint/#uint8-info)   | The last Log Sequence Number (LSN) consumed from the upstream PostgreSQL replication stream.

And can be queried using:

```mzsql
SELECT lsn
FROM <src_name>_progress;
```

The reported LSN should increase as Materialize consumes **new** WAL records
from the upstream PostgreSQL database. For more details on monitoring source
ingestion progress and debugging related issues, see [Troubleshooting](/ops/troubleshooting/).

## Known limitations

### Schema changes

Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes (Legacy syntax)

> **Note:** This section refer to the legacy [`CREATE SOURCE ... FOR
> ...`](/sql/create-source/postgres/) that creates subsources as part of the
> `CREATE SOURCE` operation.  To be able to handle the upstream column
> additions and drops, see [`CREATE SOURCE (New
> Syntax)`](/sql/create-source/postgres-v2/) and [`CREATE TABLE FROM
> SOURCE`](/sql/create-table).
>
>

<ul>
<li>
<p>Adding columns to tables. Materialize will <strong>not ingest</strong> new columns
added upstream unless you use <a href="/sql/alter-source/#context" ><code>DROP SOURCE</code></a> to
first drop the affected subsource, and then add the table back to the source
using <a href="/sql/alter-source/" ><code>ALTER SOURCE...ADD SUBSOURCE</code></a>.</p>
</li>
<li>
<p>Dropping columns that were added after the source was created. These
columns are never ingested, so you can drop them without issue.</p>
</li>
<li>
<p>Adding or removing <code>NOT NULL</code> constraints to tables that were nullable
when the source was created.</p>
</li>
</ul>


#### Incompatible schema changes

<p>All other schema changes to upstream tables will set the corresponding
subsource into an error state, which prevents you from reading from the
source.</p>
<p>To handle incompatible <a href="#schema-changes" >schema changes</a>, use <a href="/sql/alter-source/#context" ><code>DROP SOURCE</code></a> and <a href="/sql/alter-source/" ><code>ALTER SOURCE...ADD SUBSOURCE</code></a> to first drop the affected subsource, and
then add the table back to the source. When you add the subsource, it will
have the updated schema from the corresponding upstream table.</p>


### Publication membership

<p>PostgreSQL&rsquo;s logical replication API does not provide a signal when users
remove tables from publications. Because of this, Materialize relies on
periodic checks to determine if a table has been removed from a publication,
at which time it generates an irrevocable error, preventing any values from
being read from the table.</p>
<p>However, it is possible to remove a table from a publication and then re-add
it before Materialize notices that the table was removed. In this case,
Materialize can no longer provide any consistency guarantees about the data
we present from the table and, unfortunately, is wholly unaware that this
occurred.</p>


To mitigate this issue, if you need to drop and re-add a table to a
publication, ensure that you remove the table/subsource from the source
<em>before</em> re-adding it using the <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> command.

### Supported types

<p>Materialize natively supports the following PostgreSQL types (including the
array type for each of the types):</p>
<ul style="column-count: 3">
<li><code>bool</code></li>
<li><code>bpchar</code></li>
<li><code>bytea</code></li>
<li><code>char</code></li>
<li><code>date</code></li>
<li><code>daterange</code></li>
<li><code>float4</code></li>
<li><code>float8</code></li>
<li><code>int2</code></li>
<li><code>int2vector</code></li>
<li><code>int4</code></li>
<li><code>int4range</code></li>
<li><code>int8</code></li>
<li><code>int8range</code></li>
<li><code>interval</code></li>
<li><code>json</code></li>
<li><code>jsonb</code></li>
<li><code>numeric</code></li>
<li><code>numrange</code></li>
<li><code>oid</code></li>
<li><code>text</code></li>
<li><code>time</code></li>
<li><code>timestamp</code></li>
<li><code>timestamptz</code></li>
<li><code>tsrange</code></li>
<li><code>tstzrange</code></li>
<li><code>uuid</code></li>
<li><code>varchar</code></li>
</ul>

<p>Replicating tables that contain <strong>unsupported <a href="/sql/types/" >data types</a></strong> is
possible via the <code>TEXT COLUMNS</code> option. The specified columns will be
treated as <code>text</code>; i.e., will not have the expected PostgreSQL type
features. For example:</p>
<ul>
<li>
<p><a href="https://www.postgresql.org/docs/current/datatype-enum.html" ><code>enum</code></a>: When decoded as <code>text</code>, the implicit ordering of the original
PostgreSQL <code>enum</code> type is not preserved; instead, Materialize will sort values
as <code>text</code>.</p>
</li>
<li>
<p><a href="https://www.postgresql.org/docs/current/datatype-money.html" ><code>money</code></a>: When decoded as <code>text</code>, resulting <code>text</code> value cannot be cast
back to <code>numeric</code>, since PostgreSQL adds typical currency formatting to the
output.</p>
</li>
</ul>


### Truncation

<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource(s)/table(s) in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>

### Inherited tables

<p>When using <a href="https://www.postgresql.org/docs/current/tutorial-inheritance.html" >PostgreSQL table inheritance</a>,
PostgreSQL serves data from <code>SELECT</code>s as if the inheriting tables&rsquo; data is
also present in the inherited table. However, both PostgreSQL&rsquo;s logical
replication and <code>COPY</code> only present data written to the tables themselves,
i.e. the inheriting data is <em>not</em> treated as part of the inherited table.</p>
<p>PostgreSQL sources use logical replication and <code>COPY</code> to ingest table data,
so inheriting tables&rsquo; data will only be ingested as part of the inheriting
table, i.e. in Materialize, the data will not be returned when serving
<code>SELECT</code>s from the inherited table.</p>


You can mimic PostgreSQL&rsquo;s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>ADD SUBSOURCE</code> and
create a new view (materialized or non-) that unions the new table.

## Examples

> **Important:** Before creating a PostgreSQL source, you must set up logical replication in the
> upstream database. For step-by-step instructions, see the integration guide for
> your PostgreSQL service: [AlloyDB](/ingest-data/postgres-alloydb/),
> [Amazon RDS](/ingest-data/postgres-amazon-rds/),
> [Amazon Aurora](/ingest-data/postgres-amazon-aurora/),
> [Azure DB](/ingest-data/postgres-azure-db/),
> [Google Cloud SQL](/ingest-data/postgres-google-cloud-sql/),
> [Self-hosted](/ingest-data/postgres-self-hosted/).
>


### Creating a connection

A connection describes how to connect and authenticate to an external system you
want Materialize to read data from.

Once created, a connection is **reusable** across multiple `CREATE SOURCE`
statements. For more details on creating connections, check the
[`CREATE CONNECTION`](/sql/create-connection/#postgresql) documentation page.

```mzsql
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

If your PostgreSQL server is not exposed to the public internet, you can
[tunnel the connection](/sql/create-connection/#network-security-connections)
through an AWS PrivateLink service (Materialize Cloud) or an SSH bastion host.


**AWS PrivateLink:**

> **Note:** Connections using AWS PrivateLink is for Materialize Cloud only.
>



```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);
```

```mzsql
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

For step-by-step instructions on creating AWS PrivateLink connections and
configuring an AWS PrivateLink service to accept connections from Materialize,
check [this guide](/ops/network-security/privatelink/).


**SSH tunnel:**
```mzsql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize',
);
```

```mzsql
CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    SSH TUNNEL ssh_connection,
    DATABASE 'postgres'
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check
[this guide](/ops/network-security/ssh-tunnel/).




### Creating a source {#create-source-example}

_Create subsources for all tables included in the PostgreSQL publication_

```mzsql
CREATE SOURCE mz_source
    FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
    FOR ALL TABLES;
```

_Create subsources for all tables from specific schemas included in the
 PostgreSQL publication_

```mzsql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR SCHEMAS (public, project);
```

_Create subsources for specific tables included in the PostgreSQL publication_

```mzsql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR TABLES (table_1, table_2 AS alias_table_2);
```

#### Handling unsupported types

If the publication contains tables that use [data types](/sql/types/)
unsupported by Materialize, use the `TEXT COLUMNS` option to decode data as
`text` for the affected columns. This option expects the upstream names of the
replicated table and column (i.e. as defined in your PostgreSQL database).

```mzsql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (
    PUBLICATION 'mz_source',
    TEXT COLUMNS (upstream_table_name.column_of_unsupported_type)
  ) FOR ALL TABLES;
```

### Handling errors and schema changes


  {{__hugo_ctx pid=37}}
> **Note:** Work to more smoothly support ddl changes to upstream tables is currently in
> progress. The work introduces the ability to re-ingest the same upstream table
> under a new schema and switch over without downtime.
>
>

{{__hugo_ctx/}}





To handle upstream [schema changes](#schema-changes) or errored subsources, use
the [`DROP SOURCE`](/sql/alter-source/#context) syntax to drop the affected
subsource, and then [`ALTER SOURCE...ADD SUBSOURCE`](/sql/alter-source/) to add
the subsource back to the source.

```mzsql
-- List all subsources in mz_source
SHOW SUBSOURCES ON mz_source;

-- Get rid of an outdated or errored subsource
DROP SOURCE table_1;

-- Start ingesting the table with the updated schema or fix
ALTER SOURCE mz_source ADD SUBSOURCE table_1;
```
#### Adding subsources

When adding subsources to a PostgreSQL source, Materialize opens a temporary
replication slot to snapshot the new subsources' current states. After
completing the snapshot, the table will be kept up-to-date, like all other
tables in the publication.

#### Dropping subsources

Dropping a subsource prevents Materialize from ingesting any data from it, in
addition to dropping any state that Materialize previously had for the table.

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)
- PostgreSQL integration guides:
  - [AlloyDB](/ingest-data/postgres-alloydb/)
  - [Amazon RDS](/ingest-data/postgres-amazon-rds/)
  - [Amazon Aurora](/ingest-data/postgres-amazon-aurora/)
  - [Azure DB](/ingest-data/postgres-azure-db/)
  - [Google Cloud SQL](/ingest-data/postgres-google-cloud-sql/)
  - [Self-hosted](/ingest-data/postgres-self-hosted/)

[`enum`]: https://www.postgresql.org/docs/current/datatype-enum.html
[`money`]: https://www.postgresql.org/docs/current/datatype-money.html
