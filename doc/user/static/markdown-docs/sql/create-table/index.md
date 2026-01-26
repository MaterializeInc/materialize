# CREATE TABLE

`CREATE TABLE` creates a table that is persisted in durable storage.



`CREATE TABLE` defines a table that is persisted in durable storage.

In Materialize, you can create:
- Read-write tables. With read-write tables, users can read ([`SELECT`]) and
  write to the tables ([`INSERT`], [`UPDATE`], [`DELETE`]).

-  ***Private Preview***. Read-only tables from [PostgreSQL sources (new
  syntax)](/sql/create-source/postgres-v2/). Users cannot be write ([`INSERT`],
  [`UPDATE`], [`DELETE`]) to these tables. These tables are populated by [data
  ingestion from a source](/ingest-data/postgres/).


Tables in Materialize are similar to tables in standard relational databases:
they consist of rows and columns where the columns are fixed when the table is
created.

Tables can be joined with other tables, materialized views, views, and
subsources; and you can create views/materialized views/indexes on tables.


[//]: # "TODO(morsapaes) Bring back When to use a table? once there's more
clarity around best practices."

## Syntax


**Read-write table:**
### Read-write table

To create a new read-write table (i.e., users can perform
[`SELECT`](/sql/select/), [`INSERT`](/sql/insert/),
[`UPDATE`](/sql/update/), and [`DELETE`](/sql/delete/) operations):


```mzsql
CREATE [TEMP|TEMPORARY] TABLE [IF NOT EXISTS] <table_name> (
  <column_name> <column_type> [NOT NULL][DEFAULT <default_expr>]
  [, ...]
)
[WITH (
  PARTITION BY (<column_name> [, ...]) |
  RETAIN HISTORY [=] FOR <duration>
)]
;

```

| Syntax element | Description |
| --- | --- |
| **TEMP** / **TEMPORARY** | *Optional.* If specified, mark the table as temporary.  Temporary tables are: - Automatically dropped at the end of the session; - Not visible to other connections; - Created in the special `mz_temp` schema.  Temporary tables may depend upon other temporary database objects, but non-temporary tables may not depend on temporary objects.  |
| **IF NOT EXISTS** | *Optional.* If specified, do not throw an error if the table with the same name already exists. Instead, issue a notice and skip the table creation.  |
| `<table_name>` |  The name of the table to create. Names for tables must follow the [naming guidelines](/sql/identifiers/#naming-restrictions).  |
| `<column_name>` |  The name of a column to be created in the new table. Names for columns must follow the [naming guidelines](/sql/identifiers/#naming-restrictions).  |
| `<column_type>` |  The type of the column. For supported types, see [SQL data types](/sql/types/).  |
| **NOT NULL** | *Optional.* If specified, disallow  _NULL_ values for the column. Columns without this constraint can contain _NULL_ values.  |
| **DEFAULT <default_expr>** | *Optional.* If specified, use the `<default_expr>` as the default value for the column. If not specified, `NULL` is used as the default value.  |
| **WITH (<with_option>[,...])** |  The following `<with_option>`s are supported:  \| Option \| Description \| \|--------\|-------------\| \| `PARTITION BY (<column> [, ...])` \| {{< include-md file="shared-content/partition-by-option-description.md" >}} \| \| `RETAIN HISTORY <duration>` \| *Optional.* ***Private preview.** This option has known performance or stability issues and is under active development.* <br>If specified, Materialize retains historical data for the specified duration, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period).<br>Accepts positive [interval](/sql/types/interval/) values (e.g., `'1hr'`).\|  |



**PostgreSQL source table:**
### PostgreSQL source table



> **Note:** You must be on **v26+** to use the new syntax.
>
>


To create a read-only table from a [source](/sql/create-source/) connected
(via native connector) to an external PostgreSQL:


```mzsql
CREATE TABLE [IF NOT EXISTS] <table_name> FROM SOURCE <source_name> (REFERENCE <upstream_table>)
[WITH (
    TEXT COLUMNS (<column_name> [, ...])
  | EXCLUDE COLUMNS (<column_name> [, ...])
  | PARTITION BY (<column_name> [, ...])
  [, ...]
)]
;

```

| Syntax element | Description |
| --- | --- |
| **IF NOT EXISTS** | *Optional.* If specified, do not throw an error if the table with the same name already exists. Instead, issue a notice and skip the table creation.  {{< include-md file="shared-content/create-table-if-not-exists-tip.md" >}}  |
| `<table_name>` |  The name of the table to create. Names for tables must follow the [naming guidelines](/sql/identifiers/#naming-restrictions).  |
| `<source_name>` |  The name of the [source](/sql/create-source/) associated with the reference object from which to create the table.  |
| **(REFERENCE <upstream_table>)** |  The name of the upstream table from which to create the table. You can create multiple tables from the same upstream table.  To find the upstream tables available in your [source](/sql/create-source/), you can use the following query, substituting your source name for `<source_name>`:  <br>  ```mzsql SELECT refs.* FROM mz_internal.mz_source_references refs, mz_sources s WHERE s.name = '<source_name>' -- substitute with your source name AND refs.source_id = s.id; ```  |
| **WITH (<with_option>[,...])** | The following `<with_option>`s are supported:  \| Option \| Description \| \|--------\|-------------\| \| `TEXT COLUMNS (<column_name> [, ...])` \|*Optional.* If specified, decode data as `text` for the listed column(s),such as for unsupported data types. See also [supported types](#supported-data-types). \| \| `EXCLUDE COLUMNS (<column_name> [, ...])`\| *Optional.* If specified,exclude the listed column(s) from the table, such as for unsupported data types. See also [supported types](#supported-data-types).\| \| `PARTITION BY (<column_name> [, ...])` \| {{< include-md file="shared-content/partition-by-option-description.md" >}} \|  |


For an example, see [Create a table (PostgreSQL
source)](/sql/create-table/#create-a-table-postgresql-source).






## Read-write tables

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



> **Note:** You must be on **v26+** to use the new syntax.
>
>


### Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/sql/identifiers/#naming-restrictions).

<a name="supported-db-source-types"></a>

### Read-only tables

Source-populated tables are <strong>read-only</strong> tables. Users <strong>cannot</strong> perform write
operations
(<a href="/sql/insert/" ><code>INSERT</code></a>/<a href="/sql/update/" ><code>UPDATE</code></a>/<a href="/sql/delete/" ><code>DELETE</code></a>) on
these tables.

### Source-populated tables and snapshotting

<p>Creating the tables from sources starts the <a href="/ingest-data/#snapshotting" >snapshotting</a> process. Snapshotting syncs the
currently available data into Materialize. Because the initial snapshot is
persisted in the storage layer atomically (i.e., at the same ingestion
timestamp), you are not able to query the table until snapshotting is complete.</p>
> **Note:** During the snapshotting, the data ingestion for
> the existing tables for the same source is temporarily blocked. As such, if
> possible, you can resize the cluster to speed up the snapshotting process and
> once the process finishes, resize the cluster for steady-state.
>
>

### Supported data types

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


### Handling table schema changes

The use of <a href="/sql/create-source/postgres-v2/" ><code>CREATE SOURCE</code></a> with <code>CREATE TABLE FROM SOURCE</code> allows for the handling of the upstream DDL changes,
specifically adding or dropping columns in the upstream tables, without
downtime. See <a href="/ingest-data/postgres/source-versioning/" >Handling upstream schema changes with zero
downtime</a> for more details.

#### Incompatible schema changes

<p>All other schema changes to upstream tables will set the corresponding
Materialize tables into an error state, preventing reads from these tables.</p>
<p>To handle <a href="#incompatible-schema-changes" >incompatible schema changes</a>, drop
the affected table <a href="/sql/drop-table/" ><code>DROP TABLE</code></a> , and then, <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> to recreate the table with the
updated schema.</p>


### Upstream table truncation restrictions

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
view. You will need to add the inheriting tables via <code>CREATE TABLE .. FROM SOURCE</code> and create a new view (materialized or non-) that unions the new
table.

## Privileges

The privileges required to execute this statement are:

<ul>
<li><code>CREATE</code> privileges on the containing schema.</li>
<li><code>USAGE</code> privileges on all types used in the table definition.</li>
<li><code>USAGE</code> privileges on the schemas that all types in the statement are
contained in.</li>
</ul>


## Examples

### Create a table (User-populated)

The following example uses `CREATE TABLE` to create a new read-write table
`mytable` with two columns `a` (of type `int`) and `b` (of type `text` and
not nullable):


```mzsql
CREATE TABLE mytable (a int, b text NOT NULL);

```

Once a user-populated table is created, you can perform CRUD
(Create/Read/Update/Write) operations on it.

The following example uses [`INSERT`](/sql/insert/) to write two rows to the table:


```mzsql
INSERT INTO mytable VALUES
(1, 'hello'),
(2, 'goodbye')
;

```

The following example uses [`SELECT`](/sql/select/) to read all rows from the table:


```mzsql
SELECT * FROM mytable;

```The results should display the two rows inserted:

```hc {hl_lines="3-4"}
| a | b       |
| - | ------- |
| 1 | hello   |
| 2 | goodbye |
```


### Create a table (PostgreSQL source)



> **Note:** You must be on **v26+** to use the new syntax.
>
>
> The example assumes you have configured your upstream PostgreSQL 11+ (i.e.,
> enabled logical replication, created the publication for the various tables and
> replication user, and updated the network configuration).
>
> For details about configuring your upstream system, see the [PostgreSQL
> integration guides](/ingest-data/postgres/#supported-versions-and-services).
>
>



To create new **read-only** tables from a source table, use the `CREATE
TABLE ... FROM SOURCE ... (REFERENCE <upstream_table>)` statement. The
following example creates **read-only** tables `items` and `orders` from the
PostgreSQL source's `public.items` and `public.orders` tables (the schema is `public`).

{{< note >}}

- Although the example creates the tables with the same names as the
upstream tables, the tables in Materialize can have names that differ from
the referenced table names.

- For supported PostgreSQL data types, refer to [supported
types](/sql/create-table/#supported-data-types).

{{< /note >}}


```mzsql
/* This example assumes:
  - In the upstream PostgreSQL, you have defined:
    - replication user and password with the appropriate access.
    - a publication named `mz_source` for the `items` and `orders` tables.
  - In Materialize:
    - You have created a secret for the PostgreSQL password.
    - You have defined the connection to the upstream PostgreSQL.
    - You have used the connection to create a source.

   For example (substitute with your configuration):
      CREATE SECRET pgpass AS '<replication user password>'; -- substitute
      CREATE CONNECTION pg_connection TO POSTGRES (
        HOST '<hostname>',          -- substitute
        DATABASE <db>,              -- substitute
        USER <replication user>,    -- substitute
        PASSWORD SECRET pgpass
        -- [, <network security configuration> ]
      );

      CREATE SOURCE pg_source
      FROM POSTGRES CONNECTION pg_connection (
        PUBLICATION 'mz_source'       -- substitute
      );
*/

CREATE TABLE items
FROM SOURCE pg_source(REFERENCE public.items)
;
CREATE TABLE orders
FROM SOURCE pg_source(REFERENCE public.orders)
;

```
{{< include-md
file="shared-content/create-table-from-source-snapshotting.md" >}}

{{< include-md file="shared-content/create-table-if-not-exists-tip.md" >}}


Source-populated tables are <strong>read-only</strong> tables. Users <strong>cannot</strong> perform write
operations
(<a href="/sql/insert/" ><code>INSERT</code></a>/<a href="/sql/update/" ><code>UPDATE</code></a>/<a href="/sql/delete/" ><code>DELETE</code></a>) on
these tables.


Once the snapshotting process completes and the table is in the running state, you can query the table:


```mzsql
SELECT * FROM items;

```


## Related pages

- [`INSERT`]
- [`DROP TABLE`](/sql/drop-table)

[`INSERT`]: /sql/insert/
[`SELECT`]: /sql/select/
[`UPDATE`]: /sql/update/
[`DELETE`]: /sql/delete/
