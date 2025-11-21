<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)

</div>

# CREATE TABLE

`CREATE TABLE` defines a table that is persisted in durable storage.

In Materialize, you can create:

- Read-write tables. With read-write tables, users can read
  ([`SELECT`](/docs/sql/select/)) and write to the tables
  ([`INSERT`](/docs/sql/insert/), [`UPDATE`](/docs/sql/update/),
  [`DELETE`](/docs/sql/delete/)).

- ***Private Preview***. Read-only tables from [PostgreSQL sources (new
  syntax)](/docs/sql/create-source/postgres-v2/). Users cannot be write
  ([`INSERT`](/docs/sql/insert/), [`UPDATE`](/docs/sql/update/),
  [`DELETE`](/docs/sql/delete/)) to these tables. These tables are
  populated by [data ingestion from a
  source](/docs/ingest-data/postgres/). You must be on **v26+** to use
  the new syntax.

Tables in Materialize are similar to tables in standard relational
databases: they consist of rows and columns where the columns are fixed
when the table is created.

Tables can be joined with other tables, materialized views, views, and
subsources; and you can create views/materialized views/indexes on
tables.

## Syntax

<div class="code-tabs">

<div class="tab-content">

<div id="tab-read-write-table" class="tab-pane"
title="Read-write table">

### Read-write table

To create a new read-write table (i.e., users can perform
[`SELECT`](/docs/sql/select/), [`INSERT`](/docs/sql/insert/),
[`UPDATE`](/docs/sql/update/), and [`DELETE`](/docs/sql/delete/)
operations):

<div class="highlight">

``` chroma
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

</div>

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Parameter</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>TEMP</strong> / <strong>TEMPORARY</strong></td>
<td><p><em>Optional.</em> If specified, mark the table as temporary.</p>
<p>Temporary tables are:</p>
<ul>
<li>Automatically dropped at the end of the session;</li>
<li>Not visible to other connections;</li>
<li>Created in the special <code>mz_temp</code> schema.</li>
</ul>
<p>Temporary tables may depend upon other temporary database objects,
but non-temporary tables may not depend on temporary objects.</p></td>
</tr>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td><em>Optional.</em> If specified, do not throw an error if the table
with the same name already exists. Instead, issue a notice and skip the
table creation.</td>
</tr>
<tr>
<td><code>&lt;table_name&gt;</code></td>
<td>The name of the table to create. Names for tables must follow the <a
href="/docs/sql/identifiers/#naming-restrictions">naming
guidelines</a>.</td>
</tr>
<tr>
<td><code>&lt;column_name&gt;</code></td>
<td>The name of a column to be created in the new table. Names for
columns must follow the <a
href="/docs/sql/identifiers/#naming-restrictions">naming
guidelines</a>.</td>
</tr>
<tr>
<td><code>&lt;column_type&gt;</code></td>
<td>The type of the column. For supported types, see <a
href="/docs/sql/types/">SQL data types</a>.</td>
</tr>
<tr>
<td><strong>NOT NULL</strong></td>
<td><em>Optional.</em> If specified, disallow <em>NULL</em> values for
the column. Columns without this constraint can contain <em>NULL</em>
values.</td>
</tr>
<tr>
<td><strong>DEFAULT &lt;default_expr&gt;</strong></td>
<td><em>Optional.</em> If specified, use the
<code>&lt;default_expr&gt;</code> as the default value for the column.
If not specified, <code>NULL</code> is used as the default value.</td>
</tr>
<tr>
<td><strong>WITH (&lt;with_option&gt;[,…])</strong></td>
<td><p>The following <code>&lt;with_option&gt;</code>s are
supported:</p>
<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>PARTITION BY (&lt;column&gt; [, ...])</code></td>
<td></td>
</tr>
<tr>
<td><code>RETAIN HISTORY &lt;duration&gt;</code></td>
<td><em>Optional.</em> <em><strong>Private preview.</strong> This option
has known performance or stability issues and is under active
development.</em><br />
If specified, Materialize retains historical data for the specified
duration, which is useful to implement <a
href="/docs/transform-data/patterns/durable-subscriptions/#history-retention-period">durable
subscriptions</a>.<br />
Accepts positive <a href="/docs/sql/types/interval/">interval</a> values
(e.g., <code>'1hr'</code>).</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

For examples, see [Create a table
(user-populated)](/docs/sql/create-table/#create-a-table-user-populated).

</div>

<div id="tab-postgresql-source-table" class="tab-pane"
title="PostgreSQL source table">

### PostgreSQL source table

<div class="private-preview">

**PREVIEW** This feature is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

<div class="note">

**NOTE:** You must be on **v26+** to use the new syntax.

</div>

To create a read-only table from a [source](/docs/sql/create-source/)
connected (via native connector) to an external PostgreSQL:

<div class="highlight">

``` chroma
CREATE TABLE [IF NOT EXISTS] <table_name> FROM SOURCE <source_name> (REFERENCE <upstream_table>)
[WITH (
    TEXT COLUMNS (<column_name> [, ...])
  | EXCLUDE COLUMNS (<column_name> [, ...])
  [, ...]
)]
;
```

</div>

Creating the tables from sources starts the
[snapshotting](/docs/ingest-data/#snapshotting) process. Snapshotting
syncs the currently available data into Materialize. Because the initial
snapshot is persisted in the storage layer atomically (i.e., at the same
ingestion timestamp), you are not able to query the table until
snapshotting is complete.

<div class="note">

**NOTE:** During the snapshotting, the data ingestion for the existing
tables for the same source is temporarily blocked. As such, if possible,
you can resize the cluster to speed up the snapshotting process and once
the process finishes, resize the cluster for steady-state.

</div>

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Parameter</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td><p><em>Optional.</em> If specified, do not throw an error if the
table with the same name already exists. Instead, issue a notice and
skip the table creation.</p>
<div class="tip">
<strong>💡 Tip:</strong> The <code>IF NOT EXISTS</code> option can be
useful for idempotent table creation scripts. However, it only checks
whether a table with the same name exists, not whether the existing
table matches the specified table definition. Use with validation logic
to ensure the existing table is the one you intended to create.
</div></td>
</tr>
<tr>
<td><code>&lt;table_name&gt;</code></td>
<td>The name of the table to create. Names for tables must follow the <a
href="/docs/sql/identifiers/#naming-restrictions">naming
guidelines</a>.</td>
</tr>
<tr>
<td><code>&lt;source_name&gt;</code></td>
<td>The name of the <a href="/docs/sql/create-source/">source</a>
associated with the reference object from which to create the
table.</td>
</tr>
<tr>
<td><strong>(REFERENCE &lt;upstream_table&gt;)</strong></td>
<td><p>The name of the upstream table from which to create the table.
You can create multiple tables from the same upstream table.</p>
<p>To find the upstream tables available in your <a
href="/docs/sql/create-source/">source</a>, you can use the following
query, substituting your source name for
<code>&lt;source_name&gt;</code>:</p>
<br />
&#10;<div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT refs.*
FROM mz_internal.mz_source_references refs, mz_sources s
WHERE s.name = &#39;&lt;source_name&gt;&#39; -- substitute with your source name
AND refs.source_id = s.id;</code></pre>
</div></td>
</tr>
<tr>
<td><strong>WITH (&lt;with_option&gt;[,…])</strong></td>
<td><p>The following <code>&lt;with_option&gt;</code>s are
supported:</p>
<table>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>TEXT COLUMNS (&lt;column_name&gt; [, ...])</code></td>
<td><em>Optional.</em> If specified, decode data as <code>text</code>
for the listed column(s),such as for unsupported data types. See also <a
href="#supported-data-types">supported types</a>.</td>
</tr>
<tr>
<td><code>EXCLUDE COLUMNS (&lt;column_name&gt; [, ...])</code></td>
<td><em>Optional.</em> If specified,exclude the listed column(s) from
the table, such as for unsupported data types. See also <a
href="#supported-data-types">supported types</a>.</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

For examples, see [Create a table (PostgreSQL
source)](/docs/sql/create-table/#create-a-table-postgresql-source).

</div>

</div>

</div>

## Read-write tables

### Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/docs/sql/identifiers/#naming-restrictions).

### Known limitations

Tables do not currently support:

- Primary keys
- Unique constraints
- Check constraints

See also the known limitations for
[`INSERT`](/docs/sql/insert#known-limitations),
[`UPDATE`](/docs/sql/update#known-limitations), and
[`DELETE`](/docs/sql/delete#known-limitations).

## PostgreSQL source tables

<div class="private-preview">

**PREVIEW** This feature is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

<div class="note">

**NOTE:** You must be on **v26+** to use the new syntax.

</div>

### Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/docs/sql/identifiers/#naming-restrictions).

<span id="supported-db-source-types"></span>

### Read-only tables

Source-populated tables are **read-only** tables. Users **cannot**
perform write operations
([`INSERT`](/docs/sql/insert/)/[`UPDATE`](/docs/sql/update/)/[`DELETE`](/docs/sql/delete/))
on these tables.

### Source-populated tables and snapshotting

Creating the tables from sources starts the
[snapshotting](/docs/ingest-data/#snapshotting) process. Snapshotting
syncs the currently available data into Materialize. Because the initial
snapshot is persisted in the storage layer atomically (i.e., at the same
ingestion timestamp), you are not able to query the table until
snapshotting is complete.

<div class="note">

**NOTE:** During the snapshotting, the data ingestion for the existing
tables for the same source is temporarily blocked. As such, if possible,
you can resize the cluster to speed up the snapshotting process and once
the process finishes, resize the cluster for steady-state.

</div>

### Supported data types

Materialize natively supports the following PostgreSQL types (including
the array type for each of the types):

- `bool`
- `bpchar`
- `bytea`
- `char`
- `date`
- `daterange`
- `float4`
- `float8`
- `int2`
- `int2vector`
- `int4`
- `int4range`
- `int8`
- `int8range`
- `interval`
- `json`
- `jsonb`
- `numeric`
- `numrange`
- `oid`
- `text`
- `time`
- `timestamp`
- `timestamptz`
- `tsrange`
- `tstzrange`
- `uuid`
- `varchar`

Replicating tables that contain **unsupported [data
types](/docs/sql/types/)** is possible via the `TEXT COLUMNS` option.
The specified columns will be treated as `text`; i.e., will not have the
expected PostgreSQL type features. For example:

- [`enum`](https://www.postgresql.org/docs/current/datatype-enum.html):
  When decoded as `text`, the implicit ordering of the original
  PostgreSQL `enum` type is not preserved; instead, Materialize will
  sort values as `text`.

- [`money`](https://www.postgresql.org/docs/current/datatype-money.html):
  When decoded as `text`, resulting `text` value cannot be cast back to
  `numeric`, since PostgreSQL adds typical currency formatting to the
  output.

### Handling table schema changes

The use of [`CREATE SOURCE`](/docs/sql/create-source/postgres-v2/) with
`CREATE TABLE FROM SOURCE` allows for the handling of the upstream DDL
changes, specifically adding or dropping columns in the upstream tables,
without downtime. See [Handling upstream schema changes with zero
downtime](/docs/ingest-data/postgres/source-versioning/) for more
details.

#### Incompatible schema changes

All other schema changes to upstream tables will set the corresponding
Materialize tables into an error state, preventing reads from these
tables.

To handle [incompatible schema changes](#incompatible-schema-changes),
drop the affected table [`DROP TABLE`](/docs/sql/drop-table/) , and
then, [`CREATE TABLE FROM SOURCE`](/docs/sql/create-table/) to recreate
the table with the updated schema.

### Upstream table truncation restrictions

Avoid truncating upstream tables that are being replicated into
Materialize. If a replicated upstream table is truncated, the
corresponding subsource(s)/table(s) in Materialize becomes inaccessible
and will not produce any data until it is recreated.

Instead of truncating, use an unqualified `DELETE` to remove all rows
from the upstream table:

<div class="highlight">

``` chroma
DELETE FROM t;
```

</div>

### Inherited tables

When using [PostgreSQL table
inheritance](https://www.postgresql.org/docs/current/tutorial-inheritance.html),
PostgreSQL serves data from `SELECT`s as if the inheriting tables’ data
is also present in the inherited table. However, both PostgreSQL’s
logical replication and `COPY` only present data written to the tables
themselves, i.e. the inheriting data is *not* treated as part of the
inherited table.

PostgreSQL sources use logical replication and `COPY` to ingest table
data, so inheriting tables’ data will only be ingested as part of the
inheriting table, i.e. in Materialize, the data will not be returned
when serving `SELECT`s from the inherited table.

You can mimic PostgreSQL’s `SELECT` behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using `UNION ALL`). However, if new tables inherit
from the table, data from the inheriting tables will not be available in
the view. You will need to add the inheriting tables via
`CREATE TABLE .. FROM SOURCE` and create a new view (materialized or
non-) that unions the new table.

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all types used in the table definition.
- `USAGE` privileges on the schemas that all types in the statement are
  contained in.

## Examples

### Create a table (User-populated)

The following example uses `CREATE TABLE` to create a new read-write
table `mytable` with two columns `a` (of type `int`) and `b` (of type
`text` and not nullable):

<div class="highlight">

``` chroma
CREATE TABLE mytable (a int, b text NOT NULL);
```

</div>

Once a user-populated table is created, you can perform CRUD
(Create/Read/Update/Write) operations on it.

The following example uses [`INSERT`](/docs/sql/insert/) to write two
rows to the table:

<div class="highlight">

``` chroma
INSERT INTO mytable VALUES
(1, 'hello'),
(2, 'goodbye')
;
```

</div>

The following example uses [`SELECT`](/docs/sql/select/) to read all
rows from the table:

<div class="highlight">

``` chroma
SELECT * FROM mytable;
```

</div>

The results should display the two rows inserted:

<div class="highlight">

``` chroma
| a | b       |
| - | ------- |
| 1 | hello   |
| 2 | goodbye |
```

</div>

### Create a table (PostgreSQL source)

<div class="private-preview">

**PREVIEW** This feature is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

<div class="note">

**NOTE:**

You must be on **v26+** to use the new syntax.

The example assumes you have configured your upstream PostgreSQL 11+
(i.e., enabled logical replication, created the publication for the
various tables and replication user, and updated the network
configuration).

For details about configuring your upstream system, see the [PostgreSQL
integration
guides](/docs/ingest-data/postgres/#supported-versions-and-services).

</div>

To create new **read-only** tables from a source table, use the
`CREATE TABLE ... FROM SOURCE ... (REFERENCE <upstream_table>)`
statement. The following example creates **read-only** tables `items`
and `orders` from the PostgreSQL source’s `public.items` and
`public.orders` tables (the schema is `public`).

<div class="note">

**NOTE:**

- Although the example creates the tables with the same names as the
  upstream tables, the tables in Materialize can have names that differ
  from the referenced table names.

- For supported PostgreSQL data types, refer to [supported
  types](/docs/sql/create-table/#supported-data-types).

</div>

<div class="highlight">

``` chroma
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

</div>

Creating the tables from sources starts the
[snapshotting](/docs/ingest-data/#snapshotting) process. Snapshotting
syncs the currently available data into Materialize. Because the initial
snapshot is persisted in the storage layer atomically (i.e., at the same
ingestion timestamp), you are not able to query the table until
snapshotting is complete.

<div class="note">

**NOTE:** During the snapshotting, the data ingestion for the existing
tables for the same source is temporarily blocked. As such, if possible,
you can resize the cluster to speed up the snapshotting process and once
the process finishes, resize the cluster for steady-state.

</div>

<div class="tip">

**💡 Tip:** The `IF NOT EXISTS` option can be useful for idempotent
table creation scripts. However, it only checks whether a table with the
same name exists, not whether the existing table matches the specified
table definition. Use with validation logic to ensure the existing table
is the one you intended to create.

</div>

Source-populated tables are **read-only** tables. Users **cannot**
perform write operations
([`INSERT`](/docs/sql/insert/)/[`UPDATE`](/docs/sql/update/)/[`DELETE`](/docs/sql/delete/))
on these tables.

Once the snapshotting process completes and the table is in the running
state, you can query the table:

<div class="highlight">

``` chroma
SELECT * FROM items;
```

</div>

## Related pages

- [`INSERT`](/docs/sql/insert/)
- [`DROP TABLE`](/docs/sql/drop-table)

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/create-table.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
