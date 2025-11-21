<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)  /  [CREATE
SOURCE](/docs/sql/create-source/)

</div>

# CREATE SOURCE: PostgreSQL (New Syntax)

<div class="private-preview">

**PREVIEW** This feature is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

<div class="annotation">

<div class="annotation-title">

Disambiguation

</div>

<div>

This page reflects the new syntax which allows Materialize to handle
upstream DDL changes, specifically adding or dropping columns, without
downtime. For the deprecated syntax, see the [old reference
page](/docs/sql/create-source/postgres/).

</div>

</div>

Creates a new source from PostgreSQL. Materialize supports creating
sources from PostgreSQL version 11+. Once a new source is created, you
can [`CREATE TABLE FROM SOURCE`](/docs/sql/create-table/) from the
source to create the corresponding tables in Materialize and start the
data ingestion process.

## Prerequisites

To create a source from PostgreSQL 11+, you must first:

- **Configure upstream PostgreSQL instance**
  - Set up logical replication.
  - Create a publication.
  - Create a replication user and password for Materialize to use to
    connect.
- **Configure network security**
  - Ensure Materialize can connect to your PostgreSQL instance.
- **Create a connection to PostgreSQL in Materialize**
  - The connection setup depends on the network security configuration.

For details, see the [PostgreSQL integration
guides](/docs/ingest-data/postgres/#integration-guides).

## Syntax

To create a source from an external PostgreSQL:

<div class="highlight">

``` chroma
CREATE SOURCE [IF NOT EXISTS] <source_name>
[IN CLUSTER <cluster_name>]
FROM POSTGRES CONNECTION <connection_name> (PUBLICATION '<publication_name>')
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
<td><strong>IF NOT EXISTS</strong></td>
<td><em>Optional.</em> If specified, do not throw an error if a source
with the same name already exists. Instead, issue a notice and skip the
source creation.</td>
</tr>
<tr>
<td><code>&lt;source_name&gt;</code></td>
<td>The name of the source to create. Names for sources must follow the
<a href="/docs/sql/identifiers/#naming-restrictions">naming
guidelines</a>.</td>
</tr>
<tr>
<td><strong>IN CLUSTER</strong> <code>&lt;cluster_name&gt;</code></td>
<td><p><em>Optional.</em> The <a
href="/docs/sql/create-cluster">cluster</a> to maintain this source.
Otherwise, the source will be created in the active cluster.</p>
<div class="tip">
<strong>💡 Tip:</strong> If possible, use a cluster dedicated just for
sources. See also <a
href="/docs/manage/operational-guidelines/#sources">Operational
guidelines</a>.
</div></td>
</tr>
<tr>
<td><code>&lt;connection_name&gt;</code></td>
<td><p>The name of the PostgreSQL connection to use for the source. For
details on creating connections, check the <a
href="/docs/sql/create-connection/#postgresql"><code>CREATE CONNECTION</code></a>
documentation page.</p>
<p>A connection is <strong>reusable</strong> across multiple
<code>CREATE SOURCE</code> statements.</p></td>
</tr>
<tr>
<td><code>&lt;publication_name&gt;</code></td>
<td>The name of the PostgreSQL publication to associate with the source.
For details on creating a publication in your PostgreSQL database, see
the <a href="/docs/ingest-data/postgres/#integration-guides">integration
guides for your PostgreSQL</a>.</td>
</tr>
</tbody>
</table>

## Details

### Ingesting data

After a source is created, you can create tables from the source,
referencing the tables in the publication, to start ingesting data. You
can create multiple tables that reference the same table in the
publication.

See [`CREATE TABLE FROM SOURCE`](/docs/sql/create-table/) for details.

#### Handling table schema changes

The use of the `CREATE SOURCE` with the new
[`CREATE TABLE FROM SOURCE`](/docs/sql/create-table/) allows for the
handling of certain upstream DDL changes without downtime.

See
[`CREATE TABLE FROM SOURCE`](/docs/sql/create-table/#handling-table-schema-changes)
for details.

#### Supported types

With the new syntax, after a PostgreSQL source is created, you
[`CREATE TABLE FROM SOURCE`](/docs/sql/create-table/) to create a
corresponding table in Matererialize and start ingesting data.

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

For more information, including strategies for handling unsupported
types, see [`CREATE TABLE FROM SOURCE`](/docs/sql/create-table/).

#### Upstream table truncation restrictions

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

For additional considerations, see also
[`CREATE TABLE`](/docs/sql/create-table/).

### Publication membership

PostgreSQL’s logical replication API does not provide a signal when
users remove tables from publications. Because of this, Materialize
relies on periodic checks to determine if a table has been removed from
a publication, at which time it generates an irrevocable error,
preventing any values from being read from the table.

However, it is possible to remove a table from a publication and then
re-add it before Materialize notices that the table was removed. In this
case, Materialize can no longer provide any consistency guarantees about
the data we present from the table and, unfortunately, is wholly unaware
that this occurred.

To mitigate this issue, if you need to drop and re-add a table to a
publication, ensure that you remove the table/subsource from the source
*before* re-adding it using the [`DROP SOURCE`](/docs/sql/drop-source/)
command.

### PostgreSQL replication slots

When you define a source, Materialize will automatically create a
**replication slot** in the upstream PostgreSQL database (see
[PostgreSQL replication slots](#postgresql-replication-slots)). Each
source ingests the raw replication stream data for all tables in the
specified publication using **a single** replication slot. This allows
you to minimize the performance impact on the upstream database as well
as reuse the same source across multiple materializations.

The name of the replication slot created by Materialize is prefixed with
`materialize_`. In Materialize, you can query the
`mz_internal.mz_postgres_sources` to find the replication slots created:

<div class="highlight">

``` chroma
SELECT id, replication_slot FROM mz_internal.mz_postgres_sources;
```

</div>

```
    id   |             replication_slot
---------+----------------------------------------------
  u8     | materialize_7f8a72d0bf2a4b6e9ebc4e61ba769b71
```

<div class="tip">

**💡 Tip:**

- For PostgreSQL 13+, set a reasonable value for
  [`max_slot_wal_keep_size`](https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE)
  to limit the amount of storage used by replication slots.

- If you stop using Materialize, or if either the Materialize instance
  or the PostgreSQL instance crash, delete any replication slots. You
  can query the `mz_internal.mz_postgres_sources` table to look up the
  name of the replication slot created for each source.

- If you delete all objects that depend on a source without also
  dropping the source, the upstream replication slot remains and will
  continue to accumulate data so that the source can resume in the
  future. To avoid unbounded disk space usage, make sure to use
  [`DROP SOURCE`](/docs/sql/drop-source/) or manually delete the
  replication slot.

</div>

## Examples

### Prerequisites

To create a source from PostgreSQL 11+, you must first:

- **Configure upstream PostgreSQL instance**
  - Set up logical replication.
  - Create a publication.
  - Create a replication user and password for Materialize to use to
    connect.
- **Configure network security**
  - Ensure Materialize can connect to your PostgreSQL instance.
- **Create a connection to PostgreSQL in Materialize**
  - The connection setup depends on the network security configuration.

For details, see the [PostgreSQL integration
guides](/docs/ingest-data/postgres/#integration-guides).

### Create a source

Once you have configured the upstream PostgreSQL, network security, and
created the connection, you can create the source. In this example, the
PostgreSQL publication is `mz_source` and the connection to PostgreSQL
is `pg_connection`.

<div class="highlight">

``` chroma
/* This example assumes:
- In the upstream PostgreSQL, you have defined:
  - replication user and password with the appropriate access.
  - a publication named `mz_source` for the `public.items` and `public.orders` tables.
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
*/

CREATE SOURCE pg_source
FROM POSTGRES CONNECTION pg_connection (
  PUBLICATION 'mz_source'
);
```

</div>

After a source is created, you can create tables from the source,
referencing specific table(s). For example, the following creates a
table in Materialize from the upstream table `public.items`.

<div class="highlight">

``` chroma
CREATE TABLE items
FROM SOURCE pg_source(REFERENCE public.items)
;
```

</div>

<div class="note">

**NOTE:**

- Although the example creates the table with the same name as the
  upstream table, the table in Materialize can have a different name.
- You can create multiple tables that reference the same upstream table.

</div>

For more information, see [`CREATE TABLE`](/docs/sql/create-table/).

## Related pages

- [`CREATE TABLE`](/docs/sql/create-table/)
- [`CREATE SECRET`](/docs/sql/create-secret)
- [`CREATE CONNECTION`](/docs/sql/create-connection)
- [PostgreSQL integration
  guides](/docs/ingest-data/postgres/#integration-guides)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/create-source/postgres-v2.md"
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
