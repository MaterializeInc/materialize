# PostgreSQL

Connecting Materialize to a PostgreSQL database for Change Data Capture (CDC).



## Change Data Capture (CDC)

Materialize supports PostgreSQL as a real-time data source. The
[PostgreSQL source](/sql/create-source/postgres//) uses PostgreSQL's
[replication protocol](/sql/create-source/postgres/#change-data-capture)
to **continually ingest changes** resulting from CRUD operations in the upstream
database. The native support for PostgreSQL Change Data Capture (CDC) in
Materialize gives you the following benefits:

* **No additional infrastructure:** Ingest PostgreSQL change data into
    Materialize in real-time with no architectural changes or additional
    operational overhead. In particular, you **do not need to deploy Kafka and
    Debezium** for PostgreSQL CDC.

* **Transactional consistency:** The PostgreSQL source ensures that transactions
    in the upstream PostgreSQL database are respected downstream. Materialize
    will **never show partial results** based on partially replicated
    transactions.

* **Incrementally updated materialized views:** Materialized views in PostgreSQL
    are computationally expensive and require manual refreshes. You can use
    Materialize as a read-replica to build views on top of your PostgreSQL data
    that are efficiently maintained and always up-to-date.

## Supported versions and services

The PostgreSQL source requires **PostgreSQL 11+** and is compatible with most
common PostgreSQL hosted services.

## Integration guides

The following integration guides are available:

<ul>
<li><a href="/ingest-data/postgres/alloydb/" >AlloyDB for PostgreSQL</a></li>
<li><a href="/ingest-data/postgres/amazon-aurora/" >Amazon Aurora for PostgreSQL</a></li>
<li><a href="/ingest-data/postgres/amazon-rds/" >Amazon RDS for PostgreSQL</a></li>
<li><a href="/ingest-data/postgres/azure-db/" >Azure DB for PostgreSQL</a></li>
<li><a href="/ingest-data/postgres/cloud-sql/" >Google Cloud SQL for PostgreSQL</a></li>
<li><a href="/ingest-data/postgres/neon/" >Neon</a></li>
<li><a href="/ingest-data/postgres/self-hosted/" >Self-hosted PostgreSQL</a></li>
</ul>


## Considerations

<h3 id="schema-changes">Schema changes</h3>
<p>Materialize supports schema changes in the upstream database as follows:</p>
<h4 id="compatible-schema-changes-legacy-syntax">Compatible schema changes (Legacy syntax)</h4>
<blockquote>
<p><strong>Note:</strong> This section refer to the legacy <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a> that creates subsources as part of the
<code>CREATE SOURCE</code> operation.  To be able to handle the upstream column
additions and drops, see <a href="/sql/create-source/postgres-v2/" ><code>CREATE SOURCE (New Syntax)</code></a> and <a href="/sql/create-table" ><code>CREATE TABLE FROM SOURCE</code></a>.</p>
</blockquote>
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
<h4 id="incompatible-schema-changes">Incompatible schema changes</h4>
<p>All other schema changes to upstream tables will set the corresponding
Materialize tables into an error state, preventing reads from these tables.</p>
<p>To handle <a href="#incompatible-schema-changes" >incompatible schema changes</a>, drop
the affected table <a href="/sql/drop-table/" ><code>DROP TABLE</code></a> , and then, <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> to recreate the table with the
updated schema.</p>
<h3 id="publication-membership">Publication membership</h3>
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
<p>To mitigate this issue, if you need to drop and re-add a table to a
publication, ensure that you remove the table/subsource from the source
<em>before</em> re-adding it using the <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> command.</p>
<h3 id="supported-types">Supported types</h3>
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
<h3 id="truncation">Truncation</h3>
<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource(s)/table(s) in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>
<h3 id="inherited-tables">Inherited tables</h3>
<p>When using <a href="https://www.postgresql.org/docs/current/tutorial-inheritance.html" >PostgreSQL table inheritance</a>,
PostgreSQL serves data from <code>SELECT</code>s as if the inheriting tables&rsquo; data is
also present in the inherited table. However, both PostgreSQL&rsquo;s logical
replication and <code>COPY</code> only present data written to the tables themselves,
i.e. the inheriting data is <em>not</em> treated as part of the inherited table.</p>
<p>PostgreSQL sources use logical replication and <code>COPY</code> to ingest table data,
so inheriting tables&rsquo; data will only be ingested as part of the inheriting
table, i.e. in Materialize, the data will not be returned when serving
<code>SELECT</code>s from the inherited table.</p>
<ul>
<li>
<p>If using legacy syntax <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a>:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>ADD SUBSOURCE</code> and
create a new view (materialized or non-) that unions the new table.</p>
</li>
<li>
<p>If using new <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> syntax:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>CREATE TABLE .. FROM SOURCE</code> and create a new view (materialized or non-) that unions the new
table.</p>
</li>
</ul>
<h3 id="replication-slots">Replication slots</h3>
<p>Each source ingests the raw replication stream data for all tables in the
specified publication using <strong>a single</strong> replication slot. To manage
replication slots:</p>
<ul>
<li>
<p>For PostgreSQL 13+, set a reasonable value
for <a href="https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE" ><code>max_slot_wal_keep_size</code></a>
to limit the amount of storage used by replication slots.</p>
</li>
<li>
<p>If you stop using Materialize, or if either the Materialize instance or
the PostgreSQL instance crash, delete any replication slots. You can query
the <code>mz_internal.mz_postgres_sources</code> table to look up the name of the
replication slot created for each source.</p>
</li>
<li>
<p>If you delete all objects that depend on a source without also dropping
the source, the upstream replication slot remains and will continue to
accumulate data so that the source can resume in the future. To avoid
unbounded disk space usage, make sure to use <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> or manually delete the replication slot.</p>
</li>
</ul>
<h3 id="modifying-an-existing-source">Modifying an existing source</h3>
<p>When you add a new subsource to an existing source (<a href="/sql/alter-source/" ><code>ALTER SOURCE ... ADD SUBSOURCE ...</code></a>), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.
</p>




---

## FAQ: PostgreSQL sources


This page addresses common questions and challenges when working with PostgreSQL
sources in Materialize. For general ingestion questions/troubleshooting, see:
- [Monitoring data ingestion](/ingest-data/monitoring-data-ingestion/).
- [Troubleshooting/FAQ](/ingest-data/troubleshooting/).

## For my trial/POC, what if I cannot use `REPLICA IDENTITY FULL`?

Materialize requires `REPLICA IDENTITY FULL` on PostgreSQL tables to capture all
column values in change events. If for your trial/POC (Proof-of-concept) you cannot modify your existing tables, here are two common alternatives:

- **Outbox Pattern (shadow tables)**

  > **Note:** With the Outbox pattern, you will need to implement dual writes so that all changes apply to both the original and shadow tables.
>
>


  With the Outbox pattern, you create duplicate "shadow" tables for the ones you
  want to replicate and set the shadow tables to `REPLICA IDENTITY FULL`. You
  can then use these shadow tables for Materialize instead of the originals.

- **Sidecar Pattern**

  > **Note:** With the Sidecar pattern, you will need to keep the sidecar in sync with the
>   source database (e.g., via logical replication or ETL processes).
>
>


  With the Sidecar pattern, you create a separate PostgreSQL instance as an
  integration layer. That is, in the sidecar instance, you recreate the tables
  you want to replicate, setting these tableswith `REPLICA IDENTITY FULL`. You
  can then use the sidecar for Materialiez instead of your primary database.

## What if my table contains data types that are unsupported in Materialize?

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


See also: [PostgreSQL considerations](/ingest-data/postgres/#considerations).


---

## Guide: Handle upstream schema changes with zero downtime



> **Note:** - Changing column types is currently unsupported.
>
> -
>


Materialize allows you to handle certain types of upstream
table schema changes seamlessly, specifically:

- Adding a column in the upstream database.
- Dropping a column in the upstream database.

This guide walks you through how to handle these changes without any downtime in Materialize.

## Prerequisites

Some familiarity with Materialize. If you've never used Materialize before,
start with our [guide to getting started](/get-started/quickstart/) to learn
how to connect a database to Materialize.

### Set up a PostgreSQL database

For this guide, setup a PostgreSQL 11+ database. In your PostgreSQL, create a
table `T` and populate:

```sql
CREATE TABLE T (
    A INT
);

INSERT INTO T (A) VALUES
    (10);
```

### Connect your source database to Materialize

<p>To create a source from PostgreSQL 11+, you must first:</p>
<ul>
<li><strong>Configure upstream PostgreSQL instance</strong>
<ul>
<li>Set up logical replication.</li>
<li>Create a publication.</li>
<li>Create a replication user and password for Materialize to use to connect.</li>
</ul>
</li>
<li><strong>Configure network security</strong>
<ul>
<li>Ensure Materialize can connect to your PostgreSQL instance.</li>
</ul>
</li>
<li><strong>Create a connection to PostgreSQL in Materialize</strong>
<ul>
<li>The connection setup depends on the network security configuration.</li>
</ul>
</li>
</ul>
<p>For details, see the <a href="/ingest-data/postgres/#integration-guides" >PostgreSQL integration
guides</a>.</p>


## Create a source using the new syntax

In Materialize, create a source using the updated [`CREATE SOURCE`
syntax](/sql/create-source/postgres-v2/).

```sql
CREATE SOURCE IF NOT EXISTS my_source
    FROM POSTGRES CONNECTION my_connection (PUBLICATION 'mz_source');
```

Unlike the [legacy syntax](/sql/create-source/postgres/), the new syntax does
not include the `FOR [[ALL] TABLES|SCHEMAS]` clause; i.e., the new syntax does
not create corresponding subsources in Materialize automatically. Instead, the
new syntax requires a separate [`CREATE TABLE ... FROM
SOURCE`](/sql/create-table/), which will create the corresponding tables and
start the snapshotting process. See [Create a table from the
source](#create-a-table-from-the-source).

> **Note:** The [legacy syntax](/sql/create-source/postgres/) is still supported. However,
> the legacy syntax doesn't support upstream schema changes.
>


## Create a table from the source
To start ingesting specific tables from your source database, you can create a
table in Materialize. We'll add it into the v1 schema in Materialize.

```sql
CREATE SCHEMA v1;

CREATE TABLE v1.T
    FROM SOURCE my_source(REFERENCE public.T);
```

Once you've created a table from source, the [initial
snapshot](/ingest-data/#snapshotting) of table `v1.T` will begin.

> **Note:** During the snapshotting, the data ingestion for the other tables associated with
> the source is temporarily blocked. As before, you can monitor progress for the
> snapshot operation on the overview page for the source in the Materialize
> console.
>
>


## Create a view on top of the table.

For this guide, add a materialized view `matview` (also in schema `v1`) that
sums column `A` from table `T`.

```sql
CREATE MATERIALIZED VIEW v1.matview AS
    SELECT SUM(A) from v1.T;
```

## Handle upstream column addition

### A. Add a column in your upstream PostgreSQL database

In your upstream PostgreSQL database, add a new column `B` to the table `T`:

```sql
ALTER TABLE T
    ADD COLUMN B BOOLEAN DEFAULT false;

INSERT INTO T (A, B) VALUES
    (20, true);
```

This operation will have no immediate effect in Materialize. In Materialize,
`v1.T` will continue to ingest only column `A`. The materialized view
`v1.matview` will continue to have access to column `A` as well.

### B. Incorporate the new column in Materialize

To incorporate the new column into Materialize, create a new `v2` schema and
recreate the table in the new schema:

```sql
CREATE SCHEMA v2;

CREATE TABLE v2.T
    FROM SOURCE my_source(REFERENCE public.T);
```

The [snapshotting](/ingest-data/#snapshotting) of table `v2.T` will begin.
`v2.T` will include columns `A` and `B`.

> **Note:** During the snapshotting, the data ingestion for the other tables associated with
> the source is temporarily blocked. As before, you can monitor progress for the
> snapshot operation on the overview page for the source in the Materialize
> console.
>
>



When the new `v2.T` table has finished snapshotting, create a new materialized
view `matview` in the new schema.  Since the new `v2.matview` is referencing the
new `v2.T`, it can reference column `B`:

```sql {hl_lines="4"}
CREATE MATERIALIZED VIEW v2.matview AS
    SELECT SUM(A)
    FROM v2.T
    WHERE B = true;
```

## Handle upstream column drop

### A. Exclude the column in Materialize

To drop a column safely, in Materialize, first, create a new `v3` schema, and
recreate table `T` in the new schema but exclude the column to drop. In this
example, we'll drop the column B.

```sql
CREATE SCHEMA v3;
CREATE TABLE v3.T
    FROM SOURCE my_source(REFERENCE public.T) WITH (EXCLUDE COLUMNS (B));
```

> **Note:** During the snapshotting, the data ingestion for the other tables associated with
> the source is temporarily blocked. As before, you can monitor progress for the
> snapshot operation on the overview page for the source in the Materialize
> console.
>
>


### B. Drop a column in your upstream PostgreSQL database

In your upstream PostgreSQL database, drop the column `B` from the table `T`:

```sql
ALTER TABLE T DROP COLUMN B;
```

Dropping the column B will have no effect on `v3.T`. However, the drop affects
`v2.T` and `v2.matview` from our earlier examples. When the user attempts to
read from either, Materialize will report an error that the source table schema
has been altered.

## Optional: Swap schemas

When you're ready to fully cut over to the new source version, you can optionally swap the schemas and drop the old objects.

```sql
ALTER SCHEMA v1 SWAP WITH v3;

DROP SCHEMA v3 CASCADE;
```


---

## Ingest data from AlloyDB


This page shows you how to stream data from [AlloyDB for PostgreSQL](https://cloud.google.com/alloydb)
to Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).
>
>


## Before you begin

<ul>
<li>
<p>Make sure you are running PostgreSQL 11 or higher.</p>
</li>
<li>
<p>Make sure you have access to your PostgreSQL instance via <a href="https://www.postgresql.org/docs/current/app-psql.html" ><code>psql</code></a>,
or your preferred SQL client.</p>
</li>
</ul>


If you don't already have an AlloyDB instance, creating one involves several
steps, including configuring your cluster and setting up network connections.
For detailed instructions, refer to the [AlloyDB documentation](https://cloud.google.com/alloydb/docs).

## A. Configure AlloyDB

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

To enable logical replication in AlloyDB, see the
[AlloyDB documentation](https://cloud.google.com/datastream/docs/configure-your-source-postgresql-database#configure_alloydb_for_replication).

### 2. Create a publication and a replication user

<p>Once logical replication is enabled, the next step is to create a publication
with the tables that you want to replicate to Materialize. You&rsquo;ll also need a
user for Materialize with sufficient privileges to manage replication.</p>
<ol>
<li>
<p>For each table that you want to replicate to Materialize, set the
<a href="https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY" >replica identity</a>
to <code>FULL</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span> <span class="k">REPLICA</span> <span class="k">IDENTITY</span> <span class="k">FULL</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span> <span class="k">REPLICA</span> <span class="k">IDENTITY</span> <span class="k">FULL</span><span class="p">;</span>
</span></span></code></pre></div><p><code>REPLICA IDENTITY FULL</code> ensures that the replication stream includes the
previous data of changed rows, in the case of <code>UPDATE</code> and <code>DELETE</code>
operations. This setting enables Materialize to ingest PostgreSQL data with
minimal in-memory state. However, you should expect increased disk usage in
your PostgreSQL database.</p>
</li>
<li>
<p>Create a <a href="https://www.postgresql.org/docs/current/logical-replication-publication.html" >publication</a>
with the tables you want to replicate:</p>
<p><em>For specific tables:</em></p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="n">PUBLICATION</span> <span class="n">mz_source</span> <span class="k">FOR</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span><span class="p">,</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span><span class="p">;</span>
</span></span></code></pre></div><p><em>For all tables in the database:</em></p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="n">PUBLICATION</span> <span class="n">mz_source</span> <span class="k">FOR</span> <span class="k">ALL</span> <span class="k">TABLES</span><span class="p">;</span>
</span></span></code></pre></div><p>The <code>mz_source</code> publication will contain the set of change events generated
from the specified tables, and will later be used to ingest the replication
stream.</p>
<p>Be sure to include only the tables you need. If the publication includes
additional tables, Materialize will waste resources on ingesting and then
immediately discarding the data.</p>
</li>
<li>
<p>Create a user for Materialize, if you don&rsquo;t already have one:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">USER</span> <span class="n">materialize</span> <span class="k">PASSWORD</span> <span class="s1">&#39;&lt;password&gt;&#39;</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>Grant the user permission to manage replication:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">ROLE</span> <span class="n">materialize</span> <span class="k">WITH</span> <span class="n">REPLICATION</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>Grant the user the required permissions on the tables you want to replicate:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="n">CONNECT</span> <span class="k">ON</span> <span class="k">DATABASE</span> <span class="o">&lt;</span><span class="n">dbname</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="n">USAGE</span> <span class="k">ON</span> <span class="k">SCHEMA</span> <span class="o">&lt;</span><span class="k">schema</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><p>Once connected to your database, Materialize will take an initial snapshot
of the tables in your publication. <code>SELECT</code> privileges are required for
this initial snapshot.</p>
<p>If you expect to add tables to your publication, you can grant <code>SELECT</code> on
all tables in the schema instead of naming the specific tables:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="k">ALL</span> <span class="k">TABLES</span> <span class="k">IN</span> <span class="k">SCHEMA</span> <span class="o">&lt;</span><span class="k">schema</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div></li>
</ol>


## B. (Optional) Configure network security

> **Note:** If you are prototyping and your AlloyDB instance is publicly accessible, **you
> can skip this step**. For production scenarios, we recommend configuring one of
> the network security options below.
>




**Cloud:**

To establish authorized and secure connections to an AlloyDB instance, an
authentication proxy is necessary. Google Cloud Platform provides [a guide](https://cloud.google.com/alloydb/docs/auth-proxy/connect)
to assist you in setting up this proxy and generating a connection string that
can be utilized with Materialize. Further down, we will provide you with a
tailored approach specific to integrating Materialize.

Next, choose the best network configuration for your setup to connect
Materialize with AlloyDB:

- **Allow Materialize IPs:** If your AlloyDB instance is publicly accessible,
    configure your firewall to allow connections from Materialize IP
    addresses.
- **Use an SSH tunnel:** For private networks, use an SSH tunnel to connect
    Materialize to AlloyDB.



**Allow Materialize IPs:**

1. In the [Materialize console's SQL Shell](/console/),
   or your preferred SQL client connected to Materialize, find the static egress
   IP addresses for the Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

1. Update your Google Cloud firewall rules to allow traffic to your AlloyDB auth
   proxy instance from each IP address from the previous step.



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

1. [Launch a GCE instance](https://cloud.google.com/compute/docs/instances/create-start-instance) to
    serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your
      database.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a [static public IP address](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address).
      You'll use this IP address when connecting Materialize to your bastion
      host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

    1. In the [Materialize console's SQL
       Shell](/console/), or your preferred SQL client
       connected to Materialize, get the static egress IP addresses for the
       Materialize region you are running in:

       ```mzsql
       SELECT * FROM mz_egress_ips;
       ```

    1. Update your SSH bastion host's firewall rules to allow traffic from each
       IP address from the previous step.

1. Update your Google Cloud firewall rules to allow traffic to your AlloyDB auth
   proxy instance from the SSH bastion host.







**Self-Managed:**

To establish authorized and secure connections to an AlloyDB instance, an
authentication proxy is necessary. Google Cloud Platform provides [a guide](https://cloud.google.com/alloydb/docs/auth-proxy/connect)
to assist you in setting up this proxy and generating a connection string that
can be utilized with Materialize. Further down, we will provide you with a
tailored approach specific to integrating Materialize.

<p>Configure your network to allow Materialize to connect to your database. For
example, you can:</p>
<ul>
<li>
<p><strong>Allow Materialize IPs:</strong> Configure your database&rsquo;s security group to allow
connections from Materialize.</p>
</li>
<li>
<p><strong>Use an SSH tunnel:</strong> Use an SSH tunnel to connect Materialize to the
database.</p>
</li>
</ul>
<div class="note">
  <strong class="gutter">NOTE:</strong>
  <p>The steps to allow Materialize to connect to your database  depends on your
  deployment setup. Refer to your company’s network/security policies and
  procedures.</p>
</div>




**Allow Materialize IPs:**

1. Update your Google Cloud firewall rules to allow traffic to your AlloyDB auth
   proxy instance from Materialize IPs.



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

1. [Launch a GCE instance](https://cloud.google.com/compute/docs/instances/create-start-instance) to
    serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your
      database.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a [static public IP address](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address).
      You'll use this IP address when connecting Materialize to your bastion
      host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

1. Update your Google Cloud firewall rules to allow traffic to your AlloyDB auth
   proxy instance from the SSH bastion host.









## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

> **Note:** If you are prototyping and already have a cluster to host your PostgreSQL
> source (e.g. `quickstart`), **you can skip this step**. For production
> scenarios, we recommend separating your workloads into multiple clusters for
> [resource isolation](/sql/create-cluster/#resource-isolation).
>


<p>In Materialize, a <a href="/concepts/clusters/" >cluster</a> is an isolated environment,
similar to a virtual warehouse in Snowflake. When you create a cluster, you
choose the size of its compute resource allocation based on the work you need
the cluster to do, whether ingesting data from a source, computing
always-up-to-date query results, serving results to external clients, or a
combination.</p>
<p>In this step, you&rsquo;ll create a dedicated cluster for ingesting source data from
your PostgreSQL database.</p>
<ol>
<li>
<p>In the <a href="/console/" >SQL Shell</a>, or your preferred SQL
client connected to Materialize, use the <a href="/sql/create-cluster/" ><code>CREATE CLUSTER</code></a>
command to create the new cluster:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">CLUSTER</span> <span class="n">ingest_postgres</span> <span class="p">(</span><span class="k">SIZE</span> <span class="o">=</span> <span class="s1">&#39;50cc&#39;</span><span class="p">);</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">SET</span> <span class="k">CLUSTER</span> <span class="o">=</span> <span class="n">ingest_postgres</span><span class="p">;</span>
</span></span></code></pre></div><p>A cluster of <a href="/sql/create-cluster/#size" >size</a> <code>50cc</code> should be enough to
accommodate multiple PostgreSQL sources, depending on the source
characteristics (e.g., sources with <a href="/sql/create-source/kafka/#upsert-envelope" ><code>ENVELOPE UPSERT</code></a>
or <a href="/sql/create-source/kafka/#debezium-envelope" ><code>ENVELOPE DEBEZIUM</code></a> will be more
memory-intensive) and the upstream traffic patterns. You can readjust the
size of the cluster at any time using the <a href="/sql/alter-cluster" ><code>ALTER CLUSTER</code></a> command:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">CLUSTER</span> <span class="o">&lt;</span><span class="n">cluster_name</span><span class="o">&gt;</span> <span class="k">SET</span> <span class="p">(</span> <span class="k">SIZE</span> <span class="o">=</span> <span class="o">&lt;</span><span class="n">new_size</span><span class="o">&gt;</span> <span class="p">);</span>
</span></span></code></pre></div></li>
</ol>


### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.



**Allow Materialize IPs:**

1. In the [Materialize Console's SQL Shell](/console/), or your preferred SQL
client connected to Materialize, use the [`CREATE
SECRET`](/sql/create-secret/) command to securely store the password for the
`materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):

   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
connection object with access and authentication details for Materialize to
use:


   ```mzsql
   CREATE CONNECTION pg_connection TO POSTGRES (
     HOST '<host>',
     PORT 5432,
     USER materialize,
     PASSWORD SECRET pgpass,
     SSL MODE 'require',
     DATABASE '<database>'
   );

   ```


   - Replace `<host>` with your PostgreSQL endpoint.

   - Replace `<database>` with the name of the database containing the tables
     you want to replicate to Materialize.




**Use an SSH tunnel:**

1. In the [Materialize Console's SQL Shell](/console/), or your preferred SQL
client connected to Materialize, use the [`CREATE
CONNECTION`](/sql/create-connection/#ssh-tunnel) command to create an SSH
tunnel connection:

   ```mzsql
   CREATE CONNECTION ssh_connection TO SSH TUNNEL (
       HOST '<SSH_BASTION_HOST>',
       PORT <SSH_BASTION_PORT>,
       USER '<SSH_BASTION_USER>'
   );

   ```

   - Replace `<SSH_BASTION_HOST>` and `<SSH_BASTION_PORT>` with the public IP
   address and port of the SSH bastion host you created
   [earlier](#b-optional-configure-network-security).

   - Replace `<SSH_BASTION_USER>` with the username for the key pair you
   created for your SSH bastion host.

1. Get Materialize's public keys for the SSH tunnel connection:


   ```mzsql
   SELECT * FROM mz_ssh_tunnel_connections;

   ```

1. Log in to your SSH bastion host and add Materialize's public keys to the
`authorized_keys` file, for example:


   ```mzsql
   echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
   echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys

   ```

1. Back in the SQL client connected to Materialize, validate the SSH tunnel
connection you created using the [`VALIDATE
CONNECTION`](/sql/validate-connection) command:


   ```mzsql
   VALIDATE CONNECTION ssh_connection;

   ```   If no validation error is returned, move to the next step.


1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
password for the `materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):


   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```

1.
Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create another connection object, this time with database access and authentication details for Materialize to use:


   ```mzsql
   CREATE CONNECTION pg_connection TO POSTGRES (
     HOST '<host>',
     PORT 5432,
     USER 'materialize',
     PASSWORD SECRET pgpass,
     DATABASE '<database>',
     SSH TUNNEL ssh_connection
     );

   ```

   - Replace `<host>` with your PostgreSQL endpoint.

   - Replace `<database>` with the name of the database containing the tables
   you want to replicate to Materialize.





### 3. Start ingesting data

{{< tabs >}}
{{< tab "Legacy Syntax" >}}
#### Legacy syntax

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source-legacy" %}}
{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="schema-changes" %}}
{{< /tab >}}

{{< tab "New Syntax" >}}
#### New syntax

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source" %}}
{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="schema-changes" %}}
{{< /tab >}}
{{< /tabs >}}


### 4. Monitor the ingestion status

<p>Before it starts consuming the replication stream, Materialize takes a snapshot
of the relevant tables in your publication. Until this snapshot is complete,
Materialize won&rsquo;t have the same view of your data as your PostgreSQL database.</p>
<p>In this step, you&rsquo;ll first verify that the source is running and then check the
status of the snapshotting process.</p>
<ol>
<li>
<p>Back in the SQL client connected to Materialize, use the
<a href="/sql/system-catalog/mz_internal/#mz_source_statuses" ><code>mz_source_statuses</code></a>
table to check the overall status of your source:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">WITH</span>
</span></span><span class="line"><span class="cl">  <span class="n">source_ids</span> <span class="k">AS</span>
</span></span><span class="line"><span class="cl">  <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">mz_sources</span> <span class="k">WHERE</span> <span class="k">name</span> <span class="o">=</span> <span class="s1">&#39;mz_source&#39;</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">  <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_source_statuses</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span>
</span></span><span class="line"><span class="cl">      <span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="k">SELECT</span> <span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">        <span class="k">FROM</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_object_dependencies</span>
</span></span><span class="line"><span class="cl">        <span class="k">WHERE</span>
</span></span><span class="line"><span class="cl">          <span class="n">object_id</span> <span class="k">IN</span> <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">UNION</span> <span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span>
</span></span><span class="line"><span class="cl">      <span class="p">)</span>
</span></span><span class="line"><span class="cl">      <span class="k">AS</span> <span class="k">sources</span>
</span></span><span class="line"><span class="cl">    <span class="k">ON</span> <span class="n">mz_source_statuses</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span><span class="p">;</span>
</span></span></code></pre></div><p>For each <code>subsource</code>, make sure the <code>status</code> is <code>running</code>. If you see
<code>stalled</code> or <code>failed</code>, there&rsquo;s likely a configuration issue for you to fix.
Check the <code>error</code> field for details and fix the issue before moving on.
Also, if the <code>status</code> of any subsource is <code>starting</code> for more than a few
minutes, <a href="/support/" >contact our team</a>.</p>
</li>
<li>
<p>Once the source is running, use the <a href="/sql/system-catalog/mz_internal/#mz_source_statistics" ><code>mz_source_statistics</code></a>
table to check the status of the initial snapshot:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">WITH</span>
</span></span><span class="line"><span class="cl">  <span class="n">source_ids</span> <span class="k">AS</span>
</span></span><span class="line"><span class="cl">  <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">mz_sources</span> <span class="k">WHERE</span> <span class="k">name</span> <span class="o">=</span> <span class="s1">&#39;mz_source&#39;</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span> <span class="k">AS</span> <span class="k">id</span><span class="p">,</span> <span class="n">mz_sources</span><span class="mf">.</span><span class="k">name</span><span class="p">,</span> <span class="n">snapshot_committed</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">  <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_source_statistics</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span>
</span></span><span class="line"><span class="cl">      <span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="k">SELECT</span> <span class="n">object_id</span><span class="p">,</span> <span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">        <span class="k">FROM</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_object_dependencies</span>
</span></span><span class="line"><span class="cl">        <span class="k">WHERE</span>
</span></span><span class="line"><span class="cl">          <span class="n">object_id</span> <span class="k">IN</span> <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">UNION</span> <span class="k">SELECT</span> <span class="k">id</span><span class="p">,</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span>
</span></span><span class="line"><span class="cl">      <span class="p">)</span>
</span></span><span class="line"><span class="cl">      <span class="k">AS</span> <span class="k">sources</span>
</span></span><span class="line"><span class="cl">    <span class="k">ON</span> <span class="n">mz_source_statistics</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_sources</span> <span class="k">ON</span> <span class="n">mz_sources</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span><span class="p">;</span>
</span></span></code></pre></div> <p></p>
<pre tabindex="0"><code class="language-nofmt" data-lang="nofmt">object_id | snapshot_committed
----------|------------------
 u144     | t
(1 row)
</code></pre><p>Once <code>snapshot_commited</code> is <code>t</code>, move on to the next step. Snapshotting can
take between a few minutes to several hours, depending on the size of your
dataset and the size of the cluster the source is running in.</p>
</li>
</ol>


### 5. Right-size the cluster

<p>After the snapshotting phase, Materialize starts ingesting change events from
the PostgreSQL replication stream. For this work, Materialize generally
performs well with an <code>100cc</code> replica, so you can resize the cluster
accordingly.</p>
<ol>
<li>
<p>Still in a SQL client connected to Materialize, use the <a href="/sql/alter-cluster/" ><code>ALTER CLUSTER</code></a>
command to downsize the cluster to <code>100cc</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">CLUSTER</span> <span class="n">ingest_postgres</span> <span class="k">SET</span> <span class="p">(</span><span class="k">SIZE</span> <span class="s1">&#39;100cc&#39;</span><span class="p">);</span>
</span></span></code></pre></div><p>Behind the scenes, this command adds a new <code>100cc</code> replica and removes the
<code>50cc</code> replica.</p>
</li>
<li>
<p>Use the <a href="/sql/show-cluster-replicas/" ><code>SHOW CLUSTER REPLICAS</code></a> command to
check the status of the new replica:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SHOW</span> <span class="k">CLUSTER</span> <span class="k">REPLICAS</span> <span class="k">WHERE</span> <span class="k">cluster</span> <span class="o">=</span> <span class="s1">&#39;ingest_postgres&#39;</span><span class="p">;</span>
</span></span></code></pre></div> <p></p>
<pre tabindex="0"><code class="language-nofmt" data-lang="nofmt">     cluster     | replica |  size  | ready
-----------------+---------+--------+-------
 ingest_postgres | r1      | 100cc  | t
(1 row)
</code></pre></li>
<li>
<p>Going forward, you can verify that your new cluster size is sufficient as
follows:</p>
<ol>
<li>
<p>In Materialize, get the replication slot name associated with your
PostgreSQL source from the <a href="/sql/system-catalog/mz_internal/#mz_postgres_sources" ><code>mz_internal.mz_postgres_sources</code></a>
table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span>
</span></span><span class="line"><span class="cl">    <span class="n">d</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">database_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">n</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">schema_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">s</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">source_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">pgs</span><span class="mf">.</span><span class="n">replication_slot</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">    <span class="n">mz_sources</span> <span class="k">AS</span> <span class="n">s</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_postgres_sources</span> <span class="k">AS</span> <span class="n">pgs</span> <span class="k">ON</span> <span class="n">s</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">pgs</span><span class="mf">.</span><span class="k">id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_schemas</span> <span class="k">AS</span> <span class="n">n</span> <span class="k">ON</span> <span class="n">n</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">s</span><span class="mf">.</span><span class="n">schema_id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_databases</span> <span class="k">AS</span> <span class="n">d</span> <span class="k">ON</span> <span class="n">d</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">n</span><span class="mf">.</span><span class="n">database_id</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>In PostgreSQL, check the replication slot lag, using the replication slot
name from the previous step:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">SELECT</span>
</span></span><span class="line"><span class="cl">    <span class="n">pg_size_pretty</span><span class="p">(</span><span class="n">pg_current_wal_lsn</span><span class="p">()</span> <span class="o">-</span> <span class="n">confirmed_flush_lsn</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">    <span class="k">AS</span> <span class="n">replication_lag_bytes</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span> <span class="n">pg_replication_slots</span>
</span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">slot_name</span> <span class="o">=</span> <span class="s1">&#39;&lt;slot_name&gt;&#39;</span><span class="p">;</span>
</span></span></code></pre></div><p>The result of this query is the amount of data your PostgreSQL cluster
must retain in its replication log because of this replication slot.
Typically, this means Materialize has not yet communicated back to
PostgreSQL that it has committed this data. A high value can indicate
that the source has fallen behind and that you might need to scale up
your ingestion cluster.</p>
</li>
</ol>
</li>
</ol>


## D. Explore your data

<p>With Materialize ingesting your PostgreSQL data into durable storage, you can
start exploring the data, computing real-time results that stay up-to-date as
new data arrives, and serving results efficiently.</p>
<ul>
<li>
<p>Explore your data with <a href="/sql/show-sources" ><code>SHOW SOURCES</code></a> and <a href="/sql/select/" ><code>SELECT</code></a>.</p>
</li>
<li>
<p>Compute real-time results in memory with <a href="/sql/create-view/" ><code>CREATE VIEW</code></a>
and <a href="/sql/create-index/" ><code>CREATE INDEX</code></a> or in durable
storage with <a href="/sql/create-materialized-view/" ><code>CREATE MATERIALIZED VIEW</code></a>.</p>
</li>
<li>
<p>Serve results to a PostgreSQL-compatible SQL client or driver with <a href="/sql/select/" ><code>SELECT</code></a>
or <a href="/sql/subscribe/" ><code>SUBSCRIBE</code></a> or to an external message broker with
<a href="/sql/create-sink/" ><code>CREATE SINK</code></a>.</p>
</li>
<li>
<p>Check out the <a href="/integrations/" >tools and integrations</a> supported by
Materialize.</p>
</li>
</ul>


## Considerations

<h3 id="schema-changes">Schema changes</h3>
<p>Materialize supports schema changes in the upstream database as follows:</p>
<h4 id="compatible-schema-changes-legacy-syntax">Compatible schema changes (Legacy syntax)</h4>
<blockquote>
<p><strong>Note:</strong> This section refer to the legacy <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a> that creates subsources as part of the
<code>CREATE SOURCE</code> operation.  To be able to handle the upstream column
additions and drops, see <a href="/sql/create-source/postgres-v2/" ><code>CREATE SOURCE (New Syntax)</code></a> and <a href="/sql/create-table" ><code>CREATE TABLE FROM SOURCE</code></a>.</p>
</blockquote>
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
<h4 id="incompatible-schema-changes">Incompatible schema changes</h4>
<p>All other schema changes to upstream tables will set the corresponding
Materialize tables into an error state, preventing reads from these tables.</p>
<p>To handle <a href="#incompatible-schema-changes" >incompatible schema changes</a>, drop
the affected table <a href="/sql/drop-table/" ><code>DROP TABLE</code></a> , and then, <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> to recreate the table with the
updated schema.</p>
<h3 id="publication-membership">Publication membership</h3>
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
<p>To mitigate this issue, if you need to drop and re-add a table to a
publication, ensure that you remove the table/subsource from the source
<em>before</em> re-adding it using the <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> command.</p>
<h3 id="supported-types">Supported types</h3>
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
<h3 id="truncation">Truncation</h3>
<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource(s)/table(s) in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>
<h3 id="inherited-tables">Inherited tables</h3>
<p>When using <a href="https://www.postgresql.org/docs/current/tutorial-inheritance.html" >PostgreSQL table inheritance</a>,
PostgreSQL serves data from <code>SELECT</code>s as if the inheriting tables&rsquo; data is
also present in the inherited table. However, both PostgreSQL&rsquo;s logical
replication and <code>COPY</code> only present data written to the tables themselves,
i.e. the inheriting data is <em>not</em> treated as part of the inherited table.</p>
<p>PostgreSQL sources use logical replication and <code>COPY</code> to ingest table data,
so inheriting tables&rsquo; data will only be ingested as part of the inheriting
table, i.e. in Materialize, the data will not be returned when serving
<code>SELECT</code>s from the inherited table.</p>
<ul>
<li>
<p>If using legacy syntax <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a>:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>ADD SUBSOURCE</code> and
create a new view (materialized or non-) that unions the new table.</p>
</li>
<li>
<p>If using new <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> syntax:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>CREATE TABLE .. FROM SOURCE</code> and create a new view (materialized or non-) that unions the new
table.</p>
</li>
</ul>
<h3 id="replication-slots">Replication slots</h3>
<p>Each source ingests the raw replication stream data for all tables in the
specified publication using <strong>a single</strong> replication slot. To manage
replication slots:</p>
<ul>
<li>
<p>For PostgreSQL 13+, set a reasonable value
for <a href="https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE" ><code>max_slot_wal_keep_size</code></a>
to limit the amount of storage used by replication slots.</p>
</li>
<li>
<p>If you stop using Materialize, or if either the Materialize instance or
the PostgreSQL instance crash, delete any replication slots. You can query
the <code>mz_internal.mz_postgres_sources</code> table to look up the name of the
replication slot created for each source.</p>
</li>
<li>
<p>If you delete all objects that depend on a source without also dropping
the source, the upstream replication slot remains and will continue to
accumulate data so that the source can resume in the future. To avoid
unbounded disk space usage, make sure to use <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> or manually delete the replication slot.</p>
</li>
</ul>
<h3 id="modifying-an-existing-source">Modifying an existing source</h3>
<p>When you add a new subsource to an existing source (<a href="/sql/alter-source/" ><code>ALTER SOURCE ... ADD SUBSOURCE ...</code></a>), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.
</p>



---

## Ingest data from Amazon Aurora


This page shows you how to stream data from [Amazon Aurora for PostgreSQL](https://aws.amazon.com/rds/aurora/)
to Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).
>
>


## Before you begin

<ul>
<li>
<p>Make sure you are running PostgreSQL 11 or higher.</p>
</li>
<li>
<p>Make sure you have access to your PostgreSQL instance via <a href="https://www.postgresql.org/docs/current/app-psql.html" ><code>psql</code></a>,
or your preferred SQL client.</p>
</li>
</ul>


> **Warning:** There is a known issue with Aurora PostgreSQL 16.1 that can cause logical replication to fail with the following error:
> - `postgres: sql client error: db error: ERROR: could not map filenumber "base/16402/3147867235" to relation OID`
>
> This is due to a bug in Aurora's implementation of logical replication in PostgreSQL 16.1, where the system fails to correctly fetch relation metadata from the catalogs. If you encounter these errors, you should upgrade your Aurora PostgreSQL instance to a newer minor version (16.2 or later).
>
> For more information, see [this AWS discussion](https://repost.aws/questions/QU4RXUrLNQS_2oSwV34pmwww/error-could-not-map-filenumber-after-aurora-upgrade-to-16-1).
>


## A. Configure Amazon Aurora

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

To enable logical replication in Aurora, see the
[Aurora documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html#AuroraPostgreSQL.Replication.Logical.Configure).

> **Note:** Aurora Serverless (v1) [does **not** support](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless.html#aurora-serverless.limitations)
> logical replication, so it's not possible to use this service with
> Materialize.
>


### 2. Create a publication and a replication user

<p>Once logical replication is enabled, create a publication with the tables that
you want to replicate to Materialize. You&rsquo;ll also need a user for Materialize
with sufficient privileges to manage replication.</p>
<ol>
<li>
<p>As a <em>superuser</em>, use <code>psql</code> (or your preferred SQL client) to connect to
your database.</p>
</li>
<li>
<p>For each table that you want to replicate to Materialize, set the
<a href="https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY" >replica identity</a>
to <code>FULL</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span> <span class="k">REPLICA</span> <span class="k">IDENTITY</span> <span class="k">FULL</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span> <span class="k">REPLICA</span> <span class="k">IDENTITY</span> <span class="k">FULL</span><span class="p">;</span>
</span></span></code></pre></div><p><code>REPLICA IDENTITY FULL</code> ensures that the replication stream includes the
previous data of changed rows, in the case of <code>UPDATE</code> and <code>DELETE</code>
operations. This setting enables Materialize to ingest PostgreSQL data with
minimal in-memory state. However, you should expect increased disk usage in
your PostgreSQL database.</p>
</li>
<li>
<p>Create a <a href="https://www.postgresql.org/docs/current/logical-replication-publication.html" >publication</a>
with the tables you want to replicate:</p>
<p><em>For specific tables:</em></p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="n">PUBLICATION</span> <span class="n">mz_source</span> <span class="k">FOR</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span><span class="p">,</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span><span class="p">;</span>
</span></span></code></pre></div><p><em>For all tables in the database:</em></p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="n">PUBLICATION</span> <span class="n">mz_source</span> <span class="k">FOR</span> <span class="k">ALL</span> <span class="k">TABLES</span><span class="p">;</span>
</span></span></code></pre></div><p>The <code>mz_source</code> publication will contain the set of change events generated
from the specified tables, and will later be used to ingest the replication
stream.</p>
<p>Be sure to include only the tables you need. If the publication includes
additional tables, Materialize will waste resources on ingesting and then
immediately discarding the data.</p>
</li>
<li>
<p>Create a user for Materialize, if you don&rsquo;t already have one:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">USER</span> <span class="n">materialize</span> <span class="k">PASSWORD</span> <span class="s1">&#39;&lt;password&gt;&#39;</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>Grant the user permission to manage replication:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="n">rds_replication</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>Grant the user the required permissions on the tables you want to replicate:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="n">CONNECT</span> <span class="k">ON</span> <span class="k">DATABASE</span> <span class="o">&lt;</span><span class="n">dbname</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="n">USAGE</span> <span class="k">ON</span> <span class="k">SCHEMA</span> <span class="o">&lt;</span><span class="k">schema</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><p>Once connected to your database, Materialize will take an initial snapshot
of the tables in your publication. <code>SELECT</code> privileges are required for
this initial snapshot.</p>
<p>If you expect to add tables to your publication, you can grant <code>SELECT</code> on
all tables in the schema instead of naming the specific tables:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="k">ALL</span> <span class="k">TABLES</span> <span class="k">IN</span> <span class="k">SCHEMA</span> <span class="o">&lt;</span><span class="k">schema</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div></li>
</ol>


## B. (Optional) Configure network security

> **Note:** If you are prototyping and your Aurora instance is publicly accessible, **you can
> skip this step**. For production scenarios, we recommend configuring one of the
> network security options below.
>




**Cloud:**

There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's security group to allow connections from a set of
    static Materialize IP addresses.

- **Use AWS PrivateLink**: If your database is running in a private network, you
    can use [AWS PrivateLink](/ingest-data/network-security/privatelink/) to
    connect Materialize to the database. For details, see [AWS PrivateLink](/ingest-data/network-security/privatelink/).

- **Use an SSH tunnel:** If your database is running in a private network, you
    can use an SSH tunnel to connect Materialize to the database.



**Allow Materialize IPs:**

1. In the [SQL Shell](/console/) or your preferred SQL
   client connected to Materialize, find the static egress IP addresses for the
   Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

1. In the AWS Management Console, [add an inbound rule to your Aurora security group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/changing-security-group.html#add-remove-instance-security-groups)
    for each IP address from the previous step.

    In each rule:

    - Set **Type** to **PostgreSQL**.
    - Set **Source** to the IP address in CIDR notation.



**Use AWS PrivateLink:**

[AWS PrivateLink](https://aws.amazon.com/privatelink/) lets you connect
Materialize to your Aurora instance without exposing traffic to the public
internet. To use AWS PrivateLink, you create a network load balancer in the
same VPC as your Aurora instance and a VPC endpoint service that Materialize
connects to. The VPC endpoint service then routes requests from Materialize to
Aurora via the network load balancer.

> **Note:** Materialize provides a Terraform module that automates the creation and
> configuration of AWS resources for a PrivateLink connection. For more details,
> see the [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-rds-privatelink).
>


1. Get the IP address of your Aurora instance.

    You'll need this address to register your Aurora instance as the target for
    the network load balancer in the next step.

    To get the IP address of your database instance:

    1. In the AWS Management Console, select your database.
    1. Find your Aurora endpoint under **Connectivity & security**.
    1. Use the `dig` or `nslooklup` command
    to find the IP address that the endpoint resolves to:

       ```sh
       dig +short <AURORA_ENDPOINT>
       ```

1. [Create a dedicated target group for your Aurora instance](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html).

    - Choose the **IP addresses** type.

    - Set the protocol and port to **TCP** and **5432**.

    - Choose the same VPC as your RDS instance.

    - Use the IP address from the previous step to register your Aurora instance
      as the target.

    **Warning:** The IP address of your Aurora instance can change without
      notice. For this reason, it's best to set up automation to regularly
      check the IP of the instance and update your target group accordingly.
      You can use a lambda function to automate this process - see
      Materialize's [Terraform module for AWS PrivateLink](https://github.com/MaterializeInc/terraform-aws-rds-privatelink/blob/main/lambda_function.py)
      for an example. Another approach is to [configure an EC2 instance as an
      RDS router](https://aws.amazon.com/blogs/database/how-to-use-amazon-rds-and-amazon-aurora-with-a-static-ip-address/)
      for your network load balancer.

1. [Create a network load balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html).

    - For **Network mapping**, choose the same VPC as your RDS instance and
      select all of the availability zones and subnets that you RDS instance is
      in.

    - For **Listeners and routing**, set the protocol and port to **TCP**
      and **5432** and select the target group you created in the previous
      step.

1. In the security group of your Aurora instance, [allow traffic from the the
   network load balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html).

    If [client IP preservation](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-target-groups.html#client-ip-preservation)
    is disabled, the easiest approach is to add an inbound rule with the VPC
    CIDR of the network load balancer. If you don't want to grant access to the
    entire VPC CIDR, you can add inbound rules for the private IP addresses of
    the load balancer subnets.

    - To find the VPC CIDR, go to the network load balancer and look
      under **Network mapping**.

    - To find the private IP addresses of the load balancer subnets, go
      to **Network Interfaces**, search for the name of the network load
      balancer, and look on the **Details** tab for each matching network
      interface.

1. [Create a VPC endpoint service](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html).

    - For **Load balancer type**, choose **Network** and then select the network
      load balancer you created in the previous step.

    - After creating the VPC endpoint service, note its **Service name**. You'll
      use this service name when connecting Materialize later.

    **Remarks** By disabling [Acceptance Required](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests),
      while still strictly managing who can view your endpoint via IAM,
      Materialze will be able to seamlessly recreate and migrate endpoints as
      we work to stabilize this feature.

1. Go back to the target group you created for the network load balancer and
   make sure that the [health checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html)
   are reporting the targets as healthy.



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

> **Note:** Materialize provides a Terraform module that automates the creation and
> configuration of resources for an SSH tunnel. For more details, see the
> [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-ec2-ssh-bastion).
>


1. [Launch an EC2 instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/LaunchingAndUsingInstances.html)
    to serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your
      RDS instance.

    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.

    **Warning:** Auto-assigned public IP addresses can change in [certain cases](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#concepts-public-addresses).
      For this reason, it's best to associate an [elastic IP address](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#ip-addressing-eips)
      to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

    1. In the [SQL Shell](/console/), or your preferred
       SQL client connected to Materialize, get the static egress IP addresses for
       the Materialize region you are running in:

       ```mzsql
       SELECT * FROM mz_egress_ips;
       ```

    1. For each static egress IP, [add an inbound rule](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-security-groups.html)
       to your SSH bastion host's security group.

        In each rule:

        - Set **Type** to **PostgreSQL**.
        - Set **Source** to the IP address in CIDR notation.

1. In the security group of your RDS instance, [add an inbound rule](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html)
   to allow traffic from the SSH bastion host.

    - Set **Type** to **All TCP**.
    - Set **Source** to **Custom** and select the bastion host's security
      group.







**Self-Managed:**

<p>Configure your network to allow Materialize to connect to your database. For
example, you can:</p>
<ul>
<li>
<p><strong>Allow Materialize IPs:</strong> Configure your database&rsquo;s security group to allow
connections from Materialize.</p>
</li>
<li>
<p><strong>Use an SSH tunnel:</strong> Use an SSH tunnel to connect Materialize to the
database.</p>
</li>
</ul>
<div class="note">
  <strong class="gutter">NOTE:</strong>
  <p>The steps to allow Materialize to connect to your database  depends on your
  deployment setup. Refer to your company’s network/security policies and
  procedures.</p>
</div>




**Allow Materialize IPs:**

1. In the AWS Management Console, [add an inbound rule to your Aurora security group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/changing-security-group.html#add-remove-instance-security-groups)
   to allow traffic from Materialize IPs.

    In each rule:

    - Set **Type** to **PostgreSQL**.
    - Set **Source** to the IP address in CIDR notation.



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

> **Note:** Materialize provides a Terraform module that automates the creation and
> configuration of resources for an SSH tunnel. For more details, see the
> [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-ec2-ssh-bastion).
>


1. [Launch an EC2 instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/LaunchingAndUsingInstances.html)
    to serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your
      RDS instance.

    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.

    **Warning:** Auto-assigned public IP addresses can change in [certain cases](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#concepts-public-addresses).
      For this reason, it's best to associate an [elastic IP address](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#ip-addressing-eips)
      to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

1. In the security group of your RDS instance, [add an inbound rule](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html)
   to allow traffic from the SSH bastion host.

    - Set **Type** to **All TCP**.
    - Set **Source** to **Custom** and select the bastion host's security
      group.









## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

> **Note:** If you are prototyping and already have a cluster to host your PostgreSQL
> source (e.g. `quickstart`), **you can skip this step**. For production
> scenarios, we recommend separating your workloads into multiple clusters for
> [resource isolation](/sql/create-cluster/#resource-isolation).
>



<p>In Materialize, a <a href="/concepts/clusters/" >cluster</a> is an isolated environment,
similar to a virtual warehouse in Snowflake. When you create a cluster, you
choose the size of its compute resource allocation based on the work you need
the cluster to do, whether ingesting data from a source, computing
always-up-to-date query results, serving results to external clients, or a
combination.</p>
<p>In this step, you&rsquo;ll create a dedicated cluster for ingesting source data from
your PostgreSQL database.</p>
<ol>
<li>
<p>In the <a href="/console/" >SQL Shell</a>, or your preferred SQL
client connected to Materialize, use the <a href="/sql/create-cluster/" ><code>CREATE CLUSTER</code></a>
command to create the new cluster:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">CLUSTER</span> <span class="n">ingest_postgres</span> <span class="p">(</span><span class="k">SIZE</span> <span class="o">=</span> <span class="s1">&#39;50cc&#39;</span><span class="p">);</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">SET</span> <span class="k">CLUSTER</span> <span class="o">=</span> <span class="n">ingest_postgres</span><span class="p">;</span>
</span></span></code></pre></div><p>A cluster of <a href="/sql/create-cluster/#size" >size</a> <code>50cc</code> should be enough to
accommodate multiple PostgreSQL sources, depending on the source
characteristics (e.g., sources with <a href="/sql/create-source/kafka/#upsert-envelope" ><code>ENVELOPE UPSERT</code></a>
or <a href="/sql/create-source/kafka/#debezium-envelope" ><code>ENVELOPE DEBEZIUM</code></a> will be more
memory-intensive) and the upstream traffic patterns. You can readjust the
size of the cluster at any time using the <a href="/sql/alter-cluster" ><code>ALTER CLUSTER</code></a> command:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">CLUSTER</span> <span class="o">&lt;</span><span class="n">cluster_name</span><span class="o">&gt;</span> <span class="k">SET</span> <span class="p">(</span> <span class="k">SIZE</span> <span class="o">=</span> <span class="o">&lt;</span><span class="n">new_size</span><span class="o">&gt;</span> <span class="p">);</span>
</span></span></code></pre></div></li>
</ol>


### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.



**Allow Materialize IPs:**

1. In the [Materialize Console's SQL Shell](/console/), or your preferred SQL
client connected to Materialize, use the [`CREATE
SECRET`](/sql/create-secret/) command to securely store the password for the
`materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):

   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
connection object with access and authentication details for Materialize to
use:


   ```mzsql
   CREATE CONNECTION pg_connection TO POSTGRES (
     HOST '<host>',
     PORT 5432,
     USER materialize,
     PASSWORD SECRET pgpass,
     SSL MODE 'require',
     DATABASE '<database>'
   );

   ```

   - Replace `<host>` with the **Writer** endpoint for your Aurora database. To
     find the endpoint, select your database in the AWS Management Console,
     then click the **Connectivity & security** tab and look for the endpoint
     with type **Writer**.

       <div class="warning">
           <strong class="gutter">WARNING!</strong>
           You must use the <strong>Writer</strong> endpoint for the database. Using a <strong>Reader</strong> endpoint will not work.
       </div>

   - Replace `<database>` with the name of the database containing the tables
     you want to replicate to Materialize.




**Use AWS PrivateLink (Cloud-only):**

1. In the [Materialize Console's SQL Shell](/console/), or your preferred SQL
client connected to Materialize, use the [`CREATE
CONNECTION`](/sql/create-connection/#aws-privatelink) command to create an
AWS PrivateLink connection:

   ```mzsql
   CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
     SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0356210a8a432d9e9',
     AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az4')
   );

   ```

   - Replace the `SERVICE NAME` value with the service name you noted
   [earlier](#b-optional-configure-network-security).

   - Replace the `AVAILABILITY ZONES` list with the IDs of the availability
     zones in your AWS account.

     To find your availability zone IDs, select your database in the RDS
     Console and click the subnets under **Connectivity & security**. For each
     subnet, look for **Availability Zone ID** (e.g., `use1-az6`),
     not **Availability Zone** (e.g., `us-east-1d`).


1. Retrieve the AWS principal for the AWS PrivateLink connection you just created:


   ```mzsql
   SELECT principal
   FROM mz_aws_privatelink_connections plc
   JOIN mz_connections c ON plc.id = c.id
   WHERE c.name = 'privatelink_svc';

   ```

   The results should resemble:
   ```
                                    principal
   ---------------------------------------------------------------------------
    arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
   ```


1. Update your VPC endpoint service to [accept connections from the AWS principal](https://docs.aws.amazon.com/vpc/latest/privatelink/add-endpoint-service-permissions.html).


1. If your AWS PrivateLink service is configured to require acceptance of
connection requests, [manually approve the connection request from
Materialize](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).
   **Note:** It can take some time for the connection request to show up. Do
not move on to the next step until you've approved the connection.


1. Validate the AWS PrivateLink connection you created using the [`VALIDATE
CONNECTION`](/sql/validate-connection) command:


   ```mzsql
   VALIDATE CONNECTION privatelink_svc;

   ```   If no validation error is returned, move to the next step.


1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
password for the `materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):


   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```
1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create
another connection object, this time with database access and authentication
details for Materialize to use:


   ```mzsql
   CREATE CONNECTION pg_connection TO POSTGRES (
     HOST '<host>',
     PORT 5432,
     USER 'materialize',
     PASSWORD SECRET pgpass,
     DATABASE '<database>',
     AWS PRIVATELINK privatelink_svc
     );

   ```
   - Replace `<host>` with your Aurora endpoint. To find your Aurora endpoint,
     select your database in the AWS Management Console, and look
     under **Connectivity & security**.

   - Replace `<database>` with the name of the database containing the tables
     you want to replicate to Materialize.



**Use an SSH tunnel:**

1. In the [Materialize Console's SQL Shell](/console/), or your preferred SQL
client connected to Materialize, use the [`CREATE
CONNECTION`](/sql/create-connection/#ssh-tunnel) command to create an SSH
tunnel connection:

   ```mzsql
   CREATE CONNECTION ssh_connection TO SSH TUNNEL (
       HOST '<SSH_BASTION_HOST>',
       PORT <SSH_BASTION_PORT>,
       USER '<SSH_BASTION_USER>'
   );

   ```

   - Replace `<SSH_BASTION_HOST>` and `<SSH_BASTION_PORT>` with the public IP
   address and port of the SSH bastion host you created
   [earlier](#b-optional-configure-network-security).

   - Replace `<SSH_BASTION_USER>` with the username for the key pair you
   created for your SSH bastion host.

1. Get Materialize's public keys for the SSH tunnel connection:


   ```mzsql
   SELECT
       mz_connections.name,
       mz_ssh_tunnel_connections.*
   FROM
       mz_connections
   JOIN
       mz_ssh_tunnel_connections USING(id)
   WHERE
       mz_connections.name = 'ssh_connection';

   ```

1. Log in to your SSH bastion host and add Materialize's public keys to the
`authorized_keys` file, for example:


   ```mzsql
   echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
   echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys

   ```

1. Back in the SQL client connected to Materialize, validate the SSH tunnel
connection you created using the [`VALIDATE
CONNECTION`](/sql/validate-connection) command:


   ```mzsql
   VALIDATE CONNECTION ssh_connection;

   ```   If no validation error is returned, move to the next step.


1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
password for the `materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):


   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```

1.
Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create another connection object, this time with database access and authentication details for Materialize to use:


   ```mzsql
   CREATE CONNECTION pg_connection TO POSTGRES (
     HOST '<host>',
     PORT 5432,
     USER 'materialize',
     PASSWORD SECRET pgpass,
     DATABASE '<database>',
     SSH TUNNEL ssh_connection
     );

   ```

   - Replace `<host>` with your Aurora endpoint. To find your Aurora endpoint,
   select your database in the AWS Management Console, and look under
   **Connectivity & security**.

   - Replace `<database>` with the name of the database containing the tables
   you want to replicate to Materialize.






### 3. Start ingesting data

{{< tabs >}}
{{< tab "Legacy Syntax" >}}
#### Legacy syntax

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source-legacy" %}}
{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="schema-changes" %}}
{{< /tab >}}

{{< tab "New Syntax" >}}
#### New syntax

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source" %}}
{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="schema-changes" %}}
{{< /tab >}}
{{< /tabs >}}


### 4. Monitor the ingestion status

<p>Before it starts consuming the replication stream, Materialize takes a snapshot
of the relevant tables in your publication. Until this snapshot is complete,
Materialize won&rsquo;t have the same view of your data as your PostgreSQL database.</p>
<p>In this step, you&rsquo;ll first verify that the source is running and then check the
status of the snapshotting process.</p>
<ol>
<li>
<p>Back in the SQL client connected to Materialize, use the
<a href="/sql/system-catalog/mz_internal/#mz_source_statuses" ><code>mz_source_statuses</code></a>
table to check the overall status of your source:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">WITH</span>
</span></span><span class="line"><span class="cl">  <span class="n">source_ids</span> <span class="k">AS</span>
</span></span><span class="line"><span class="cl">  <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">mz_sources</span> <span class="k">WHERE</span> <span class="k">name</span> <span class="o">=</span> <span class="s1">&#39;mz_source&#39;</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">  <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_source_statuses</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span>
</span></span><span class="line"><span class="cl">      <span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="k">SELECT</span> <span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">        <span class="k">FROM</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_object_dependencies</span>
</span></span><span class="line"><span class="cl">        <span class="k">WHERE</span>
</span></span><span class="line"><span class="cl">          <span class="n">object_id</span> <span class="k">IN</span> <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">UNION</span> <span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span>
</span></span><span class="line"><span class="cl">      <span class="p">)</span>
</span></span><span class="line"><span class="cl">      <span class="k">AS</span> <span class="k">sources</span>
</span></span><span class="line"><span class="cl">    <span class="k">ON</span> <span class="n">mz_source_statuses</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span><span class="p">;</span>
</span></span></code></pre></div><p>For each <code>subsource</code>, make sure the <code>status</code> is <code>running</code>. If you see
<code>stalled</code> or <code>failed</code>, there&rsquo;s likely a configuration issue for you to fix.
Check the <code>error</code> field for details and fix the issue before moving on.
Also, if the <code>status</code> of any subsource is <code>starting</code> for more than a few
minutes, <a href="/support/" >contact our team</a>.</p>
</li>
<li>
<p>Once the source is running, use the <a href="/sql/system-catalog/mz_internal/#mz_source_statistics" ><code>mz_source_statistics</code></a>
table to check the status of the initial snapshot:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">WITH</span>
</span></span><span class="line"><span class="cl">  <span class="n">source_ids</span> <span class="k">AS</span>
</span></span><span class="line"><span class="cl">  <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">mz_sources</span> <span class="k">WHERE</span> <span class="k">name</span> <span class="o">=</span> <span class="s1">&#39;mz_source&#39;</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span> <span class="k">AS</span> <span class="k">id</span><span class="p">,</span> <span class="n">mz_sources</span><span class="mf">.</span><span class="k">name</span><span class="p">,</span> <span class="n">snapshot_committed</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">  <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_source_statistics</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span>
</span></span><span class="line"><span class="cl">      <span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="k">SELECT</span> <span class="n">object_id</span><span class="p">,</span> <span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">        <span class="k">FROM</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_object_dependencies</span>
</span></span><span class="line"><span class="cl">        <span class="k">WHERE</span>
</span></span><span class="line"><span class="cl">          <span class="n">object_id</span> <span class="k">IN</span> <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">UNION</span> <span class="k">SELECT</span> <span class="k">id</span><span class="p">,</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span>
</span></span><span class="line"><span class="cl">      <span class="p">)</span>
</span></span><span class="line"><span class="cl">      <span class="k">AS</span> <span class="k">sources</span>
</span></span><span class="line"><span class="cl">    <span class="k">ON</span> <span class="n">mz_source_statistics</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_sources</span> <span class="k">ON</span> <span class="n">mz_sources</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span><span class="p">;</span>
</span></span></code></pre></div> <p></p>
<pre tabindex="0"><code class="language-nofmt" data-lang="nofmt">object_id | snapshot_committed
----------|------------------
 u144     | t
(1 row)
</code></pre><p>Once <code>snapshot_commited</code> is <code>t</code>, move on to the next step. Snapshotting can
take between a few minutes to several hours, depending on the size of your
dataset and the size of the cluster the source is running in.</p>
</li>
</ol>


### 5. Right-size the cluster

<p>After the snapshotting phase, Materialize starts ingesting change events from
the PostgreSQL replication stream. For this work, Materialize generally
performs well with an <code>100cc</code> replica, so you can resize the cluster
accordingly.</p>
<ol>
<li>
<p>Still in a SQL client connected to Materialize, use the <a href="/sql/alter-cluster/" ><code>ALTER CLUSTER</code></a>
command to downsize the cluster to <code>100cc</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">CLUSTER</span> <span class="n">ingest_postgres</span> <span class="k">SET</span> <span class="p">(</span><span class="k">SIZE</span> <span class="s1">&#39;100cc&#39;</span><span class="p">);</span>
</span></span></code></pre></div><p>Behind the scenes, this command adds a new <code>100cc</code> replica and removes the
<code>50cc</code> replica.</p>
</li>
<li>
<p>Use the <a href="/sql/show-cluster-replicas/" ><code>SHOW CLUSTER REPLICAS</code></a> command to
check the status of the new replica:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SHOW</span> <span class="k">CLUSTER</span> <span class="k">REPLICAS</span> <span class="k">WHERE</span> <span class="k">cluster</span> <span class="o">=</span> <span class="s1">&#39;ingest_postgres&#39;</span><span class="p">;</span>
</span></span></code></pre></div> <p></p>
<pre tabindex="0"><code class="language-nofmt" data-lang="nofmt">     cluster     | replica |  size  | ready
-----------------+---------+--------+-------
 ingest_postgres | r1      | 100cc  | t
(1 row)
</code></pre></li>
<li>
<p>Going forward, you can verify that your new cluster size is sufficient as
follows:</p>
<ol>
<li>
<p>In Materialize, get the replication slot name associated with your
PostgreSQL source from the <a href="/sql/system-catalog/mz_internal/#mz_postgres_sources" ><code>mz_internal.mz_postgres_sources</code></a>
table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span>
</span></span><span class="line"><span class="cl">    <span class="n">d</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">database_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">n</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">schema_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">s</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">source_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">pgs</span><span class="mf">.</span><span class="n">replication_slot</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">    <span class="n">mz_sources</span> <span class="k">AS</span> <span class="n">s</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_postgres_sources</span> <span class="k">AS</span> <span class="n">pgs</span> <span class="k">ON</span> <span class="n">s</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">pgs</span><span class="mf">.</span><span class="k">id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_schemas</span> <span class="k">AS</span> <span class="n">n</span> <span class="k">ON</span> <span class="n">n</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">s</span><span class="mf">.</span><span class="n">schema_id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_databases</span> <span class="k">AS</span> <span class="n">d</span> <span class="k">ON</span> <span class="n">d</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">n</span><span class="mf">.</span><span class="n">database_id</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>In PostgreSQL, check the replication slot lag, using the replication slot
name from the previous step:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">SELECT</span>
</span></span><span class="line"><span class="cl">    <span class="n">pg_size_pretty</span><span class="p">(</span><span class="n">pg_current_wal_lsn</span><span class="p">()</span> <span class="o">-</span> <span class="n">confirmed_flush_lsn</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">    <span class="k">AS</span> <span class="n">replication_lag_bytes</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span> <span class="n">pg_replication_slots</span>
</span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">slot_name</span> <span class="o">=</span> <span class="s1">&#39;&lt;slot_name&gt;&#39;</span><span class="p">;</span>
</span></span></code></pre></div><p>The result of this query is the amount of data your PostgreSQL cluster
must retain in its replication log because of this replication slot.
Typically, this means Materialize has not yet communicated back to
PostgreSQL that it has committed this data. A high value can indicate
that the source has fallen behind and that you might need to scale up
your ingestion cluster.</p>
</li>
</ol>
</li>
</ol>


## D. Explore your data

<p>With Materialize ingesting your PostgreSQL data into durable storage, you can
start exploring the data, computing real-time results that stay up-to-date as
new data arrives, and serving results efficiently.</p>
<ul>
<li>
<p>Explore your data with <a href="/sql/show-sources" ><code>SHOW SOURCES</code></a> and <a href="/sql/select/" ><code>SELECT</code></a>.</p>
</li>
<li>
<p>Compute real-time results in memory with <a href="/sql/create-view/" ><code>CREATE VIEW</code></a>
and <a href="/sql/create-index/" ><code>CREATE INDEX</code></a> or in durable
storage with <a href="/sql/create-materialized-view/" ><code>CREATE MATERIALIZED VIEW</code></a>.</p>
</li>
<li>
<p>Serve results to a PostgreSQL-compatible SQL client or driver with <a href="/sql/select/" ><code>SELECT</code></a>
or <a href="/sql/subscribe/" ><code>SUBSCRIBE</code></a> or to an external message broker with
<a href="/sql/create-sink/" ><code>CREATE SINK</code></a>.</p>
</li>
<li>
<p>Check out the <a href="/integrations/" >tools and integrations</a> supported by
Materialize.</p>
</li>
</ul>


## Considerations

<h3 id="schema-changes">Schema changes</h3>
<p>Materialize supports schema changes in the upstream database as follows:</p>
<h4 id="compatible-schema-changes-legacy-syntax">Compatible schema changes (Legacy syntax)</h4>
<blockquote>
<p><strong>Note:</strong> This section refer to the legacy <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a> that creates subsources as part of the
<code>CREATE SOURCE</code> operation.  To be able to handle the upstream column
additions and drops, see <a href="/sql/create-source/postgres-v2/" ><code>CREATE SOURCE (New Syntax)</code></a> and <a href="/sql/create-table" ><code>CREATE TABLE FROM SOURCE</code></a>.</p>
</blockquote>
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
<h4 id="incompatible-schema-changes">Incompatible schema changes</h4>
<p>All other schema changes to upstream tables will set the corresponding
Materialize tables into an error state, preventing reads from these tables.</p>
<p>To handle <a href="#incompatible-schema-changes" >incompatible schema changes</a>, drop
the affected table <a href="/sql/drop-table/" ><code>DROP TABLE</code></a> , and then, <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> to recreate the table with the
updated schema.</p>
<h3 id="publication-membership">Publication membership</h3>
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
<p>To mitigate this issue, if you need to drop and re-add a table to a
publication, ensure that you remove the table/subsource from the source
<em>before</em> re-adding it using the <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> command.</p>
<h3 id="supported-types">Supported types</h3>
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
<h3 id="truncation">Truncation</h3>
<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource(s)/table(s) in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>
<h3 id="inherited-tables">Inherited tables</h3>
<p>When using <a href="https://www.postgresql.org/docs/current/tutorial-inheritance.html" >PostgreSQL table inheritance</a>,
PostgreSQL serves data from <code>SELECT</code>s as if the inheriting tables&rsquo; data is
also present in the inherited table. However, both PostgreSQL&rsquo;s logical
replication and <code>COPY</code> only present data written to the tables themselves,
i.e. the inheriting data is <em>not</em> treated as part of the inherited table.</p>
<p>PostgreSQL sources use logical replication and <code>COPY</code> to ingest table data,
so inheriting tables&rsquo; data will only be ingested as part of the inheriting
table, i.e. in Materialize, the data will not be returned when serving
<code>SELECT</code>s from the inherited table.</p>
<ul>
<li>
<p>If using legacy syntax <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a>:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>ADD SUBSOURCE</code> and
create a new view (materialized or non-) that unions the new table.</p>
</li>
<li>
<p>If using new <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> syntax:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>CREATE TABLE .. FROM SOURCE</code> and create a new view (materialized or non-) that unions the new
table.</p>
</li>
</ul>
<h3 id="replication-slots">Replication slots</h3>
<p>Each source ingests the raw replication stream data for all tables in the
specified publication using <strong>a single</strong> replication slot. To manage
replication slots:</p>
<ul>
<li>
<p>For PostgreSQL 13+, set a reasonable value
for <a href="https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE" ><code>max_slot_wal_keep_size</code></a>
to limit the amount of storage used by replication slots.</p>
</li>
<li>
<p>If you stop using Materialize, or if either the Materialize instance or
the PostgreSQL instance crash, delete any replication slots. You can query
the <code>mz_internal.mz_postgres_sources</code> table to look up the name of the
replication slot created for each source.</p>
</li>
<li>
<p>If you delete all objects that depend on a source without also dropping
the source, the upstream replication slot remains and will continue to
accumulate data so that the source can resume in the future. To avoid
unbounded disk space usage, make sure to use <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> or manually delete the replication slot.</p>
</li>
</ul>
<h3 id="modifying-an-existing-source">Modifying an existing source</h3>
<p>When you add a new subsource to an existing source (<a href="/sql/alter-source/" ><code>ALTER SOURCE ... ADD SUBSOURCE ...</code></a>), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.
</p>



---

## Ingest data from Amazon RDS


This page shows you how to stream data from [Amazon RDS for PostgreSQL](https://aws.amazon.com/rds/postgresql/)
to Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).
>
>


## Before you begin

<ul>
<li>
<p>Make sure you are running PostgreSQL 11 or higher.</p>
</li>
<li>
<p>Make sure you have access to your PostgreSQL instance via <a href="https://www.postgresql.org/docs/current/app-psql.html" ><code>psql</code></a>,
or your preferred SQL client.</p>
</li>
</ul>


## A. Configure Amazon RDS

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

As a first step, you need to make sure logical replication is enabled.

1. As a user with the `rds_superuser` role, use `psql` (or your preferred SQL
   client) to connect to your database.

1. Check if logical replication is enabled:

    ```postgres
    SELECT name, setting
      FROM pg_settings
      WHERE name = 'rds.logical_replication';
    ```
    <p></p>

    ```nofmt
            name             | setting
    -------------------------+---------
    rds.logical_replication  | off
    (1 row)
    ```

    - If logical replication is off, continue to the next step.

    - If logical replication is already on, skip to [Create a publication and a
      Materialize user section](#2-create-a-publication-and-a-replication-user).

1. Using the AWS Management Console, [create a DB parameter group in RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithParamGroups.Creating.html).

    - Set **Parameter group family** to your PostgreSQL version.
    - Set **Type** to **DB Parameter Group**.
    - Set **Engine type** to PostgreSQL.

1. Edit the new parameter group and set the `rds.logical_replication` parameter
   to `1`.

1. [Associate the DB parameter group with your database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithParamGroups.Associating.html).

    Use the **Apply Immediately** option to immediately reboot your database and
    apply the change. Keep in mind that rebooting the RDS instance can affect
    database performance.

    Do not move on to the next step until the database **Status**
    is **Available** in the RDS Console.

1. Back in the SQL client connected to PostgreSQL, verify that replication is
   now enabled:

    ```postgres
    SELECT name, setting
      FROM pg_settings
      WHERE name = 'rds.logical_replication';
    ```
    <p></p>

    ``` nofmt
            name             | setting
    -------------------------+---------
    rds.logical_replication  | on
    (1 row)
    ```

    If replication is still not enabled, [reboot the database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_RebootInstance.html).

### 2. Create a publication and a replication user

<p>Once logical replication is enabled, create a publication with the tables that
you want to replicate to Materialize. You&rsquo;ll also need a user for Materialize
with sufficient privileges to manage replication.</p>
<ol>
<li>
<p>As a <em>superuser</em>, use <code>psql</code> (or your preferred SQL client) to connect to
your database.</p>
</li>
<li>
<p>For each table that you want to replicate to Materialize, set the
<a href="https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY" >replica identity</a>
to <code>FULL</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span> <span class="k">REPLICA</span> <span class="k">IDENTITY</span> <span class="k">FULL</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span> <span class="k">REPLICA</span> <span class="k">IDENTITY</span> <span class="k">FULL</span><span class="p">;</span>
</span></span></code></pre></div><p><code>REPLICA IDENTITY FULL</code> ensures that the replication stream includes the
previous data of changed rows, in the case of <code>UPDATE</code> and <code>DELETE</code>
operations. This setting enables Materialize to ingest PostgreSQL data with
minimal in-memory state. However, you should expect increased disk usage in
your PostgreSQL database.</p>
</li>
<li>
<p>Create a <a href="https://www.postgresql.org/docs/current/logical-replication-publication.html" >publication</a>
with the tables you want to replicate:</p>
<p><em>For specific tables:</em></p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="n">PUBLICATION</span> <span class="n">mz_source</span> <span class="k">FOR</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span><span class="p">,</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span><span class="p">;</span>
</span></span></code></pre></div><p><em>For all tables in the database:</em></p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="n">PUBLICATION</span> <span class="n">mz_source</span> <span class="k">FOR</span> <span class="k">ALL</span> <span class="k">TABLES</span><span class="p">;</span>
</span></span></code></pre></div><p>The <code>mz_source</code> publication will contain the set of change events generated
from the specified tables, and will later be used to ingest the replication
stream.</p>
<p>Be sure to include only the tables you need. If the publication includes
additional tables, Materialize will waste resources on ingesting and then
immediately discarding the data.</p>
</li>
<li>
<p>Create a user for Materialize, if you don&rsquo;t already have one:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">USER</span> <span class="n">materialize</span> <span class="k">PASSWORD</span> <span class="s1">&#39;&lt;password&gt;&#39;</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>Grant the user permission to manage replication:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="n">rds_replication</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>Grant the user the required permissions on the tables you want to replicate:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="n">CONNECT</span> <span class="k">ON</span> <span class="k">DATABASE</span> <span class="o">&lt;</span><span class="n">dbname</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="n">USAGE</span> <span class="k">ON</span> <span class="k">SCHEMA</span> <span class="o">&lt;</span><span class="k">schema</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><p>Once connected to your database, Materialize will take an initial snapshot
of the tables in your publication. <code>SELECT</code> privileges are required for
this initial snapshot.</p>
<p>If you expect to add tables to your publication, you can grant <code>SELECT</code> on
all tables in the schema instead of naming the specific tables:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="k">ALL</span> <span class="k">TABLES</span> <span class="k">IN</span> <span class="k">SCHEMA</span> <span class="o">&lt;</span><span class="k">schema</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div></li>
</ol>


## B. (Optional) Configure network security

> **Note:** If you are prototyping and your RDS instance is publicly accessible, **you can
> skip this step**. For production scenarios, we recommend configuring one of the
> network security options below.
>




**Cloud:**

There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's security group to allow connections from a set of
    static Materialize IP addresses.

- **Use AWS PrivateLink**: If your database is running in a private network, you
    can use [AWS PrivateLink](/ingest-data/network-security/privatelink/) to
    connect Materialize to the database. For details, see [AWS PrivateLink](/ingest-data/network-security/privatelink/).

- **Use an SSH tunnel:** If your database is running in a private network, you
    can use an SSH tunnel to connect Materialize to the database.



**Allow Materialize IPs:**

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, find the static egress IP addresses for the
   Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

1. In the AWS Management Console, [add an inbound rule to your RDS security group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/changing-security-group.html#add-remove-instance-security-groups)
   for each IP address from the previous step.

    In each rule:

    - Set **Type** to **PostgreSQL**.
    - Set **Source** to the IP address in CIDR notation.



**Use AWS PrivateLink:**

[AWS PrivateLink](https://aws.amazon.com/privatelink/) lets you connect
Materialize to your RDS instance without exposing traffic to the public
internet. To use AWS PrivateLink, you create a network load balancer in the
same VPC as your RDS instance and a VPC endpoint service that Materialize
connects to. The VPC endpoint service then routes requests from Materialize to
RDS via the network load balancer.

> **Note:** Materialize provides a Terraform module that automates the creation and
> configuration of AWS resources for a PrivateLink connection. For more details,
> see the [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-rds-privatelink).
>


1. Get the IP address of your RDS instance. You'll need this address to register
   your RDS instance as the target for the network load balancer in the next
   step.

    To get the IP address of your RDS instance:

    1. Select your database in the RDS Console.

    1. Find your RDS endpoint under **Connectivity & security**.

    1. Use the `dig` or `nslooklup` command to find the IP address that the
    endpoint resolves to:

       ```sh
       dig +short <RDS_ENDPOINT>
       ```

1. [Create a dedicated target group for your RDS instance](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html).

    - Choose the **IP addresses** type.

    - Set the protocol and port to **TCP** and **5432**.

    - Choose the same VPC as your RDS instance.

    - Use the IP address from the previous step to register your RDS instance as
      the target.

    **Warning:** The IP address of your RDS instance can change without notice.
      For this reason, it's best to set up automation to regularly check the IP
      of the instance and update your target group accordingly. You can use a
      lambda function to automate this process - see Materialize's
      [Terraform module for AWS PrivateLink](https://github.com/MaterializeInc/terraform-aws-rds-privatelink/blob/main/lambda_function.py)
      for an example. Another approach is to [configure an EC2 instance as an
      RDS router](https://aws.amazon.com/blogs/database/how-to-use-amazon-rds-and-amazon-aurora-with-a-static-ip-address/)
      for your network load balancer.

1. [Create a network load balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html).

    - For **Network mapping**, choose the same VPC as your RDS instance and
      select all of the availability zones and subnets that you RDS instance is
      in.

    - For **Listeners and routing**, set the protocol and port to **TCP**
      and **5432** and select the target group you created in the previous
      step.

1. In the security group of your RDS instance, [allow traffic from the network load balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html).

    If [client IP preservation](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-target-groups.html#client-ip-preservation)
    is disabled, the easiest approach is to add an inbound rule with the VPC
    CIDR of the network load balancer. If you don't want to grant access to the
    entire VPC CIDR, you can add inbound rules for the private IP addresses of
    the load balancer subnets.

    - To find the VPC CIDR, go to your network load balancer and look
      under **Network mapping**.
    - To find the private IP addresses of the load balancer subnets, go
      to **Network Interfaces**, search for the name of the network load
      balancer, and look on the **Details** tab for each matching network
      interface.

1. [Create a VPC endpoint service](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html).

    - For **Load balancer type**, choose **Network** and then select the network
      load balancer you created in the previous step.

    - After creating the VPC endpoint service, note its **Service name**. You'll
      use this service name when connecting Materialize later.

    **Remarks**: By disabling [Acceptance Required](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests),
      while still strictly managing who can view your endpoint via IAM,
      Materialze will be able to seamlessly recreate and migrate endpoints as
      we work to stabilize this feature.

1. Go back to the target group you created for the network load balancer and
   make sure that the [health checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html)
   are reporting the targets as healthy.



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

> **Note:** Materialize provides a Terraform module that automates the creation and
> configuration of resources for an SSH tunnel. For more details, see the
> [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-ec2-ssh-bastion).
>


1. [Launch an EC2 instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/LaunchingAndUsingInstances.html)
   to serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your
      RDS instance.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.

    **Warning:** Auto-assigned public IP addresses can change in [certain cases](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#concepts-public-addresses).

    For this reason, it's best to associate an [elastic IP address](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#ip-addressing-eips)
    to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

    1. In the [Materialize console's SQL
       Shell](/console/), or your preferred SQL client
       connected to Materialize, get the static egress IP addresses for the
       Materialize region you are running in:

       ```mzsql
       SELECT * FROM mz_egress_ips;
       ```

    1. For each static egress IP, [add an inbound rule](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-security-groups.html)
       to your SSH bastion host's security group.

        In each rule:
        - Set **Type** to **PostgreSQL**.
        - Set **Source** to the IP address in CIDR notation.

1. In the security group of your RDS instance, [add an inbound rule](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html)
   to allow traffic from the SSH bastion host.

    - Set **Type** to **All TCP**.
    - Set **Source** to **Custom** and select the bastion host's security
      group.







**Self-Managed:**

<p>Configure your network to allow Materialize to connect to your database. For
example, you can:</p>
<ul>
<li>
<p><strong>Allow Materialize IPs:</strong> Configure your database&rsquo;s security group to allow
connections from Materialize.</p>
</li>
<li>
<p><strong>Use an SSH tunnel:</strong> Use an SSH tunnel to connect Materialize to the
database.</p>
</li>
</ul>
<div class="note">
  <strong class="gutter">NOTE:</strong>
  <p>The steps to allow Materialize to connect to your database  depends on your
  deployment setup. Refer to your company’s network/security policies and
  procedures.</p>
</div>




**Allow Materialize IPs:**

1. In the AWS Management Console, [add an inbound rule to your RDS security group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/changing-security-group.html#add-remove-instance-security-groups)
   to allow traffic from Materialize IPs.

    In each rule:

    - Set **Type** to **PostgreSQL**.
    - Set **Source** to the IP address in CIDR notation.



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

> **Note:** Materialize provides a Terraform module that automates the creation and
> configuration of resources for an SSH tunnel. For more details, see the
> [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-ec2-ssh-bastion).
>


1. [Launch an EC2 instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/LaunchingAndUsingInstances.html)
    to serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your
      RDS instance.

    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.

    **Warning:** Auto-assigned public IP addresses can change in [certain cases](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#concepts-public-addresses).
      For this reason, it's best to associate an [elastic IP address](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#ip-addressing-eips)
      to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

1. In the security group of your RDS instance, [add an inbound rule](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html)
   to allow traffic from the SSH bastion host.

    - Set **Type** to **All TCP**.
    - Set **Source** to **Custom** and select the bastion host's security
      group.









## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

> **Note:** If you are prototyping and already have a cluster to host your PostgreSQL
> source (e.g. `quickstart`), **you can skip this step**. For production
> scenarios, we recommend separating your workloads into multiple clusters for
> [resource isolation](/sql/create-cluster/#resource-isolation).
>


<p>In Materialize, a <a href="/concepts/clusters/" >cluster</a> is an isolated environment,
similar to a virtual warehouse in Snowflake. When you create a cluster, you
choose the size of its compute resource allocation based on the work you need
the cluster to do, whether ingesting data from a source, computing
always-up-to-date query results, serving results to external clients, or a
combination.</p>
<p>In this step, you&rsquo;ll create a dedicated cluster for ingesting source data from
your PostgreSQL database.</p>
<ol>
<li>
<p>In the <a href="/console/" >SQL Shell</a>, or your preferred SQL
client connected to Materialize, use the <a href="/sql/create-cluster/" ><code>CREATE CLUSTER</code></a>
command to create the new cluster:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">CLUSTER</span> <span class="n">ingest_postgres</span> <span class="p">(</span><span class="k">SIZE</span> <span class="o">=</span> <span class="s1">&#39;50cc&#39;</span><span class="p">);</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">SET</span> <span class="k">CLUSTER</span> <span class="o">=</span> <span class="n">ingest_postgres</span><span class="p">;</span>
</span></span></code></pre></div><p>A cluster of <a href="/sql/create-cluster/#size" >size</a> <code>50cc</code> should be enough to
accommodate multiple PostgreSQL sources, depending on the source
characteristics (e.g., sources with <a href="/sql/create-source/kafka/#upsert-envelope" ><code>ENVELOPE UPSERT</code></a>
or <a href="/sql/create-source/kafka/#debezium-envelope" ><code>ENVELOPE DEBEZIUM</code></a> will be more
memory-intensive) and the upstream traffic patterns. You can readjust the
size of the cluster at any time using the <a href="/sql/alter-cluster" ><code>ALTER CLUSTER</code></a> command:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">CLUSTER</span> <span class="o">&lt;</span><span class="n">cluster_name</span><span class="o">&gt;</span> <span class="k">SET</span> <span class="p">(</span> <span class="k">SIZE</span> <span class="o">=</span> <span class="o">&lt;</span><span class="n">new_size</span><span class="o">&gt;</span> <span class="p">);</span>
</span></span></code></pre></div></li>
</ol>


### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.


**Allow Materialize IPs:**

1. In the [Materialize Console's SQL Shell](/console/), or your preferred SQL
client connected to Materialize, use the [`CREATE
SECRET`](/sql/create-secret/) command to securely store the password for the
`materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):

   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
connection object with access and authentication details for Materialize to
use:


   ```mzsql
   CREATE CONNECTION pg_connection TO POSTGRES (
     HOST '<host>',
     PORT 5432,
     USER materialize,
     PASSWORD SECRET pgpass,
     SSL MODE 'require',
     DATABASE '<database>'
   );

   ```

   - Replace `<host>` with your RDS endpoint. To find your RDS endpoint, select
   your database in the RDS Console, and look under **Connect & security**

   - Replace `<database>` with the name of the database containing the tables
     you want to replicate to Materialize.




**Use AWS PrivateLink (Cloud-only):**

1. In the [SQL Shell](https://console.materialize.com/), or your preferred SQL
client connected to Materialize, use the [`CREATE
CONNECTION`](/sql/create-connection/#aws-privatelink) command to create an
**in-region** or **cross-region** AWS PrivateLink connection.

   ↕️ **In-region connections**

   To connect to an AWS PrivateLink endpoint service in the **same region** as
   your Materialize environment:

      ```mzsql
      CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK ( SERVICE
            NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
            AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az4') );

      ```

   - Replace the `SERVICE NAME` value with the service name you noted
   [earlier](#b-optional-configure-network-security).

   - Replace the `AVAILABILITY ZONES` list with the IDs of the availability
   zones in your AWS account. For in-region connections the availability zones
   of the NLB and the consumer VPC **must match**.

     To find your availability zone IDs, select your database in the RDS
     Console and click the subnets under **Connectivity & security**. For each
     subnet, look for **Availability Zone ID** (e.g., `use1-az6`), not
     **Availability Zone** (e.g., `us-east-1d`).


   ↔️ **Cross-region connections**

   To connect to an AWS PrivateLink endpoint service in a **different region**
   to the one where your Materialize environment is deployed:

      ```mzsql
      CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK ( SERVICE
      NAME 'com.amazonaws.vpce.us-west-1.vpce-svc-<endpoint_service_id>', -- For
      now, the AVAILABILITY ZONES clause **is** required, but will be -- made
      optional in a future release. AVAILABILITY ZONES () );

      ```

   - Replace the `SERVICE NAME` value with the service name you noted
   [earlier](#b-optional-configure-network-security).

   - The service name region refers to where the endpoint service was created.
   You **do not need** to specify `AVAILABILITY ZONES` manually — these will be
   optimally auto-assigned when none are provided.

1. Retrieve the AWS principal for the AWS PrivateLink connection you just created:


   ```mzsql
   SELECT principal
   FROM mz_aws_privatelink_connections plc
   JOIN mz_connections c ON plc.id = c.id
   WHERE c.name = 'privatelink_svc';

   ```

   The results should resemble:
   ```
                                    principal
   ---------------------------------------------------------------------------
    arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
   ```


1. Update your VPC endpoint service to [accept connections from the AWS principal](https://docs.aws.amazon.com/vpc/latest/privatelink/add-endpoint-service-permissions.html).


1. If your AWS PrivateLink service is configured to require acceptance of
connection requests, [manually approve the connection request from
Materialize](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).
   **Note:** It can take some time for the connection request to show up. Do
not move on to the next step until you've approved the connection.


1. Validate the AWS PrivateLink connection you created using the [`VALIDATE
CONNECTION`](/sql/validate-connection) command:


   ```mzsql
   VALIDATE CONNECTION privatelink_svc;

   ```   If no validation error is returned, move to the next step.


1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
password for the `materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):


   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```
1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create
another connection object, this time with database access and authentication
details for Materialize to use:


   ```mzsql
   CREATE CONNECTION pg_connection TO POSTGRES (
     HOST '<host>',
     PORT 5432,
     USER 'materialize',
     PASSWORD SECRET pgpass,
     DATABASE '<database>',
     AWS PRIVATELINK privatelink_svc
     );

   ```
   - Replace `<host>` with your RDS endpoint. To find your RDS endpoint, select
   your database in the RDS Console, and look under **Connectivity &
   security**.

   - Replace `<database>` with the name of the database containing the tables
     you want to replicate to Materialize.



**Use an SSH tunnel:**

1. In the [Materialize Console's SQL Shell](/console/), or your preferred SQL
client connected to Materialize, use the [`CREATE
CONNECTION`](/sql/create-connection/#ssh-tunnel) command to create an SSH
tunnel connection:

   ```mzsql
   CREATE CONNECTION ssh_connection TO SSH TUNNEL (
       HOST '<SSH_BASTION_HOST>',
       PORT <SSH_BASTION_PORT>,
       USER '<SSH_BASTION_USER>'
   );

   ```

   - Replace `<SSH_BASTION_HOST>` and `<SSH_BASTION_PORT>` with the public IP
   address and port of the SSH bastion host you created
   [earlier](#b-optional-configure-network-security).

   - Replace `<SSH_BASTION_USER>` with the username for the key pair you
   created for your SSH bastion host.

1. Get Materialize's public keys for the SSH tunnel connection:


   ```mzsql
   SELECT
       mz_connections.name,
       mz_ssh_tunnel_connections.*
   FROM
       mz_connections
   JOIN
       mz_ssh_tunnel_connections USING(id)
   WHERE
       mz_connections.name = 'ssh_connection';

   ```

1. Log in to your SSH bastion host and add Materialize's public keys to the
`authorized_keys` file, for example:


   ```mzsql
   echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
   echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys

   ```

1. Back in the SQL client connected to Materialize, validate the SSH tunnel
connection you created using the [`VALIDATE
CONNECTION`](/sql/validate-connection) command:


   ```mzsql
   VALIDATE CONNECTION ssh_connection;

   ```   If no validation error is returned, move to the next step.


1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
password for the `materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):


   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```

1.
Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create another connection object, this time with database access and authentication details for Materialize to use:


   ```mzsql
   CREATE CONNECTION pg_connection TO POSTGRES (
     HOST '<host>',
     PORT 5432,
     USER 'materialize',
     PASSWORD SECRET pgpass,
     DATABASE '<database>',
     SSH TUNNEL ssh_connection
     );

   ```

   - Replace `<host>` with your RDS endpoint. To find your RDS endpoint,
   select your database in the RDS Console, and look under
   **Connectivity & security**.

   - Replace `<database>` with the name of the database containing the tables
   you want to replicate to Materialize.






### 3. Start ingesting data

{{< tabs >}}
{{< tab "Legacy Syntax" >}}
#### Legacy syntax

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source-legacy" %}}
{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="schema-changes" %}}
{{< /tab >}}

{{< tab "New Syntax" >}}
#### New syntax

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source" %}}
{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="schema-changes" %}}
{{< /tab >}}
{{< /tabs >}}


### 4. Monitor the ingestion status

<p>Before it starts consuming the replication stream, Materialize takes a snapshot
of the relevant tables in your publication. Until this snapshot is complete,
Materialize won&rsquo;t have the same view of your data as your PostgreSQL database.</p>
<p>In this step, you&rsquo;ll first verify that the source is running and then check the
status of the snapshotting process.</p>
<ol>
<li>
<p>Back in the SQL client connected to Materialize, use the
<a href="/sql/system-catalog/mz_internal/#mz_source_statuses" ><code>mz_source_statuses</code></a>
table to check the overall status of your source:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">WITH</span>
</span></span><span class="line"><span class="cl">  <span class="n">source_ids</span> <span class="k">AS</span>
</span></span><span class="line"><span class="cl">  <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">mz_sources</span> <span class="k">WHERE</span> <span class="k">name</span> <span class="o">=</span> <span class="s1">&#39;mz_source&#39;</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">  <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_source_statuses</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span>
</span></span><span class="line"><span class="cl">      <span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="k">SELECT</span> <span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">        <span class="k">FROM</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_object_dependencies</span>
</span></span><span class="line"><span class="cl">        <span class="k">WHERE</span>
</span></span><span class="line"><span class="cl">          <span class="n">object_id</span> <span class="k">IN</span> <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">UNION</span> <span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span>
</span></span><span class="line"><span class="cl">      <span class="p">)</span>
</span></span><span class="line"><span class="cl">      <span class="k">AS</span> <span class="k">sources</span>
</span></span><span class="line"><span class="cl">    <span class="k">ON</span> <span class="n">mz_source_statuses</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span><span class="p">;</span>
</span></span></code></pre></div><p>For each <code>subsource</code>, make sure the <code>status</code> is <code>running</code>. If you see
<code>stalled</code> or <code>failed</code>, there&rsquo;s likely a configuration issue for you to fix.
Check the <code>error</code> field for details and fix the issue before moving on.
Also, if the <code>status</code> of any subsource is <code>starting</code> for more than a few
minutes, <a href="/support/" >contact our team</a>.</p>
</li>
<li>
<p>Once the source is running, use the <a href="/sql/system-catalog/mz_internal/#mz_source_statistics" ><code>mz_source_statistics</code></a>
table to check the status of the initial snapshot:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">WITH</span>
</span></span><span class="line"><span class="cl">  <span class="n">source_ids</span> <span class="k">AS</span>
</span></span><span class="line"><span class="cl">  <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">mz_sources</span> <span class="k">WHERE</span> <span class="k">name</span> <span class="o">=</span> <span class="s1">&#39;mz_source&#39;</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span> <span class="k">AS</span> <span class="k">id</span><span class="p">,</span> <span class="n">mz_sources</span><span class="mf">.</span><span class="k">name</span><span class="p">,</span> <span class="n">snapshot_committed</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">  <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_source_statistics</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span>
</span></span><span class="line"><span class="cl">      <span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="k">SELECT</span> <span class="n">object_id</span><span class="p">,</span> <span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">        <span class="k">FROM</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_object_dependencies</span>
</span></span><span class="line"><span class="cl">        <span class="k">WHERE</span>
</span></span><span class="line"><span class="cl">          <span class="n">object_id</span> <span class="k">IN</span> <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">UNION</span> <span class="k">SELECT</span> <span class="k">id</span><span class="p">,</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span>
</span></span><span class="line"><span class="cl">      <span class="p">)</span>
</span></span><span class="line"><span class="cl">      <span class="k">AS</span> <span class="k">sources</span>
</span></span><span class="line"><span class="cl">    <span class="k">ON</span> <span class="n">mz_source_statistics</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_sources</span> <span class="k">ON</span> <span class="n">mz_sources</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span><span class="p">;</span>
</span></span></code></pre></div> <p></p>
<pre tabindex="0"><code class="language-nofmt" data-lang="nofmt">object_id | snapshot_committed
----------|------------------
 u144     | t
(1 row)
</code></pre><p>Once <code>snapshot_commited</code> is <code>t</code>, move on to the next step. Snapshotting can
take between a few minutes to several hours, depending on the size of your
dataset and the size of the cluster the source is running in.</p>
</li>
</ol>


### 5. Right-size the cluster

<p>After the snapshotting phase, Materialize starts ingesting change events from
the PostgreSQL replication stream. For this work, Materialize generally
performs well with an <code>100cc</code> replica, so you can resize the cluster
accordingly.</p>
<ol>
<li>
<p>Still in a SQL client connected to Materialize, use the <a href="/sql/alter-cluster/" ><code>ALTER CLUSTER</code></a>
command to downsize the cluster to <code>100cc</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">CLUSTER</span> <span class="n">ingest_postgres</span> <span class="k">SET</span> <span class="p">(</span><span class="k">SIZE</span> <span class="s1">&#39;100cc&#39;</span><span class="p">);</span>
</span></span></code></pre></div><p>Behind the scenes, this command adds a new <code>100cc</code> replica and removes the
<code>50cc</code> replica.</p>
</li>
<li>
<p>Use the <a href="/sql/show-cluster-replicas/" ><code>SHOW CLUSTER REPLICAS</code></a> command to
check the status of the new replica:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SHOW</span> <span class="k">CLUSTER</span> <span class="k">REPLICAS</span> <span class="k">WHERE</span> <span class="k">cluster</span> <span class="o">=</span> <span class="s1">&#39;ingest_postgres&#39;</span><span class="p">;</span>
</span></span></code></pre></div> <p></p>
<pre tabindex="0"><code class="language-nofmt" data-lang="nofmt">     cluster     | replica |  size  | ready
-----------------+---------+--------+-------
 ingest_postgres | r1      | 100cc  | t
(1 row)
</code></pre></li>
<li>
<p>Going forward, you can verify that your new cluster size is sufficient as
follows:</p>
<ol>
<li>
<p>In Materialize, get the replication slot name associated with your
PostgreSQL source from the <a href="/sql/system-catalog/mz_internal/#mz_postgres_sources" ><code>mz_internal.mz_postgres_sources</code></a>
table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span>
</span></span><span class="line"><span class="cl">    <span class="n">d</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">database_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">n</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">schema_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">s</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">source_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">pgs</span><span class="mf">.</span><span class="n">replication_slot</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">    <span class="n">mz_sources</span> <span class="k">AS</span> <span class="n">s</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_postgres_sources</span> <span class="k">AS</span> <span class="n">pgs</span> <span class="k">ON</span> <span class="n">s</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">pgs</span><span class="mf">.</span><span class="k">id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_schemas</span> <span class="k">AS</span> <span class="n">n</span> <span class="k">ON</span> <span class="n">n</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">s</span><span class="mf">.</span><span class="n">schema_id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_databases</span> <span class="k">AS</span> <span class="n">d</span> <span class="k">ON</span> <span class="n">d</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">n</span><span class="mf">.</span><span class="n">database_id</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>In PostgreSQL, check the replication slot lag, using the replication slot
name from the previous step:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">SELECT</span>
</span></span><span class="line"><span class="cl">    <span class="n">pg_size_pretty</span><span class="p">(</span><span class="n">pg_current_wal_lsn</span><span class="p">()</span> <span class="o">-</span> <span class="n">confirmed_flush_lsn</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">    <span class="k">AS</span> <span class="n">replication_lag_bytes</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span> <span class="n">pg_replication_slots</span>
</span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">slot_name</span> <span class="o">=</span> <span class="s1">&#39;&lt;slot_name&gt;&#39;</span><span class="p">;</span>
</span></span></code></pre></div><p>The result of this query is the amount of data your PostgreSQL cluster
must retain in its replication log because of this replication slot.
Typically, this means Materialize has not yet communicated back to
PostgreSQL that it has committed this data. A high value can indicate
that the source has fallen behind and that you might need to scale up
your ingestion cluster.</p>
</li>
</ol>
</li>
</ol>


## D. Explore your data

<p>With Materialize ingesting your PostgreSQL data into durable storage, you can
start exploring the data, computing real-time results that stay up-to-date as
new data arrives, and serving results efficiently.</p>
<ul>
<li>
<p>Explore your data with <a href="/sql/show-sources" ><code>SHOW SOURCES</code></a> and <a href="/sql/select/" ><code>SELECT</code></a>.</p>
</li>
<li>
<p>Compute real-time results in memory with <a href="/sql/create-view/" ><code>CREATE VIEW</code></a>
and <a href="/sql/create-index/" ><code>CREATE INDEX</code></a> or in durable
storage with <a href="/sql/create-materialized-view/" ><code>CREATE MATERIALIZED VIEW</code></a>.</p>
</li>
<li>
<p>Serve results to a PostgreSQL-compatible SQL client or driver with <a href="/sql/select/" ><code>SELECT</code></a>
or <a href="/sql/subscribe/" ><code>SUBSCRIBE</code></a> or to an external message broker with
<a href="/sql/create-sink/" ><code>CREATE SINK</code></a>.</p>
</li>
<li>
<p>Check out the <a href="/integrations/" >tools and integrations</a> supported by
Materialize.</p>
</li>
</ul>


## Considerations

<h3 id="schema-changes">Schema changes</h3>
<p>Materialize supports schema changes in the upstream database as follows:</p>
<h4 id="compatible-schema-changes-legacy-syntax">Compatible schema changes (Legacy syntax)</h4>
<blockquote>
<p><strong>Note:</strong> This section refer to the legacy <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a> that creates subsources as part of the
<code>CREATE SOURCE</code> operation.  To be able to handle the upstream column
additions and drops, see <a href="/sql/create-source/postgres-v2/" ><code>CREATE SOURCE (New Syntax)</code></a> and <a href="/sql/create-table" ><code>CREATE TABLE FROM SOURCE</code></a>.</p>
</blockquote>
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
<h4 id="incompatible-schema-changes">Incompatible schema changes</h4>
<p>All other schema changes to upstream tables will set the corresponding
Materialize tables into an error state, preventing reads from these tables.</p>
<p>To handle <a href="#incompatible-schema-changes" >incompatible schema changes</a>, drop
the affected table <a href="/sql/drop-table/" ><code>DROP TABLE</code></a> , and then, <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> to recreate the table with the
updated schema.</p>
<h3 id="publication-membership">Publication membership</h3>
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
<p>To mitigate this issue, if you need to drop and re-add a table to a
publication, ensure that you remove the table/subsource from the source
<em>before</em> re-adding it using the <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> command.</p>
<h3 id="supported-types">Supported types</h3>
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
<h3 id="truncation">Truncation</h3>
<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource(s)/table(s) in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>
<h3 id="inherited-tables">Inherited tables</h3>
<p>When using <a href="https://www.postgresql.org/docs/current/tutorial-inheritance.html" >PostgreSQL table inheritance</a>,
PostgreSQL serves data from <code>SELECT</code>s as if the inheriting tables&rsquo; data is
also present in the inherited table. However, both PostgreSQL&rsquo;s logical
replication and <code>COPY</code> only present data written to the tables themselves,
i.e. the inheriting data is <em>not</em> treated as part of the inherited table.</p>
<p>PostgreSQL sources use logical replication and <code>COPY</code> to ingest table data,
so inheriting tables&rsquo; data will only be ingested as part of the inheriting
table, i.e. in Materialize, the data will not be returned when serving
<code>SELECT</code>s from the inherited table.</p>
<ul>
<li>
<p>If using legacy syntax <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a>:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>ADD SUBSOURCE</code> and
create a new view (materialized or non-) that unions the new table.</p>
</li>
<li>
<p>If using new <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> syntax:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>CREATE TABLE .. FROM SOURCE</code> and create a new view (materialized or non-) that unions the new
table.</p>
</li>
</ul>
<h3 id="replication-slots">Replication slots</h3>
<p>Each source ingests the raw replication stream data for all tables in the
specified publication using <strong>a single</strong> replication slot. To manage
replication slots:</p>
<ul>
<li>
<p>For PostgreSQL 13+, set a reasonable value
for <a href="https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE" ><code>max_slot_wal_keep_size</code></a>
to limit the amount of storage used by replication slots.</p>
</li>
<li>
<p>If you stop using Materialize, or if either the Materialize instance or
the PostgreSQL instance crash, delete any replication slots. You can query
the <code>mz_internal.mz_postgres_sources</code> table to look up the name of the
replication slot created for each source.</p>
</li>
<li>
<p>If you delete all objects that depend on a source without also dropping
the source, the upstream replication slot remains and will continue to
accumulate data so that the source can resume in the future. To avoid
unbounded disk space usage, make sure to use <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> or manually delete the replication slot.</p>
</li>
</ul>
<h3 id="modifying-an-existing-source">Modifying an existing source</h3>
<p>When you add a new subsource to an existing source (<a href="/sql/alter-source/" ><code>ALTER SOURCE ... ADD SUBSOURCE ...</code></a>), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.
</p>



---

## Ingest data from Azure DB


This page shows you how to stream data from [Azure DB for PostgreSQL](https://azure.microsoft.com/en-us/products/postgresql)
to Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).
>
>


## Before you begin

<ul>
<li>
<p>Make sure you are running PostgreSQL 11 or higher.</p>
</li>
<li>
<p>Make sure you have access to your PostgreSQL instance via <a href="https://www.postgresql.org/docs/current/app-psql.html" ><code>psql</code></a>,
or your preferred SQL client.</p>
</li>
</ul>


## A. Configure Azure DB

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

To enable logical replication in Azure DB, see the
[Azure documentation](https://learn.microsoft.com/en-us/azure/postgresql/single-server/concepts-logical#set-up-your-server).

### 2. Create a publication and a replication user

<p>Once logical replication is enabled, the next step is to create a publication
with the tables that you want to replicate to Materialize. You&rsquo;ll also need a
user for Materialize with sufficient privileges to manage replication.</p>
<ol>
<li>
<p>For each table that you want to replicate to Materialize, set the
<a href="https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY" >replica identity</a>
to <code>FULL</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span> <span class="k">REPLICA</span> <span class="k">IDENTITY</span> <span class="k">FULL</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span> <span class="k">REPLICA</span> <span class="k">IDENTITY</span> <span class="k">FULL</span><span class="p">;</span>
</span></span></code></pre></div><p><code>REPLICA IDENTITY FULL</code> ensures that the replication stream includes the
previous data of changed rows, in the case of <code>UPDATE</code> and <code>DELETE</code>
operations. This setting enables Materialize to ingest PostgreSQL data with
minimal in-memory state. However, you should expect increased disk usage in
your PostgreSQL database.</p>
</li>
<li>
<p>Create a <a href="https://www.postgresql.org/docs/current/logical-replication-publication.html" >publication</a>
with the tables you want to replicate:</p>
<p><em>For specific tables:</em></p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="n">PUBLICATION</span> <span class="n">mz_source</span> <span class="k">FOR</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span><span class="p">,</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span><span class="p">;</span>
</span></span></code></pre></div><p><em>For all tables in the database:</em></p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="n">PUBLICATION</span> <span class="n">mz_source</span> <span class="k">FOR</span> <span class="k">ALL</span> <span class="k">TABLES</span><span class="p">;</span>
</span></span></code></pre></div><p>The <code>mz_source</code> publication will contain the set of change events generated
from the specified tables, and will later be used to ingest the replication
stream.</p>
<p>Be sure to include only the tables you need. If the publication includes
additional tables, Materialize will waste resources on ingesting and then
immediately discarding the data.</p>
</li>
<li>
<p>Create a user for Materialize, if you don&rsquo;t already have one:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">USER</span> <span class="n">materialize</span> <span class="k">PASSWORD</span> <span class="s1">&#39;&lt;password&gt;&#39;</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>Grant the user permission to manage replication:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">ROLE</span> <span class="n">materialize</span> <span class="k">WITH</span> <span class="n">REPLICATION</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>Grant the user the required permissions on the tables you want to replicate:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="n">CONNECT</span> <span class="k">ON</span> <span class="k">DATABASE</span> <span class="o">&lt;</span><span class="n">dbname</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="n">USAGE</span> <span class="k">ON</span> <span class="k">SCHEMA</span> <span class="o">&lt;</span><span class="k">schema</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><p>Once connected to your database, Materialize will take an initial snapshot
of the tables in your publication. <code>SELECT</code> privileges are required for
this initial snapshot.</p>
<p>If you expect to add tables to your publication, you can grant <code>SELECT</code> on
all tables in the schema instead of naming the specific tables:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="k">ALL</span> <span class="k">TABLES</span> <span class="k">IN</span> <span class="k">SCHEMA</span> <span class="o">&lt;</span><span class="k">schema</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div></li>
</ol>


## B. (Optional) Configure network security

> **Note:** If you are prototyping and your AzureDB instance is publicly accessible, **you
> can skip this step**. For production scenarios, we recommend configuring one of
> the network security options below.
>




**Cloud:**

There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's firewall to allow connections from a set of
    static Materialize IP addresses.

- **Use an SSH tunnel:** If your database is running in a private network, you
    can use an SSH tunnel to connect Materialize to the database.

Select the option that works best for you.



**Allow Materialize IPs:**

1. In the [Materialize console's SQL Shell](/console/),
   or your preferred SQL client connected to Materialize, find the static egress
   IP addresses for the Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

1. Update your [Azure DB firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql)
   to allow traffic from each IP address from the previous step.



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

1. [Launch an Azure VM with a static public IP address](https://learn.microsoft.com/en-us/azure/virtual-network/ip-services/virtual-network-deploy-static-pip-arm-portal?toc=%2Fazure%2Fvirtual-machines%2Ftoc.json)
to serve as your SSH bastion host.

    - Make sure the VM is publicly accessible and in the same VPC as your
      database.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a static public IP address. You'll use this IP
      address when connecting Materialize to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

    1. In the [Materialize console's SQL
       Shell](/console/), or your preferred SQL client
       connected to Materialize, get the static egress IP addresses for the
       Materialize region you are running in:

       ```mzsql
       SELECT * FROM mz_egress_ips;
       ```

    1. Update your SSH bastion host's [firewall rules](https://learn.microsoft.com/en-us/azure/virtual-network/tutorial-filter-network-traffic?toc=%2Fazure%2Fvirtual-machines%2Ftoc.json)
    to allow traffic from each IP address from the previous step.

1. Update your [Azure DB firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql)
   to allow traffic from the SSH bastion host.







**Self-Managed:**

<p>Configure your network to allow Materialize to connect to your database. For
example, you can:</p>
<ul>
<li>
<p><strong>Allow Materialize IPs:</strong> Configure your database&rsquo;s security group to allow
connections from Materialize.</p>
</li>
<li>
<p><strong>Use an SSH tunnel:</strong> Use an SSH tunnel to connect Materialize to the
database.</p>
</li>
</ul>
<div class="note">
  <strong class="gutter">NOTE:</strong>
  <p>The steps to allow Materialize to connect to your database  depends on your
  deployment setup. Refer to your company’s network/security policies and
  procedures.</p>
</div>




**Allow Materialize IPs:**

1. Update your [Azure DB firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql)
   to allow traffic from Materialize IPs.



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

1. [Launch an Azure VM with a static public IP address](https://learn.microsoft.com/en-us/azure/virtual-network/ip-services/virtual-network-deploy-static-pip-arm-portal?toc=%2Fazure%2Fvirtual-machines%2Ftoc.json)
to serve as your SSH bastion host.

    - Make sure the VM is publicly accessible and in the same VPC as your
      database.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a static public IP address. You'll use this IP
      address when connecting Materialize to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

1. Update your [Azure DB firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql)
   to allow traffic from the SSH bastion host.









## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

> **Note:** If you are prototyping and already have a cluster to host your PostgreSQL
> source (e.g. `quickstart`), **you can skip this step**. For production
> scenarios, we recommend separating your workloads into multiple clusters for
> [resource isolation](/sql/create-cluster/#resource-isolation).
>


<p>In Materialize, a <a href="/concepts/clusters/" >cluster</a> is an isolated environment,
similar to a virtual warehouse in Snowflake. When you create a cluster, you
choose the size of its compute resource allocation based on the work you need
the cluster to do, whether ingesting data from a source, computing
always-up-to-date query results, serving results to external clients, or a
combination.</p>
<p>In this step, you&rsquo;ll create a dedicated cluster for ingesting source data from
your PostgreSQL database.</p>
<ol>
<li>
<p>In the <a href="/console/" >SQL Shell</a>, or your preferred SQL
client connected to Materialize, use the <a href="/sql/create-cluster/" ><code>CREATE CLUSTER</code></a>
command to create the new cluster:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">CLUSTER</span> <span class="n">ingest_postgres</span> <span class="p">(</span><span class="k">SIZE</span> <span class="o">=</span> <span class="s1">&#39;50cc&#39;</span><span class="p">);</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">SET</span> <span class="k">CLUSTER</span> <span class="o">=</span> <span class="n">ingest_postgres</span><span class="p">;</span>
</span></span></code></pre></div><p>A cluster of <a href="/sql/create-cluster/#size" >size</a> <code>50cc</code> should be enough to
accommodate multiple PostgreSQL sources, depending on the source
characteristics (e.g., sources with <a href="/sql/create-source/kafka/#upsert-envelope" ><code>ENVELOPE UPSERT</code></a>
or <a href="/sql/create-source/kafka/#debezium-envelope" ><code>ENVELOPE DEBEZIUM</code></a> will be more
memory-intensive) and the upstream traffic patterns. You can readjust the
size of the cluster at any time using the <a href="/sql/alter-cluster" ><code>ALTER CLUSTER</code></a> command:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">CLUSTER</span> <span class="o">&lt;</span><span class="n">cluster_name</span><span class="o">&gt;</span> <span class="k">SET</span> <span class="p">(</span> <span class="k">SIZE</span> <span class="o">=</span> <span class="o">&lt;</span><span class="n">new_size</span><span class="o">&gt;</span> <span class="p">);</span>
</span></span></code></pre></div></li>
</ol>


### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.



**Allow Materialize IPs:**

1. In the [Materialize Console's SQL Shell](/console/), or your preferred SQL
client connected to Materialize, use the [`CREATE
SECRET`](/sql/create-secret/) command to securely store the password for the
`materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):

   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
connection object with access and authentication details for Materialize to
use:


   ```mzsql
   CREATE CONNECTION pg_connection TO POSTGRES (
     HOST '<host>',
     PORT 5432,
     USER materialize,
     PASSWORD SECRET pgpass,
     SSL MODE 'require',
     DATABASE '<database>'
   );

   ```


   - Replace `<host>` with your PostgreSQL endpoint.

   - Replace `<database>` with the name of the database containing the tables
     you want to replicate to Materialize.




**Use an SSH tunnel:**

1. In the [Materialize Console's SQL Shell](/console/), or your preferred SQL
client connected to Materialize, use the [`CREATE
CONNECTION`](/sql/create-connection/#ssh-tunnel) command to create an SSH
tunnel connection:

   ```mzsql
   CREATE CONNECTION ssh_connection TO SSH TUNNEL (
       HOST '<SSH_BASTION_HOST>',
       PORT <SSH_BASTION_PORT>,
       USER '<SSH_BASTION_USER>'
   );

   ```

   - Replace `<SSH_BASTION_HOST>` and `<SSH_BASTION_PORT>` with the public IP
   address and port of the SSH bastion host you created
   [earlier](#b-optional-configure-network-security).

   - Replace `<SSH_BASTION_USER>` with the username for the key pair you
   created for your SSH bastion host.

1. Get Materialize's public keys for the SSH tunnel connection:


   ```mzsql
   SELECT * FROM mz_ssh_tunnel_connections;

   ```

1. Log in to your SSH bastion host and add Materialize's public keys to the
`authorized_keys` file, for example:


   ```mzsql
   echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
   echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys

   ```

1. Back in the SQL client connected to Materialize, validate the SSH tunnel
connection you created using the [`VALIDATE
CONNECTION`](/sql/validate-connection) command:


   ```mzsql
   VALIDATE CONNECTION ssh_connection;

   ```   If no validation error is returned, move to the next step.


1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
password for the `materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):


   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```

1.
Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create another connection object, this time with database access and authentication details for Materialize to use:


   ```mzsql
   CREATE CONNECTION pg_connection TO POSTGRES (
     HOST '<host>',
     PORT 5432,
     USER 'materialize',
     PASSWORD SECRET pgpass,
     DATABASE '<database>',
     SSH TUNNEL ssh_connection
     );

   ```

   - Replace `<host>` with your PostgreSQL endpoint.

   - Replace `<database>` with the name of the database containing the tables
   you want to replicate to Materialize.





### 3. Start ingesting data

{{< tabs >}}
{{< tab "Legacy Syntax" >}}
#### Legacy syntax

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source-legacy" %}}
{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="schema-changes" %}}
{{< /tab >}}

{{< tab "New Syntax" >}}
#### New syntax

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source" %}}
{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="schema-changes" %}}
{{< /tab >}}
{{< /tabs >}}


### 4. Monitor the ingestion status

<p>Before it starts consuming the replication stream, Materialize takes a snapshot
of the relevant tables in your publication. Until this snapshot is complete,
Materialize won&rsquo;t have the same view of your data as your PostgreSQL database.</p>
<p>In this step, you&rsquo;ll first verify that the source is running and then check the
status of the snapshotting process.</p>
<ol>
<li>
<p>Back in the SQL client connected to Materialize, use the
<a href="/sql/system-catalog/mz_internal/#mz_source_statuses" ><code>mz_source_statuses</code></a>
table to check the overall status of your source:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">WITH</span>
</span></span><span class="line"><span class="cl">  <span class="n">source_ids</span> <span class="k">AS</span>
</span></span><span class="line"><span class="cl">  <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">mz_sources</span> <span class="k">WHERE</span> <span class="k">name</span> <span class="o">=</span> <span class="s1">&#39;mz_source&#39;</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">  <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_source_statuses</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span>
</span></span><span class="line"><span class="cl">      <span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="k">SELECT</span> <span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">        <span class="k">FROM</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_object_dependencies</span>
</span></span><span class="line"><span class="cl">        <span class="k">WHERE</span>
</span></span><span class="line"><span class="cl">          <span class="n">object_id</span> <span class="k">IN</span> <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">UNION</span> <span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span>
</span></span><span class="line"><span class="cl">      <span class="p">)</span>
</span></span><span class="line"><span class="cl">      <span class="k">AS</span> <span class="k">sources</span>
</span></span><span class="line"><span class="cl">    <span class="k">ON</span> <span class="n">mz_source_statuses</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span><span class="p">;</span>
</span></span></code></pre></div><p>For each <code>subsource</code>, make sure the <code>status</code> is <code>running</code>. If you see
<code>stalled</code> or <code>failed</code>, there&rsquo;s likely a configuration issue for you to fix.
Check the <code>error</code> field for details and fix the issue before moving on.
Also, if the <code>status</code> of any subsource is <code>starting</code> for more than a few
minutes, <a href="/support/" >contact our team</a>.</p>
</li>
<li>
<p>Once the source is running, use the <a href="/sql/system-catalog/mz_internal/#mz_source_statistics" ><code>mz_source_statistics</code></a>
table to check the status of the initial snapshot:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">WITH</span>
</span></span><span class="line"><span class="cl">  <span class="n">source_ids</span> <span class="k">AS</span>
</span></span><span class="line"><span class="cl">  <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">mz_sources</span> <span class="k">WHERE</span> <span class="k">name</span> <span class="o">=</span> <span class="s1">&#39;mz_source&#39;</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span> <span class="k">AS</span> <span class="k">id</span><span class="p">,</span> <span class="n">mz_sources</span><span class="mf">.</span><span class="k">name</span><span class="p">,</span> <span class="n">snapshot_committed</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">  <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_source_statistics</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span>
</span></span><span class="line"><span class="cl">      <span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="k">SELECT</span> <span class="n">object_id</span><span class="p">,</span> <span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">        <span class="k">FROM</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_object_dependencies</span>
</span></span><span class="line"><span class="cl">        <span class="k">WHERE</span>
</span></span><span class="line"><span class="cl">          <span class="n">object_id</span> <span class="k">IN</span> <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">UNION</span> <span class="k">SELECT</span> <span class="k">id</span><span class="p">,</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span>
</span></span><span class="line"><span class="cl">      <span class="p">)</span>
</span></span><span class="line"><span class="cl">      <span class="k">AS</span> <span class="k">sources</span>
</span></span><span class="line"><span class="cl">    <span class="k">ON</span> <span class="n">mz_source_statistics</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_sources</span> <span class="k">ON</span> <span class="n">mz_sources</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span><span class="p">;</span>
</span></span></code></pre></div> <p></p>
<pre tabindex="0"><code class="language-nofmt" data-lang="nofmt">object_id | snapshot_committed
----------|------------------
 u144     | t
(1 row)
</code></pre><p>Once <code>snapshot_commited</code> is <code>t</code>, move on to the next step. Snapshotting can
take between a few minutes to several hours, depending on the size of your
dataset and the size of the cluster the source is running in.</p>
</li>
</ol>


### 5. Right-size the cluster

<p>After the snapshotting phase, Materialize starts ingesting change events from
the PostgreSQL replication stream. For this work, Materialize generally
performs well with an <code>100cc</code> replica, so you can resize the cluster
accordingly.</p>
<ol>
<li>
<p>Still in a SQL client connected to Materialize, use the <a href="/sql/alter-cluster/" ><code>ALTER CLUSTER</code></a>
command to downsize the cluster to <code>100cc</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">CLUSTER</span> <span class="n">ingest_postgres</span> <span class="k">SET</span> <span class="p">(</span><span class="k">SIZE</span> <span class="s1">&#39;100cc&#39;</span><span class="p">);</span>
</span></span></code></pre></div><p>Behind the scenes, this command adds a new <code>100cc</code> replica and removes the
<code>50cc</code> replica.</p>
</li>
<li>
<p>Use the <a href="/sql/show-cluster-replicas/" ><code>SHOW CLUSTER REPLICAS</code></a> command to
check the status of the new replica:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SHOW</span> <span class="k">CLUSTER</span> <span class="k">REPLICAS</span> <span class="k">WHERE</span> <span class="k">cluster</span> <span class="o">=</span> <span class="s1">&#39;ingest_postgres&#39;</span><span class="p">;</span>
</span></span></code></pre></div> <p></p>
<pre tabindex="0"><code class="language-nofmt" data-lang="nofmt">     cluster     | replica |  size  | ready
-----------------+---------+--------+-------
 ingest_postgres | r1      | 100cc  | t
(1 row)
</code></pre></li>
<li>
<p>Going forward, you can verify that your new cluster size is sufficient as
follows:</p>
<ol>
<li>
<p>In Materialize, get the replication slot name associated with your
PostgreSQL source from the <a href="/sql/system-catalog/mz_internal/#mz_postgres_sources" ><code>mz_internal.mz_postgres_sources</code></a>
table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span>
</span></span><span class="line"><span class="cl">    <span class="n">d</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">database_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">n</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">schema_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">s</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">source_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">pgs</span><span class="mf">.</span><span class="n">replication_slot</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">    <span class="n">mz_sources</span> <span class="k">AS</span> <span class="n">s</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_postgres_sources</span> <span class="k">AS</span> <span class="n">pgs</span> <span class="k">ON</span> <span class="n">s</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">pgs</span><span class="mf">.</span><span class="k">id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_schemas</span> <span class="k">AS</span> <span class="n">n</span> <span class="k">ON</span> <span class="n">n</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">s</span><span class="mf">.</span><span class="n">schema_id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_databases</span> <span class="k">AS</span> <span class="n">d</span> <span class="k">ON</span> <span class="n">d</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">n</span><span class="mf">.</span><span class="n">database_id</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>In PostgreSQL, check the replication slot lag, using the replication slot
name from the previous step:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">SELECT</span>
</span></span><span class="line"><span class="cl">    <span class="n">pg_size_pretty</span><span class="p">(</span><span class="n">pg_current_wal_lsn</span><span class="p">()</span> <span class="o">-</span> <span class="n">confirmed_flush_lsn</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">    <span class="k">AS</span> <span class="n">replication_lag_bytes</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span> <span class="n">pg_replication_slots</span>
</span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">slot_name</span> <span class="o">=</span> <span class="s1">&#39;&lt;slot_name&gt;&#39;</span><span class="p">;</span>
</span></span></code></pre></div><p>The result of this query is the amount of data your PostgreSQL cluster
must retain in its replication log because of this replication slot.
Typically, this means Materialize has not yet communicated back to
PostgreSQL that it has committed this data. A high value can indicate
that the source has fallen behind and that you might need to scale up
your ingestion cluster.</p>
</li>
</ol>
</li>
</ol>


## D. Explore your data

<p>With Materialize ingesting your PostgreSQL data into durable storage, you can
start exploring the data, computing real-time results that stay up-to-date as
new data arrives, and serving results efficiently.</p>
<ul>
<li>
<p>Explore your data with <a href="/sql/show-sources" ><code>SHOW SOURCES</code></a> and <a href="/sql/select/" ><code>SELECT</code></a>.</p>
</li>
<li>
<p>Compute real-time results in memory with <a href="/sql/create-view/" ><code>CREATE VIEW</code></a>
and <a href="/sql/create-index/" ><code>CREATE INDEX</code></a> or in durable
storage with <a href="/sql/create-materialized-view/" ><code>CREATE MATERIALIZED VIEW</code></a>.</p>
</li>
<li>
<p>Serve results to a PostgreSQL-compatible SQL client or driver with <a href="/sql/select/" ><code>SELECT</code></a>
or <a href="/sql/subscribe/" ><code>SUBSCRIBE</code></a> or to an external message broker with
<a href="/sql/create-sink/" ><code>CREATE SINK</code></a>.</p>
</li>
<li>
<p>Check out the <a href="/integrations/" >tools and integrations</a> supported by
Materialize.</p>
</li>
</ul>


## Considerations

<h3 id="schema-changes">Schema changes</h3>
<p>Materialize supports schema changes in the upstream database as follows:</p>
<h4 id="compatible-schema-changes-legacy-syntax">Compatible schema changes (Legacy syntax)</h4>
<blockquote>
<p><strong>Note:</strong> This section refer to the legacy <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a> that creates subsources as part of the
<code>CREATE SOURCE</code> operation.  To be able to handle the upstream column
additions and drops, see <a href="/sql/create-source/postgres-v2/" ><code>CREATE SOURCE (New Syntax)</code></a> and <a href="/sql/create-table" ><code>CREATE TABLE FROM SOURCE</code></a>.</p>
</blockquote>
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
<h4 id="incompatible-schema-changes">Incompatible schema changes</h4>
<p>All other schema changes to upstream tables will set the corresponding
Materialize tables into an error state, preventing reads from these tables.</p>
<p>To handle <a href="#incompatible-schema-changes" >incompatible schema changes</a>, drop
the affected table <a href="/sql/drop-table/" ><code>DROP TABLE</code></a> , and then, <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> to recreate the table with the
updated schema.</p>
<h3 id="publication-membership">Publication membership</h3>
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
<p>To mitigate this issue, if you need to drop and re-add a table to a
publication, ensure that you remove the table/subsource from the source
<em>before</em> re-adding it using the <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> command.</p>
<h3 id="supported-types">Supported types</h3>
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
<h3 id="truncation">Truncation</h3>
<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource(s)/table(s) in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>
<h3 id="inherited-tables">Inherited tables</h3>
<p>When using <a href="https://www.postgresql.org/docs/current/tutorial-inheritance.html" >PostgreSQL table inheritance</a>,
PostgreSQL serves data from <code>SELECT</code>s as if the inheriting tables&rsquo; data is
also present in the inherited table. However, both PostgreSQL&rsquo;s logical
replication and <code>COPY</code> only present data written to the tables themselves,
i.e. the inheriting data is <em>not</em> treated as part of the inherited table.</p>
<p>PostgreSQL sources use logical replication and <code>COPY</code> to ingest table data,
so inheriting tables&rsquo; data will only be ingested as part of the inheriting
table, i.e. in Materialize, the data will not be returned when serving
<code>SELECT</code>s from the inherited table.</p>
<ul>
<li>
<p>If using legacy syntax <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a>:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>ADD SUBSOURCE</code> and
create a new view (materialized or non-) that unions the new table.</p>
</li>
<li>
<p>If using new <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> syntax:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>CREATE TABLE .. FROM SOURCE</code> and create a new view (materialized or non-) that unions the new
table.</p>
</li>
</ul>
<h3 id="replication-slots">Replication slots</h3>
<p>Each source ingests the raw replication stream data for all tables in the
specified publication using <strong>a single</strong> replication slot. To manage
replication slots:</p>
<ul>
<li>
<p>For PostgreSQL 13+, set a reasonable value
for <a href="https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE" ><code>max_slot_wal_keep_size</code></a>
to limit the amount of storage used by replication slots.</p>
</li>
<li>
<p>If you stop using Materialize, or if either the Materialize instance or
the PostgreSQL instance crash, delete any replication slots. You can query
the <code>mz_internal.mz_postgres_sources</code> table to look up the name of the
replication slot created for each source.</p>
</li>
<li>
<p>If you delete all objects that depend on a source without also dropping
the source, the upstream replication slot remains and will continue to
accumulate data so that the source can resume in the future. To avoid
unbounded disk space usage, make sure to use <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> or manually delete the replication slot.</p>
</li>
</ul>
<h3 id="modifying-an-existing-source">Modifying an existing source</h3>
<p>When you add a new subsource to an existing source (<a href="/sql/alter-source/" ><code>ALTER SOURCE ... ADD SUBSOURCE ...</code></a>), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.
</p>



---

## Ingest data from Google Cloud SQL


This page shows you how to stream data from [Google Cloud SQL for PostgreSQL](https://cloud.google.com/sql/postgresql)
to Materialize using the[PostgreSQL source](/sql/create-source/postgres/).

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).
>
>



## Before you begin

<ul>
<li>
<p>Make sure you are running PostgreSQL 11 or higher.</p>
</li>
<li>
<p>Make sure you have access to your PostgreSQL instance via <a href="https://www.postgresql.org/docs/current/app-psql.html" ><code>psql</code></a>,
or your preferred SQL client.</p>
</li>
</ul>


## A. Configure Google Cloud SQL

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

To enable logical replication in Cloud SQL, see the [Cloud SQL
documentation](https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication#configuring-your-postgresql-instance).

### 2. Create a publication and a replication user

<p>Once logical replication is enabled, the next step is to create a publication
with the tables that you want to replicate to Materialize. You&rsquo;ll also need a
user for Materialize with sufficient privileges to manage replication.</p>
<ol>
<li>
<p>For each table that you want to replicate to Materialize, set the
<a href="https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY" >replica identity</a>
to <code>FULL</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span> <span class="k">REPLICA</span> <span class="k">IDENTITY</span> <span class="k">FULL</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span> <span class="k">REPLICA</span> <span class="k">IDENTITY</span> <span class="k">FULL</span><span class="p">;</span>
</span></span></code></pre></div><p><code>REPLICA IDENTITY FULL</code> ensures that the replication stream includes the
previous data of changed rows, in the case of <code>UPDATE</code> and <code>DELETE</code>
operations. This setting enables Materialize to ingest PostgreSQL data with
minimal in-memory state. However, you should expect increased disk usage in
your PostgreSQL database.</p>
</li>
<li>
<p>Create a <a href="https://www.postgresql.org/docs/current/logical-replication-publication.html" >publication</a>
with the tables you want to replicate:</p>
<p><em>For specific tables:</em></p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="n">PUBLICATION</span> <span class="n">mz_source</span> <span class="k">FOR</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span><span class="p">,</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span><span class="p">;</span>
</span></span></code></pre></div><p><em>For all tables in the database:</em></p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="n">PUBLICATION</span> <span class="n">mz_source</span> <span class="k">FOR</span> <span class="k">ALL</span> <span class="k">TABLES</span><span class="p">;</span>
</span></span></code></pre></div><p>The <code>mz_source</code> publication will contain the set of change events generated
from the specified tables, and will later be used to ingest the replication
stream.</p>
<p>Be sure to include only the tables you need. If the publication includes
additional tables, Materialize will waste resources on ingesting and then
immediately discarding the data.</p>
</li>
<li>
<p>Create a user for Materialize, if you don&rsquo;t already have one:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">USER</span> <span class="n">materialize</span> <span class="k">PASSWORD</span> <span class="s1">&#39;&lt;password&gt;&#39;</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>Grant the user permission to manage replication:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">ROLE</span> <span class="n">materialize</span> <span class="k">WITH</span> <span class="n">REPLICATION</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>Grant the user the required permissions on the tables you want to replicate:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="n">CONNECT</span> <span class="k">ON</span> <span class="k">DATABASE</span> <span class="o">&lt;</span><span class="n">dbname</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="n">USAGE</span> <span class="k">ON</span> <span class="k">SCHEMA</span> <span class="o">&lt;</span><span class="k">schema</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><p>Once connected to your database, Materialize will take an initial snapshot
of the tables in your publication. <code>SELECT</code> privileges are required for
this initial snapshot.</p>
<p>If you expect to add tables to your publication, you can grant <code>SELECT</code> on
all tables in the schema instead of naming the specific tables:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="k">ALL</span> <span class="k">TABLES</span> <span class="k">IN</span> <span class="k">SCHEMA</span> <span class="o">&lt;</span><span class="k">schema</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div></li>
</ol>


## B. (Optional) Configure network security

> **Note:** If you are prototyping and your Google Cloud SQL instance is publicly
> accessible, **you can skip this step**. For production scenarios, we recommend
> configuring one of the network security options below.
>




**Cloud:**

There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's firewall to allow connections from a set of
    static Materialize IP addresses.

- **Use an SSH tunnel:** If your database is running in a private network, you
    can use an SSH tunnel to connect Materialize to the database.

Select the option that works best for you.



**Allow Materialize IPs:**

1. In the [Materialize console's SQL Shell](/console/),
   or your preferred SQL client connected to Materialize, find the static egress
   IP addresses for the Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

1. Update your Google Cloud SQL firewall rules to allow traffic from each IP
   address from the previous step.



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

1. [Launch a GCE instance](https://cloud.google.com/compute/docs/instances/create-start-instance) to serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your
      database.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a [static public IP address](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address).
      You'll use this IP address when connecting Materialize to your bastion
      host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

    1. In the [Materialize console's SQL
       Shell](/console/), or your preferred SQL client
       connected to Materialize, get the static egress IP addresses for the
       Materialize region you are running in:

       ```mzsql
       SELECT * FROM mz_egress_ips;
       ```

    1. Update your SSH bastion host's firewall rules to allow traffic from each
    IP address from the previous step.

1. Update your Google Cloud SQL firewall rules to allow traffic from the SSH
bastion host.







**Self-Managed:**

<p>Configure your network to allow Materialize to connect to your database. For
example, you can:</p>
<ul>
<li>
<p><strong>Allow Materialize IPs:</strong> Configure your database&rsquo;s security group to allow
connections from Materialize.</p>
</li>
<li>
<p><strong>Use an SSH tunnel:</strong> Use an SSH tunnel to connect Materialize to the
database.</p>
</li>
</ul>
<div class="note">
  <strong class="gutter">NOTE:</strong>
  <p>The steps to allow Materialize to connect to your database  depends on your
  deployment setup. Refer to your company’s network/security policies and
  procedures.</p>
</div>




**Allow Materialize IPs:**

1. Update your Google Cloud SQL firewall rules to allow traffic from Materialize
   IPs.



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

1. [Launch a GCE instance](https://cloud.google.com/compute/docs/instances/create-start-instance) to serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your
      database.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a [static public IP address](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address).
      You'll use this IP address when connecting Materialize to your bastion
      host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

1. Update your Google Cloud SQL firewall rules to allow traffic from the SSH
bastion host.









## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

> **Note:** If you are prototyping and already have a cluster to host your PostgreSQL
> source (e.g. `quickstart`), **you can skip this step**. For production
> scenarios, we recommend separating your workloads into multiple clusters for
> [resource isolation](/sql/create-cluster/#resource-isolation).
>


<p>In Materialize, a <a href="/concepts/clusters/" >cluster</a> is an isolated environment,
similar to a virtual warehouse in Snowflake. When you create a cluster, you
choose the size of its compute resource allocation based on the work you need
the cluster to do, whether ingesting data from a source, computing
always-up-to-date query results, serving results to external clients, or a
combination.</p>
<p>In this step, you&rsquo;ll create a dedicated cluster for ingesting source data from
your PostgreSQL database.</p>
<ol>
<li>
<p>In the <a href="/console/" >SQL Shell</a>, or your preferred SQL
client connected to Materialize, use the <a href="/sql/create-cluster/" ><code>CREATE CLUSTER</code></a>
command to create the new cluster:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">CLUSTER</span> <span class="n">ingest_postgres</span> <span class="p">(</span><span class="k">SIZE</span> <span class="o">=</span> <span class="s1">&#39;50cc&#39;</span><span class="p">);</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">SET</span> <span class="k">CLUSTER</span> <span class="o">=</span> <span class="n">ingest_postgres</span><span class="p">;</span>
</span></span></code></pre></div><p>A cluster of <a href="/sql/create-cluster/#size" >size</a> <code>50cc</code> should be enough to
accommodate multiple PostgreSQL sources, depending on the source
characteristics (e.g., sources with <a href="/sql/create-source/kafka/#upsert-envelope" ><code>ENVELOPE UPSERT</code></a>
or <a href="/sql/create-source/kafka/#debezium-envelope" ><code>ENVELOPE DEBEZIUM</code></a> will be more
memory-intensive) and the upstream traffic patterns. You can readjust the
size of the cluster at any time using the <a href="/sql/alter-cluster" ><code>ALTER CLUSTER</code></a> command:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">CLUSTER</span> <span class="o">&lt;</span><span class="n">cluster_name</span><span class="o">&gt;</span> <span class="k">SET</span> <span class="p">(</span> <span class="k">SIZE</span> <span class="o">=</span> <span class="o">&lt;</span><span class="n">new_size</span><span class="o">&gt;</span> <span class="p">);</span>
</span></span></code></pre></div></li>
</ol>


### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.



**Allow Materialize IPs:**
1. In the [Materialize Console's SQL Shell](/console/), or your preferred SQL
client connected to Materialize, use the [`CREATE
SECRET`](/sql/create-secret/) command to securely store the password for the
`materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):

   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
connection object with access and authentication details for Materialize to
use:


   ```mzsql
   CREATE CONNECTION pg_connection TO POSTGRES (
     HOST '<host>',
     PORT 5432,
     USER materialize,
     PASSWORD SECRET pgpass,
     SSL MODE 'require',
     DATABASE '<database>'
   );

   ```


   - Replace `<host>` with your PostgreSQL endpoint.

   - Replace `<database>` with the name of the database containing the tables
     you want to replicate to Materialize.



**Use an SSH tunnel:**

1. In the [Materialize Console's SQL Shell](/console/), or your preferred SQL
client connected to Materialize, use the [`CREATE
CONNECTION`](/sql/create-connection/#ssh-tunnel) command to create an SSH
tunnel connection:

   ```mzsql
   CREATE CONNECTION ssh_connection TO SSH TUNNEL (
       HOST '<SSH_BASTION_HOST>',
       PORT <SSH_BASTION_PORT>,
       USER '<SSH_BASTION_USER>'
   );

   ```

   - Replace `<SSH_BASTION_HOST>` and `<SSH_BASTION_PORT>` with the public IP
   address and port of the SSH bastion host you created
   [earlier](#b-optional-configure-network-security).

   - Replace `<SSH_BASTION_USER>` with the username for the key pair you
   created for your SSH bastion host.

1. Get Materialize's public keys for the SSH tunnel connection:


   ```mzsql
   SELECT * FROM mz_ssh_tunnel_connections;

   ```

1. Log in to your SSH bastion host and add Materialize's public keys to the
`authorized_keys` file, for example:


   ```mzsql
   echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
   echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys

   ```

1. Back in the SQL client connected to Materialize, validate the SSH tunnel
connection you created using the [`VALIDATE
CONNECTION`](/sql/validate-connection) command:


   ```mzsql
   VALIDATE CONNECTION ssh_connection;

   ```   If no validation error is returned, move to the next step.


1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
password for the `materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):


   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```

1.
Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create another connection object, this time with database access and authentication details for Materialize to use:


   ```mzsql
   CREATE CONNECTION pg_connection TO POSTGRES (
     HOST '<host>',
     PORT 5432,
     USER 'materialize',
     PASSWORD SECRET pgpass,
     DATABASE '<database>',
     SSH TUNNEL ssh_connection
     );

   ```

   - Replace `<host>` with your PostgreSQL endpoint.

   - Replace `<database>` with the name of the database containing the tables
   you want to replicate to Materialize.






### 3. Start ingesting data

{{< tabs >}}
{{< tab "Legacy Syntax" >}}
#### Legacy syntax

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source-legacy" %}}
{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="schema-changes" %}}
{{< /tab >}}

{{< tab "New Syntax" >}}
#### New syntax

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source" %}}
{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="schema-changes" %}}
{{< /tab >}}
{{< /tabs >}}


### 4. Monitor the ingestion status

<p>Before it starts consuming the replication stream, Materialize takes a snapshot
of the relevant tables in your publication. Until this snapshot is complete,
Materialize won&rsquo;t have the same view of your data as your PostgreSQL database.</p>
<p>In this step, you&rsquo;ll first verify that the source is running and then check the
status of the snapshotting process.</p>
<ol>
<li>
<p>Back in the SQL client connected to Materialize, use the
<a href="/sql/system-catalog/mz_internal/#mz_source_statuses" ><code>mz_source_statuses</code></a>
table to check the overall status of your source:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">WITH</span>
</span></span><span class="line"><span class="cl">  <span class="n">source_ids</span> <span class="k">AS</span>
</span></span><span class="line"><span class="cl">  <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">mz_sources</span> <span class="k">WHERE</span> <span class="k">name</span> <span class="o">=</span> <span class="s1">&#39;mz_source&#39;</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">  <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_source_statuses</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span>
</span></span><span class="line"><span class="cl">      <span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="k">SELECT</span> <span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">        <span class="k">FROM</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_object_dependencies</span>
</span></span><span class="line"><span class="cl">        <span class="k">WHERE</span>
</span></span><span class="line"><span class="cl">          <span class="n">object_id</span> <span class="k">IN</span> <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">UNION</span> <span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span>
</span></span><span class="line"><span class="cl">      <span class="p">)</span>
</span></span><span class="line"><span class="cl">      <span class="k">AS</span> <span class="k">sources</span>
</span></span><span class="line"><span class="cl">    <span class="k">ON</span> <span class="n">mz_source_statuses</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span><span class="p">;</span>
</span></span></code></pre></div><p>For each <code>subsource</code>, make sure the <code>status</code> is <code>running</code>. If you see
<code>stalled</code> or <code>failed</code>, there&rsquo;s likely a configuration issue for you to fix.
Check the <code>error</code> field for details and fix the issue before moving on.
Also, if the <code>status</code> of any subsource is <code>starting</code> for more than a few
minutes, <a href="/support/" >contact our team</a>.</p>
</li>
<li>
<p>Once the source is running, use the <a href="/sql/system-catalog/mz_internal/#mz_source_statistics" ><code>mz_source_statistics</code></a>
table to check the status of the initial snapshot:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">WITH</span>
</span></span><span class="line"><span class="cl">  <span class="n">source_ids</span> <span class="k">AS</span>
</span></span><span class="line"><span class="cl">  <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">mz_sources</span> <span class="k">WHERE</span> <span class="k">name</span> <span class="o">=</span> <span class="s1">&#39;mz_source&#39;</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span> <span class="k">AS</span> <span class="k">id</span><span class="p">,</span> <span class="n">mz_sources</span><span class="mf">.</span><span class="k">name</span><span class="p">,</span> <span class="n">snapshot_committed</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">  <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_source_statistics</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span>
</span></span><span class="line"><span class="cl">      <span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="k">SELECT</span> <span class="n">object_id</span><span class="p">,</span> <span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">        <span class="k">FROM</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_object_dependencies</span>
</span></span><span class="line"><span class="cl">        <span class="k">WHERE</span>
</span></span><span class="line"><span class="cl">          <span class="n">object_id</span> <span class="k">IN</span> <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">UNION</span> <span class="k">SELECT</span> <span class="k">id</span><span class="p">,</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span>
</span></span><span class="line"><span class="cl">      <span class="p">)</span>
</span></span><span class="line"><span class="cl">      <span class="k">AS</span> <span class="k">sources</span>
</span></span><span class="line"><span class="cl">    <span class="k">ON</span> <span class="n">mz_source_statistics</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_sources</span> <span class="k">ON</span> <span class="n">mz_sources</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span><span class="p">;</span>
</span></span></code></pre></div> <p></p>
<pre tabindex="0"><code class="language-nofmt" data-lang="nofmt">object_id | snapshot_committed
----------|------------------
 u144     | t
(1 row)
</code></pre><p>Once <code>snapshot_commited</code> is <code>t</code>, move on to the next step. Snapshotting can
take between a few minutes to several hours, depending on the size of your
dataset and the size of the cluster the source is running in.</p>
</li>
</ol>


### 5. Right-size the cluster

<p>After the snapshotting phase, Materialize starts ingesting change events from
the PostgreSQL replication stream. For this work, Materialize generally
performs well with an <code>100cc</code> replica, so you can resize the cluster
accordingly.</p>
<ol>
<li>
<p>Still in a SQL client connected to Materialize, use the <a href="/sql/alter-cluster/" ><code>ALTER CLUSTER</code></a>
command to downsize the cluster to <code>100cc</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">CLUSTER</span> <span class="n">ingest_postgres</span> <span class="k">SET</span> <span class="p">(</span><span class="k">SIZE</span> <span class="s1">&#39;100cc&#39;</span><span class="p">);</span>
</span></span></code></pre></div><p>Behind the scenes, this command adds a new <code>100cc</code> replica and removes the
<code>50cc</code> replica.</p>
</li>
<li>
<p>Use the <a href="/sql/show-cluster-replicas/" ><code>SHOW CLUSTER REPLICAS</code></a> command to
check the status of the new replica:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SHOW</span> <span class="k">CLUSTER</span> <span class="k">REPLICAS</span> <span class="k">WHERE</span> <span class="k">cluster</span> <span class="o">=</span> <span class="s1">&#39;ingest_postgres&#39;</span><span class="p">;</span>
</span></span></code></pre></div> <p></p>
<pre tabindex="0"><code class="language-nofmt" data-lang="nofmt">     cluster     | replica |  size  | ready
-----------------+---------+--------+-------
 ingest_postgres | r1      | 100cc  | t
(1 row)
</code></pre></li>
<li>
<p>Going forward, you can verify that your new cluster size is sufficient as
follows:</p>
<ol>
<li>
<p>In Materialize, get the replication slot name associated with your
PostgreSQL source from the <a href="/sql/system-catalog/mz_internal/#mz_postgres_sources" ><code>mz_internal.mz_postgres_sources</code></a>
table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span>
</span></span><span class="line"><span class="cl">    <span class="n">d</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">database_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">n</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">schema_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">s</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">source_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">pgs</span><span class="mf">.</span><span class="n">replication_slot</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">    <span class="n">mz_sources</span> <span class="k">AS</span> <span class="n">s</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_postgres_sources</span> <span class="k">AS</span> <span class="n">pgs</span> <span class="k">ON</span> <span class="n">s</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">pgs</span><span class="mf">.</span><span class="k">id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_schemas</span> <span class="k">AS</span> <span class="n">n</span> <span class="k">ON</span> <span class="n">n</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">s</span><span class="mf">.</span><span class="n">schema_id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_databases</span> <span class="k">AS</span> <span class="n">d</span> <span class="k">ON</span> <span class="n">d</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">n</span><span class="mf">.</span><span class="n">database_id</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>In PostgreSQL, check the replication slot lag, using the replication slot
name from the previous step:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">SELECT</span>
</span></span><span class="line"><span class="cl">    <span class="n">pg_size_pretty</span><span class="p">(</span><span class="n">pg_current_wal_lsn</span><span class="p">()</span> <span class="o">-</span> <span class="n">confirmed_flush_lsn</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">    <span class="k">AS</span> <span class="n">replication_lag_bytes</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span> <span class="n">pg_replication_slots</span>
</span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">slot_name</span> <span class="o">=</span> <span class="s1">&#39;&lt;slot_name&gt;&#39;</span><span class="p">;</span>
</span></span></code></pre></div><p>The result of this query is the amount of data your PostgreSQL cluster
must retain in its replication log because of this replication slot.
Typically, this means Materialize has not yet communicated back to
PostgreSQL that it has committed this data. A high value can indicate
that the source has fallen behind and that you might need to scale up
your ingestion cluster.</p>
</li>
</ol>
</li>
</ol>


## D. Explore your data

<p>With Materialize ingesting your PostgreSQL data into durable storage, you can
start exploring the data, computing real-time results that stay up-to-date as
new data arrives, and serving results efficiently.</p>
<ul>
<li>
<p>Explore your data with <a href="/sql/show-sources" ><code>SHOW SOURCES</code></a> and <a href="/sql/select/" ><code>SELECT</code></a>.</p>
</li>
<li>
<p>Compute real-time results in memory with <a href="/sql/create-view/" ><code>CREATE VIEW</code></a>
and <a href="/sql/create-index/" ><code>CREATE INDEX</code></a> or in durable
storage with <a href="/sql/create-materialized-view/" ><code>CREATE MATERIALIZED VIEW</code></a>.</p>
</li>
<li>
<p>Serve results to a PostgreSQL-compatible SQL client or driver with <a href="/sql/select/" ><code>SELECT</code></a>
or <a href="/sql/subscribe/" ><code>SUBSCRIBE</code></a> or to an external message broker with
<a href="/sql/create-sink/" ><code>CREATE SINK</code></a>.</p>
</li>
<li>
<p>Check out the <a href="/integrations/" >tools and integrations</a> supported by
Materialize.</p>
</li>
</ul>


## Considerations

<h3 id="schema-changes">Schema changes</h3>
<p>Materialize supports schema changes in the upstream database as follows:</p>
<h4 id="compatible-schema-changes-legacy-syntax">Compatible schema changes (Legacy syntax)</h4>
<blockquote>
<p><strong>Note:</strong> This section refer to the legacy <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a> that creates subsources as part of the
<code>CREATE SOURCE</code> operation.  To be able to handle the upstream column
additions and drops, see <a href="/sql/create-source/postgres-v2/" ><code>CREATE SOURCE (New Syntax)</code></a> and <a href="/sql/create-table" ><code>CREATE TABLE FROM SOURCE</code></a>.</p>
</blockquote>
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
<h4 id="incompatible-schema-changes">Incompatible schema changes</h4>
<p>All other schema changes to upstream tables will set the corresponding
Materialize tables into an error state, preventing reads from these tables.</p>
<p>To handle <a href="#incompatible-schema-changes" >incompatible schema changes</a>, drop
the affected table <a href="/sql/drop-table/" ><code>DROP TABLE</code></a> , and then, <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> to recreate the table with the
updated schema.</p>
<h3 id="publication-membership">Publication membership</h3>
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
<p>To mitigate this issue, if you need to drop and re-add a table to a
publication, ensure that you remove the table/subsource from the source
<em>before</em> re-adding it using the <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> command.</p>
<h3 id="supported-types">Supported types</h3>
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
<h3 id="truncation">Truncation</h3>
<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource(s)/table(s) in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>
<h3 id="inherited-tables">Inherited tables</h3>
<p>When using <a href="https://www.postgresql.org/docs/current/tutorial-inheritance.html" >PostgreSQL table inheritance</a>,
PostgreSQL serves data from <code>SELECT</code>s as if the inheriting tables&rsquo; data is
also present in the inherited table. However, both PostgreSQL&rsquo;s logical
replication and <code>COPY</code> only present data written to the tables themselves,
i.e. the inheriting data is <em>not</em> treated as part of the inherited table.</p>
<p>PostgreSQL sources use logical replication and <code>COPY</code> to ingest table data,
so inheriting tables&rsquo; data will only be ingested as part of the inheriting
table, i.e. in Materialize, the data will not be returned when serving
<code>SELECT</code>s from the inherited table.</p>
<ul>
<li>
<p>If using legacy syntax <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a>:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>ADD SUBSOURCE</code> and
create a new view (materialized or non-) that unions the new table.</p>
</li>
<li>
<p>If using new <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> syntax:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>CREATE TABLE .. FROM SOURCE</code> and create a new view (materialized or non-) that unions the new
table.</p>
</li>
</ul>
<h3 id="replication-slots">Replication slots</h3>
<p>Each source ingests the raw replication stream data for all tables in the
specified publication using <strong>a single</strong> replication slot. To manage
replication slots:</p>
<ul>
<li>
<p>For PostgreSQL 13+, set a reasonable value
for <a href="https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE" ><code>max_slot_wal_keep_size</code></a>
to limit the amount of storage used by replication slots.</p>
</li>
<li>
<p>If you stop using Materialize, or if either the Materialize instance or
the PostgreSQL instance crash, delete any replication slots. You can query
the <code>mz_internal.mz_postgres_sources</code> table to look up the name of the
replication slot created for each source.</p>
</li>
<li>
<p>If you delete all objects that depend on a source without also dropping
the source, the upstream replication slot remains and will continue to
accumulate data so that the source can resume in the future. To avoid
unbounded disk space usage, make sure to use <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> or manually delete the replication slot.</p>
</li>
</ul>
<h3 id="modifying-an-existing-source">Modifying an existing source</h3>
<p>When you add a new subsource to an existing source (<a href="/sql/alter-source/" ><code>ALTER SOURCE ... ADD SUBSOURCE ...</code></a>), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.
</p>



---

## Ingest data from Neon


> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).
>
>


[Neon](https://neon.tech) is a fully managed serverless PostgreSQL provider. It
separates compute and storage to offer features like **autoscaling**,
**branching** and **bottomless storage**.

This page shows you how to stream data from a Neon database to Materialize using
the [PostgreSQL source](/sql/create-source/postgres/).

## Before you begin

- Make sure you have [a Neon account](https://neon.tech).

- Make sure you have access to your Neon instance via [`psql`](https://www.postgresql.org/docs/current/app-psql.html)
  or the SQL editor in the Neon Console.

## A. Configure Neon

The steps in this section are specific to Neon. You can run them by connecting
to your Neon database using a `psql` client or the SQL editor in the Neon
Console.

### 1. Enable logical replication

> **Warning:** Enabling logical replication applies **globally** to all databases in your Neon
> project, and **cannot be reverted**. It also **restarts all computes**, which
> means that any active connections are dropped and have to reconnect.
>


Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

As a first step, you need to make sure logical replication is enabled in Neon.

1. Select your project in the Neon Console.

2. On the Neon **Dashboard**, select **Settings**.

3. Select **Logical Replication**.

4. Click **Enable** to enable logical replication.

You can verify that logical replication is enabled by running:

```sql
SHOW wal_level;
```

The result should be:

```
 wal_level
-----------
 logical
```

### 2. Create a publication and a replication user

Once logical replication is enabled, the next step is to create a publication
with the tables that you want to replicate to Materialize. You'll also need a
user for Materialize with sufficient privileges to manage replication.

1. For each table that you want to replicate to Materialize, set the
   [replica identity](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY)
   to `FULL`:

   ```postgres
   ALTER TABLE <table1> REPLICA IDENTITY FULL;
   ```

   ```postgres
   ALTER TABLE <table2> REPLICA IDENTITY FULL;
   ```

   `REPLICA IDENTITY FULL` ensures that the replication stream includes the
    previous data of changed rows, in the case of `UPDATE` and `DELETE`
    operations. This setting enables Materialize to ingest Neon data with
    minimal in-memory state. However, you should expect increased disk usage in
    your Neon database.

2. Create a [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html)
   with the tables you want to replicate:

   _For specific tables:_

    ```postgres
    CREATE PUBLICATION mz_source FOR TABLE <table1>, <table2>;
    ```

    _For all tables in the database:_

    ```postgres
    CREATE PUBLICATION mz_source FOR ALL TABLES;
    ```

    The `mz_source` publication will contain the set of change events generated
    from the specified tables, and will later be used to ingest the replication
    stream.

    Be sure to include only the tables you need. If the publication includes
    additional tables, Materialize will waste resources on ingesting and then
    immediately discarding the data.

3. Create a dedicated user for Materialize, if you don't already have one. The default user created with your Neon project and users created using the
Neon CLI, Console or API are granted membership in the [`neon_superuser`](https://neon.tech/docs/manage/roles#the-neonsuperuser-role)
role, which has the required `REPLICATION` privilege.

   While you can use the default user for replication, we recommend creating a
   dedicated user for security reasons.


**Neon CLI:**

Use the [`roles create` CLI command](https://neon.tech/docs/reference/cli-roles)
to create a new role.

```bash
neon roles create --name materialize
```



**Neon Console:**

1. Navigate to the [Neon Console](https://console.neon.tech).
2. Select a project.
3. Select **Branches**.
4. Select the branch where you want to create the role.
5. Select the **Roles & Databases** tab.
6. Click **Add Role**.
7. In the role creation dialog, specify the role name as "materialize".
8. Click **Create**. The role is created, and you are provided with the
password for the role.



**API:**

Use the [`roles` endpoint](https://api-docs.neon.tech/reference/createprojectbranchrole)
to create a new role.

```bash
curl 'https://console.neon.tech/api/v2/projects/<project_id>/branches/<branch_id>/roles' \
-H 'Accept: application/json' \
-H "Authorization: Bearer $NEON_API_KEY" \
-H 'Content-Type: application/json' \
-d '{
"role": {
    "name": "materialize"
}
}' | jq
```





4. Grant the user the required permissions on the schema(s) you want to
   replicate:

   ```postgres
   GRANT USAGE ON SCHEMA public TO materialize;

   GRANT SELECT ON ALL TABLES IN SCHEMA public TO materialize;

   ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO materialize;
   ```

   Granting `SELECT ON ALL TABLES IN SCHEMA` instead of on specific tables
   avoids having to add privileges later if you add tables to your
   publication.

## B. (Optional) Configure network security

> **Note:** If you are prototyping and your Neon instance is publicly accessible, **you can
> skip this step**. For production scenarios, we recommend using [**IP Allow**](https://neon.tech/docs/introduction/ip-allow)
> to limit the IP addresses that can connect to your Neon instance.
>




**Cloud:**

If you use Neon's [**IP Allow**](https://neon.tech/docs/introduction/ip-allow)
feature to limit the IP addresses that can connect to your Neon instance, you
will need to allow inbound traffic from Materialize IP addresses.

1. In the [Materialize console's SQL Shell](/console/),
   or your preferred SQL client connected to
   Materialize, run the following query to find the static egress IP addresses,
   for the Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

2. In your Neon project, add the IPs to your **IP Allow** list:

   1. Select your project in the Neon Console.
   2. On the Neon **Dashboard**, select **Settings**.
   3. Select **IP Allow**.
   4. Add each Materialize IP address to the list.



**Self-Managed:**

> **Note:** If you are prototyping and your Neon instance is publicly accessible, **you can
> skip this step**. For production scenarios, we recommend using [**IP Allow**](https://neon.tech/docs/introduction/ip-allow)
> to limit the IP addresses that can connect to your Neon instance.
>


If you use Neon's [**IP Allow**](https://neon.tech/docs/introduction/ip-allow)
feature to limit the IP addresses that can connect to your Neon instance, you
will need to allow inbound traffic from Materialize IP addresses.

2. In your Neon project, add the IPs to your **IP Allow** list:

   1. Select your project in the Neon Console.
   2. On the Neon **Dashboard**, select **Settings**.
   3. Select **IP Allow**.
   4. Add Materialize IP addresses to the list.




## C. Ingest data in Materialize

The steps in this section are specific to Materialize. You can run them in the
[Materialize console's SQL Shell](/console/) or your
preferred SQL client connected to Materialize.

### 1. (Optional) Create a cluster

> **Note:** If you are prototyping and already have a cluster to host your PostgreSQL
> source (e.g. `quickstart`), **you can skip this step**. For production
> scenarios, we recommend separating your workloads into multiple clusters for
> [resource isolation](/sql/create-cluster/#resource-isolation).
>


<p>In Materialize, a <a href="/concepts/clusters/" >cluster</a> is an isolated environment,
similar to a virtual warehouse in Snowflake. When you create a cluster, you
choose the size of its compute resource allocation based on the work you need
the cluster to do, whether ingesting data from a source, computing
always-up-to-date query results, serving results to external clients, or a
combination.</p>
<p>In this step, you&rsquo;ll create a dedicated cluster for ingesting source data from
your PostgreSQL database.</p>
<ol>
<li>
<p>In the <a href="/console/" >SQL Shell</a>, or your preferred SQL
client connected to Materialize, use the <a href="/sql/create-cluster/" ><code>CREATE CLUSTER</code></a>
command to create the new cluster:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">CLUSTER</span> <span class="n">ingest_postgres</span> <span class="p">(</span><span class="k">SIZE</span> <span class="o">=</span> <span class="s1">&#39;50cc&#39;</span><span class="p">);</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">SET</span> <span class="k">CLUSTER</span> <span class="o">=</span> <span class="n">ingest_postgres</span><span class="p">;</span>
</span></span></code></pre></div><p>A cluster of <a href="/sql/create-cluster/#size" >size</a> <code>50cc</code> should be enough to
accommodate multiple PostgreSQL sources, depending on the source
characteristics (e.g., sources with <a href="/sql/create-source/kafka/#upsert-envelope" ><code>ENVELOPE UPSERT</code></a>
or <a href="/sql/create-source/kafka/#debezium-envelope" ><code>ENVELOPE DEBEZIUM</code></a> will be more
memory-intensive) and the upstream traffic patterns. You can readjust the
size of the cluster at any time using the <a href="/sql/alter-cluster" ><code>ALTER CLUSTER</code></a> command:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">CLUSTER</span> <span class="o">&lt;</span><span class="n">cluster_name</span><span class="o">&gt;</span> <span class="k">SET</span> <span class="p">(</span> <span class="k">SIZE</span> <span class="o">=</span> <span class="o">&lt;</span><span class="n">new_size</span><span class="o">&gt;</span> <span class="p">);</span>
</span></span></code></pre></div></li>
</ol>


### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.

1. Run the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
   password for the `materialize` PostgreSQL user you created [earlier](#2-create-a-publication-and-a-replication-user):

    ```mzsql
    CREATE SECRET pgpass AS '<PASSWORD>';
    ```

    You can access the password for your Neon user from
    the **Connection Details** widget on the Neon **Dashboard**.


2. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
   connection object with access and authentication details for Materialize to
   use:

    ```mzsql
    CREATE CONNECTION pg_connection TO POSTGRES (
      HOST '<host>',
      PORT 5432,
      USER '<user_name>',
      PASSWORD SECRET pgpass,
      SSL MODE 'require',
      DATABASE '<database>'
    );
    ```

    You can find the connection details for your replication user in
    the **Connection Details** widget on the Neon **Dashboard**. A Neon
    connection string looks like this:

    ```bash
    postgresql://materialize:AbC123dEf@ep-cool-darkness-123456.us-east-2.aws.neon.tech/dbname?sslmode=require
    ```

    - Replace `<host>` with your Neon hostname
      (e.g., `ep-cool-darkness-123456.us-east-2.aws.neon.tech`).
    - Replace `<role_name>` with the dedicated replication user
      (e.g., `materialize`).
    - Replace `<database>` with the name of the database containing the tables
      you want to replicate to Materialize (e.g., `dbname`).

### 3. Start ingesting data

{{< tabs >}}
{{< tab "Legacy Syntax" >}}
#### Legacy syntax

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source-legacy" %}}
{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="schema-changes" %}}
{{< /tab >}}

{{< tab "New Syntax" >}}
#### New syntax

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source" %}}
{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="schema-changes" %}}
{{< /tab >}}
{{< /tabs >}}


### 4. Monitor the ingestion status

<p>Before it starts consuming the replication stream, Materialize takes a snapshot
of the relevant tables in your publication. Until this snapshot is complete,
Materialize won&rsquo;t have the same view of your data as your PostgreSQL database.</p>
<p>In this step, you&rsquo;ll first verify that the source is running and then check the
status of the snapshotting process.</p>
<ol>
<li>
<p>Back in the SQL client connected to Materialize, use the
<a href="/sql/system-catalog/mz_internal/#mz_source_statuses" ><code>mz_source_statuses</code></a>
table to check the overall status of your source:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">WITH</span>
</span></span><span class="line"><span class="cl">  <span class="n">source_ids</span> <span class="k">AS</span>
</span></span><span class="line"><span class="cl">  <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">mz_sources</span> <span class="k">WHERE</span> <span class="k">name</span> <span class="o">=</span> <span class="s1">&#39;mz_source&#39;</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">  <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_source_statuses</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span>
</span></span><span class="line"><span class="cl">      <span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="k">SELECT</span> <span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">        <span class="k">FROM</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_object_dependencies</span>
</span></span><span class="line"><span class="cl">        <span class="k">WHERE</span>
</span></span><span class="line"><span class="cl">          <span class="n">object_id</span> <span class="k">IN</span> <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">UNION</span> <span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span>
</span></span><span class="line"><span class="cl">      <span class="p">)</span>
</span></span><span class="line"><span class="cl">      <span class="k">AS</span> <span class="k">sources</span>
</span></span><span class="line"><span class="cl">    <span class="k">ON</span> <span class="n">mz_source_statuses</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span><span class="p">;</span>
</span></span></code></pre></div><p>For each <code>subsource</code>, make sure the <code>status</code> is <code>running</code>. If you see
<code>stalled</code> or <code>failed</code>, there&rsquo;s likely a configuration issue for you to fix.
Check the <code>error</code> field for details and fix the issue before moving on.
Also, if the <code>status</code> of any subsource is <code>starting</code> for more than a few
minutes, <a href="/support/" >contact our team</a>.</p>
</li>
<li>
<p>Once the source is running, use the <a href="/sql/system-catalog/mz_internal/#mz_source_statistics" ><code>mz_source_statistics</code></a>
table to check the status of the initial snapshot:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">WITH</span>
</span></span><span class="line"><span class="cl">  <span class="n">source_ids</span> <span class="k">AS</span>
</span></span><span class="line"><span class="cl">  <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">mz_sources</span> <span class="k">WHERE</span> <span class="k">name</span> <span class="o">=</span> <span class="s1">&#39;mz_source&#39;</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span> <span class="k">AS</span> <span class="k">id</span><span class="p">,</span> <span class="n">mz_sources</span><span class="mf">.</span><span class="k">name</span><span class="p">,</span> <span class="n">snapshot_committed</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">  <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_source_statistics</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span>
</span></span><span class="line"><span class="cl">      <span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="k">SELECT</span> <span class="n">object_id</span><span class="p">,</span> <span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">        <span class="k">FROM</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_object_dependencies</span>
</span></span><span class="line"><span class="cl">        <span class="k">WHERE</span>
</span></span><span class="line"><span class="cl">          <span class="n">object_id</span> <span class="k">IN</span> <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">UNION</span> <span class="k">SELECT</span> <span class="k">id</span><span class="p">,</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span>
</span></span><span class="line"><span class="cl">      <span class="p">)</span>
</span></span><span class="line"><span class="cl">      <span class="k">AS</span> <span class="k">sources</span>
</span></span><span class="line"><span class="cl">    <span class="k">ON</span> <span class="n">mz_source_statistics</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_sources</span> <span class="k">ON</span> <span class="n">mz_sources</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span><span class="p">;</span>
</span></span></code></pre></div> <p></p>
<pre tabindex="0"><code class="language-nofmt" data-lang="nofmt">object_id | snapshot_committed
----------|------------------
 u144     | t
(1 row)
</code></pre><p>Once <code>snapshot_commited</code> is <code>t</code>, move on to the next step. Snapshotting can
take between a few minutes to several hours, depending on the size of your
dataset and the size of the cluster the source is running in.</p>
</li>
</ol>


### 5. Right-size the cluster

<p>After the snapshotting phase, Materialize starts ingesting change events from
the PostgreSQL replication stream. For this work, Materialize generally
performs well with an <code>100cc</code> replica, so you can resize the cluster
accordingly.</p>
<ol>
<li>
<p>Still in a SQL client connected to Materialize, use the <a href="/sql/alter-cluster/" ><code>ALTER CLUSTER</code></a>
command to downsize the cluster to <code>100cc</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">CLUSTER</span> <span class="n">ingest_postgres</span> <span class="k">SET</span> <span class="p">(</span><span class="k">SIZE</span> <span class="s1">&#39;100cc&#39;</span><span class="p">);</span>
</span></span></code></pre></div><p>Behind the scenes, this command adds a new <code>100cc</code> replica and removes the
<code>50cc</code> replica.</p>
</li>
<li>
<p>Use the <a href="/sql/show-cluster-replicas/" ><code>SHOW CLUSTER REPLICAS</code></a> command to
check the status of the new replica:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SHOW</span> <span class="k">CLUSTER</span> <span class="k">REPLICAS</span> <span class="k">WHERE</span> <span class="k">cluster</span> <span class="o">=</span> <span class="s1">&#39;ingest_postgres&#39;</span><span class="p">;</span>
</span></span></code></pre></div> <p></p>
<pre tabindex="0"><code class="language-nofmt" data-lang="nofmt">     cluster     | replica |  size  | ready
-----------------+---------+--------+-------
 ingest_postgres | r1      | 100cc  | t
(1 row)
</code></pre></li>
<li>
<p>Going forward, you can verify that your new cluster size is sufficient as
follows:</p>
<ol>
<li>
<p>In Materialize, get the replication slot name associated with your
PostgreSQL source from the <a href="/sql/system-catalog/mz_internal/#mz_postgres_sources" ><code>mz_internal.mz_postgres_sources</code></a>
table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span>
</span></span><span class="line"><span class="cl">    <span class="n">d</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">database_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">n</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">schema_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">s</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">source_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">pgs</span><span class="mf">.</span><span class="n">replication_slot</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">    <span class="n">mz_sources</span> <span class="k">AS</span> <span class="n">s</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_postgres_sources</span> <span class="k">AS</span> <span class="n">pgs</span> <span class="k">ON</span> <span class="n">s</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">pgs</span><span class="mf">.</span><span class="k">id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_schemas</span> <span class="k">AS</span> <span class="n">n</span> <span class="k">ON</span> <span class="n">n</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">s</span><span class="mf">.</span><span class="n">schema_id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_databases</span> <span class="k">AS</span> <span class="n">d</span> <span class="k">ON</span> <span class="n">d</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">n</span><span class="mf">.</span><span class="n">database_id</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>In PostgreSQL, check the replication slot lag, using the replication slot
name from the previous step:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">SELECT</span>
</span></span><span class="line"><span class="cl">    <span class="n">pg_size_pretty</span><span class="p">(</span><span class="n">pg_current_wal_lsn</span><span class="p">()</span> <span class="o">-</span> <span class="n">confirmed_flush_lsn</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">    <span class="k">AS</span> <span class="n">replication_lag_bytes</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span> <span class="n">pg_replication_slots</span>
</span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">slot_name</span> <span class="o">=</span> <span class="s1">&#39;&lt;slot_name&gt;&#39;</span><span class="p">;</span>
</span></span></code></pre></div><p>The result of this query is the amount of data your PostgreSQL cluster
must retain in its replication log because of this replication slot.
Typically, this means Materialize has not yet communicated back to
PostgreSQL that it has committed this data. A high value can indicate
that the source has fallen behind and that you might need to scale up
your ingestion cluster.</p>
</li>
</ol>
</li>
</ol>


## D. Explore your data

<p>With Materialize ingesting your PostgreSQL data into durable storage, you can
start exploring the data, computing real-time results that stay up-to-date as
new data arrives, and serving results efficiently.</p>
<ul>
<li>
<p>Explore your data with <a href="/sql/show-sources" ><code>SHOW SOURCES</code></a> and <a href="/sql/select/" ><code>SELECT</code></a>.</p>
</li>
<li>
<p>Compute real-time results in memory with <a href="/sql/create-view/" ><code>CREATE VIEW</code></a>
and <a href="/sql/create-index/" ><code>CREATE INDEX</code></a> or in durable
storage with <a href="/sql/create-materialized-view/" ><code>CREATE MATERIALIZED VIEW</code></a>.</p>
</li>
<li>
<p>Serve results to a PostgreSQL-compatible SQL client or driver with <a href="/sql/select/" ><code>SELECT</code></a>
or <a href="/sql/subscribe/" ><code>SUBSCRIBE</code></a> or to an external message broker with
<a href="/sql/create-sink/" ><code>CREATE SINK</code></a>.</p>
</li>
<li>
<p>Check out the <a href="/integrations/" >tools and integrations</a> supported by
Materialize.</p>
</li>
</ul>


## Considerations

<h3 id="schema-changes">Schema changes</h3>
<p>Materialize supports schema changes in the upstream database as follows:</p>
<h4 id="compatible-schema-changes-legacy-syntax">Compatible schema changes (Legacy syntax)</h4>
<blockquote>
<p><strong>Note:</strong> This section refer to the legacy <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a> that creates subsources as part of the
<code>CREATE SOURCE</code> operation.  To be able to handle the upstream column
additions and drops, see <a href="/sql/create-source/postgres-v2/" ><code>CREATE SOURCE (New Syntax)</code></a> and <a href="/sql/create-table" ><code>CREATE TABLE FROM SOURCE</code></a>.</p>
</blockquote>
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
<h4 id="incompatible-schema-changes">Incompatible schema changes</h4>
<p>All other schema changes to upstream tables will set the corresponding
Materialize tables into an error state, preventing reads from these tables.</p>
<p>To handle <a href="#incompatible-schema-changes" >incompatible schema changes</a>, drop
the affected table <a href="/sql/drop-table/" ><code>DROP TABLE</code></a> , and then, <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> to recreate the table with the
updated schema.</p>
<h3 id="publication-membership">Publication membership</h3>
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
<p>To mitigate this issue, if you need to drop and re-add a table to a
publication, ensure that you remove the table/subsource from the source
<em>before</em> re-adding it using the <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> command.</p>
<h3 id="supported-types">Supported types</h3>
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
<h3 id="truncation">Truncation</h3>
<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource(s)/table(s) in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>
<h3 id="inherited-tables">Inherited tables</h3>
<p>When using <a href="https://www.postgresql.org/docs/current/tutorial-inheritance.html" >PostgreSQL table inheritance</a>,
PostgreSQL serves data from <code>SELECT</code>s as if the inheriting tables&rsquo; data is
also present in the inherited table. However, both PostgreSQL&rsquo;s logical
replication and <code>COPY</code> only present data written to the tables themselves,
i.e. the inheriting data is <em>not</em> treated as part of the inherited table.</p>
<p>PostgreSQL sources use logical replication and <code>COPY</code> to ingest table data,
so inheriting tables&rsquo; data will only be ingested as part of the inheriting
table, i.e. in Materialize, the data will not be returned when serving
<code>SELECT</code>s from the inherited table.</p>
<ul>
<li>
<p>If using legacy syntax <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a>:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>ADD SUBSOURCE</code> and
create a new view (materialized or non-) that unions the new table.</p>
</li>
<li>
<p>If using new <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> syntax:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>CREATE TABLE .. FROM SOURCE</code> and create a new view (materialized or non-) that unions the new
table.</p>
</li>
</ul>
<h3 id="replication-slots">Replication slots</h3>
<p>Each source ingests the raw replication stream data for all tables in the
specified publication using <strong>a single</strong> replication slot. To manage
replication slots:</p>
<ul>
<li>
<p>For PostgreSQL 13+, set a reasonable value
for <a href="https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE" ><code>max_slot_wal_keep_size</code></a>
to limit the amount of storage used by replication slots.</p>
</li>
<li>
<p>If you stop using Materialize, or if either the Materialize instance or
the PostgreSQL instance crash, delete any replication slots. You can query
the <code>mz_internal.mz_postgres_sources</code> table to look up the name of the
replication slot created for each source.</p>
</li>
<li>
<p>If you delete all objects that depend on a source without also dropping
the source, the upstream replication slot remains and will continue to
accumulate data so that the source can resume in the future. To avoid
unbounded disk space usage, make sure to use <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> or manually delete the replication slot.</p>
</li>
</ul>
<h3 id="modifying-an-existing-source">Modifying an existing source</h3>
<p>When you add a new subsource to an existing source (<a href="/sql/alter-source/" ><code>ALTER SOURCE ... ADD SUBSOURCE ...</code></a>), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.
</p>



---

## Ingest data from self-hosted PostgreSQL


This page shows you how to stream data from a self-hosted PostgreSQL database to
Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).
>
>


## Before you begin

<ul>
<li>
<p>Make sure you are running PostgreSQL 11 or higher.</p>
</li>
<li>
<p>Make sure you have access to your PostgreSQL instance via <a href="https://www.postgresql.org/docs/current/app-psql.html" ><code>psql</code></a>,
or your preferred SQL client.</p>
</li>
</ul>


## A. Configure PostgreSQL

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical
replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.
Enable your PostgreSQL's logical replication.

1. As a _superuser_, use `psql` (or your preferred SQL client) to connect to
   your PostgreSQL database.

1. Check if logical replication is enabled; that is, check if the `wal_level` is
   set to `logical`:

    ```postgres
    SHOW wal_level;
    ```

1. If `wal_level` setting is **not** set to `logical`:

    1. In the  database configuration file (`postgresql.conf`), set `wal_level`
       value to `logical`.

    1. Restart the database in order for the new `wal_level` to take effect.
       Restarting can affect database performance.

    1. In the SQL client connected to PostgreSQL, verify that replication is now
  enabled (i.e., verify `wal_level` setting is set to `logical`).

        ```postgres
        SHOW wal_level;
        ```

### 2. Create a publication and a replication user

<p>Once logical replication is enabled, the next step is to create a publication
with the tables that you want to replicate to Materialize. You&rsquo;ll also need a
user for Materialize with sufficient privileges to manage replication.</p>
<ol>
<li>
<p>For each table that you want to replicate to Materialize, set the
<a href="https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY" >replica identity</a>
to <code>FULL</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span> <span class="k">REPLICA</span> <span class="k">IDENTITY</span> <span class="k">FULL</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span> <span class="k">REPLICA</span> <span class="k">IDENTITY</span> <span class="k">FULL</span><span class="p">;</span>
</span></span></code></pre></div><p><code>REPLICA IDENTITY FULL</code> ensures that the replication stream includes the
previous data of changed rows, in the case of <code>UPDATE</code> and <code>DELETE</code>
operations. This setting enables Materialize to ingest PostgreSQL data with
minimal in-memory state. However, you should expect increased disk usage in
your PostgreSQL database.</p>
</li>
<li>
<p>Create a <a href="https://www.postgresql.org/docs/current/logical-replication-publication.html" >publication</a>
with the tables you want to replicate:</p>
<p><em>For specific tables:</em></p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="n">PUBLICATION</span> <span class="n">mz_source</span> <span class="k">FOR</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span><span class="p">,</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span><span class="p">;</span>
</span></span></code></pre></div><p><em>For all tables in the database:</em></p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="n">PUBLICATION</span> <span class="n">mz_source</span> <span class="k">FOR</span> <span class="k">ALL</span> <span class="k">TABLES</span><span class="p">;</span>
</span></span></code></pre></div><p>The <code>mz_source</code> publication will contain the set of change events generated
from the specified tables, and will later be used to ingest the replication
stream.</p>
<p>Be sure to include only the tables you need. If the publication includes
additional tables, Materialize will waste resources on ingesting and then
immediately discarding the data.</p>
</li>
<li>
<p>Create a user for Materialize, if you don&rsquo;t already have one:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">USER</span> <span class="n">materialize</span> <span class="k">PASSWORD</span> <span class="s1">&#39;&lt;password&gt;&#39;</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>Grant the user permission to manage replication:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">ROLE</span> <span class="n">materialize</span> <span class="k">WITH</span> <span class="n">REPLICATION</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>Grant the user the required permissions on the tables you want to replicate:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="n">CONNECT</span> <span class="k">ON</span> <span class="k">DATABASE</span> <span class="o">&lt;</span><span class="n">dbname</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="n">USAGE</span> <span class="k">ON</span> <span class="k">SCHEMA</span> <span class="o">&lt;</span><span class="k">schema</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><p>Once connected to your database, Materialize will take an initial snapshot
of the tables in your publication. <code>SELECT</code> privileges are required for
this initial snapshot.</p>
<p>If you expect to add tables to your publication, you can grant <code>SELECT</code> on
all tables in the schema instead of naming the specific tables:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="k">ALL</span> <span class="k">TABLES</span> <span class="k">IN</span> <span class="k">SCHEMA</span> <span class="o">&lt;</span><span class="k">schema</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div></li>
</ol>


## B. (Optional) Configure network security

> **Note:** If you are prototyping and your PostgreSQL instance is publicly
> accessible, **you can skip this step**. For production scenarios, we recommend
> configuring one of the network security options below.
>




**Cloud:**

There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's firewall to allow connections from a set of
    static Materialize IP addresses.

- **Use an SSH tunnel:** If your database is running in a private network, you
    can use an SSH tunnel to connect Materialize to the database.

Select the option that works best for you.



**Allow Materialize IPs:**

1. In the [Materialize console's SQL Shell](/console/),
   or your preferred SQL client connected to Materialize, find the static egress
   IP addresses for the Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

1. Update your database firewall rules to allow traffic from each IP address
   from the previous step.



**Use AWS PrivateLink:**

Materialize can connect to a PostgreSQL database through an [AWS PrivateLink](https://aws.amazon.com/privatelink/)
service. Your PostgreSQL database must be running on AWS in order to use this
option.

1. Create a dedicated [target
    group](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html)
    for your Postgres instance with the following details:

    a. Target type as **IP address**.

    b. Protocol as **TCP**.

    c. Port as **5432**, or the port that you are using in case it is not 5432.

    d. Make sure that the target group is in the same VPC as the PostgreSQL
    instance.

    e. Click next, and register the respective PostgreSQL instance to the target
    group using its IP address.

1. Create a [Network Load
    Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html)
    that is **enabled for the same subnets** that the PostgreSQL instance is in.

1. Create a [TCP
    listener](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-listener.html)
    for your PostgreSQL instance that forwards to the corresponding target group
    you created.

1. Once the TCP listener has been created, make sure that the [health
    checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html)
    are passing and that the target is reported as healthy.

    If you have set up a security group for your PostgreSQL instance, you must
    ensure that it allows traffic on the health check port.

    **Remarks**:

    a. Network Load Balancers do not have associated security groups. Therefore,
    the security groups for your targets must use IP addresses to allow
    traffic.

    b. You can't use the security groups for the clients as a source in the
    security groups for the targets. Therefore, the security groups for your
    targets must use the IP addresses of the clients to allow traffic. For more
    details, check the [AWS documentation](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html).

1. Create a VPC [endpoint
    service](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html)
    and associate it with the **Network Load Balancer** that you’ve just
    created.

    Note the **service name** that is generated for the endpoint service.

    **Remarks**:

    By disabling [Acceptance Required](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests),
    while still strictly managing who can view your endpoint via IAM,
    Materialze will be able to seamlessly recreate and migrate endpoints as we
    work to stabilize this feature.

1. In Materialize, create a [`AWS
     PRIVATELINK`](/sql/create-connection/#aws-privatelink) connection that
     references the endpoint service that you created in the previous step.

     ```mzsql
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
        SERVICE NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
        AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az3')
    );
    ```

    Update the list of the availability zones to match the ones that you are
    using in your AWS account.

1. Retrieve the AWS principal for the AWS PrivateLink connection you just
    created:

    ```mzsql
    SELECT principal
    FROM mz_aws_privatelink_connections plc
    JOIN mz_connections c ON plc.id = c.id
    WHERE c.name = 'privatelink_svc';
    ```

    ```
                                     principal
    ---------------------------------------------------------------------------
     arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
    ```

    Follow the instructions in the [AWS PrivateLink documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/add-endpoint-service-permissions.html)
    to configure your VPC endpoint service to accept connections from the
    provided AWS principal.

    If your AWS PrivateLink service is configured to require acceptance of
    connection requests, you must manually approve the connection request from
    Materialize after executing the `CREATE CONNECTION` statement. For more
    details, check the [AWS PrivateLink documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).

    **Note:** It might take some time for the endpoint service connection to
      show up, so you would need to wait for the endpoint service connection to
      be ready before you create a source.


**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an VM to
serve as an SSH bastion host, configure the bastion host to allow traffic only
from Materialize, and then configure your database's private network to allow
traffic from the bastion host.

1. Launch a VM to serve as your SSH bastion host.

    - Make sure the VM is publicly accessible and in the same VPC as your
      database.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a static public IP address. You'll use this IP
      address when connecting Materialize to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

    1. In the [Materialize console's SQL
       Shell](/console/), or your preferred SQL client
       connected to Materialize, get the static egress IP addresses for the
       Materialize region you are running in:

       ```mzsql
       SELECT * FROM mz_egress_ips;
       ```

    1. Update your SSH bastion host's firewall rules to allow traffic from each
       IP address from the previous step.

1. Update your database firewall rules to allow traffic from the SSH bastion
   host.







**Self-Managed:**

<p>Configure your network to allow Materialize to connect to your database. For
example, you can:</p>
<ul>
<li>
<p><strong>Allow Materialize IPs:</strong> Configure your database&rsquo;s security group to allow
connections from Materialize.</p>
</li>
<li>
<p><strong>Use an SSH tunnel:</strong> Use an SSH tunnel to connect Materialize to the
database.</p>
</li>
</ul>
<div class="note">
  <strong class="gutter">NOTE:</strong>
  <p>The steps to allow Materialize to connect to your database  depends on your
  deployment setup. Refer to your company’s network/security policies and
  procedures.</p>
</div>




**Allow Materialize IPs:**

1. Update your database firewall rules to allow traffic from Materialize.



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an VM to
serve as an SSH bastion host, configure the bastion host to allow traffic only
from Materialize, and then configure your database's private network to allow
traffic from the bastion host.

1. Launch a VM to serve as your SSH bastion host.

    - Make sure the VM is publicly accessible and in the same VPC as your
      database.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a static public IP address. You'll use this IP
      address when connecting Materialize to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

1. Update your database firewall rules to allow traffic from the SSH bastion
   host.









## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

> **Note:** If you are prototyping and already have a cluster to host your PostgreSQL
> source (e.g. `quickstart`), **you can skip this step**. For production
> scenarios, we recommend separating your workloads into multiple clusters for
> [resource isolation](/sql/create-cluster/#resource-isolation).
>


<p>In Materialize, a <a href="/concepts/clusters/" >cluster</a> is an isolated environment,
similar to a virtual warehouse in Snowflake. When you create a cluster, you
choose the size of its compute resource allocation based on the work you need
the cluster to do, whether ingesting data from a source, computing
always-up-to-date query results, serving results to external clients, or a
combination.</p>
<p>In this step, you&rsquo;ll create a dedicated cluster for ingesting source data from
your PostgreSQL database.</p>
<ol>
<li>
<p>In the <a href="/console/" >SQL Shell</a>, or your preferred SQL
client connected to Materialize, use the <a href="/sql/create-cluster/" ><code>CREATE CLUSTER</code></a>
command to create the new cluster:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">CLUSTER</span> <span class="n">ingest_postgres</span> <span class="p">(</span><span class="k">SIZE</span> <span class="o">=</span> <span class="s1">&#39;50cc&#39;</span><span class="p">);</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">SET</span> <span class="k">CLUSTER</span> <span class="o">=</span> <span class="n">ingest_postgres</span><span class="p">;</span>
</span></span></code></pre></div><p>A cluster of <a href="/sql/create-cluster/#size" >size</a> <code>50cc</code> should be enough to
accommodate multiple PostgreSQL sources, depending on the source
characteristics (e.g., sources with <a href="/sql/create-source/kafka/#upsert-envelope" ><code>ENVELOPE UPSERT</code></a>
or <a href="/sql/create-source/kafka/#debezium-envelope" ><code>ENVELOPE DEBEZIUM</code></a> will be more
memory-intensive) and the upstream traffic patterns. You can readjust the
size of the cluster at any time using the <a href="/sql/alter-cluster" ><code>ALTER CLUSTER</code></a> command:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">CLUSTER</span> <span class="o">&lt;</span><span class="n">cluster_name</span><span class="o">&gt;</span> <span class="k">SET</span> <span class="p">(</span> <span class="k">SIZE</span> <span class="o">=</span> <span class="o">&lt;</span><span class="n">new_size</span><span class="o">&gt;</span> <span class="p">);</span>
</span></span></code></pre></div></li>
</ol>


### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.



**Allow Materialize IPs:**

1. In the [Materialize Console's SQL Shell](/console/), or your preferred SQL
client connected to Materialize, use the [`CREATE
SECRET`](/sql/create-secret/) command to securely store the password for the
`materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):

   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
connection object with access and authentication details for Materialize to
use:


   ```mzsql
   CREATE CONNECTION pg_connection TO POSTGRES (
     HOST '<host>',
     PORT 5432,
     USER materialize,
     PASSWORD SECRET pgpass,
     SSL MODE 'require',
     DATABASE '<database>'
   );

   ```


   - Replace `<host>` with your PostgreSQL endpoint.

   - Replace `<database>` with the name of the database containing the tables
     you want to replicate to Materialize.




**Use AWS PrivateLink (Cloud-only):**

1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
password for the `materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):


   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create
another connection object, this time with database access and authentication
details for Materialize to use:


   ```mzsql
   CREATE CONNECTION pg_connection TO POSTGRES (
     HOST '<host>',
     PORT 5432,
     USER 'materialize',
     PASSWORD SECRET pgpass,
     DATABASE '<database>',
     AWS PRIVATELINK privatelink_svc
     );

   ```

   - Replace `<host>` with your database endpoint.

   - Replace `<database>` with the name of the database containing the tables
     you want to replicate to Materialize.



**Use an SSH tunnel:**

1. In the [Materialize Console's SQL Shell](/console/), or your preferred SQL
client connected to Materialize, use the [`CREATE
CONNECTION`](/sql/create-connection/#ssh-tunnel) command to create an SSH
tunnel connection:

   ```mzsql
   CREATE CONNECTION ssh_connection TO SSH TUNNEL (
       HOST '<SSH_BASTION_HOST>',
       PORT <SSH_BASTION_PORT>,
       USER '<SSH_BASTION_USER>'
   );

   ```

   - Replace `<SSH_BASTION_HOST>` and `<SSH_BASTION_PORT>` with the public IP
   address and port of the SSH bastion host you created
   [earlier](#b-optional-configure-network-security).

   - Replace `<SSH_BASTION_USER>` with the username for the key pair you
   created for your SSH bastion host.

1. Get Materialize's public keys for the SSH tunnel connection:


   ```mzsql
   SELECT
       mz_connections.name,
       mz_ssh_tunnel_connections.*
   FROM
       mz_connections
   JOIN
       mz_ssh_tunnel_connections USING(id)
   WHERE
       mz_connections.name = 'ssh_connection';

   ```

1. Log in to your SSH bastion host and add Materialize's public keys to the
`authorized_keys` file, for example:


   ```mzsql
   echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
   echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys

   ```

1. Back in the SQL client connected to Materialize, validate the SSH tunnel
connection you created using the [`VALIDATE
CONNECTION`](/sql/validate-connection) command:


   ```mzsql
   VALIDATE CONNECTION ssh_connection;

   ```   If no validation error is returned, move to the next step.


1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
password for the `materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):


   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```

1.
Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create another connection object, this time with database access and authentication details for Materialize to use:


   ```mzsql
   CREATE CONNECTION pg_connection TO POSTGRES (
     HOST '<host>',
     PORT 5432,
     USER 'materialize',
     PASSWORD SECRET pgpass,
     DATABASE '<database>',
     SSH TUNNEL ssh_connection
     );

   ```

   - Replace `<host>` with your PostgreSQL endpoint.

   - Replace `<database>` with the name of the database containing the tables
   you want to replicate to Materialize.





### 3. Start ingesting data

{{< tabs >}}
{{< tab "Legacy Syntax" >}}
#### Legacy syntax

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source-legacy" %}}
{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="schema-changes" %}}
{{< /tab >}}

{{< tab "New Syntax" >}}
#### New syntax

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source" %}}
{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="schema-changes" %}}
{{< /tab >}}
{{< /tabs >}}


### 4. Monitor the ingestion status

<p>Before it starts consuming the replication stream, Materialize takes a snapshot
of the relevant tables in your publication. Until this snapshot is complete,
Materialize won&rsquo;t have the same view of your data as your PostgreSQL database.</p>
<p>In this step, you&rsquo;ll first verify that the source is running and then check the
status of the snapshotting process.</p>
<ol>
<li>
<p>Back in the SQL client connected to Materialize, use the
<a href="/sql/system-catalog/mz_internal/#mz_source_statuses" ><code>mz_source_statuses</code></a>
table to check the overall status of your source:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">WITH</span>
</span></span><span class="line"><span class="cl">  <span class="n">source_ids</span> <span class="k">AS</span>
</span></span><span class="line"><span class="cl">  <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">mz_sources</span> <span class="k">WHERE</span> <span class="k">name</span> <span class="o">=</span> <span class="s1">&#39;mz_source&#39;</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">  <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_source_statuses</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span>
</span></span><span class="line"><span class="cl">      <span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="k">SELECT</span> <span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">        <span class="k">FROM</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_object_dependencies</span>
</span></span><span class="line"><span class="cl">        <span class="k">WHERE</span>
</span></span><span class="line"><span class="cl">          <span class="n">object_id</span> <span class="k">IN</span> <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">UNION</span> <span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span>
</span></span><span class="line"><span class="cl">      <span class="p">)</span>
</span></span><span class="line"><span class="cl">      <span class="k">AS</span> <span class="k">sources</span>
</span></span><span class="line"><span class="cl">    <span class="k">ON</span> <span class="n">mz_source_statuses</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span><span class="p">;</span>
</span></span></code></pre></div><p>For each <code>subsource</code>, make sure the <code>status</code> is <code>running</code>. If you see
<code>stalled</code> or <code>failed</code>, there&rsquo;s likely a configuration issue for you to fix.
Check the <code>error</code> field for details and fix the issue before moving on.
Also, if the <code>status</code> of any subsource is <code>starting</code> for more than a few
minutes, <a href="/support/" >contact our team</a>.</p>
</li>
<li>
<p>Once the source is running, use the <a href="/sql/system-catalog/mz_internal/#mz_source_statistics" ><code>mz_source_statistics</code></a>
table to check the status of the initial snapshot:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">WITH</span>
</span></span><span class="line"><span class="cl">  <span class="n">source_ids</span> <span class="k">AS</span>
</span></span><span class="line"><span class="cl">  <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">mz_sources</span> <span class="k">WHERE</span> <span class="k">name</span> <span class="o">=</span> <span class="s1">&#39;mz_source&#39;</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span> <span class="k">AS</span> <span class="k">id</span><span class="p">,</span> <span class="n">mz_sources</span><span class="mf">.</span><span class="k">name</span><span class="p">,</span> <span class="n">snapshot_committed</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">  <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_source_statistics</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span>
</span></span><span class="line"><span class="cl">      <span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="k">SELECT</span> <span class="n">object_id</span><span class="p">,</span> <span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">        <span class="k">FROM</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_object_dependencies</span>
</span></span><span class="line"><span class="cl">        <span class="k">WHERE</span>
</span></span><span class="line"><span class="cl">          <span class="n">object_id</span> <span class="k">IN</span> <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">UNION</span> <span class="k">SELECT</span> <span class="k">id</span><span class="p">,</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span>
</span></span><span class="line"><span class="cl">      <span class="p">)</span>
</span></span><span class="line"><span class="cl">      <span class="k">AS</span> <span class="k">sources</span>
</span></span><span class="line"><span class="cl">    <span class="k">ON</span> <span class="n">mz_source_statistics</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_sources</span> <span class="k">ON</span> <span class="n">mz_sources</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span><span class="p">;</span>
</span></span></code></pre></div> <p></p>
<pre tabindex="0"><code class="language-nofmt" data-lang="nofmt">object_id | snapshot_committed
----------|------------------
 u144     | t
(1 row)
</code></pre><p>Once <code>snapshot_commited</code> is <code>t</code>, move on to the next step. Snapshotting can
take between a few minutes to several hours, depending on the size of your
dataset and the size of the cluster the source is running in.</p>
</li>
</ol>


### 5. Right-size the cluster

<p>After the snapshotting phase, Materialize starts ingesting change events from
the PostgreSQL replication stream. For this work, Materialize generally
performs well with an <code>100cc</code> replica, so you can resize the cluster
accordingly.</p>
<ol>
<li>
<p>Still in a SQL client connected to Materialize, use the <a href="/sql/alter-cluster/" ><code>ALTER CLUSTER</code></a>
command to downsize the cluster to <code>100cc</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">CLUSTER</span> <span class="n">ingest_postgres</span> <span class="k">SET</span> <span class="p">(</span><span class="k">SIZE</span> <span class="s1">&#39;100cc&#39;</span><span class="p">);</span>
</span></span></code></pre></div><p>Behind the scenes, this command adds a new <code>100cc</code> replica and removes the
<code>50cc</code> replica.</p>
</li>
<li>
<p>Use the <a href="/sql/show-cluster-replicas/" ><code>SHOW CLUSTER REPLICAS</code></a> command to
check the status of the new replica:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SHOW</span> <span class="k">CLUSTER</span> <span class="k">REPLICAS</span> <span class="k">WHERE</span> <span class="k">cluster</span> <span class="o">=</span> <span class="s1">&#39;ingest_postgres&#39;</span><span class="p">;</span>
</span></span></code></pre></div> <p></p>
<pre tabindex="0"><code class="language-nofmt" data-lang="nofmt">     cluster     | replica |  size  | ready
-----------------+---------+--------+-------
 ingest_postgres | r1      | 100cc  | t
(1 row)
</code></pre></li>
<li>
<p>Going forward, you can verify that your new cluster size is sufficient as
follows:</p>
<ol>
<li>
<p>In Materialize, get the replication slot name associated with your
PostgreSQL source from the <a href="/sql/system-catalog/mz_internal/#mz_postgres_sources" ><code>mz_internal.mz_postgres_sources</code></a>
table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span>
</span></span><span class="line"><span class="cl">    <span class="n">d</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">database_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">n</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">schema_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">s</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">source_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">pgs</span><span class="mf">.</span><span class="n">replication_slot</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">    <span class="n">mz_sources</span> <span class="k">AS</span> <span class="n">s</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_postgres_sources</span> <span class="k">AS</span> <span class="n">pgs</span> <span class="k">ON</span> <span class="n">s</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">pgs</span><span class="mf">.</span><span class="k">id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_schemas</span> <span class="k">AS</span> <span class="n">n</span> <span class="k">ON</span> <span class="n">n</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">s</span><span class="mf">.</span><span class="n">schema_id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_databases</span> <span class="k">AS</span> <span class="n">d</span> <span class="k">ON</span> <span class="n">d</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">n</span><span class="mf">.</span><span class="n">database_id</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>In PostgreSQL, check the replication slot lag, using the replication slot
name from the previous step:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">SELECT</span>
</span></span><span class="line"><span class="cl">    <span class="n">pg_size_pretty</span><span class="p">(</span><span class="n">pg_current_wal_lsn</span><span class="p">()</span> <span class="o">-</span> <span class="n">confirmed_flush_lsn</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">    <span class="k">AS</span> <span class="n">replication_lag_bytes</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span> <span class="n">pg_replication_slots</span>
</span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">slot_name</span> <span class="o">=</span> <span class="s1">&#39;&lt;slot_name&gt;&#39;</span><span class="p">;</span>
</span></span></code></pre></div><p>The result of this query is the amount of data your PostgreSQL cluster
must retain in its replication log because of this replication slot.
Typically, this means Materialize has not yet communicated back to
PostgreSQL that it has committed this data. A high value can indicate
that the source has fallen behind and that you might need to scale up
your ingestion cluster.</p>
</li>
</ol>
</li>
</ol>


## D. Explore your data

<p>With Materialize ingesting your PostgreSQL data into durable storage, you can
start exploring the data, computing real-time results that stay up-to-date as
new data arrives, and serving results efficiently.</p>
<ul>
<li>
<p>Explore your data with <a href="/sql/show-sources" ><code>SHOW SOURCES</code></a> and <a href="/sql/select/" ><code>SELECT</code></a>.</p>
</li>
<li>
<p>Compute real-time results in memory with <a href="/sql/create-view/" ><code>CREATE VIEW</code></a>
and <a href="/sql/create-index/" ><code>CREATE INDEX</code></a> or in durable
storage with <a href="/sql/create-materialized-view/" ><code>CREATE MATERIALIZED VIEW</code></a>.</p>
</li>
<li>
<p>Serve results to a PostgreSQL-compatible SQL client or driver with <a href="/sql/select/" ><code>SELECT</code></a>
or <a href="/sql/subscribe/" ><code>SUBSCRIBE</code></a> or to an external message broker with
<a href="/sql/create-sink/" ><code>CREATE SINK</code></a>.</p>
</li>
<li>
<p>Check out the <a href="/integrations/" >tools and integrations</a> supported by
Materialize.</p>
</li>
</ul>


## Considerations

<h3 id="schema-changes">Schema changes</h3>
<p>Materialize supports schema changes in the upstream database as follows:</p>
<h4 id="compatible-schema-changes-legacy-syntax">Compatible schema changes (Legacy syntax)</h4>
<blockquote>
<p><strong>Note:</strong> This section refer to the legacy <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a> that creates subsources as part of the
<code>CREATE SOURCE</code> operation.  To be able to handle the upstream column
additions and drops, see <a href="/sql/create-source/postgres-v2/" ><code>CREATE SOURCE (New Syntax)</code></a> and <a href="/sql/create-table" ><code>CREATE TABLE FROM SOURCE</code></a>.</p>
</blockquote>
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
<h4 id="incompatible-schema-changes">Incompatible schema changes</h4>
<p>All other schema changes to upstream tables will set the corresponding
Materialize tables into an error state, preventing reads from these tables.</p>
<p>To handle <a href="#incompatible-schema-changes" >incompatible schema changes</a>, drop
the affected table <a href="/sql/drop-table/" ><code>DROP TABLE</code></a> , and then, <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> to recreate the table with the
updated schema.</p>
<h3 id="publication-membership">Publication membership</h3>
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
<p>To mitigate this issue, if you need to drop and re-add a table to a
publication, ensure that you remove the table/subsource from the source
<em>before</em> re-adding it using the <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> command.</p>
<h3 id="supported-types">Supported types</h3>
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
<h3 id="truncation">Truncation</h3>
<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource(s)/table(s) in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>
<h3 id="inherited-tables">Inherited tables</h3>
<p>When using <a href="https://www.postgresql.org/docs/current/tutorial-inheritance.html" >PostgreSQL table inheritance</a>,
PostgreSQL serves data from <code>SELECT</code>s as if the inheriting tables&rsquo; data is
also present in the inherited table. However, both PostgreSQL&rsquo;s logical
replication and <code>COPY</code> only present data written to the tables themselves,
i.e. the inheriting data is <em>not</em> treated as part of the inherited table.</p>
<p>PostgreSQL sources use logical replication and <code>COPY</code> to ingest table data,
so inheriting tables&rsquo; data will only be ingested as part of the inheriting
table, i.e. in Materialize, the data will not be returned when serving
<code>SELECT</code>s from the inherited table.</p>
<ul>
<li>
<p>If using legacy syntax <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a>:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>ADD SUBSOURCE</code> and
create a new view (materialized or non-) that unions the new table.</p>
</li>
<li>
<p>If using new <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> syntax:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>CREATE TABLE .. FROM SOURCE</code> and create a new view (materialized or non-) that unions the new
table.</p>
</li>
</ul>
<h3 id="replication-slots">Replication slots</h3>
<p>Each source ingests the raw replication stream data for all tables in the
specified publication using <strong>a single</strong> replication slot. To manage
replication slots:</p>
<ul>
<li>
<p>For PostgreSQL 13+, set a reasonable value
for <a href="https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE" ><code>max_slot_wal_keep_size</code></a>
to limit the amount of storage used by replication slots.</p>
</li>
<li>
<p>If you stop using Materialize, or if either the Materialize instance or
the PostgreSQL instance crash, delete any replication slots. You can query
the <code>mz_internal.mz_postgres_sources</code> table to look up the name of the
replication slot created for each source.</p>
</li>
<li>
<p>If you delete all objects that depend on a source without also dropping
the source, the upstream replication slot remains and will continue to
accumulate data so that the source can resume in the future. To avoid
unbounded disk space usage, make sure to use <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> or manually delete the replication slot.</p>
</li>
</ul>
<h3 id="modifying-an-existing-source">Modifying an existing source</h3>
<p>When you add a new subsource to an existing source (<a href="/sql/alter-source/" ><code>ALTER SOURCE ... ADD SUBSOURCE ...</code></a>), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.
</p>



---

## PostgreSQL CDC using Kafka and Debezium


> **Warning:** You can use [Debezium](https://debezium.io/) to propagate Change Data Capture
> (CDC) data to Materialize from a PostgreSQL database, but we **strongly
> recommend** using the native [PostgreSQL](/sql/create-source/postgres/) source
> instead.
>


For help getting started with your own data, you can schedule a [free guided
trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).


Change Data Capture (CDC) allows you to track and propagate changes in a
PostgreSQL database to downstream consumers based on its Write-Ahead Log
(`WAL`). In this guide, we’ll cover how to use Materialize to create and
efficiently maintain real-time views with incrementally updated results
on top of CDC data.

## Kafka + Debezium

You can use [Debezium](https://debezium.io/) and the [Kafka source](/sql/create-source/kafka/#debezium-envelope)
to propagate CDC data from PostgreSQL to Materialize in the unlikely event that
using the[native PostgreSQL source](/sql/create-source/postgres/) is not an
option. Debezium captures row-level changes resulting from `INSERT`, `UPDATE`
and `DELETE` operations in the upstream database and publishes them as events
to Kafka using Kafka Connect-compatible connectors.

### A. Configure database

**Minimum requirements:** PostgreSQL 11+

Before deploying a Debezium connector, you need to ensure that the upstream
database is configured to support [logical replication](https://www.postgresql.org/docs/current/logical-replication.html).


**Self-hosted:**

As a _superuser_:

1. Check the [`wal_level` configuration](https://www.postgresql.org/docs/current/wal-configuration.html)
   setting:

    ```postgres
    SHOW wal_level;
    ```

    The default value is `replica`. For CDC, you'll need to set it to `logical`
    in the database configuration file (`postgresql.conf`). Keep in mind that
    changing the `wal_level` requires a restart of the PostgreSQL instance and
    can affect database performance.

1. Restart the database so all changes can take effect.



**AWS RDS:**

We recommend following the [AWS RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html#PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication)
documentation for detailed information on logical replication configuration and
best practices.

As a _superuser_ (`rds_superuser`):

1. Create a custom RDS parameter group and associate it with your instance. You
   will not be able to set custom parameters on the default RDS parameter groups.

1. In the custom RDS parameter group, set the `rds.logical_replication` static
   parameter to `1`.

1. Add the egress IP addresses associated with your Materialize region to the
   security group of the RDS instance. You can find these addresses by querying
   the `mz_egress_ips` table in Materialize.

1. Restart the database so all changes can take effect.



**AWS Aurora:**

> **Note:** Aurora Serverless (v1) [does **not** support](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless.html#aurora-serverless.limitations)
> logical replication, so it's not possible to use this service with
> Materialize.
>


We recommend following the [AWS Aurora](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html#AuroraPostgreSQL.Replication.Logical.Configure)
documentation for detailed information on logical replication configuration and
best practices.

As a _superuser_:

1. Create a DB cluster parameter group for your instance using the following
   settings:

    Set **Parameter group family** to your version of Aurora PostgreSQL.

    Set **Type** to **DB Cluster Parameter Group**.

1. In the DB cluster parameter group, set the `rds.logical_replication` static
   parameter to `1`.

1. In the DB cluster parameter group, set reasonable values for
   `max_replication_slots`, `max_wal_senders`, `max_logical_replication_workers`,
   and `max_worker_processes parameters`  based on your expected usage.

1. Add the egress IP addresses associated with your Materialize region to the
   security group of the DB instance. You can find these addresses by querying the
   `mz_egress_ips` table in Materialize.

1. Restart the database so all changes can take effect.



**Azure DB:**

We recommend following the [Azure DB for PostgreSQL](https://docs.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-logical#pre-requisites-for-logical-replication-and-logical-decoding)
documentation for detailed information on logical replication configuration and
best practices.

1. In the Azure portal, or using the Azure CLI, [enable logical replication](https://docs.microsoft.com/en-us/azure/postgresql/concepts-logical#set-up-your-server)
   for the PostgreSQL instance.

1. Add the egress IP addresses associated with your Materialize region to the
   list of allowed IP addresses under the "Connections security" menu. You can
   find these addresses by querying the `mz_egress_ips` table in Materialize.

1. Restart the database so all changes can take effect.



**Cloud SQL:**

We recommend following the [Cloud SQL for PostgreSQL](https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication#configuring-your-postgresql-instance)
documentation for detailed information on logical replication configuration and
best practices.

As a _superuser_ (`cloudsqlsuperuser`):

1. In the Google Cloud Console, enable logical replication by setting the
`cloudsql.logical_decoding` configuration parameter to `on`.

1. Add the egress IP addresses associated with your Materialize region to the
list of allowed IP addresses. You can find these addresses by querying the
`mz_egress_ips` table in Materialize.

1. Restart the database so all changes can take effect.





Once logical replication is enabled:

1. Grant enough privileges to ensure Debezium can operate in the database. The
   specific privileges will depend on how much control you want to give to the
   replication user, so we recommend following the [Debezium documentation](https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-replication-user-privileges).

1. If a table that you want to replicate has a **primary key** defined, you can
   use your default replica identity value. If a table you want to replicate
   has **no primary key** defined, you must set the replica identity value to
   `FULL`:

    ```postgres
    ALTER TABLE repl_table REPLICA IDENTITY FULL;
    ```

    This setting determines the amount of information that is written to the WAL
    in `UPDATE` and `DELETE` operations. Setting it to `FULL` will include the
    previous values of all the table’s columns in the change events.

    As a heads up, you should expect a performance hit in the database from
    increased CPU usage. For more information, see the
    [PostgreSQL documentation](https://www.postgresql.org/docs/current/logical-replication-publication.html).

### B. Deploy Debezium

**Minimum requirements:** Debezium 1.5+

Debezium is deployed as a set of Kafka Connect-compatible connectors, so you
first need to define a SQL connector configuration and then start the connector
by adding it to Kafka Connect.

> **Warning:** If you deploy the PostgreSQL Debezium connector in [Confluent Cloud](https://docs.confluent.io/cloud/current/connectors/cc-mysql-source-cdc-debezium.html),
> you **must** override the default value of `After-state only` to `false`.
>
>



**Debezium 1.5+:**

1. Create a connector configuration file and save it as `register-postgres.json`:

    ```json
    {
        "name": "your-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "plugin.name":"pgoutput",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "postgres",
            "database.password": "postgres",
            "database.dbname" : "postgres",
            "database.server.name": "pg_repl",
            "table.include.list": "public.table1",
            "publication.autocreate.mode":"filtered",
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schemas.enable": false
        }
    }
    ```

    You can read more about each configuration property in the [Debezium documentation](https://debezium.io/documentation/reference/1.6/connectors/postgresql.html#postgresql-connector-properties).
    By default, the connector writes events for each table to a Kafka topic
    named `serverName.schemaName.tableName`.

1. Start the PostgreSQL Debezium connector using the configuration file:

    ```bash
    export CURRENT_HOST='<your-host>'

    curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" \
    http://$CURRENT_HOST:8083/connectors/ -d @register-postgres.json
    ```

1. Check that the connector is running:

    ```bash
    curl http://$CURRENT_HOST:8083/connectors/your-connector/status
    ```

    The first time it connects to a PostgreSQL server, Debezium takes a
    [consistent snapshot](https://debezium.io/documentation/reference/1.6/connectors/postgresql.html#postgresql-snapshots)
    of the tables selected for replication, so you should see that the
    pre-existing records in the replicated table are initially pushed into your
    Kafka topic:

    ```bash
    /usr/bin/kafka-avro-console-consumer \
      --bootstrap-server kafka:9092 \
      --from-beginning \
      --topic pg_repl.public.table1
    ```


**Debezium 2.0+:**

1. Beginning with Debezium 2.0.0, Confluent Schema Registry support is not
   included in the Debezium containers. To enable the Confluent Schema Registry
   for a Debezium container, install the following Confluent Avro converter JAR
   files into the Connect plugin directory:

    * `kafka-connect-avro-converter`
    * `kafka-connect-avro-data`
    * `kafka-avro-serializer`
    * `kafka-schema-serializer`
    * `kafka-schema-registry-client`
    * `common-config`
    * `common-utils`

    You can read more about this in the [Debezium documentation](https://debezium.io/documentation/reference/stable/configuration/avro.html#deploying-confluent-schema-registry-with-debezium-containers).

1. Create a connector configuration file and save it as
   `register-postgres.json`:

    ```json
    {
        "name": "your-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "plugin.name":"pgoutput",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "postgres",
            "database.password": "postgres",
            "database.dbname" : "postgres",
            "topic.prefix": "pg_repl",
            "schema.include.list": "public",
            "table.include.list": "public.table1",
            "publication.autocreate.mode":"filtered",
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url": "http://<scheme-registry>:8081",
            "value.converter.schema.registry.url": "http://<scheme-registry>:8081",
            "value.converter.schemas.enable": false
        }
    }
    ```

    You can read more about each configuration property in the [Debezium documentation](https://debezium.io/documentation/reference/2.4/connectors/postgresql.html#postgresql-connector-properties).
    By default, the connector writes events for each table to a Kafka topic
    named `serverName.schemaName.tableName`.

1. Start the Debezium Postgres connector using the configuration file:

    ```bash
    export CURRENT_HOST='<your-host>'

    curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" \
    http://$CURRENT_HOST:8083/connectors/ -d @register-postgres.json
    ```

1. Check that the connector is running:

    ```bash
    curl http://$CURRENT_HOST:8083/connectors/your-connector/status
    ```

    The first time it connects to a Postgres server, Debezium takes a
    [consistent snapshot](https://debezium.io/documentation/reference/1.6/connectors/postgresql.html#postgresql-snapshots)
    of the tables selected for replication, so you should see that the
    pre-existing records in the replicated table are initially pushed into your
    Kafka topic:

    ```bash
    /usr/bin/kafka-avro-console-consumer \
      --bootstrap-server kafka:9092 \
      --from-beginning \
      --topic pg_repl.public.table1
    ```




### C. Create a source

<div class="note">
  <strong class="gutter">NOTE:</strong> Currently, Materialize only supports Avro-encoded Debezium records. If you're interested in JSON support, please reach out in the community Slack or submit a <a href="https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests">feature request</a>.
</div>


Debezium emits change events using an envelope that contains detailed
information about upstream database operations, like the `before` and `after`
values for each record. To create a source that interprets the
[Debezium envelope](/sql/create-source/kafka/#debezium-envelope) in Materialize:

```mzsql
CREATE SOURCE kafka_repl
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'pg_repl.public.table1')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
    ENVELOPE DEBEZIUM;
```

By default, the source will be created in the active cluster; to use a different
cluster, use the `IN CLUSTER` clause.

This allows you to replicate tables with `REPLICA IDENTITY DEFAULT`, `INDEX`, or
`FULL`.

### D. Create a view on the source

A [view](/concepts/views/) saves a query under a name to provide a shorthand for
referencing the query. During view creation, the underlying query is not
executed.

```mzsql
CREATE VIEW cnt_table1 AS
    SELECT field1,
           COUNT(*) AS cnt
    FROM kafka_repl
    GROUP BY field1;
```


### E. Create an index on the view

In Materialize, [indexes](/concepts/indexes) on views compute and, as new data
arrives, incrementally update view results in memory within a
[cluster](/concepts/clusters/) instead of recomputing the results from scratch.

Create an index on `cnt_table1` view. Then, as new change events stream in
through Kafka (as the result of `INSERT`, `UPDATE` and `DELETE` operations in
the upstream database), the index incrementally updates the view
results in memory, such that the in-memory up-to-date results are immediately
available and computationally free to query.

```mzsql
CREATE INDEX idx_cnt_table1_field1 ON cnt_table1(field1);
```

For best practices on when to index a view, see
[Indexes](/concepts/indexes/) and [Views](/concepts/views/).



---

## Troubleshooting


This section contains troubleshooting guides for specific errors you may
encounter when using PostgreSQL sources in Materialize. These guides focus on
errors that are unique to the PostgreSQL replication workflow, including issues
with replication slots, WAL management, and other CDC-specific scenarios.

For general data ingestion troubleshooting that applies to all source types, see
the main [Troubleshooting](/ingest-data/troubleshooting/) guide. For answers to
common questions about PostgreSQL sources, see the [FAQ](/ingest-data/postgres/faq/).

## Troubleshooting guides

| Guide | Description |
|-------|-------------|
| [Slot overcompacted](/ingest-data/postgres/slot-overcompacted/) | Resolve errors when PostgreSQL removes WAL data before Materialize can read it |
| [Connection Closed](/ingest-data/postgres/connection-closed/) | Resolve unexpected networking connection terminations between Materialize and PostgreSQL |


---

## Troubleshooting: Connection closed


This guide helps you troubleshoot and resolve the "connection closed" error that
can occur with PostgreSQL sources in Materialize.

## What this error means

When you see an error like:

```nofmt
postgres: connection closed
```

This means the network connection between Materialize and your PostgreSQL
database was unexpectedly terminated. The connection that Materialize uses to
replicate data from PostgreSQL was closed, interrupting the replication process.

> **Note:** This error is known to occur during Materialize maintenance windows and can be
> safely ignored if that is the case. Sources will automatically reconnect after
> maintenance is complete.
>


## Common causes

- **Network instability**: Intermittent network issues between Materialize and
  your PostgreSQL database can cause connections to drop.
- **Firewall or security group changes**: Changes to firewall rules, security
  groups, or network policies may block or terminate existing connections.
- **Database restarts or maintenance**: PostgreSQL server restarts, maintenance
  operations, or failovers can close active connections.
- **Connection timeouts**: Idle connection timeouts configured on PostgreSQL,
  load balancers, or network infrastructure may close connections that appear
  inactive.
- **Resource exhaustion**: PostgreSQL running out of available connections or
  memory may forcibly close connections.
- **Load balancer issues**: If connecting through a load balancer or proxy,
  connection pooling or timeout settings may cause unexpected disconnections.
- **Client certificate expiration**: If using SSL/TLS with client certificates,
  expired certificates can cause connection failures.

## Diagnosing the issue

### Check connection parameters

Verify your PostgreSQL connection configuration in Materialize:

```mzsql
SELECT name, connection_type
FROM mz_connections
WHERE type = 'postgres';
```

### Check for network connectivity

Test basic connectivity from Materialize to your PostgreSQL host. Verify that:

- The PostgreSQL host is reachable
- Firewall rules allow traffic
- DNS resolution is working correctly

### Check PostgreSQL logs

Review your PostgreSQL logs for connection-related messages:

```sql
SELECT * FROM pg_stat_activity
WHERE state_change < NOW() - INTERVAL '60 minutes';
```

Look for log entries indicating:

- Connection timeouts
- Authentication failures
- Resource exhaustion
- Server shutdowns or restarts

### Check PostgreSQL connection limits

Verify you haven't exceeded connection limits:

```sql
SELECT
  max_conn,
  used,
  res_for_super,
  max_conn - used - res_for_super AS remaining
FROM
  (SELECT count(*) AS used FROM pg_stat_activity) t1,
  (SELECT setting::int AS res_for_super FROM pg_settings WHERE name = 'superuser_reserved_connections') t2,
  (SELECT setting::int AS max_conn FROM pg_settings WHERE name = 'max_connections') t3;
```

### Check for idle connection timeouts

Check timeout settings that might close connections:

```sql
SHOW tcp_keepalives_idle;
SHOW tcp_keepalives_interval;
SHOW tcp_keepalives_count;
SHOW statement_timeout;
```

## Resolution

### Immediate fix: Materialize will automatically reconnect

In most cases, Materialize will automatically attempt to reconnect to PostgreSQL
when a connection is closed. Monitor your source to see if it recovers on its
own.

You can check the source status with:

```mzsql
SELECT *
FROM mz_internal.mz_source_statuses
WHERE id = 'your_source_id';
```

### Long-term fixes

**1. Increase connection keepalive settings**

Configure PostgreSQL to keep connections alive longer by adjusting TCP keepalive
settings in `postgresql.conf`:

```nofmt
tcp_keepalives_idle = 60
tcp_keepalives_interval = 10
tcp_keepalives_count = 5
```

Then reload the configuration:

```sql
SELECT pg_reload_conf();
```

**2. Configure connection timeout on network devices**

If using load balancers or proxies, ensure their idle timeout settings are
appropriate for long-lived replication connections:

- Set idle timeouts to at least 10-15 minutes
- Configure keepalive probes to detect stale connections

**3. Increase PostgreSQL connection limits**

If hitting connection limits, increase `max_connections` in `postgresql.conf`:

```nofmt
max_connections = 200
```

> **Note:** Increasing max_connections may require more shared memory. You may also need to
> adjust `shared_buffers` and other memory settings.
>


**4. Review and update SSL certificates**

If using SSL, verify certificate validity:

```sql
SELECT ssl,
       sslversion,
       sslcipher
FROM pg_stat_ssl
JOIN pg_stat_activity ON pg_stat_ssl.pid = pg_stat_activity.pid;
```

Ensure certificates are renewed before expiration.

**5. Implement network stability improvements**

- Use dedicated network paths for replication traffic
- Ensure adequate bandwidth between Materialize and PostgreSQL
- Minimize network hops and latency
- Consider using VPC peering or private connectivity options

## Prevention

**Best practices to avoid this error:**

- Configure appropriate TCP keepalive settings on PostgreSQL.
- Set reasonable connection timeouts on load balancers and proxies (10+ minutes
  for replication).
- Monitor network connectivity and latency between Materialize and PostgreSQL.
- Ensure PostgreSQL has adequate connection capacity (`max_connections`).
- Use SSL/TLS with valid, up-to-date certificates.
- Implement monitoring and alerting for connection failures.
- Schedule PostgreSQL maintenance during low-traffic periods.
- Use connection poolers like PgBouncer carefully (they may not work well with
  replication slots).

## Provider-specific considerations

### Amazon RDS

RDS may terminate idle connections after a period of inactivity. Ensure:

- Security groups allow traffic from Materialize
- Parameter groups have appropriate keepalive settings
- RDS maintenance windows are scheduled appropriately

### Google Cloud SQL

Cloud SQL has connection limits based on instance size:

- Monitor connection usage via Cloud SQL metrics
- Consider upgrading instance size if hitting limits
- Use private IP connectivity when possible for better stability

### Azure Database for PostgreSQL

Azure databases have connection limits and idle timeout policies:

- Review connection limits for your service tier
- Configure firewall rules to allow Materialize IP addresses
- Enable connection retry logic by ensuring Materialize can reconnect

### Self-managed PostgreSQL

You have full control over connection settings:

- Configure keepalive settings as recommended above
- Monitor system resources (memory, connections)
- Implement robust firewall rules that don't interfere with long-lived
  connections
- Consider using dedicated hardware or VMs for database hosting


---

## Troubleshooting: Slot overcompacted


This guide helps you troubleshoot and resolve the "slot overcompacted" error that
can occur with PostgreSQL sources in Materialize.

## What this error means

When you see an error like:

```nofmt
postgres: slot overcompacted. Requested LSN 181146050392 but only LSNs >= 332129862840 are available
```

This means Materialize tried to read from a PostgreSQL replication slot at a
specific Log Sequence Number (LSN), but that data has already been removed from
PostgreSQL's Write-Ahead Log (WAL). The WAL was "compacted" or cleaned up before
Materialize could read the data it needed.

## Common causes

- **WAL retention limits**: PostgreSQL has a setting called
  `max_slot_wal_keep_size` that limits how much WAL data is kept for replication
  slots. If this value is too small, PostgreSQL may delete WAL data that
  Materialize still needs.
- **Long-running snapshot operations**: If your source is taking a long time to
  complete its initial snapshot (e.g., for very large tables), the upstream
  PostgreSQL database may clean up WAL data before Materialize finishes.
- **Paused or slow replication**: If your Materialize cluster is paused,
  undersized, or experiencing performance issues, the replication slot may not
  advance quickly enough, causing PostgreSQL to reclaim WAL space.
- **Provider-specific WAL policies**: Some managed PostgreSQL providers (such
  as Neon) may have aggressive WAL cleanup policies that can trigger this error
  more frequently.

## Diagnosing the issue

### Check replication slot status in PostgreSQL

Connect to your PostgreSQL database and run:

```sql
SELECT
  slot_name,
  active,
  restart_lsn,
  confirmed_flush_lsn,
  pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS replication_lag
FROM pg_replication_slots
WHERE slot_name LIKE 'materialize%';
```

Look for:

- **Large replication lag** - Indicates Materialize is falling behind
- **Inactive slots** - May indicate connection issues

### Check PostgreSQL WAL settings

Check your `max_slot_wal_keep_size` setting:

```sql
SHOW max_slot_wal_keep_size;
```

If this is set too low (or to `-1` which means unlimited but may be overridden
by provider policies), you may experience this error.

### Check for long-running transactions

Long-running transactions can prevent WAL cleanup:

```sql
SELECT
  pid,
  age(clock_timestamp(), xact_start) AS transaction_age,
  state,
  query
FROM pg_stat_activity
WHERE xact_start IS NOT NULL
ORDER BY age DESC;
```

## Resolution

### Immediate fix: Recreate the source

> **Warning:** This will cause Materialize to take a new snapshot, which may take
> time and temporarily increase load on your PostgreSQL database.
>


Once a slot has been overcompacted, the data is permanently lost from the WAL.
You must **drop and recreate the source**. Dropping the source will also drop
any dependent objects; be prepared to recreate them as part of the recovery process.

### Long-term fixes

**1. Increase WAL retention**

Increase `max_slot_wal_keep_size` in your PostgreSQL configuration:

```sql
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';
SELECT pg_reload_conf();
```

The appropriate value depends on:

- Your data change rate
- How long snapshots take
- How often you pause/unpause clusters

**2. Ensure adequate cluster sizing**

Make sure your Materialize source cluster has enough resources to keep up with
replication:

```mzsql
ALTER CLUSTER your_source_cluster SET (SIZE = 'M.1-large');
```

**3. Monitor replication lag**

Regularly check that your sources are keeping up:

```mzsql
-- Check source statistics
SELECT *
FROM mz_internal.mz_source_statistics
WHERE id = 'your_source_id';
```

## Prevention

**Best practices to avoid this error:**

- Set `max_slot_wal_keep_size` to a value appropriate for your workload
  (typically 5-10GB or more).
- Size your source clusters appropriately for your data ingestion rate.
- Avoid pausing clusters for extended periods when sources are active.
- Monitor replication lag regularly.
- Consider limiting initial snapshot size by using `FOR TABLES` instead of
  `FOR ALL TABLES` if you have very large databases.
- If using a managed PostgreSQL provider, verify their replication slot and WAL
  retention policies.

## Provider-specific considerations

### Neon

Neon has been observed to have more aggressive WAL cleanup policies. If you're
using Neon:

- Monitor replication lag more frequently.
- Consider using a dedicated Neon branch for replication.
- Contact Neon support about their replication slot retention policies.

### Amazon RDS

RDS respects `max_slot_wal_keep_size` but also has instance storage limits.
Ensure your RDS instance has adequate storage for WAL retention.

### Self-managed PostgreSQL

You have full control over WAL retention settings, but ensure you also monitor
disk space to prevent storage issues.
