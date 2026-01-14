<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)  /  [CREATE
SOURCE](/docs/sql/create-source/)

</div>

# CREATE SOURCE: MySQL

[`CREATE SOURCE`](/docs/sql/create-source/) connects Materialize to an
external system you want to read data from, and provides details about
how to decode and interpret that data.

Materialize supports MySQL (5.7+) as a real-time data source. To connect
to a MySQL database, you first need to tweak its configuration to enable
[GTID-based binary log (binlog) replication](#change-data-capture), and
then [create a connection](#creating-a-connection) in Materialize that
specifies access and authentication parameters.

<div class="note">

**NOTE:** Connections using AWS PrivateLink is for Materialize Cloud
only.

</div>

## Syntax

<div class="note">

**NOTE:** Although `schema` and `database` are [synonyms in
MySQL](https://dev.mysql.com/doc/refman/8.0/en/glossary.html#glos_schema),
the MySQL source documentation and syntax **standardize on `schema`** as
the preferred keyword.

</div>

<div class="highlight">

``` chroma
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM MYSQL CONNECTION <connection_name> [
  (
    [TEXT COLUMNS ( <col1> [, ...] ) ]
    [, EXCLUDE COLUMNS ( <col1> [, ...] ) ]
  )
]
<FOR ALL TABLES | FOR SCHEMAS ( <schema1> [, ...] ) | FOR TABLES ( <table1> [AS <subsrc_name>] [, ...] )>
[EXPOSE PROGRESS AS <progress_subsource_name>]
[WITH (RETAIN HISTORY FOR <retention_period>)]
```

</div>

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Syntax element</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>&lt;src_name&gt;</code></td>
<td>The name for the source.</td>
</tr>
<tr>
<td><strong>IF NOT EXISTS</strong></td>
<td>Optional. If specified, do not throw an error if a source with the
same name already exists. Instead, issue a notice and skip the source
creation.</td>
</tr>
<tr>
<td><strong>IN CLUSTER</strong> <code>&lt;cluster_name&gt;</code></td>
<td>Optional. The <a href="/docs/sql/create-cluster">cluster</a> to
maintain this source.</td>
</tr>
<tr>
<td><strong>CONNECTION</strong>
<code>&lt;connection_name&gt;</code></td>
<td>The name of the MySQL connection to use in the source. For details
on creating connections, check the <a
href="/docs/sql/create-connection/#mysql"><code>CREATE CONNECTION</code></a>
documentation page.</td>
</tr>
<tr>
<td><strong>TEXT COLUMNS</strong> ( <code>&lt;col1&gt;</code> [, …]
)</td>
<td>Optional. Decode data as <code>text</code> for specific columns that
contain MySQL types that are <a href="#supported-types">unsupported in
Materialize</a>.</td>
</tr>
<tr>
<td><strong>EXCLUDE COLUMNS</strong> ( <code>&lt;col1&gt;</code> [, …]
)</td>
<td>Optional. Exclude specific columns that cannot be decoded or should
not be included in the subsources created in Materialize.</td>
</tr>
<tr>
<td><strong>FOR</strong>
<code>&lt;table_schema_specification&gt;</code></td>
<td><p>Specifies which tables to create subsources for. The following
<code>&lt;table_schema_specification&gt;</code>s are supported:</p>
<table>
<thead>
<tr>
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>ALL TABLES</code></td>
<td>Create subsources for all tables in all schemas upstream. The <a
href="https://dev.mysql.com/doc/refman/8.3/en/system-schema.html"><code>mysql</code>
system schema</a> is ignored.</td>
</tr>
<tr>
<td><code>SCHEMAS ( &lt;schema1&gt; [, ...] )</code></td>
<td>Create subsources for specific schemas upstream.</td>
</tr>
<tr>
<td><code>TABLES ( &lt;table1&gt; [AS &lt;subsrc_name&gt;] [, ...] )</code></td>
<td>Create subsources for specific tables upstream. Requires
fully-qualified table names
(<code>&lt;schema1&gt;.&lt;table1&gt;</code>).</td>
</tr>
</tbody>
</table></td>
</tr>
<tr>
<td><strong>EXPOSE PROGRESS AS</strong>
<code>&lt;progress_subsource_name&gt;</code></td>
<td>Optional. The name of the progress collection for the source. If
this is not specified, the progress collection will be named
<code>&lt;src_name&gt;_progress</code>. For more information, see <a
href="#monitoring-source-progress">Monitoring source progress</a>.</td>
</tr>
<tr>
<td><strong>WITH</strong> (<code>&lt;with_option&gt;</code> [, …])</td>
<td><p>Optional. The following <code>&lt;with_option&gt;</code>s are
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
<td><code>RETAIN HISTORY FOR &lt;retention_period&gt;</code></td>
<td><em><strong>Private preview.</strong> This option has known
performance or stability issues and is under active development.</em>
Duration for which Materialize retains historical data, which is useful
to implement <a
href="/docs/transform-data/patterns/durable-subscriptions/#history-retention-period">durable
subscriptions</a>. Accepts positive <a
href="/docs/sql/types/interval/">interval</a> values (e.g.
<code>'1hr'</code>). Default: <code>1s</code>.</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

### `CONNECTION` options

| Field | Value | Description |
|----|----|----|
| `EXCLUDE COLUMNS` | A list of fully-qualified names | Exclude specific columns that cannot be decoded or should not be included in the subsources created in Materialize. |
| `TEXT COLUMNS` | A list of fully-qualified names | Decode data as `text` for specific columns that contain MySQL types that are [unsupported in Materialize](#supported-types). |

## Features

### Change data capture

<div class="note">

**NOTE:** For step-by-step instructions on enabling GTID-based binlog
replication for your MySQL service, see the integration guides: [Amazon
RDS](/docs/ingest-data/mysql/amazon-rds/), [Amazon
Aurora](/docs/ingest-data/mysql/amazon-aurora/), [Azure
DB](/docs/ingest-data/mysql/azure-db/), [Google Cloud
SQL](/docs/ingest-data/mysql/google-cloud-sql/),
[Self-hosted](/docs/ingest-data/mysql/self-hosted/).

</div>

The source uses MySQL’s binlog replication protocol to **continually
ingest changes** resulting from `INSERT`, `UPDATE` and `DELETE`
operations in the upstream database. This process is known as *change
data capture*.

The replication method used is based on [global transaction identifiers
(GTIDs)](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids.html),
and guarantees **transactional consistency** — any operation inside a
MySQL transaction is assigned the same timestamp in Materialize, which
means that the source will never show partial results based on partially
replicated transactions.

Before creating a source in Materialize, you **must** configure the
upstream MySQL database for GTID-based binlog replication. Ensure the
upstream MySQL database has been configured for GTID-based binlog
replication:

| MySQL Configuration | Value | Notes |
|----|----|----|
| `log_bin` | `ON` |  |
| `binlog_format` | `ROW` | [Deprecated as of MySQL 8.0.34](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format). Newer versions of MySQL default to row-based logging. |
| `binlog_row_image` | `FULL` |  |
| `gtid_mode` | `ON` |  |
| `enforce_gtid_consistency` | `ON` |  |
| `replica_preserve_commit_order` | `ON` | Only required when connecting Materialize to a read-replica. |

If you’re running MySQL using a managed service, additional
configuration changes might be required. For step-by-step instructions
on enabling GTID-based binlog replication for your MySQL service, see
the integration guides.

#### Binlog retention

<div class="warning">

**WARNING!** If Materialize tries to resume replication and finds GTID
gaps due to missing binlog files, the source enters an errored state and
you have to drop and recreate it.

</div>

By default, MySQL retains binlog files for **30 days** (i.e., 2592000
seconds) before automatically removing them. This is configurable via
the
[`binlog_expire_logs_seconds`](https://dev.mysql.com/doc/mysql-replication-excerpt/8.0/en/replication-options-binary-log.html#sysvar_binlog_expire_logs_seconds)
system variable. We recommend using the default value for this
configuration in order to not compromise Materialize’s ability to resume
replication in case of failures or restarts.

In some MySQL managed services, binlog expiration can be overriden by a
service-specific configuration parameter. It’s important that you
double-check if such a configuration exists, and ensure it’s set to the
maximum interval available.

As an example, [Amazon RDS for
MySQL](/docs/ingest-data/mysql/amazon-rds/) has its own configuration
parameter for binlog retention
([`binlog retention hours`](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/mysql-stored-proc-configuring.html#mysql_rds_set_configuration-usage-notes.binlog-retention-hours))
that overrides `binlog_expire_logs_seconds` and is set to `NULL` by
default.

#### Creating a source

Materialize ingests the raw replication stream data for all (or a
specific set of) tables in your upstream MySQL database.

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR ALL TABLES;
```

</div>

When you define a source, Materialize will automatically:

1.  Create a **subsource** for each original table upstream, and perform
    an initial, snapshot-based sync of the tables before it starts
    ingesting change events.

    <div class="highlight">

    ``` chroma
    SHOW SOURCES;
    ```

    </div>

    ```
             name         |   type    |  cluster  |
    ----------------------+-----------+------------
     mz_source            | mysql     |
     mz_source_progress   | progress  |
     table_1              | subsource |
     table_2              | subsource |
    ```

2.  Incrementally update any materialized or indexed views that depend
    on the source as change events stream in, as a result of `INSERT`,
    `UPDATE` and `DELETE` operations in the upstream MySQL database.

##### MySQL schemas

`CREATE SOURCE` will attempt to create each upstream table in the same
schema as the source. This may lead to naming collisions if, for
example, you are replicating `schema1.table_1` and `schema2.table_1`.
Use the `FOR TABLES` clause to provide aliases for each upstream table,
in such cases, or to specify an alternative destination schema in
Materialize.

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR TABLES (schema1.table_1 AS s1_table_1, schema2.table_1 AS s2_table_1);
```

</div>

### Monitoring source progress

By default, MySQL sources expose progress metadata as a subsource that
you can use to monitor source **ingestion progress**. The name of the
progress subsource can be specified when creating a source using the
`EXPOSE PROGRESS AS` clause; otherwise, it will be named
`<src_name>_progress`.

The following metadata is available for each source as a progress
subsource:

| Field | Type | Details |
|----|----|----|
| `source_id_lower` | [`uuid`](/docs/sql/types/uuid/) | The lower-bound GTID `source_id` of the GTIDs covered by this range. |
| `source_id_upper` | [`uuid`](/docs/sql/types/uuid/) | The upper-bound GTID `source_id` of the GTIDs covered by this range. |
| `transaction_id` | [`uint8`](/docs/sql/types/uint/#uint8-info) | The `transaction_id` of the next GTID possible from the GTID `source_id`s covered by this range. |

And can be queried using:

<div class="highlight">

``` chroma
SELECT transaction_id
FROM <src_name>_progress;
```

</div>

Progress metadata is represented as a [GTID
set](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-concepts.html)
of future possible GTIDs, which is similar to the
[`gtid_executed`](https://dev.mysql.com/doc/refman/8.0/en/replication-options-gtids.html#sysvar_gtid_executed)
system variable on a MySQL replica. The reported `transaction_id` should
increase as Materialize consumes **new** binlog records from the
upstream MySQL database. For more details on monitoring source ingestion
progress and debugging related issues, see
[Troubleshooting](/docs/ops/troubleshooting/).

## Known limitations

### Schema changes

<div class="note">

**NOTE:** Work to more smoothly support ddl changes to upstream tables
is currently in progress. The work introduces the ability to re-ingest
the same upstream table under a new schema and switch over without
downtime.

</div>

Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes

- Adding columns to tables. Materialize will **not ingest** new columns
  added upstream unless you use
  [`DROP SOURCE`](/docs/sql/alter-source/#context) to first drop the
  affected subsource, and then add the table back to the source using
  [`ALTER SOURCE...ADD SUBSOURCE`](/docs/sql/alter-source/).

- Dropping columns that were added after the source was created. These
  columns are never ingested, so you can drop them without issue.

- Adding or removing `NOT NULL` constraints to tables that were nullable
  when the source was created.

#### Incompatible schema changes

All other schema changes to upstream tables will set the corresponding
subsource into an error state, which prevents you from reading from the
subsource.

To handle incompatible [schema changes](#schema-changes), use
[`DROP SOURCE`](/docs/sql/alter-source/#context) to first drop the
affected subsource, and then
[`ALTER SOURCE...ADD SUBSOURCE`](/docs/sql/alter-source/) to add the
subsource back to the source. When you add the subsource, it will have
the updated schema from the corresponding upstream table.

### Supported types

Materialize natively supports the following MySQL types:

- `bigint`
- `binary`
- `bit`
- `blob`
- `boolean`
- `char`
- `date`
- `datetime`
- `decimal`
- `double`
- `float`
- `int`
- `json`
- `longblob`
- `longtext`
- `mediumblob`
- `mediumint`
- `mediumtext`
- `numeric`
- `real`
- `smallint`
- `text`
- `time`
- `timestamp`
- `tinyblob`
- `tinyint`
- `tinytext`
- `varbinary`
- `varchar`

When replicating tables that contain the **unsupported [data
types](/docs/sql/types/)**, you can:

- Use [`TEXT COLUMNS`
  option](/docs/sql/create-source/mysql/#handling-unsupported-types) for
  the following unsupported MySQL types:

  - `enum`
  - `year`

  The specified columns will be treated as `text` and will not offer the
  expected MySQL type features.

- Use the
  [`EXCLUDE COLUMNS`](/docs/sql/create-source/mysql/#excluding-columns)
  option to exclude any columns that contain unsupported data types.

### Truncation

Avoid truncating upstream tables that are being replicated into
Materialize. If a replicated upstream table is truncated, the
corresponding subsource in Materialize becomes inaccessible and will not
produce any data until it is recreated.

Instead of truncating, use an unqualified `DELETE` to remove all rows
from the upstream table:

<div class="highlight">

``` chroma
DELETE FROM t;
```

</div>

### Modifying an existing source

When you add a new subsource to an existing source
([`ALTER SOURCE ... ADD SUBSOURCE ...`](/docs/sql/alter-source/)),
Materialize starts the snapshotting process for the new subsource.
During this snapshotting, the data ingestion for the existing subsources
for the same source is temporarily blocked. As such, if possible, you
can resize the cluster to speed up the snapshotting process and once the
process finishes, resize the cluster for steady-state.

## Examples

<div class="important">

**! Important:** Before creating a MySQL source, you must enable
GTID-based binlog replication in the upstream database. For step-by-step
instructions, see the integration guide for your MySQL service: [Amazon
RDS](/docs/ingest-data/mysql/amazon-rds/), [Amazon
Aurora](/docs/ingest-data/mysql/amazon-aurora/), [Azure
DB](/docs/ingest-data/mysql/azure-db/), [Google Cloud
SQL](/docs/ingest-data/mysql/google-cloud-sql/),
[Self-hosted](/docs/ingest-data/mysql/self-hosted/).

</div>

### Creating a connection

A connection describes how to connect and authenticate to an external
system you want Materialize to read data from.

Once created, a connection is **reusable** across multiple
`CREATE SOURCE` statements. For more details on creating connections,
check the [`CREATE CONNECTION`](/docs/sql/create-connection/#mysql)
documentation page.

<div class="highlight">

``` chroma
CREATE SECRET mysqlpass AS '<MYSQL_PASSWORD>';

CREATE CONNECTION mysql_connection TO MYSQL (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 3306,
    USER 'materialize',
    PASSWORD SECRET mysqlpass
);
```

</div>

If your MySQL server is not exposed to the public internet, you can
[tunnel the
connection](/docs/sql/create-connection/#network-security-connections)
through an AWS PrivateLink service (Materialize Cloud) or an SSH bastion
host SSH bastion host.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-aws-privatelink-materialize-cloud" class="tab-pane"
title="AWS PrivateLink (Materialize Cloud)">

<div class="note">

**NOTE:** Connections using AWS PrivateLink is for Materialize Cloud
only.

</div>

<div class="highlight">

``` chroma
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
   SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
   AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);

CREATE CONNECTION mysql_connection TO MYSQL (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 3306,
    USER 'root',
    PASSWORD SECRET mysqlpass,
    AWS PRIVATELINK privatelink_svc
);
```

</div>

For step-by-step instructions on creating AWS PrivateLink connections
and configuring an AWS PrivateLink service to accept connections from
Materialize, check [this
guide](/docs/ops/network-security/privatelink/).

</div>

<div id="tab-ssh-tunnel" class="tab-pane" title="SSH tunnel">

<div class="highlight">

``` chroma
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize'
);
```

</div>

<div class="highlight">

``` chroma
CREATE CONNECTION mysql_connection TO MYSQL (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    SSH TUNNEL ssh_connection
);
```

</div>

For step-by-step instructions on creating SSH tunnel connections and
configuring an SSH bastion server to accept connections from
Materialize, check [this guide](/docs/ops/network-security/ssh-tunnel/).

</div>

</div>

</div>

### Creating a source

*Create subsources for all tables in MySQL*

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
    FROM MYSQL CONNECTION mysql_connection
    FOR ALL TABLES;
```

</div>

*Create subsources for all tables from specific schemas in MySQL*

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR SCHEMAS (mydb, project);
```

</div>

*Create subsources for specific tables in MySQL*

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR TABLES (mydb.table_1, mydb.table_2 AS alias_table_2);
```

</div>

#### Handling unsupported types

If you’re replicating tables that use [data types
unsupported](#supported-types) by Materialize, use the `TEXT COLUMNS`
option to decode data as `text` for the affected columns. This option
expects the upstream fully-qualified names of the replicated table and
column (i.e. as defined in your MySQL database).

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection (
    TEXT COLUMNS (mydb.table_1.column_of_unsupported_type)
  )
  FOR ALL TABLES;
```

</div>

#### Excluding columns

MySQL doesn’t provide a way to filter out columns from the replication
stream. To exclude specific upstream columns from being ingested, use
the `EXCLUDE COLUMNS` option.

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection (
    EXCLUDE COLUMNS (mydb.table_1.column_to_ignore)
  )
  FOR ALL TABLES;
```

</div>

### Handling errors and schema changes

<div class="note">

**NOTE:** Work to more smoothly support ddl changes to upstream tables
is currently in progress. The work introduces the ability to re-ingest
the same upstream table under a new schema and switch over without
downtime.

</div>

To handle upstream [schema changes](#schema-changes) or errored
subsources, use the [`DROP SOURCE`](/docs/sql/alter-source/#context)
syntax to drop the affected subsource, and then
[`ALTER SOURCE...ADD SUBSOURCE`](/docs/sql/alter-source/) to add the
subsource back to the source.

<div class="highlight">

``` chroma
-- List all subsources in mz_source
SHOW SUBSOURCES ON mz_source;

-- Get rid of an outdated or errored subsource
DROP SOURCE table_1;

-- Start ingesting the table with the updated schema or fix
ALTER SOURCE mz_source ADD SUBSOURCE table_1;
```

</div>

## Related pages

- [`CREATE SECRET`](/docs/sql/create-secret)
- [`CREATE CONNECTION`](/docs/sql/create-connection)
- [`CREATE SOURCE`](../)
- MySQL integration guides:
  - [Amazon RDS](/docs/ingest-data/mysql/amazon-rds/)
  - [Amazon Aurora](/docs/ingest-data/mysql/amazon-aurora/)
  - [Azure DB](/docs/ingest-data/mysql/azure-db/)
  - [Google Cloud SQL](/docs/ingest-data/mysql/google-cloud-sql/)
  - [Self-hosted](/docs/ingest-data/mysql/self-hosted/)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/create-source/mysql.md"
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

© 2026 Materialize Inc.

</div>
