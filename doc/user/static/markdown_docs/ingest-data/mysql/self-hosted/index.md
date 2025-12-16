<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)  /  [Ingest
data](/docs/self-managed/v25.2/ingest-data/)
 /  [MySQL](/docs/self-managed/v25.2/ingest-data/mysql/)

</div>

# Ingest data from self-hosted MySQL

This page shows you how to stream data from a self-hosted MySQL database
to Materialize using the [MySQL
source](/docs/self-managed/v25.2/sql/create-source/mysql/).

<div class="tip">

**💡 Tip:** For help getting started with your own data, you can
schedule a [free guided
trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).

</div>

## Before you begin

- Make sure you are running MySQL 5.7 or higher. Materialize uses
  [GTID-based binary log (binlog)
  replication](/docs/self-managed/v25.2/sql/create-source/mysql/#change-data-capture),
  which is not available in older versions of MySQL.

- Ensure you have access to your MySQL instance via the [`mysql`
  client](https://dev.mysql.com/doc/refman/8.0/en/mysql.html), or your
  preferred SQL client.

## A. Configure MySQL

### 1. Enable GTID-based binlog replication

Before creating a source in Materialize, you **must** configure your
MySQL database for GTID-based binlog replication. Ensure the upstream
MySQL database has been configured for GTID-based binlog replication:

| MySQL Configuration | Value | Notes |
|----|----|----|
| `log_bin` | `ON` |  |
| `binlog_format` | `ROW` | [Deprecated as of MySQL 8.0.34](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format). Newer versions of MySQL default to row-based logging. |
| `binlog_row_image` | `FULL` |  |
| `gtid_mode` | `ON` |  |
| `enforce_gtid_consistency` | `ON` |  |
| `replica_preserve_commit_order` | `ON` | Only required when connecting Materialize to a read-replica. |

For guidance on enabling GTID-based binlog replication, see the [MySQL
documentation](https://dev.mysql.com/blog-archive/enabling-gtids-without-downtime-in-mysql-5-7-6/).

### 2. Create a user for replication

Once GTID-based binlog replication is enabled, we recommend creating a
dedicated user for Materialize with sufficient privileges to manage
replication.

1.  As a *superuser*, use `mysql` (or your preferred SQL client) to
    connect to your database.

2.  Create a dedicated user for Materialize, if you don’t already have
    one:

    <div class="highlight">

    ``` chroma
    CREATE USER 'materialize'@'%' IDENTIFIED BY '<password>';

    ALTER USER 'materialize'@'%' REQUIRE SSL;
    ```

    </div>

    IAM authentication with AWS RDS for MySQL is also supported. See the
    [Amazon RDS User
    Guide](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html)
    for instructions on enabling IAM database authentication, creating
    IAM policies, and creating a database account.

3.  Grant the user permission to manage replication:

    <div class="highlight">

    ``` chroma
    GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES ON *.* TO 'materialize'@'%';
    ```

    </div>

    Once connected to your database, Materialize will take an initial
    snapshot of the tables in your MySQL server. `SELECT` privileges are
    required for this initial snapshot.

4.  Apply the changes:

    <div class="highlight">

    ``` chroma
    FLUSH PRIVILEGES;
    ```

    </div>

## B. Configure network security

Configure your network to allow Materialize to connect to your database.
For example, you can:

- **Allow Materialize IPs:** Configure your database’s security group to
  allow connections from Materialize.

- **Use an SSH tunnel:** Use an SSH tunnel to connect Materialize to the
  database.

<div class="note">

**NOTE:**

The steps to allow Materialize to connect to your database depends on
your deployment setup. Refer to your company’s network/security policies
and procedures.

</div>

<div class="code-tabs">

<div class="tab-content">

<div id="tab-allow-materialize-ips" class="tab-pane"
title="Allow Materialize IPs">

1.  Update your database firewall rules to allow traffic from
    Materialize IPs.

</div>

<div id="tab-use-an-ssh-tunnel" class="tab-pane"
title="Use an SSH tunnel">

To create an SSH tunnel from Materialize to your database, you launch an
VM to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database’s
private network to allow traffic from the bastion host.

1.  Launch a VM to serve as your SSH bastion host.

    - Make sure the VM is publicly accessible and in the same VPC as
      your database.
    - Add a key pair and note the username. You’ll use this username
      when connecting Materialize to your bastion host.
    - Make sure the VM has a static public IP address. You’ll use this
      IP address when connecting Materialize to your bastion host.

2.  Configure the SSH bastion host to allow traffic only from
    Materialize.

3.  Update your database firewall rules to allow traffic from the SSH
    bastion host.

</div>

</div>

</div>

## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

<div class="note">

**NOTE:** If you are prototyping and already have a cluster to host your
MySQL source (e.g. `quickstart`), **you can skip this step**. For
production scenarios, we recommend separating your workloads into
multiple clusters for [resource
isolation](/docs/self-managed/v25.2/sql/create-cluster/#resource-isolation).

</div>

In Materialize, a [cluster](/docs/self-managed/v25.2/concepts/clusters/)
is an isolated environment, similar to a virtual warehouse in Snowflake.
When you create a cluster, you choose the size of its compute resource
allocation based on the work you need the cluster to do, whether
ingesting data from a source, computing always-up-to-date query results,
serving results to clients, or a combination.

In this case, you’ll create a dedicated cluster for ingesting source
data from your MySQL database.

1.  In the [SQL Shell](https://console.materialize.com/), or your
    preferred SQL client connected to Materialize, use the
    [`CREATE CLUSTER`](/docs/self-managed/v25.2/sql/create-cluster/)
    command to create the new cluster:

    <div class="highlight">

    ``` chroma
    CREATE CLUSTER ingest_mysql (SIZE = '200cc');

    SET CLUSTER = ingest_mysql;
    ```

    </div>

    A cluster of
    [size](/docs/self-managed/v25.2/sql/create-cluster/#size) `200cc`
    should be enough to process the initial snapshot of the tables in
    your MySQL database. For very large snapshots, consider using a
    larger size to speed up processing. Once the snapshot is finished,
    you can readjust the size of the cluster to fit the volume of
    changes being replicated from your upstream MySQL database.

### 2. Start ingesting data

Now that you’ve configured your database network, you can connect
Materialize to your MySQL database and start ingesting data. The exact
steps depend on your networking configuration, so start by selecting the
relevant option.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-allow-materialize-ips" class="tab-pane"
title="Allow Materialize IPs">

1.  In the Console’s SQL Shell, or your preferred SQL client connected
    to Materialize, use the
    [`CREATE SECRET`](/docs/self-managed/v25.2/sql/create-secret/)
    command to securely store the password for the `materialize` MySQL
    user you created [earlier](#2-create-a-user-for-replication):

    <div class="highlight">

    ``` chroma
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

    </div>

    For AWS IAM authentication, you must create a connection to AWS. See
    the
    [`CREATE CONNECTION`](/docs/self-managed/v25.2/sql/create-connection/#aws)
    command for details.

2.  Use the
    [`CREATE CONNECTION`](/docs/self-managed/v25.2/sql/create-connection/)
    command to create a connection object with access and authentication
    details for Materialize to use:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION mysql_connection TO MYSQL (
        HOST <host>,
        PORT 3306,
        USER 'materialize',
        PASSWORD SECRET mysqlpass,
        SSL MODE REQUIRED
    );
    ```

    </div>

    - Replace `<host>` with your MySQL endpoint.

3.  Use the
    [`CREATE SOURCE`](/docs/self-managed/v25.2/sql/create-source/)
    command to connect Materialize to your MySQL instance and start
    ingesting data:

    <div class="highlight">

    ``` chroma
    CREATE SOURCE mz_source
      FROM mysql CONNECTION mysql_connection
      FOR ALL TABLES;
    ```

    </div>

    - By default, the source will be created in the active cluster; to
      use a different cluster, use the `IN CLUSTER` clause.

    - To ingest data from specific schemas or tables, use the
      `FOR SCHEMAS (<schema1>,<schema2>)` or
      `FOR TABLES (<table1>, <table2>)` options instead of
      `FOR ALL TABLES`.

    - To handle unsupported data types, use the `TEXT COLUMNS` or
      `IGNORE COLUMNS` options. Check out the [reference
      documentation](/docs/self-managed/v25.2/sql/create-source/mysql/#supported-types)
      for guidance.

4.  After source creation, you can handle upstream [schema
    changes](/docs/self-managed/v25.2/sql/create-source/mysql/#schema-changes)
    by dropping and recreating the source.

</div>

<div id="tab-use-an-ssh-tunnel" class="tab-pane"
title="Use an SSH tunnel">

1.  In the [SQL Shell](https://console.materialize.com/), or your
    preferred SQL client connected to Materialize, use the
    [`CREATE CONNECTION`](/docs/self-managed/v25.2/sql/create-connection/#ssh-tunnel)
    command to create an SSH tunnel connection:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        PORT <SSH_BASTION_PORT>,
        USER '<SSH_BASTION_USER>'
    );
    ```

    </div>

    - Replace `<SSH_BASTION_HOST>` and `<SSH_BASTION_PORT`\> with the
      public IP address and port of the SSH bastion host you created
      [earlier](#b-configure-network-security).

    - Replace `<SSH_BASTION_USER>` with the username for the key pair
      you created for your SSH bastion host.

2.  Get Materialize’s public keys for the SSH tunnel connection:

    <div class="highlight">

    ``` chroma
    SELECT * FROM mz_ssh_tunnel_connections;
    ```

    </div>

3.  Log in to your SSH bastion host and add Materialize’s public keys to
    the `authorized_keys` file, for example:

    <div class="highlight">

    ``` chroma
    # Command for Linux
    echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
    echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys
    ```

    </div>

4.  Back in the SQL client connected to Materialize, validate the SSH
    tunnel connection you created using the
    [`VALIDATE CONNECTION`](/docs/self-managed/v25.2/sql/validate-connection)
    command:

    <div class="highlight">

    ``` chroma
    VALIDATE CONNECTION ssh_connection;
    ```

    </div>

    If no validation error is returned, move to the next step.

5.  Use the
    [`CREATE SECRET`](/docs/self-managed/v25.2/sql/create-secret/)
    command to securely store the password for the `materialize` MySQL
    user you created [earlier](#2-create-a-user-for-replication):

    <div class="highlight">

    ``` chroma
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

    </div>

    For AWS IAM authentication, you must create a connection to AWS. See
    the
    [`CREATE CONNECTION`](/docs/self-managed/v25.2/sql/create-connection/#aws)
    command for details.

6.  Use the
    [`CREATE CONNECTION`](/docs/self-managed/v25.2/sql/create-connection/)
    command to create another connection object, this time with database
    access and authentication details for Materialize to use:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION mysql_connection TO MYSQL (
    HOST '<host>',
    SSH TUNNEL ssh_connection
    );
    ```

    </div>

    - Replace `<host>` with your MySQL endpoint.

AWS IAM authentication is also available, see the
[`CREATE CONNECTION`](/docs/self-managed/v25.2/sql/create-connection/#mysql)
command for details.

1.  Use the
    [`CREATE SOURCE`](/docs/self-managed/v25.2/sql/create-source/)
    command to connect Materialize to your MySQL instance and start
    ingesting data:

    <div class="highlight">

    ``` chroma
    CREATE SOURCE mz_source
      FROM mysql CONNECTION mysql_connection
      FOR ALL TABLES;
    ```

    </div>

    - By default, the source will be created in the active cluster; to
      use a different cluster, use the `IN CLUSTER` clause.

    - To ingest data from specific schemas or tables, use the
      `FOR SCHEMAS (<schema1>,<schema2>)` or
      `FOR TABLES (<table1>, <table2>)` options instead of
      `FOR ALL TABLES`.

    - To handle unsupported data types, use the `TEXT COLUMNS` or
      `IGNORE COLUMNS` options. Check out the [reference
      documentation](/docs/self-managed/v25.2/sql/create-source/mysql/#supported-types)
      for guidance.

</div>

</div>

</div>

### 3. Monitor the ingestion status

Before it starts consuming the replication stream, Materialize takes a
snapshot of the relevant tables. Until this snapshot is complete,
Materialize won’t have the same view of your data as your MySQL
database.

In this step, you’ll first verify that the source is running and then
check the status of the snapshotting process.

1.  Back in the SQL client connected to Materialize, use the
    [`mz_source_statuses`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_source_statuses)
    table to check the overall status of your source:

    <div class="highlight">

    ``` chroma
    WITH
      source_ids AS
      (SELECT id FROM mz_sources WHERE name = 'mz_source')
    SELECT *
    FROM
      mz_internal.mz_source_statuses
        JOIN
          (
            SELECT referenced_object_id
            FROM mz_internal.mz_object_dependencies
            WHERE
              object_id IN (SELECT id FROM source_ids)
            UNION SELECT id FROM source_ids
          )
          AS sources
        ON mz_source_statuses.id = sources.referenced_object_id;
    ```

    </div>

    For each `subsource`, make sure the `status` is `running`. If you
    see `stalled` or `failed`, there’s likely a configuration issue for
    you to fix. Check the `error` field for details and fix the issue
    before moving on. Also, if the `status` of any subsource is
    `starting` for more than a few minutes, [contact our
    team](/docs/self-managed/v25.2/support/).

2.  Once the source is running, use the
    [`mz_source_statistics`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_source_statistics)
    table to check the status of the initial snapshot:

    <div class="highlight">

    ``` chroma
    WITH
      source_ids AS
      (SELECT id FROM mz_sources WHERE name = 'mz_source')
    SELECT sources.referenced_object_id AS id, mz_sources.name, snapshot_committed
    FROM
      mz_internal.mz_source_statistics
        JOIN
          (
            SELECT object_id, referenced_object_id
            FROM mz_internal.mz_object_dependencies
            WHERE
              object_id IN (SELECT id FROM source_ids)
            UNION SELECT id, id FROM source_ids
          )
          AS sources
        ON mz_source_statistics.id = sources.referenced_object_id
        JOIN mz_sources ON mz_sources.id = sources.referenced_object_id;
    ```

    </div>

    ```
    object_id | snapshot_committed
    ----------|------------------
     u144     | t
    (1 row)
    ```

    Once `snapshot_commited` is `t`, move on to the next step.
    Snapshotting can take between a few minutes to several hours,
    depending on the size of your dataset and the size of the cluster
    the source is running in.

### 4. Right-size the cluster

After the snapshotting phase, Materialize starts ingesting change events
from the MySQL replication stream. For this work, Materialize generally
performs well with a `100cc` replica, so you can resize the cluster
accordingly.

1.  Still in a SQL client connected to Materialize, use the
    [`ALTER CLUSTER`](/docs/self-managed/v25.2/sql/alter-cluster/)
    command to downsize the cluster to `100cc`:

    <div class="highlight">

    ``` chroma
    ALTER CLUSTER ingest_mysql SET (SIZE '100cc');
    ```

    </div>

    Behind the scenes, this command adds a new `100cc` replica and
    removes the `200cc` replica.

2.  Use the
    [`SHOW CLUSTER REPLICAS`](/docs/self-managed/v25.2/sql/show-cluster-replicas/)
    command to check the status of the new replica:

    <div class="highlight">

    ``` chroma
    SHOW CLUSTER REPLICAS WHERE cluster = 'ingest_mysql';
    ```

    </div>

    ```
         cluster     | replica |  size  | ready
    -----------------+---------+--------+-------
     ingest_mysql    | r1      | 100cc  | t
    (1 row)
    ```

## D. Explore your data

With Materialize ingesting your MySQL data into durable storage, you can
start exploring the data, computing real-time results that stay
up-to-date as new data arrives, and serving results efficiently.

- Explore your data with
  [`SHOW SOURCES`](/docs/self-managed/v25.2/sql/show-sources) and
  [`SELECT`](/docs/self-managed/v25.2/sql/select/).

- Compute real-time results in memory with
  [`CREATE VIEW`](/docs/self-managed/v25.2/sql/create-view/) and
  [`CREATE INDEX`](/docs/self-managed/v25.2/sql/create-index/) or in
  durable storage with
  [`CREATE MATERIALIZED VIEW`](/docs/self-managed/v25.2/sql/create-materialized-view/).

- Serve results to a PostgreSQL-compatible SQL client or driver with
  [`SELECT`](/docs/self-managed/v25.2/sql/select/) or
  [`SUBSCRIBE`](/docs/self-managed/v25.2/sql/subscribe/) or to an
  external message broker with
  [`CREATE SINK`](/docs/self-managed/v25.2/sql/create-sink/).

- Check out the [tools and
  integrations](/docs/self-managed/v25.2/integrations/) supported by
  Materialize.

## Considerations

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
  [`DROP SOURCE`](/docs/self-managed/v25.2/sql/alter-source/#context) to
  first drop the affected subsource, and then add the table back to the
  source using
  [`ALTER SOURCE...ADD SUBSOURCE`](/docs/self-managed/v25.2/sql/alter-source/).

- Dropping columns that were added after the source was created. These
  columns are never ingested, so you can drop them without issue.

- Adding or removing `NOT NULL` constraints to tables that were nullable
  when the source was created.

#### Incompatible schema changes

All other schema changes to upstream tables will set the corresponding
subsource into an error state, which prevents you from reading from the
source.

To handle incompatible [schema changes](#schema-changes), use
[`DROP SOURCE`](/docs/self-managed/v25.2/sql/alter-source/#context) and
[`ALTER SOURCE...ADD SUBSOURCE`](/docs/self-managed/v25.2/sql/alter-source/)
to first drop the affected subsource, and then add the table back to the
source. When you add the subsource, it will have the updated schema from
the corresponding upstream table.

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

Replicating tables that contain **unsupported [data
types](/docs/self-managed/v25.2/sql/types/)** is possible via the
[`TEXT COLUMNS`
option](/docs/self-managed/v25.2/sql/create-source/mysql/#handling-unsupported-types)
for the following types:

- `enum`
- `year`

The specified columns will be treated as `text`, and will thus not offer
the expected MySQL type features. For any unsupported data types not
listed above, use the
[`EXCLUDE COLUMNS`](/docs/self-managed/v25.2/sql/create-source/mysql/#excluding-columns)
option.

### Truncation

Upstream tables replicated into Materialize should not be truncated. If
an upstream table is truncated while replicated, the whole source
becomes inaccessible and will not produce any data until it is
recreated. Instead of truncating, you can use an unqualified `DELETE` to
remove all rows from the table:

<div class="highlight">

``` chroma
DELETE FROM t;
```

</div>

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/ingest-data/mysql/self-hosted.md"
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
