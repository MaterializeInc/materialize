# SQL Server

Connecting Materialize to a SQL Server database for Change Data Capture (CDC).



## Change Data Capture (CDC)

Materialize supports SQL Server as a real-time data source. The [SQL Server source](/sql/create-source/sql-server/)
uses SQL Server's change data capture feature to **continually ingest changes**
resulting from CRUD operations in the upstream database. The native support for
SQL Server Change Data Capture (CDC) in Materialize gives you the following benefits:

* **No additional infrastructure:** Ingest SQL Server change data into Materialize in
    real-time with no architectural changes or additional operational overhead.
    In particular, you **do not need to deploy Kafka and Debezium** for SQL Server
    CDC.

* **Transactional consistency:** The SQL Server source ensures that transactions in
    the upstream SQL Server database are respected downstream. Materialize will
    **never show partial results** based on partially replicated transactions.

* **Incrementally updated materialized views:** Incrementally updated Materialized
    views are considerably **limited in SQL Server**, so you can use Materialize as
    a read-replica to build views on top of your SQL Server data that are
    efficiently maintained and always up-to-date.

## Supported versions

Materialize supports replicating data from SQL Server 2016 or higher with Change
Data Capture (CDC) support.

## Integration Guides

- [Self-hosted SQL Server](/ingest-data/sql-server/self-hosted/)

## Considerations


  {{__hugo_ctx pid=38}}
### Schema changes


  {{__hugo_ctx pid=37}}
> **Note:** Work to more smoothly support ddl changes to upstream tables is currently in
> progress. The work introduces the ability to re-ingest the same upstream table
> under a new schema and switch over without downtime.
>
>

{{__hugo_ctx/}}





Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes

- Adding columns to tables. Materialize will **not ingest** new columns added
  upstream unless you use [`DROP SOURCE`](/sql/alter-source/#context) to first
  drop the affected subsource, and then add the table back to the source using
  [`ALTER SOURCE...ADD SUBSOURCE`](/sql/alter-source/).

- Dropping columns that were added after the source was created. These columns
  are never ingested, so you can drop them without issue.

- Adding or removing `NOT NULL` constraints to tables that were nullable when
  the source was created.

#### Incompatible schema changes

All other schema changes to upstream tables will set the corresponding subsource
into an error state, which prevents you from reading from the source.

To handle incompatible [schema changes](#schema-changes), use [`DROP SOURCE`](/sql/alter-source/#context)
and [`ALTER SOURCE...ADD SUBSOURCE`](/sql/alter-source/) to first drop the
affected subsource, and then add the table back to the source. When you add the
subsource, it will have the updated schema from the corresponding upstream
table.


### Supported types

<p>Materialize natively supports the following SQL Server types:</p>
<ul style="column-count: 3">
<li><code>tinyint</code></li>
<li><code>smallint</code></li>
<li><code>int</code></li>
<li><code>bigint</code></li>
<li><code>real</code></li>
<li><code>double precision</code></li>
<li><code>float</code></li>
<li><code>bit</code></li>
<li><code>decimal</code></li>
<li><code>numeric</code></li>
<li><code>money</code></li>
<li><code>smallmoney</code></li>
<li><code>char</code></li>
<li><code>nchar</code></li>
<li><code>varchar</code></li>
<li><code>varchar(max)</code></li>
<li><code>nvarchar</code></li>
<li><code>nvarchar(max)</code></li>
<li><code>sysname</code></li>
<li><code>binary</code></li>
<li><code>varbinary</code></li>
<li><code>json</code></li>
<li><code>date</code></li>
<li><code>time</code></li>
<li><code>smalldatetime</code></li>
<li><code>datetime</code></li>
<li><code>datetime2</code></li>
<li><code>datetimeoffset</code></li>
<li><code>uniqueidentifier</code></li>
</ul>

<p>Replicating tables that contain <strong>unsupported <a href="/sql/types/">data types</a></strong> is possible via the <a href="/sql/create-source/sql-server/#handling-unsupported-types"><code>EXCLUDE COLUMNS</code> option</a> for the
following types:</p>
<ul style="column-count: 3">
<li><code>text</code></li>
<li><code>ntext</code></li>
<li><code>image</code></li>
<li><code>varbinary(max)</code></li>
</ul>
<p>Columns with the specified types need to be excluded because <a href="https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-capture-instance-ct-transact-sql?view=sql-server-2017#large-object-data-types">SQL Server does not provide
the &ldquo;before&rdquo;</a>
value when said column is updated.</p>


### Timestamp Rounding

The `time`, `datetime2`, and `datetimeoffset` types in SQL Server have a default
scale of 7 decimal places, or in other words a accuracy of 100 nanoseconds. But
the corresponding types in Materialize only support a scale of 6 decimal places.
If a column in SQL Server has a higher scale than what Materialize can support, it
will be rounded up to the largest scale possible.

```
-- In SQL Server
CREATE TABLE my_timestamps (a datetime2(7));
INSERT INTO my_timestamps VALUES
  ('2000-12-31 23:59:59.99999'),
  ('2000-12-31 23:59:59.999999'),
  ('2000-12-31 23:59:59.9999999');

-- Replicated into Materialize
SELECT * FROM my_timestamps;
'2000-12-31 23:59:59.999990'
'2000-12-31 23:59:59.999999'
'2001-01-01 00:00:00'
```

### Snapshot latency for inactive databases

When a new Source is created, Materialize performs a snapshotting operation to sync
the data. However, for a new SQL Server source, if none of the replicating tables
are receiving write queries, snapshotting may take up to an additional 5 minutes
to complete. The 5 minute interval is due to a hardcoded interval in the SQL Server
Change Data Capture (CDC) implementation which only notifies CDC consumers every
5 minutes when no changes are made to replicating tables.

See [Monitoring freshness status](/ingest-data/monitoring-data-ingestion/#monitoring-hydrationdata-freshness-status)

### Capture Instance Selection

When a new source is created, Materialize selects a capture instance for each
table. SQL Server permits at most two capture instances per table, which are
listed in the
[`sys.cdc_change_tables`](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-change-tables-transact-sql)
system table. For each table, Materialize picks the capture instance with the
most recent `create_date`.

If two capture instances for a table share the same timestamp (unlikely given the millisecond resolution), Materialize selects the `capture_instance` with the lexicographically larger name.

### Modifying an existing source


  {{__hugo_ctx pid=34}}
When you add a new subsource to an existing source ([`ALTER SOURCE ... ADD
SUBSOURCE ...`](/sql/alter-source/)), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.
{{__hugo_ctx/}}




{{__hugo_ctx/}}







---

## Ingest data from self-hosted SQL Server


This page shows you how to stream data from a self-hosted SQL Server database
to Materialize using the [SQL Server Source](/sql/create-source/sql-server/).

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).
>
>


## Before you begin

- Make sure you are running SQL Server 2016 or higher with  Change Data Capture
(CDC) support. Materialize uses [Change Data
Capture](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server)
which is not readily available on older versions of SQL Server.

- Ensure you have access to your SQL Server instance via the [`sqlcmd` client](https://learn.microsoft.com/en-us/sql/tools/sqlcmd/sqlcmd-utility),
  or your preferred SQL client.

- Ensure SQL Server Agent is running.
  ```mzsql
  USE msdb;
  SELECT
    servicename,
    status_desc,
    startup_type_desc
  FROM sys.dm_server_services
  WHERE servicename LIKE 'SQL Server Agent%';
  ```


## A. Configure SQL Server

> **Note:** To configure SQL Server for data ingestion into Materialize, you must be a user
> with privileges to enable CDC and create/manage login, users, roles, and
> privileges.
>
>


### 1. Create a Materialize user in SQL Server.

Create a user that Materialize will use to connect when ingesting data.

1. In `master`:

   1. Create a login `materialize` (replace `<PASSWORD>` with your own
      password):

      ```sql
      USE master;

      -- Specify additional options per your company's security policy
      CREATE LOGIN materialize WITH PASSWORD = '<PASSWORD>',
      DEFAULT_DATABASE = <DATABASE_NAME>;
      GO -- The GO terminator may be unsupported or unnecessary for your client.
      ```

   1. Create a user `materialize` for the login and role `materialize_role`:

      ```sql
      USE master;
      CREATE USER materialize FOR LOGIN materialize;
      CREATE ROLE materialize_role;
      ALTER ROLE materialize_role ADD MEMBER materialize;
      GO -- The GO terminator may be unsupported or unnecessary for your client.
      ```

   1. Grant permissions to the `materialize_role` to enable discovery of the
      tables to be replicated and monitoring replication progress:

      ```sql
      USE master;

      -- Required for schema discovery for replicated tables.
      GRANT SELECT ON INFORMATION_SCHEMA.KEY_COLUMN_USAGE TO materialize_role;
      GRANT SELECT ON INFORMATION_SCHEMA.TABLE_CONSTRAINTS TO materialize_role;
      GRANT SELECT ON OBJECT::INFORMATION_SCHEMA.TABLE_CONSTRAINTS TO materialize_role;

      -- Allows checking the minimum and maximum Log Sequence Numbers (LSN) for CDC,
      -- required for the Source to be able to track progress.
      GRANT EXECUTE ON sys.fn_cdc_get_min_lsn TO materialize_role;
      GRANT EXECUTE ON sys.fn_cdc_get_max_lsn TO materialize_role;
      GRANT EXECUTE ON sys.fn_cdc_increment_lsn TO materialize_role;

      GRANT VIEW SERVER STATE TO materialize;
      GO -- The GO terminator may be unsupported or unnecessary for your client.
      ```

1. In the database from which which you want to ingest data,

   1. Create a second `materialize` user and a second `materialize_role`.

   1. Add `materialize` user as a member to the `materialize_role` and
   `db_datareader` roles (replace `<DATABASE_NAME>` with your database name).

   ```sql
   USE <DATABASE_NAME>;

   -- Use the same user name and role name as those created in master
   CREATE USER materialize FOR LOGIN materialize;
   CREATE ROLE materialize_role;
   ALTER ROLE materialize_role ADD MEMBER materialize;
   ALTER ROLE db_datareader ADD MEMBER materialize;
   GO -- The GO terminator may be unsupported or unnecessary for your client.
   ```

### 2. Enable Change-Data-Capture for the database.

In SQL Server, for the database from which you want to ingest data, enable
change data capture  (replace `<DATABASE_NAME>` with your database name):

```sql
USE <DATABASE_NAME>;
GO -- The GO terminator may be unsupported or unnecessary for your client.
EXEC sys.sp_cdc_enable_db;
GO -- The GO terminator may be unsupported or unnecessary for your client.
```

For guidance on enabling Change Data Capture, see the [SQL Server documentation](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-db-transact-sql).

### 3. Enable `SNAPSHOT` transaction isolation.

Enable `SNAPSHOT` transaction isolation for the database (replace
`<DATABASE_NAME>` with your database name):

```sql
ALTER DATABASE <DATABASE_NAME> SET ALLOW_SNAPSHOT_ISOLATION ON;
GO -- The GO terminator may be unsupported or unnecessary for your client.
```

For guidance on enabling `SNAPSHOT` transaction isolation, see the [SQL Server documentation](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-table-transact-sql)


### 4. Enable Change-Data-Capture for the tables.

Enable Change Data Capture for each table you wish to replicate (replace
`<DATABASE_NAME>`, `<SCHEMA_NAME>`, and `<TABLE_NAME>` with the your database,
schema name, and table name):

```sql
USE <DATABASE_NAME>;

EXEC sys.sp_cdc_enable_table
  @source_schema = '<SCHEMA_NAME>',
  @source_name = '<TABLE_NAME>',
  @role_name = 'materialize_role',
  @supports_net_changes = 0;
GO -- The GO terminator may be unsupported or unnecessary for your client.
```


## B. (Optional) Configure network security

> **Note:** If you are prototyping and your SQL Server instance is publicly accessible, **you can
> skip this step**. For production scenarios, we recommend configuring one of the
> network security options below.
>


There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's firewall to allow connections from a set of
    static Materialize IP addresses.

- **Use an SSH tunnel:** If your database is running in a private network, you
    can use an SSH tunnel to connect Materialize to the database.

Select the option that works best for you.



**Allow Materialize IPs:**

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, find the static egress IP addresses for the
   Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

1. Update your database firewall rules to allow traffic from each IP address
   from the previous step.



**Use AWS PrivateLink:**

Materialize can connect to a SQL Server database through an [AWS PrivateLink](https://aws.amazon.com/privatelink/)
service. Your SQL Server database must be running on AWS in order to use this
option.

1. Create a dedicated [target
    group](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html)
    for your SQL Server instance with the following details:

    a. Target type as **IP address**.

    b. Protocol as **TCP**.

    c. Port as **1433**, or the port that you are using in case it is not 1433.

    d. Make sure that the target group is in the same VPC as the SQL Server
    instance.

    e. Click next, and register the respective SQL Server instance to the target
    group using its IP address.

1. Create a [Network Load
    Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html)
    that is **enabled for the same subnets** that the SQL Server instance is in.

1. Create a [TCP
    listener](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-listener.html)
    for your SQL Server instance that forwards to the corresponding target group
    you created.

1. Verify security groups and health checks. Once the TCP listener has been
    created, make sure that the [health
    checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html)
    are passing and that the target is reported as healthy.

    If you have set up a security group for your SQL Server instance, you must
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

1. Configure the AWS PrivateLink service.

    Retrieve the AWS principal for the AWS PrivateLink connection you just
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

    1. In the [SQL Shell](/console/), or your preferred
       SQL client connected to Materialize, get the static egress IP addresses for
       the Materialize region you are running in:

       ```mzsql
       SELECT * FROM mz_egress_ips;
       ```

    1. Update your SSH bastion host's firewall rules to allow traffic from each
       IP address from the previous step.

1. Update your database firewall rules to allow traffic from the SSH bastion
   host.





## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

> **Note:** If you are prototyping and already have a cluster to host your SQL Server
> source (e.g. `quickstart`), **you can skip this step**. For production
> scenarios, we recommend separating your workloads into multiple clusters for
> [resource isolation](/sql/create-cluster/#resource-isolation).
>


In Materialize, a [cluster](/concepts/clusters/) is an isolated
environment, similar to a virtual warehouse in Snowflake. When you create a
cluster, you choose the size of its compute resource allocation based on the
work you need the cluster to do, whether ingesting data from a source,
computing always-up-to-date query results, serving results to clients, or a
combination.

In this case, you'll create a dedicated cluster for ingesting source data from
your SQL Server database.

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, use the [`CREATE CLUSTER`](/sql/create-cluster/)
   command to create the new cluster:

    ```mzsql
    CREATE CLUSTER ingest_sqlserver (SIZE = '200cc');

    SET CLUSTER = ingest_sqlserver;
    ```

    A cluster of [size](/sql/create-cluster/#size) `200cc` should be enough to
    process the initial snapshot of the tables in your SQL Server database. For
    very large snapshots, consider using a larger size to speed up processing.
    Once the snapshot is finished, you can readjust the size of the cluster to fit
    the volume of changes being replicated from your upstream SQL Server database.



### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.



**Allow Materialize IPs:**

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, use the [`CREATE SECRET`](/sql/create-secret/)
   command to securely store the password for the SQL Server role you'll use to
   replicate data into Materialize:

    ```mzsql
    CREATE SECRET sqlserver_pass AS '<PASSWORD>';
    ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
   connection object with access and authentication details for Materialize to
   use:

    ```mzsql
    CREATE CONNECTION sqlserver_connection TO SQL SERVER (
        HOST <host>,
        PORT 1433,
        USER 'materialize',
        PASSWORD SECRET sqlserver_pass,
        DATABASE <database>,
        SSL MODE 'required'
    );
    ```

    - Replace `<host>` with your SQL Server endpoint, and `<database>` with the database you'd like to connect to.



**Use an AWS Privatelink (Cloud-only):**
1. In the [SQL Shell](/console/), or your preferred SQL
client connected to Materialize, use the [`CREATE CONNECTION`](/sql/create-connection/#aws-privatelink)
command to create an AWS PrivateLink connection:

    ↕️ **In-region connections**

    To connect to an AWS PrivateLink endpoint service in the **same region** as your
    Materialize environment:

      ```mzsql
      CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
        SERVICE NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
        AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az4')
      );
      ```

    - Replace the `SERVICE NAME` value with the service name you noted [earlier](#b-optional-configure-network-security).

    - Replace the `AVAILABILITY ZONES` list with the IDs of the availability
      zones in your AWS account. For in-region connections the availability
      zones of the NLB and the consumer VPC **must match**.

      To find your availability zone IDs, select your database in the RDS
      Console and click the subnets under **Connectivity & security**. For each
      subnet, look for **Availability Zone ID** (e.g., `use1-az6`),
      not **Availability Zone** (e.g., `us-east-1d`).

    ↔️ **Cross-region connections**

    To connect to an AWS PrivateLink endpoint service in a **different region** to
    the one where your Materialize environment is deployed:

      ```mzsql
      CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
        SERVICE NAME 'com.amazonaws.vpce.us-west-1.vpce-svc-<endpoint_service_id>',
        -- For now, the AVAILABILITY ZONES clause **is** required, but will be
        -- made optional in a future release.
        AVAILABILITY ZONES ()
      );
      ```

    - Replace the `SERVICE NAME` value with the service name you noted [earlier](#b-optional-configure-network-security).

    - The service name region refers to where the endpoint service was created.
      You **do not need** to specify `AVAILABILITY ZONES` manually — these will
      be optimally auto-assigned when none are provided.

1. Retrieve the AWS principal for the AWS PrivateLink connection you just
created:

     ```mzsql
     SELECT principal
       FROM mz_aws_privatelink_connections plc
       JOIN mz_connections c ON plc.id = c.id
       WHERE c.name = 'privatelink_svc';
     ```
    <p></p>

    ```
    principal
    ---------------------------------------------------------------------------
    arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
    ```

1. Update your VPC endpoint service to [accept connections from the AWS
principal](https://docs.aws.amazon.com/vpc/latest/privatelink/add-endpoint-service-permissions.html).

1. If your AWS PrivateLink service is configured to require acceptance of
connection requests, [manually approve the connection request from
Materialize](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).

    **Note:** It can take some time for the connection request to show up. Do
    not move on to the next step until you've approved the connection.

1. Validate the AWS PrivateLink connection you created using the
[`VALIDATE CONNECTION`](/sql/validate-connection) command:

    ```mzsql
    VALIDATE CONNECTION privatelink_svc;
    ```

    If no validation error is returned, move to the next step.

1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the password for the `materialize` SQL Server user [you created](#1-create-a-materialize-user-in-sql-server):

    ```mzsql
    CREATE SECRET sql_server_pass AS '<PASSWORD>';
    ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create
another connection object, this time with database access and authentication
details for Materialize to use:

    ```mzsql
    CREATE CONNECTION sql_server_connection TO SQL SERVER (
    HOST <host>,
      PORT 1433,
      USER 'materialize',
      PASSWORD SECRET sql_server_pass,
      SSL MODE REQUIRED,
      AWS PRIVATELINK privatelink_svc
    );
    ```

    - Replace `<host>` with your RDS endpoint. To find your RDS endpoint, select
      your database in the RDS Console, and look under **Connectivity &
      security**.

      - Replace `<database>` with the name of the database containing the tables
        you want to replicate to Materialize.

    AWS IAM authentication is also available, see the [`CREATE CONNECTION`](/sql/create-connection/#mysql) command for details.



**Use an SSH tunnel:**

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, use the [`CREATE CONNECTION`](/sql/create-connection/#ssh-tunnel)
   command to create an SSH tunnel connection:

    ```mzsql
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        PORT <SSH_BASTION_PORT>,
        USER '<SSH_BASTION_USER>'
    );
    ```

    - Replace `<SSH_BASTION_HOST>` and `<SSH_BASTION_PORT`> with the public IP address and port of the SSH bastion host you created [earlier](#b-optional-configure-network-security).

    - Replace `<SSH_BASTION_USER>` with the username for the key pair you created for your SSH bastion host.

1. Get Materialize's public keys for the SSH tunnel connection:

    ```mzsql
    SELECT * FROM mz_ssh_tunnel_connections;
    ```

1. Log in to your SSH bastion host and add Materialize's public keys to the `authorized_keys` file, for example:

    ```sh
    # Command for Linux
    echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
    echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys
    ```

1. Back in the SQL client connected to Materialize, validate the SSH tunnel connection you created using the [`VALIDATE CONNECTION`](/sql/validate-connection) command:

    ```mzsql
    VALIDATE CONNECTION ssh_connection;
    ```

    If no validation error is returned, move to the next step.

1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the password for the `materialize` SQL Server user [you created](#1-create-a-materialize-user-in-sql-server):

    ```mzsql
    CREATE SECRET sql_server_pass AS '<PASSWORD>';
    ```

    For AWS IAM authentication, you must create a connection to AWS.  See the [`CREATE CONNECTION`](/sql/create-connection/#aws) command for details.

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create another connection object, this time with database access and authentication details for Materialize to use:

    ```mzsql
    CREATE CONNECTION sql_server_connection TO SQL SERVER (
    HOST '<host>',
    SSH TUNNEL ssh_connection
    );
    ```

    - Replace `<host>` with your SQL Server endpoint.





### 3. Start ingesting data

> **Note:** For a new SQL Server source, if none of the replicating tables
> are receiving write queries, snapshotting may take up to an additional 5 minutes
> to complete. For details, see [snapshot latency for inactive databases](#snapshot-latency-for-inactive-databases)
>


Use the [`CREATE SOURCE`](/sql/create-source/) command to connect
Materialize to your SQL Server instance and start ingesting data:


```mzsql
CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sqlserver_connection
  FOR ALL TABLES;

```

- By default, the source will be created in the active cluster; to use a
  different cluster, use the `IN CLUSTER` clause.
- To ingest data from specific tables use the `FOR TABLES
  (<table1>, <table2>)` options instead of `FOR ALL TABLES`.
- To handle unsupported data types, use the `TEXT COLUMNS` or `EXCLUDE
  COLUMNS` options. Check out the [reference
  documentation](#supported-types) for guidance.


After source creation, refer to [schema changes
considerations](#schema-changes) for information on handling upstream schema changes.


### 4. Right-size the cluster

After the snapshotting phase, Materialize starts ingesting change events from
the SQL Server replication stream. For this work, Materialize generally
performs well with a `100cc` replica, so you can resize the cluster
accordingly.

1. Still in a SQL client connected to Materialize, use the [`ALTER CLUSTER`](/sql/alter-cluster/)
   command to downsize the cluster to `100cc`:

    ```mzsql
    ALTER CLUSTER ingest_sqlserver SET (SIZE '100cc');
    ```

    Behind the scenes, this command adds a new `100cc` replica and removes the
    `200cc` replica.

1. Use the [`SHOW CLUSTER REPLICAS`](/sql/show-cluster-replicas/) command to
   check the status of the new replica:

    ```mzsql
    SHOW CLUSTER REPLICAS WHERE cluster = 'ingest_sqlserver';
    ```
    <p></p>

    ```nofmt
         cluster       | replica |  size  | ready
    -------------------+---------+--------+-------
     ingest_sqlserver  | r1      | 100cc  | t
    (1 row)
    ```


## D. Explore your data

With Materialize ingesting your SQL Server data into durable storage, you can
start exploring the data, computing real-time results that stay up-to-date as
new data arrives, and serving results efficiently.

- Explore your data with [`SHOW SOURCES`](/sql/show-sources) and [`SELECT`](/sql/select/).

- Compute real-time results in memory with [`CREATE VIEW`](/sql/create-view/)
  and [`CREATE INDEX`](/sql/create-index/) or in durable
  storage with [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/).

- Serve results to a PostgreSQL-compatible SQL client or driver with [`SELECT`](/sql/select/)
  or [`SUBSCRIBE`](/sql/subscribe/) or to an external message broker with
  [`CREATE SINK`](/sql/create-sink/).

- Check out the [tools and integrations](/integrations/) supported by
  Materialize.


## Considerations


  {{__hugo_ctx pid=38}}
### Schema changes


  {{__hugo_ctx pid=37}}
> **Note:** Work to more smoothly support ddl changes to upstream tables is currently in
> progress. The work introduces the ability to re-ingest the same upstream table
> under a new schema and switch over without downtime.
>
>

{{__hugo_ctx/}}





Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes

- Adding columns to tables. Materialize will **not ingest** new columns added
  upstream unless you use [`DROP SOURCE`](/sql/alter-source/#context) to first
  drop the affected subsource, and then add the table back to the source using
  [`ALTER SOURCE...ADD SUBSOURCE`](/sql/alter-source/).

- Dropping columns that were added after the source was created. These columns
  are never ingested, so you can drop them without issue.

- Adding or removing `NOT NULL` constraints to tables that were nullable when
  the source was created.

#### Incompatible schema changes

All other schema changes to upstream tables will set the corresponding subsource
into an error state, which prevents you from reading from the source.

To handle incompatible [schema changes](#schema-changes), use [`DROP SOURCE`](/sql/alter-source/#context)
and [`ALTER SOURCE...ADD SUBSOURCE`](/sql/alter-source/) to first drop the
affected subsource, and then add the table back to the source. When you add the
subsource, it will have the updated schema from the corresponding upstream
table.


### Supported types

<p>Materialize natively supports the following SQL Server types:</p>
<ul style="column-count: 3">
<li><code>tinyint</code></li>
<li><code>smallint</code></li>
<li><code>int</code></li>
<li><code>bigint</code></li>
<li><code>real</code></li>
<li><code>double precision</code></li>
<li><code>float</code></li>
<li><code>bit</code></li>
<li><code>decimal</code></li>
<li><code>numeric</code></li>
<li><code>money</code></li>
<li><code>smallmoney</code></li>
<li><code>char</code></li>
<li><code>nchar</code></li>
<li><code>varchar</code></li>
<li><code>varchar(max)</code></li>
<li><code>nvarchar</code></li>
<li><code>nvarchar(max)</code></li>
<li><code>sysname</code></li>
<li><code>binary</code></li>
<li><code>varbinary</code></li>
<li><code>json</code></li>
<li><code>date</code></li>
<li><code>time</code></li>
<li><code>smalldatetime</code></li>
<li><code>datetime</code></li>
<li><code>datetime2</code></li>
<li><code>datetimeoffset</code></li>
<li><code>uniqueidentifier</code></li>
</ul>

<p>Replicating tables that contain <strong>unsupported <a href="/sql/types/">data types</a></strong> is possible via the <a href="/sql/create-source/sql-server/#handling-unsupported-types"><code>EXCLUDE COLUMNS</code> option</a> for the
following types:</p>
<ul style="column-count: 3">
<li><code>text</code></li>
<li><code>ntext</code></li>
<li><code>image</code></li>
<li><code>varbinary(max)</code></li>
</ul>
<p>Columns with the specified types need to be excluded because <a href="https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-capture-instance-ct-transact-sql?view=sql-server-2017#large-object-data-types">SQL Server does not provide
the &ldquo;before&rdquo;</a>
value when said column is updated.</p>


### Timestamp Rounding

The `time`, `datetime2`, and `datetimeoffset` types in SQL Server have a default
scale of 7 decimal places, or in other words a accuracy of 100 nanoseconds. But
the corresponding types in Materialize only support a scale of 6 decimal places.
If a column in SQL Server has a higher scale than what Materialize can support, it
will be rounded up to the largest scale possible.

```
-- In SQL Server
CREATE TABLE my_timestamps (a datetime2(7));
INSERT INTO my_timestamps VALUES
  ('2000-12-31 23:59:59.99999'),
  ('2000-12-31 23:59:59.999999'),
  ('2000-12-31 23:59:59.9999999');

-- Replicated into Materialize
SELECT * FROM my_timestamps;
'2000-12-31 23:59:59.999990'
'2000-12-31 23:59:59.999999'
'2001-01-01 00:00:00'
```

### Snapshot latency for inactive databases

When a new Source is created, Materialize performs a snapshotting operation to sync
the data. However, for a new SQL Server source, if none of the replicating tables
are receiving write queries, snapshotting may take up to an additional 5 minutes
to complete. The 5 minute interval is due to a hardcoded interval in the SQL Server
Change Data Capture (CDC) implementation which only notifies CDC consumers every
5 minutes when no changes are made to replicating tables.

See [Monitoring freshness status](/ingest-data/monitoring-data-ingestion/#monitoring-hydrationdata-freshness-status)

### Capture Instance Selection

When a new source is created, Materialize selects a capture instance for each
table. SQL Server permits at most two capture instances per table, which are
listed in the
[`sys.cdc_change_tables`](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-change-tables-transact-sql)
system table. For each table, Materialize picks the capture instance with the
most recent `create_date`.

If two capture instances for a table share the same timestamp (unlikely given the millisecond resolution), Materialize selects the `capture_instance` with the lexicographically larger name.

### Modifying an existing source


  {{__hugo_ctx pid=34}}
When you add a new subsource to an existing source ([`ALTER SOURCE ... ADD
SUBSOURCE ...`](/sql/alter-source/)), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.
{{__hugo_ctx/}}




{{__hugo_ctx/}}
