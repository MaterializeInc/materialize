<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [Ingest data](/docs/ingest-data/)  /  [SQL
Server](/docs/ingest-data/sql-server/)

</div>

# Ingest data from self-hosted SQL Server

This page shows you how to stream data from a self-hosted SQL Server
database to Materialize using the [SQL Server
Source](/docs/sql/create-source/sql-server/).

<div class="tip">

**💡 Tip:** For help getting started with your own data, you can
schedule a [free guided
trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).

</div>

## Before you begin

- Make sure you are running SQL Server 2016 or higher with Change Data
  Capture (CDC) support. Materialize uses [Change Data
  Capture](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server)
  which is not readily available on older versions of SQL Server.

- Ensure you have access to your SQL Server instance via the [`sqlcmd`
  client](https://learn.microsoft.com/en-us/sql/tools/sqlcmd/sqlcmd-utility),
  or your preferred SQL client.

- Ensure SQL Server Agent is running.

  <div class="highlight">

  ``` chroma
  USE msdb;
  SELECT
    servicename,
    status_desc,
    startup_type_desc
  FROM sys.dm_server_services
  WHERE servicename LIKE 'SQL Server Agent%';
  ```

  </div>

## A. Configure SQL Server

<div class="note">

**NOTE:** To configure SQL Server for data ingestion into Materialize,
you must be a user with privileges to enable CDC and create/manage
login, users, roles, and privileges.

</div>

### 1. Create a Materialize user in SQL Server.

Create a user that Materialize will use to connect when ingesting data.

1.  In `master`:

    1.  Create a login `materialize` (replace `<PASSWORD>` with your own
        password):

        <div class="highlight">

        ``` chroma
        USE master;

        -- Specify additional options per your company's security policy
        CREATE LOGIN materialize WITH PASSWORD = '<PASSWORD>',
        DEFAULT_DATABASE = <DATABASE_NAME>;
        GO -- The GO terminator may be unsupported or unnecessary for your client.
        ```

        </div>

    2.  Create a user `materialize` for the login and role
        `materialize_role`:

        <div class="highlight">

        ``` chroma
        USE master;
        CREATE USER materialize FOR LOGIN materialize;
        CREATE ROLE materialize_role;
        ALTER ROLE materialize_role ADD MEMBER materialize;
        GO -- The GO terminator may be unsupported or unnecessary for your client.
        ```

        </div>

    3.  Grant permissions to the `materialize_role` to enable discovery
        of the tables to be replicated and monitoring replication
        progress:

        <div class="highlight">

        ``` chroma
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

        </div>

2.  In the database from which which you want to ingest data,

    1.  Create a second `materialize` user and a second
        `materialize_role`.

    2.  Add `materialize` user as a member to the `materialize_role` and
        `db_datareader` roles (replace `<DATABASE_NAME>` with your
        database name).

    <div class="highlight">

    ``` chroma
    USE <DATABASE_NAME>;

    -- Use the same user name and role name as those created in master
    CREATE USER materialize FOR LOGIN materialize;
    CREATE ROLE materialize_role;
    ALTER ROLE materialize_role ADD MEMBER materialize;
    ALTER ROLE db_datareader ADD MEMBER materialize;
    GO -- The GO terminator may be unsupported or unnecessary for your client.
    ```

    </div>

### 2. Enable Change-Data-Capture for the database.

In SQL Server, for the database from which you want to ingest data,
enable change data capture (replace `<DATABASE_NAME>` with your database
name):

<div class="highlight">

``` chroma
USE <DATABASE_NAME>;
GO -- The GO terminator may be unsupported or unnecessary for your client.
EXEC sys.sp_cdc_enable_db;
GO -- The GO terminator may be unsupported or unnecessary for your client.
```

</div>

For guidance on enabling Change Data Capture, see the [SQL Server
documentation](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-db-transact-sql).

### 3. Enable `SNAPSHOT` transaction isolation.

Enable `SNAPSHOT` transaction isolation for the database (replace
`<DATABASE_NAME>` with your database name):

<div class="highlight">

``` chroma
ALTER DATABASE <DATABASE_NAME> SET ALLOW_SNAPSHOT_ISOLATION ON;
GO -- The GO terminator may be unsupported or unnecessary for your client.
```

</div>

For guidance on enabling `SNAPSHOT` transaction isolation, see the [SQL
Server
documentation](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-table-transact-sql)

### 4. Enable Change-Data-Capture for the tables.

Enable Change Data Capture for each table you wish to replicate (replace
`<DATABASE_NAME>`, `<SCHEMA_NAME>`, and `<TABLE_NAME>` with the your
database, schema name, and table name):

<div class="highlight">

``` chroma
USE <DATABASE_NAME>;

EXEC sys.sp_cdc_enable_table
  @source_schema = '<SCHEMA_NAME>',
  @source_name = '<TABLE_NAME>',
  @role_name = 'materialize_role',
  @supports_net_changes = 0;
GO -- The GO terminator may be unsupported or unnecessary for your client.
```

</div>

## B. (Optional) Configure network security

<div class="note">

**NOTE:** If you are prototyping and your SQL Server instance is
publicly accessible, **you can skip this step**. For production
scenarios, we recommend configuring one of the network security options
below.

</div>

There are various ways to configure your database’s network to allow
Materialize to connect:

- **Allow Materialize IPs:** If your database is publicly accessible,
  you can configure your database’s firewall to allow connections from a
  set of static Materialize IP addresses.

- **Use an SSH tunnel:** If your database is running in a private
  network, you can use an SSH tunnel to connect Materialize to the
  database.

Select the option that works best for you.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-allow-materialize-ips" class="tab-pane"
title="Allow Materialize IPs">

1.  In the [SQL Shell](/docs/console/), or your preferred SQL client
    connected to Materialize, find the static egress IP addresses for
    the Materialize region you are running in:

    <div class="highlight">

    ``` chroma
    SELECT * FROM mz_egress_ips;
    ```

    </div>

2.  Update your database firewall rules to allow traffic from each IP
    address from the previous step.

</div>

<div id="tab-use-aws-privatelink" class="tab-pane"
title="Use AWS PrivateLink">

Materialize can connect to a SQL Server database through an [AWS
PrivateLink](https://aws.amazon.com/privatelink/) service. Your SQL
Server database must be running on AWS in order to use this option.

1.  Create a dedicated [target
    group](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html)
    for your SQL Server instance with the following details:

    a\. Target type as **IP address**.

    b\. Protocol as **TCP**.

    c\. Port as **1433**, or the port that you are using in case it is
    not 1433.

    d\. Make sure that the target group is in the same VPC as the SQL
    Server instance.

    e\. Click next, and register the respective SQL Server instance to
    the target group using its IP address.

2.  Create a [Network Load
    Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html)
    that is **enabled for the same subnets** that the SQL Server
    instance is in.

3.  Create a [TCP
    listener](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-listener.html)
    for your SQL Server instance that forwards to the corresponding
    target group you created.

4.  Verify security groups and health checks. Once the TCP listener has
    been created, make sure that the [health
    checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html)
    are passing and that the target is reported as healthy.

    If you have set up a security group for your SQL Server instance,
    you must ensure that it allows traffic on the health check port.

    **Remarks**:

    a\. Network Load Balancers do not have associated security groups.
    Therefore, the security groups for your targets must use IP
    addresses to allow traffic.

    b\. You can’t use the security groups for the clients as a source in
    the security groups for the targets. Therefore, the security groups
    for your targets must use the IP addresses of the clients to allow
    traffic. For more details, check the [AWS
    documentation](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html).

5.  Create a VPC [endpoint
    service](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html)
    and associate it with the **Network Load Balancer** that you’ve just
    created.

    Note the **service name** that is generated for the endpoint
    service.

    **Remarks**:

    By disabling [Acceptance
    Required](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests),
    while still strictly managing who can view your endpoint via IAM,
    Materialze will be able to seamlessly recreate and migrate endpoints
    as we work to stabilize this feature.

6.  In Materialize, create a
    [`AWS PRIVATELINK`](/docs/sql/create-connection/#aws-privatelink)
    connection that references the endpoint service that you created in
    the previous step.

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
       SERVICE NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
       AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az3')
    );
    ```

    </div>

    Update the list of the availability zones to match the ones that you
    are using in your AWS account.

7.  Configure the AWS PrivateLink service.

    Retrieve the AWS principal for the AWS PrivateLink connection you
    just created:

    <div class="highlight">

    ``` chroma
    SELECT principal
    FROM mz_aws_privatelink_connections plc
    JOIN mz_connections c ON plc.id = c.id
    WHERE c.name = 'privatelink_svc';
    ```

    </div>

    ```
                                     principal
    ---------------------------------------------------------------------------
     arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
    ```

    Follow the instructions in the [AWS PrivateLink
    documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/add-endpoint-service-permissions.html)
    to configure your VPC endpoint service to accept connections from
    the provided AWS principal.

    If your AWS PrivateLink service is configured to require acceptance
    of connection requests, you must manually approve the connection
    request from Materialize after executing the `CREATE CONNECTION`
    statement. For more details, check the [AWS PrivateLink
    documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).

    **Note:** It might take some time for the endpoint service
    connection to show up, so you would need to wait for the endpoint
    service connection to be ready before you create a source.

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

    1.  In the [SQL Shell](/docs/console/), or your preferred SQL client
        connected to Materialize, get the static egress IP addresses for
        the Materialize region you are running in:

        <div class="highlight">

        ``` chroma
        SELECT * FROM mz_egress_ips;
        ```

        </div>

    2.  Update your SSH bastion host’s firewall rules to allow traffic
        from each IP address from the previous step.

3.  Update your database firewall rules to allow traffic from the SSH
    bastion host.

</div>

</div>

</div>

## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

<div class="note">

**NOTE:** If you are prototyping and already have a cluster to host your
SQL Server source (e.g. `quickstart`), **you can skip this step**. For
production scenarios, we recommend separating your workloads into
multiple clusters for [resource
isolation](/docs/sql/create-cluster/#resource-isolation).

</div>

In Materialize, a [cluster](/docs/concepts/clusters/) is an isolated
environment, similar to a virtual warehouse in Snowflake. When you
create a cluster, you choose the size of its compute resource allocation
based on the work you need the cluster to do, whether ingesting data
from a source, computing always-up-to-date query results, serving
results to clients, or a combination.

In this case, you’ll create a dedicated cluster for ingesting source
data from your SQL Server database.

1.  In the [SQL Shell](/docs/console/), or your preferred SQL client
    connected to Materialize, use the
    [`CREATE CLUSTER`](/docs/sql/create-cluster/) command to create the
    new cluster:

    <div class="highlight">

    ``` chroma
    CREATE CLUSTER ingest_sqlserver (SIZE = '200cc');

    SET CLUSTER = ingest_sqlserver;
    ```

    </div>

    A cluster of [size](/docs/sql/create-cluster/#size) `200cc` should
    be enough to process the initial snapshot of the tables in your SQL
    Server database. For very large snapshots, consider using a larger
    size to speed up processing. Once the snapshot is finished, you can
    readjust the size of the cluster to fit the volume of changes being
    replicated from your upstream SQL Server database.

### 2. Create a connection

Once you have configured your network, create a connection in
Materialize per your networking configuration.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-allow-materialize-ips" class="tab-pane"
title="Allow Materialize IPs">

1.  In the [SQL Shell](/docs/console/), or your preferred SQL client
    connected to Materialize, use the
    [`CREATE SECRET`](/docs/sql/create-secret/) command to securely
    store the password for the SQL Server role you’ll use to replicate
    data into Materialize:

    <div class="highlight">

    ``` chroma
    CREATE SECRET sqlserver_pass AS '<PASSWORD>';
    ```

    </div>

2.  Use the [`CREATE CONNECTION`](/docs/sql/create-connection/) command
    to create a connection object with access and authentication details
    for Materialize to use:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION sqlserver_connection TO SQL SERVER (
        HOST <host>,
        PORT 1433,
        USER 'materialize',
        PASSWORD SECRET sqlserver_pass,
        DATABASE <database>,
        SSL MODE 'required'
    );
    ```

    </div>

    - Replace `<host>` with your SQL Server endpoint, and `<database>`
      with the database you’d like to connect to.

</div>

<div id="tab-use-an-aws-privatelink-cloud-only" class="tab-pane"
title="Use an AWS Privatelink (Cloud-only)">

1.  In the [SQL Shell](/docs/console/), or your preferred SQL client
    connected to Materialize, use the
    [`CREATE CONNECTION`](/docs/sql/create-connection/#aws-privatelink)
    command to create an AWS PrivateLink connection:

    ↕️ **In-region connections**

    To connect to an AWS PrivateLink endpoint service in the **same
    region** as your Materialize environment:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
      SERVICE NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
      AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az4')
    );
    ```

    </div>

    - Replace the `SERVICE NAME` value with the service name you noted
      [earlier](#b-optional-configure-network-security).

    - Replace the `AVAILABILITY ZONES` list with the IDs of the
      availability zones in your AWS account. For in-region connections
      the availability zones of the NLB and the consumer VPC **must
      match**.

      To find your availability zone IDs, select your database in the
      RDS Console and click the subnets under **Connectivity &
      security**. For each subnet, look for **Availability Zone ID**
      (e.g., `use1-az6`), not **Availability Zone** (e.g.,
      `us-east-1d`).

    ↔️ **Cross-region connections**

    To connect to an AWS PrivateLink endpoint service in a **different
    region** to the one where your Materialize environment is deployed:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
      SERVICE NAME 'com.amazonaws.vpce.us-west-1.vpce-svc-<endpoint_service_id>',
      -- For now, the AVAILABILITY ZONES clause **is** required, but will be
      -- made optional in a future release.
      AVAILABILITY ZONES ()
    );
    ```

    </div>

    - Replace the `SERVICE NAME` value with the service name you noted
      [earlier](#b-optional-configure-network-security).

    - The service name region refers to where the endpoint service was
      created. You **do not need** to specify `AVAILABILITY ZONES`
      manually — these will be optimally auto-assigned when none are
      provided.

2.  Retrieve the AWS principal for the AWS PrivateLink connection you
    just created:

    <div class="highlight">

    ``` chroma
    SELECT principal
      FROM mz_aws_privatelink_connections plc
      JOIN mz_connections c ON plc.id = c.id
      WHERE c.name = 'privatelink_svc';
    ```

    </div>

    ```
    principal
    ---------------------------------------------------------------------------
    arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
    ```

3.  Update your VPC endpoint service to [accept connections from the AWS
    principal](https://docs.aws.amazon.com/vpc/latest/privatelink/add-endpoint-service-permissions.html).

4.  If your AWS PrivateLink service is configured to require acceptance
    of connection requests, [manually approve the connection request
    from
    Materialize](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).

    **Note:** It can take some time for the connection request to show
    up. Do not move on to the next step until you’ve approved the
    connection.

5.  Validate the AWS PrivateLink connection you created using the
    [`VALIDATE CONNECTION`](/docs/sql/validate-connection) command:

    <div class="highlight">

    ``` chroma
    VALIDATE CONNECTION privatelink_svc;
    ```

    </div>

    If no validation error is returned, move to the next step.

6.  Use the [`CREATE SECRET`](/docs/sql/create-secret/) command to
    securely store the password for the `materialize` SQL Server user
    [you created](#1-create-a-materialize-user-in-sql-server):

    <div class="highlight">

    ``` chroma
    CREATE SECRET sql_server_pass AS '<PASSWORD>';
    ```

    </div>

7.  Use the [`CREATE CONNECTION`](/docs/sql/create-connection/) command
    to create another connection object, this time with database access
    and authentication details for Materialize to use:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION sql_server_connection TO SQL SERVER (
    HOST <host>,
      PORT 1433,
      USER 'materialize',
      PASSWORD SECRET sql_server_pass,
      SSL MODE REQUIRED,
      AWS PRIVATELINK privatelink_svc
    );
    ```

    </div>

    - Replace `<host>` with your RDS endpoint. To find your RDS
      endpoint, select your database in the RDS Console, and look under
      **Connectivity & security**.

      - Replace `<database>` with the name of the database containing
        the tables you want to replicate to Materialize.

    AWS IAM authentication is also available, see the
    [`CREATE CONNECTION`](/docs/sql/create-connection/#mysql) command
    for details.

</div>

<div id="tab-use-an-ssh-tunnel" class="tab-pane"
title="Use an SSH tunnel">

1.  In the [SQL Shell](/docs/console/), or your preferred SQL client
    connected to Materialize, use the
    [`CREATE CONNECTION`](/docs/sql/create-connection/#ssh-tunnel)
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
      [earlier](#b-optional-configure-network-security).

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
    [`VALIDATE CONNECTION`](/docs/sql/validate-connection) command:

    <div class="highlight">

    ``` chroma
    VALIDATE CONNECTION ssh_connection;
    ```

    </div>

    If no validation error is returned, move to the next step.

5.  Use the [`CREATE SECRET`](/docs/sql/create-secret/) command to
    securely store the password for the `materialize` SQL Server user
    [you created](#1-create-a-materialize-user-in-sql-server):

    <div class="highlight">

    ``` chroma
    CREATE SECRET sql_server_pass AS '<PASSWORD>';
    ```

    </div>

    For AWS IAM authentication, you must create a connection to AWS. See
    the [`CREATE CONNECTION`](/docs/sql/create-connection/#aws) command
    for details.

6.  Use the [`CREATE CONNECTION`](/docs/sql/create-connection/) command
    to create another connection object, this time with database access
    and authentication details for Materialize to use:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION sql_server_connection TO SQL SERVER (
    HOST '<host>',
    SSH TUNNEL ssh_connection
    );
    ```

    </div>

    - Replace `<host>` with your SQL Server endpoint.

</div>

</div>

</div>

### 3. Start ingesting data

<div class="note">

**NOTE:** For a new SQL Server source, if none of the replicating tables
are receiving write queries, snapshotting may take up to an additional 5
minutes to complete. For details, see [snapshot latency for inactive
databases](#snapshot-latency-for-inactive-databases)

</div>

Use the [`CREATE SOURCE`](/docs/sql/create-source/) command to connect
Materialize to your SQL Server instance and start ingesting data:

<div class="highlight">

``` chroma
CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sqlserver_connection
  FOR ALL TABLES;
```

</div>

- By default, the source will be created in the active cluster; to use a
  different cluster, use the `IN CLUSTER` clause.
- To ingest data from specific tables use the
  `FOR TABLES (<table1>, <table2>)` options instead of `FOR ALL TABLES`.
- To handle unsupported data types, use the `TEXT COLUMNS` or
  `EXCLUDE COLUMNS` options. Check out the [reference
  documentation](#supported-types) for guidance.

After source creation, refer to [schema changes
considerations](#schema-changes) for information on handling upstream
schema changes.

### 4. Right-size the cluster

After the snapshotting phase, Materialize starts ingesting change events
from the SQL Server replication stream. For this work, Materialize
generally performs well with a `100cc` replica, so you can resize the
cluster accordingly.

1.  Still in a SQL client connected to Materialize, use the
    [`ALTER CLUSTER`](/docs/sql/alter-cluster/) command to downsize the
    cluster to `100cc`:

    <div class="highlight">

    ``` chroma
    ALTER CLUSTER ingest_sqlserver SET (SIZE '100cc');
    ```

    </div>

    Behind the scenes, this command adds a new `100cc` replica and
    removes the `200cc` replica.

2.  Use the [`SHOW CLUSTER REPLICAS`](/docs/sql/show-cluster-replicas/)
    command to check the status of the new replica:

    <div class="highlight">

    ``` chroma
    SHOW CLUSTER REPLICAS WHERE cluster = 'ingest_sqlserver';
    ```

    </div>

    ```
         cluster       | replica |  size  | ready
    -------------------+---------+--------+-------
     ingest_sqlserver  | r1      | 100cc  | t
    (1 row)
    ```

## D. Explore your data

With Materialize ingesting your SQL Server data into durable storage,
you can start exploring the data, computing real-time results that stay
up-to-date as new data arrives, and serving results efficiently.

- Explore your data with [`SHOW SOURCES`](/docs/sql/show-sources) and
  [`SELECT`](/docs/sql/select/).

- Compute real-time results in memory with
  [`CREATE VIEW`](/docs/sql/create-view/) and
  [`CREATE INDEX`](/docs/sql/create-index/) or in durable storage with
  [`CREATE MATERIALIZED VIEW`](/docs/sql/create-materialized-view/).

- Serve results to a PostgreSQL-compatible SQL client or driver with
  [`SELECT`](/docs/sql/select/) or [`SUBSCRIBE`](/docs/sql/subscribe/)
  or to an external message broker with
  [`CREATE SINK`](/docs/sql/create-sink/).

- Check out the [tools and integrations](/docs/integrations/) supported
  by Materialize.

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
source.

To handle incompatible [schema changes](#schema-changes), use
[`DROP SOURCE`](/docs/sql/alter-source/#context) and
[`ALTER SOURCE...ADD SUBSOURCE`](/docs/sql/alter-source/) to first drop
the affected subsource, and then add the table back to the source. When
you add the subsource, it will have the updated schema from the
corresponding upstream table.

### Supported types

Materialize natively supports the following SQL Server types:

- `tinyint`
- `smallint`
- `int`
- `bigint`
- `real`
- `double precision`
- `float`
- `bit`
- `decimal`
- `numeric`
- `money`
- `smallmoney`
- `char`
- `nchar`
- `varchar`
- `nvarchar`
- `sysname`
- `binary`
- `varbinary`
- `json`
- `date`
- `time`
- `smalldatetime`
- `datetime`
- `datetime2`
- `datetimeoffset`
- `uniqueidentifier`

Replicating tables that contain **unsupported [data
types](/docs/sql/types/)** is possible via the [`EXCLUDE COLUMNS`
option](/docs/sql/create-source/sql-server/#handling-unsupported-types)
for the following types:

- `text`
- `ntext`
- `image`
- `varchar(max)`
- `nvarchar(max)`
- `varbinary(max)`

Columns with the specified types need to be excluded because [SQL Server
does not provide the
“before”](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-capture-instance-ct-transact-sql?view=sql-server-2017#large-object-data-types)
value when said column is updated.

### Timestamp Rounding

The `time`, `datetime2`, and `datetimeoffset` types in SQL Server have a
default scale of 7 decimal places, or in other words a accuracy of 100
nanoseconds. But the corresponding types in Materialize only support a
scale of 6 decimal places. If a column in SQL Server has a higher scale
than what Materialize can support, it will be rounded up to the largest
scale possible.

```
-- In SQL Server
CREATE TABLE my_timestamps (a datetime2(7));
INSERT INTO my_timestamps VALUES
  ('2000-12-31 23:59:59.99999'),
  ('2000-12-31 23:59:59.999999'),
  ('2000-12-31 23:59:59.9999999');
– Replicated into Materialize
SELECT * FROM my_timestamps;
'2000-12-31 23:59:59.999990'
'2000-12-31 23:59:59.999999'
'2001-01-01 00:00:00'
```

### Snapshot latency for inactive databases

When a new Source is created, Materialize performs a snapshotting
operation to sync the data. However, for a new SQL Server source, if
none of the replicating tables are receiving write queries, snapshotting
may take up to an additional 5 minutes to complete. The 5 minute
interval is due to a hardcoded interval in the SQL Server Change Data
Capture (CDC) implementation which only notifies CDC consumers every 5
minutes when no changes are made to replicating tables.

See [Monitoring freshness
status](/docs/ingest-data/monitoring-data-ingestion/#monitoring-hydrationdata-freshness-status)

### Capture Instance Selection

When a new source is created, Materialize selects a capture instance for
each table. SQL Server permits at most two capture instances per table,
which are listed in the
[`sys.cdc_change_tables`](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-change-tables-transact-sql)
system table. For each table, Materialize picks the capture instance
with the most recent `create_date`.

If two capture instances for a table share the same timestamp (unlikely
given the millisecond resolution), Materialize selects the
`capture_instance` with the lexicographically larger name.

### Modifying an existing source

When you add a new subsource to an existing source
([`ALTER SOURCE ... ADD SUBSOURCE ...`](/docs/sql/alter-source/)),
Materialize starts the snapshotting process for the new subsource.
During this snapshotting, the data ingestion for the existing subsources
for the same source is temporarily blocked. As such, if possible, you
can resize the cluster to speed up the snapshotting process and once the
process finishes, resize the cluster for steady-state.

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/ingest-data/sql-server/self-hosted.md"
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
