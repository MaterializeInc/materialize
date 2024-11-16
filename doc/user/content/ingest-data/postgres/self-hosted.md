---
title: "Ingest data from self-hosted PostgreSQL"
description: "How to stream data from self-hosted PostgreSQL database to Materialize"
aliases:
  - /ingest-data/postgres-self-hosted/
menu:
  main:
    parent: "postgresql"
    name: "Self-hosted"
    identifier: "pg-self-hosted"
---

This page shows you how to stream data from a self-hosted PostgreSQL database to
Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

{{% postgres-direct/before-you-begin %}}

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

{{% postgres-direct/create-a-publication-other %}}

## B. (Optional) Configure network security

{{< note >}}
If you are prototyping and your PostgreSQL instance is publicly
accessible, **you can skip this step**. For production scenarios, we recommend
configuring one of the network security options below.
{{</ note >}}

There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's firewall to allow connections from a set of
    static Materialize IP addresses.

- **Use an SSH tunnel:** If your database is running in a private network, you
    can use an SSH tunnel to connect Materialize to the database.

Select the option that works best for you.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}

1. In the [Materialize console's SQL Shell](https://console.materialize.com/),
   or your preferred SQL client connected to Materialize, find the static egress
   IP addresses for the Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

1. Update your database firewall rules to allow traffic from each IP address
   from the previous step.

{{< /tab >}}

{{< tab "Use AWS PrivateLink">}}

Materialize can connect to a PostgreSQL database through an [AWS PrivateLink](https://aws.amazon.com/privatelink/)
service. Your PostgreSQL database must be running on AWS in order to use this
option.

1. #### Create a target group

    Create a dedicated [target group](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html)
    for your Postgres instance with the following details:

    a. Target type as **IP address**.

    b. Protocol as **TCP**.

    c. Port as **5432**, or the port that you are using in case it is not 5432.

    d. Make sure that the target group is in the same VPC as the PostgreSQL
    instance.

    e. Click next, and register the respective PostgreSQL instance to the target
    group using its IP address.

1. #### Create a Network Load Balancer (NLB)

    Create a [Network Load Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html)
    that is **enabled for the same subnets** that the PostgreSQL instance is
    in.

1. #### Create TCP listener

    Create a [TCP listener](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-listener.html)
    for your PostgreSQL instance that forwards to the corresponding target
    group you created.

1. #### Verify security groups and health checks

    Once the TCP listener has been created, make sure that the [health checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html)
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

1. #### Create a VPC endpoint service

    Create a VPC [endpoint service](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html)
    and associate it with the **Network Load Balancer** that you’ve just
    created.

    Note the **service name** that is generated for the endpoint service.

    **Remarks**:

    By disabling [Acceptance Required](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests),
    while still strictly managing who can view your endpoint via IAM,
    Materialze will be able to seamlessly recreate and migrate endpoints as we
    work to stabilize this feature.

1. #### Create an AWS PrivateLink Connection

     In Materialize, create a [`AWS PRIVATELINK`](/sql/create-connection/#aws-privatelink) connection that references the
     endpoint service that you created in the previous step.

     ```mzsql
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
        SERVICE NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
        AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az3')
    );
    ```

    Update the list of the availability zones to match the ones that you are
    using in your AWS account.

1. #### Configure the AWS PrivateLink service

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

{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

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
       Shell](https://console.materialize.com/), or your preferred SQL client
       connected to Materialize, get the static egress IP addresses for the
       Materialize region you are running in:

       ```mzsql
       SELECT * FROM mz_egress_ips;
       ```

    1. Update your SSH bastion host's firewall rules to allow traffic from each
       IP address from the previous step.

1. Update your database firewall rules to allow traffic from the SSH bastion
   host.

{{< /tab >}}

{{< /tabs >}}

## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

{{< note >}}
If you are prototyping and already have a cluster to host your PostgreSQL
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](https://materialize.com/docs/sql/create-cluster/#resource-isolation).
{{< /note >}}

![Image of the Create New Cluster flow](/images/console/console-create-new/postgresql/create-new-cluster-flow.png "Create New Cluster flow")

From the [Materialize Console](https://console.materialize.com/),

1. Click **+ Create New** and select **Cluster** to open the **New Cluster**
   panel.

1. In the **New Cluster** panel,

   1. Specify the following cluster information:

      | Field | Description | Example |
      | ----- | ----------- | ------- |
      | **Name** | A name for the cluster. | `ingest_postgres` |
      | [**Size**](/sql/create-cluster/#size) | The size of the cluster. | `200cc` <br> A cluster of [size](/concepts/clusters/#cluster-sizing) `200cc` should be enough to process the initial snapshot of the tables in your publication. For very large snapshots, consider using a larger size to speed up processing. Once the snapshot is finished, you can readjust the size of the cluster to fit the volume of changes being replicated from your upstream PostgeSQL database. |
      | [**Replica**](/concepts/clusters/#fault-tolerance) | The replication factor for the cluster. | `1` <br>Clusters that contain sources can only have a replication factor of 0 or 1.|

   1. Click **Create** to create the cluster.

1. Upon successful creation, the newly created cluster's **Overview** page
   opens.

Alternatively, you can create a cluster using the [`CREATE
CLUSTER`](/sql/create-cluster/) command in the [SQL
Shell](https://console.materialize.com/) (or your preferred SQL client
connected to Materialize). For example, to create a cluster named
`ingest_postgres` of size `200cc` (using the default replication factor of 1):

```mzsql
CREATE CLUSTER ingest_postgres (SIZE = '200cc');

SET CLUSTER = ingest_postgres;
```


### 2. Start ingesting data

Now that you've configured your database network and created an ingestion
cluster, you can connect Materialize to your PostgreSQL database and start
ingesting data. The exact steps depend on your networking configuration, so
start by selecting the relevant option.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}


1. Click **+ Create New** and select **Source** to open the **Create a Source**
   panel. From the **Create a Source** panel, select **PostgreSQL**.

   ![Image of the Create New Source start](/images/console/console-create-new/postgresql/create-new-source-start.png "Create New Source start")


1. From the **Configure a connection** panel, you can choose to:

   - Use an **Existing** connection.

   - Create a **New** connection.

   For a new connection, specify the following connection information and click **Continue**:

   | Field | Description |
   | ----- | ----------- |
   | **Name** | A name for the connection. |
   | **Schema** | A Materialize database and schema for the connection. |
   | **Host** | The host of the PostgreSQL. |
   | **Database** | The [database in PostgreSQL that contains the tables to replicate and where the replication user has CONNECT privilege](#2-create-a-publication-and-a-replication-user).|
   | **Port** | The port number of the PostgreSQL. |
   | **User** | The [replication user in PostgreSQL created earlier](#create-a-replication-user-for-materialize). |
   | **Password** | The [replication user's password in PostgreSQL](#create-a-replication-user-for-materialize). Click the **Create a new secret** button to securely store the password under a name. Once created, select the secret's name in the **Password** field. |
   | **SSL Authentication** | Toggle on the TLS/SSL mode for the connection. |
   | **SSL Key** | PEM key file for the SSL certificate. Click the **Create a new secret** button to securely store the PEM key file content under a name. Once created, select the secret's name in the **SSL Key** field. |
   | **SSL Certificate** | PEM certificate file for the SSL certificate. Click the **Create a new secret** button to securely store the PEM certificate file content under a name. Once created, select the secret's name in the **SSL Certificate** field. |
   | **SSL Mode** | Select from **require**, **verify-ca**, or **verify-full**. For **verify-ca** and **verify-full**, you also need to provide the **SSL Certificate Authority**. Click the **Create a new secret** button to securely store the CA PEM file contentunder a name. Once created, select the secret's name in the **SSL Certificate Authority** field. |

   ![Image of the Create a new source
   connection](/images/console/console-create-new/postgresql/ create-a-source-connection.png
   "Create a new source connection")

   Alternatively, in the [SQL Shell](https://console.materialize.com/) (or your
   preferred SQL client connected to Materialize), you can:

   - Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store
     the password for the `materialize` PostgreSQL user you created
     [earlier](#2-create-a-publication-and-a-replication-user):

      ```mzsql
      CREATE SECRET pgpass AS '<PASSWORD>';
      ```

   - Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
     connection object with access and authentication details for Materialize to
     use:

      ```mzsql
      CREATE CONNECTION pg_connection TO POSTGRES (
          HOST '<host>',
          PORT 5432,
          USER 'materialize',
          PASSWORD SECRET pgpass,
          SSL MODE 'require',
          DATABASE '<database>'
      );
      ```

      - Replace `<host>` with your database endpoint.

      - Replace `<database>` with the name of the database containing the tables
        you want to replicate to Materialize.

1. From the **Configure source** panel, enter the following information and
   click **Create source**:

   | Field | Description |
   | ----- | ----------- |
   | **Name** | A name for the source in Materialize. |
   | **Schema** | A Materialize database and schema for the source. |
   | **Cluster** | The cluster for the source. |
   | **Publication** | The name of the publication you created [earlier](#2-create-a-publication-and-a-replication-user). |
   | **For all tables** | Toggle on if you want to replicate all tables in the publication and you created a publication that specifies **ALL TABLES** [earlier](#2-create-a-publication-and-a-replication-user). |
   | **Table name** | Name of the table  table in the publication to replicate and you created a publication that specifies the table [earlier](#2-create-a-publication-and-a-replication-user). |
   | **Alias** | Optional alias for the table. |

   ![Image of the Create a new
   source configuration](/images/console/console-create-new/postgresql/create-new-source-configuration.png "Create
   a new source configuration")

   If successful, the **Overview** page for the source opens where you can
   monitor the ingestion status. See [Monitor the ingestion status](#3-monitor-the-ingestion-status) for details.

   Alternatively, in the [SQL Shell](https://console.materialize.com/) (or your
   preferred SQL client connected to Materialize), you can use the [`CREATE
   SOURCE`](/sql/create-source/) command to connect Materialize to your database
   and start ingesting data from the publication you created
   [earlier](#2-create-a-publication-and-a-replication-user):

    ```mzsql
    CREATE SOURCE mz_source
      IN CLUSTER ingest_postgres
      FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
      FOR ALL TABLES;
    ```

    By default, the source will be created in the active cluster; to use a
    different cluster, use the `IN CLUSTER` clause. To ingest data from
    specific schemas or tables in your publication, use `FOR SCHEMAS
    (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` instead of `FOR
    ALL TABLES`.

{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

1. In the [Materialize console's SQL Shell](https://console.materialize.com/),
   or your preferred SQL client connected to Materialize, use the [`CREATE
   CONNECTION`](/sql/create-connection/#ssh-tunnel) command to create an SSH
   tunnel connection:

    ```mzsql
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        PORT <SSH_BASTION_PORT>,
        USER '<SSH_BASTION_USER>'
    );
    ```

    - Replace `<SSH_BASTION_HOST>` and `<SSH_BASTION_PORT`> with the public IP
      address and port of the SSH bastion host you created [earlier](#b-optional-configure-network-security).

    - Replace `<SSH_BASTION_USER>` with the username for the key pair you
      created for your SSH bastion host.

1. Get Materialize's public keys for the SSH tunnel connection you just
   created:

    ```mzsql
    SELECT
        mz_connections.name,
        mz_ssh_tunnel_connections.*
    FROM
        mz_connections JOIN
        mz_ssh_tunnel_connections USING(id)
    WHERE
        mz_connections.name = 'ssh_connection';
    ```

1. Log in to your SSH bastion host and add Materialize's public keys to the
   `authorized_keys` file, for example:

    ```sh
    # Command for Linux
    echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
    echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys
    ```

1. Back in the SQL client connected to Materialize, validate the SSH tunnel
   connection you created using the [`VALIDATE CONNECTION`](/sql/validate-connection)
   command:

    ```mzsql
    VALIDATE CONNECTION ssh_connection;
    ```

    If no validation error is returned, move to the next step.

1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
   password for the `materialize` PostgreSQL user you created [earlier](#2-create-a-publication-and-a-replication-user):

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
      SSH TUNNEL ssh_connection
      );
    ```

    - Replace `<host>` with your database endpoint.

    - Replace `<database>` with the name of the database containing the tables
      you want to replicate to Materialize.

1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize
   to your Azure instance and start ingesting data from the publication you
   created [earlier](#2-create-a-publication-and-a-replication-user):

    ```mzsql
    CREATE SOURCE mz_source
      IN CLUSTER ingest_postgres
      FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
      FOR ALL TABLES;
    ```

    By default, the source will be created in the active cluster; to use a
    different cluster, use the `IN CLUSTER` clause. To ingest data from
    specific schemas or tables in your publication, use `FOR SCHEMAS
    (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` instead of `FOR
    ALL TABLES`.

1. After source creation, you can handle upstream [schema changes](/sql/create-source/postgres/#schema-changes)
   for specific replicated tables using the [`ALTER SOURCE...{ADD | DROP} SUBSOURCE`](/sql/alter-source/#context)
   syntax.

{{< /tab >}}

{{< tab "AWS PrivateLink">}}

1. Back in the SQL client connected to Materialize, use the [`CREATE SECRET`](/sql/create-secret/)
   command to securely store the password for the `materialize` PostgreSQL user you
   created [earlier](#2-create-a-publication-and-a-replication-user):

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
        USER postgres,
        PASSWORD SECRET pgpass,
        DATABASE <database>,
        AWS PRIVATELINK privatelink_svc
    );
    ```

    - Replace `<host>` with your database endpoint.

    - Replace `<database>` with the name of the database containing the tables
      you want to replicate to Materialize.

1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize
   to your database and start ingesting data from the publication you created
   [earlier](#2-create-a-publication-and-a-replication-user):

    ```mzsql
    CREATE SOURCE mz_source
      IN CLUSTER ingest_postgres
      FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
      FOR ALL TABLES;
    ```

    By default, the source will be created in the active cluster; to use a
    different cluster, use the `IN CLUSTER` clause. To ingest data from
    specific schemas or tables in your publication, use `FOR SCHEMAS
    (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` instead of `FOR
    ALL TABLES`.

{{< /tab >}}

{{< /tabs >}}

### 3. Monitor the ingestion status

{{< tabs >}}

{{< tab "Console">}}

From the [**Database object explorer**](/console/data/) in the [Materialize
console], you can see the status of the source you created.

Before it starts consuming the replication stream, Materialize takes a snapshot of the relevant tables in your publication. Until this snapshot is complete, Materialize won't have the same view of your data as your PostgreSQL database.

![Image of the new source status:
snapshotting](/images/console/console-create-new/postgresql/new-source-status-snapshotting.png
"New source status: snapshotting")

Snapshotting can take between a few minutes to several hours, depending on the
size of your dataset and the size of the cluster the source is running in.

Once the source is running, the **Status** changes to **Running**,

![Image of the new source status:
running](/images/console/console-create-new/postgresql/new-source-status-running.png
"New source status: running")

{{< /tab >}}

{{< tab "SQL commands">}}
{{% postgres-direct/check-the-ingestion-status %}}
{{< /tab >}}

{{< /tabs >}}

### 4. Right-size the cluster

{{< tabs >}}

{{< tab "Console">}}

After the snapshotting phase, Materialize starts ingesting change events from
the PostgreSQL replication stream. For this work, Materialize generally performs
well with an `100cc` replica, so you can resize the cluster accordingly.

From the source's **Overview** page,

1. Click **View cluster** to go to the cluster's **Overview** page.

1. Click on the 3 dots at the top right to expand the menu and select **Alter
   cluster**.

1. In the **Alter cluster** model, set the **Size** to `100cc` and click **Alter
   cluster**.

![Image of the alter cluster/resize](/images/console/console-create-new/postgresql/new-cluster-resize.png
"Alter cluster/resize")

{{< /tab >}}

{{< tab "SQL commands">}}
{{% postgres-direct/right-size-the-cluster %}}
{{< /tab >}}

{{< /tabs >}}
## Next steps

{{% postgres-direct/next-steps %}}
