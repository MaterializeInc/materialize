---
title: "Ingest data from self-hosted SQL Server"
description: "How to stream data from self-hosted SQL Server database to Materialize"
menu:
  main:
    parent: "sql-server"
    name: "Self-hosted SQL Server"
    identifier: "sql-server-self-hosted"
aliases:
  - /ingest-data/cdc-sql-server/
---

This page shows you how to stream data from a self-hosted SQL Server database
to Materialize using the [SQL Server Source](/sql/create-source/sql-server/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

{{% sql-server-direct/before-you-begin %}}

## A. Configure SQL Server

{{< note >}}

To configure SQL Server for data ingestion into Materialize, you must be a user
with privileges to enable CDC and create/manage login, users, roles, and
privileges.

{{</ note >}}

{{% sql-server-direct/ingesting-data/enable-cdc %}}

## B. (Optional) Configure network security

{{< note >}}
If you are prototyping and your SQL Server instance is publicly accessible, **you can
skip this step**. For production scenarios, we recommend configuring one of the
network security options below.
{{< /note >}}

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

1. In the [SQL Shell](https://console.materialize.com/), or your preferred SQL
   client connected to Materialize, find the static egress IP addresses for the
   Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

1. Update your database firewall rules to allow traffic from each IP address
   from the previous step.

{{< /tab >}}

{{< tab "Use AWS PrivateLink">}}

Materialize can connect to a SQL Server database through an [AWS PrivateLink](https://aws.amazon.com/privatelink/)
service. Your SQL Server database must be running on AWS in order to use this
option.

1. #### Create a target group

    Create a dedicated [target group](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html)
    for your SQL Server instance with the following details:

    a. Target type as **IP address**.

    b. Protocol as **TCP**.

    c. Port as **1433**, or the port that you are using in case it is not 1433.

    d. Make sure that the target group is in the same VPC as the SQL Server
    instance.

    e. Click next, and register the respective SQL Server instance to the target
    group using its IP address.

1. #### Create a Network Load Balancer (NLB)

    Create a [Network Load Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html)
    that is **enabled for the same subnets** that the SQL Server instance is
    in.

1. #### Create TCP listener

    Create a [TCP listener](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-listener.html)
    for your SQL Server instance that forwards to the corresponding target
    group you created.

1. #### Verify security groups and health checks

    Once the TCP listener has been created, make sure that the [health checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html)
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

    1. In the [SQL Shell](https://console.materialize.com/), or your preferred
       SQL client connected to Materialize, get the static egress IP addresses for
       the Materialize region you are running in:

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
If you are prototyping and already have a cluster to host your SQL Server
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](/sql/create-cluster/#resource-isolation).
{{< /note >}}

{{% sql-server-direct/create-a-cluster %}}

### 2. Start ingesting data

{{< note >}}
For a new SQL Server source, if none of the replicating tables
are receiving write queries, snapshotting may take up to an additional 5 minutes
to complete. For details, see [snapshot latency for inactive databases](#snapshot-latency-for-inactive-databases).

For production deployments with SQL Server Always On Availability Groups, see
[High Availability](#high-availability) for configuration guidance.
{{</ note >}}

Now that you've configured your database network, you can connect Materialize to
your SQL Server database and start ingesting data. The exact steps depend on your
networking configuration, so start by selecting the relevant option.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}
{{% sql-server-direct/ingesting-data/allow-materialize-ips %}}
{{< /tab >}}

{{< tab "Use an AWS Privatelink">}}
{{% sql-server-direct/ingesting-data/use-aws-privatelink %}}
{{< /tab >}}

{{< tab "Use an SSH tunnel">}}
{{% sql-server-direct/ingesting-data/use-ssh-tunnel %}}
{{< /tab >}}

{{< /tabs >}}

[//]: # "TODO(morsapaes) Replace these Step 6. and 7. with guidance using the
new progress metrics in mz_source_statistics + console monitoring, when
available(also for PostgreSQL)."

### 3. Right-size the cluster

{{% sql-server-direct/right-size-the-cluster %}}

## D. Explore your data

{{% sql-server-direct/next-steps %}}

## High Availability

### Using SQL Server Always On Availability Groups

To make your SQL Server source resilient to database failovers, configure
Materialize to connect through a SQL Server [Always On Availability Group (AG)
listener](https://learn.microsoft.com/en-us/sql/database-engine/availability-groups/windows/listeners-client-connectivity-application-failover).
When a failover occurs, SQL Server drops the existing connection and routes new
connections to the new primary replica transparently.

#### Prerequisites

Before connecting Materialize to an AG, ensure:

1. **Your AG listener is configured and accessible.** Materialize must connect
   via the listener DNS name, not individual node hostnames.

1. **CDC is enabled on all potential primary replicas.** SQL Server's Change
   Data Capture metadata is **not** replicated across AG nodes.

1. **CDC capture and cleanup jobs exist on all potential primary replicas.**
   After a role change, the new primary must have these jobs to continue
   replicating changes.

   SQL Server CDC metadata, including capture and cleanup jobs, **does not
   replicate** to AG secondary replicas. After a failover, you must ensure the new
   primary has CDC enabled and the required jobs are running.

   **Recommended approach:** Create an automated script or SQL Agent job that runs
   on each potential primary after a role change:

   ```sql
   USE YourDatabase;

   -- Enable CDC if not already enabled
   IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'YourDatabase' AND is_cdc_enabled = 1)
   BEGIN
       EXEC sys.sp_cdc_enable_db;
   END

   -- Enable CDC on tables (if not already enabled)
   IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('schema.table_name'))
   BEGIN
       EXEC sys.sp_cdc_enable_table
           @source_schema = 'schema',
           @source_name = 'table_name',
           @role_name = NULL,
           @supports_net_changes = 0;
   END

   -- Create capture job if it doesn't exist
   IF NOT EXISTS (SELECT 1 FROM msdb.dbo.cdc_jobs WHERE job_type = 'capture')
   BEGIN
       EXEC sys.sp_cdc_add_job @job_type = 'capture', @continuous = 1;
   END

   -- Create cleanup job if it doesn't exist
   IF NOT EXISTS (SELECT 1 FROM msdb.dbo.cdc_jobs WHERE job_type = 'cleanup')
   BEGIN
       EXEC sys.sp_cdc_add_job @job_type = 'cleanup';
       -- Extend retention to cover expected failover + recovery time
       EXEC sys.sp_cdc_change_job @job_type = 'cleanup', @retention = 43200;
   END
   ```

   {{< note >}}
   Adjust the `@retention` value based on your expected recovery time. The default
   retention is ~3 days (4320 minutes). If CDC change data is pruned before
   Materialize can ingest it after a failover, you must [drop and recreate the
   source](/sql/drop-source/) to trigger a new snapshot.
   {{< /note >}}

#### Connecting to an AG listener

Create your SQL Server connection using the **AG listener** as the host:

```mzsql
CREATE SECRET sqlserver_pass AS '<SQL_SERVER_PASSWORD>';

CREATE CONNECTION sqlserver_ag TO SQL SERVER (
    HOST 'my-ag-listener.example.com',  -- AG listener DNS name
    PORT 1433,
    USER 'materialize',
    PASSWORD SECRET sqlserver_pass,
    DATABASE '<DATABASE_NAME>'
);

CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sqlserver_ag
  FOR ALL TABLES;
```

When the AG fails over to a new primary, Materialize will:

1. Detect the dropped connection
1. Reconnect to the AG listener (now pointing to the new primary)
1. Resume ingestion from the last persisted LSN

## Considerations

{{% include-md file="shared-content/sql-server-considerations.md" %}}
