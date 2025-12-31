---
audience: developer
canonical_url: https://materialize.com/docs/ingest-data/sql-server/self-hosted/
complexity: beginner
description: How to stream data from self-hosted SQL Server database to Materialize
doc_type: reference
keywords:
- UPDATE YOUR
- 'you can

  skip this step'
- Ingest data from self-hosted SQL Server
- 'Allow Materialize IPs:'
- CREATE A
- SELECT THE
- 'Note:'
- 'Tip:'
product_area: Sources
status: stable
title: Ingest data from self-hosted SQL Server
---

# Ingest data from self-hosted SQL Server

## Purpose
How to stream data from self-hosted SQL Server database to Materialize

If you need to understand the syntax and options for this command, you're in the right place.


How to stream data from self-hosted SQL Server database to Materialize


This page shows you how to stream data from a self-hosted SQL Server database
to Materialize using the [SQL Server Source](/sql/create-source/sql-server/).

> **Tip:** 


## Before you begin

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: sql-server-direct/before-you-begin --> --> -->

## A. Configure SQL Server

> **Note:** 

To configure SQL Server for data ingestion into Materialize, you must be a user
with privileges to enable CDC and create/manage login, users, roles, and
privileges.


<!-- Unresolved shortcode: {{% sql-server-direct/ingesting-data/enable-cdc %}... -->

## B. (Optional) Configure network security

> **Note:** 
If you are prototyping and your SQL Server instance is publicly accessible, **you can
skip this step**. For production scenarios, we recommend configuring one of the
network security options below.


There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's firewall to allow connections from a set of
    static Materialize IP addresses.

- **Use an SSH tunnel:** If your database is running in a private network, you
    can use an SSH tunnel to connect Materialize to the database.

Select the option that works best for you.

#### Allow Materialize IPs

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, find the static egress IP addresses for the
   Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```text

1. Update your database firewall rules to allow traffic from each IP address
   from the previous step.

#### Use AWS PrivateLink

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
    ```text

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
    ```text

    ```text
                                     principal
    ---------------------------------------------------------------------------
     arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
    ```text

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

#### Use an SSH tunnel

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

This section covers c. ingest data in materialize.

### 1. (Optional) Create a cluster

> **Note:** 
If you are prototyping and already have a cluster to host your SQL Server
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](/sql/create-cluster/#resource-isolation).


<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: sql-server-direct/create-a-cluster --> --> -->


### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.

#### Allow Materialize IPs

<!-- Unresolved shortcode: {{% sql-server-direct/ingesting-data/allow-materia... -->

#### Use an AWS Privatelink (Cloud-only)

<!-- Unresolved shortcode: {{% sql-server-direct/ingesting-data/use-aws-priva... -->

#### Use an SSH tunnel

<!-- Unresolved shortcode: {{% sql-server-direct/ingesting-data/use-ssh-tunne... -->

### 3. Start ingesting data

> **Note:** 
For a new SQL Server source, if none of the replicating tables
are receiving write queries, snapshotting may take up to an additional 5 minutes
to complete. For details, see [snapshot latency for inactive databases](#snapshot-latency-for-inactive-databases)


<!-- Unresolved shortcode: {{% include-example file="examples/ingest_data/sql... -->

<!-- Unresolved shortcode: {{% include-example file="examples/ingest_data/sql... -->

<!-- Unresolved shortcode: {{% include-example file="examples/ingest_data/sql... -->

### 4. Right-size the cluster

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: sql-server-direct/right-size-the-cluster --> --> -->

## D. Explore your data

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: sql-server-direct/next-steps --> --> -->

## Considerations

<!-- Unresolved shortcode: {{% include-md file="shared-content/sql-server-con... -->