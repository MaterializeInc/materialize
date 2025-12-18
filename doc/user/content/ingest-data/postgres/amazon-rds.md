---
title: "Ingest data from Amazon RDS"
description: "How to stream data from Amazon RDS for PostgreSQL to Materialize"
aliases:
  - /guides/cdc-postgres/
  - /integrations/cdc-postgres/
  - /connect-sources/cdc-postgres-direct/
  - /ingest-data/postgres-amazon-rds/
menu:
  main:
    parent: "postgresql"
    name: "Amazon RDS"
    identifier: "pg-amazon-rds"
    weight: 20
---

This page shows you how to stream data from [Amazon RDS for PostgreSQL](https://aws.amazon.com/rds/postgresql/)
to Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

{{% postgres-direct/before-you-begin %}}

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

{{% postgres-direct/create-a-publication-aws %}}

## B. (Optional) Configure network security

{{< note >}}
If you are prototyping and your RDS instance is publicly accessible, **you can
skip this step**. For production scenarios, we recommend configuring one of the
network security options below.
{{< /note >}}

{{< tabs >}}

{{< tab "Cloud">}}

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

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}

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

{{< /tab >}}

{{< tab "Use AWS PrivateLink">}}

[AWS PrivateLink](https://aws.amazon.com/privatelink/) lets you connect
Materialize to your RDS instance without exposing traffic to the public
internet. To use AWS PrivateLink, you create a network load balancer in the
same VPC as your RDS instance and a VPC endpoint service that Materialize
connects to. The VPC endpoint service then routes requests from Materialize to
RDS via the network load balancer.

{{< note >}}
Materialize provides a Terraform module that automates the creation and
configuration of AWS resources for a PrivateLink connection. For more details,
see the [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-rds-privatelink).
{{</ note >}}

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

{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

{{< note >}}
Materialize provides a Terraform module that automates the creation and
configuration of resources for an SSH tunnel. For more details, see the
[Terraform module repository](https://github.com/MaterializeInc/terraform-aws-ec2-ssh-bastion).
{{</ note >}}

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

{{< /tab >}}

{{< /tabs >}}

{{< /tab >}}

{{< tab "Self-Managed">}}

{{% include-md
file="shared-content/self-managed/configure-network-security-intro.md" %}}

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}

1. In the AWS Management Console, [add an inbound rule to your RDS security group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/changing-security-group.html#add-remove-instance-security-groups)
   to allow traffic from Materialize IPs.

    In each rule:

    - Set **Type** to **PostgreSQL**.
    - Set **Source** to the IP address in CIDR notation.

{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

{{< note >}}
Materialize provides a Terraform module that automates the creation and
configuration of resources for an SSH tunnel. For more details, see the
[Terraform module repository](https://github.com/MaterializeInc/terraform-aws-ec2-ssh-bastion).
{{</ note >}}

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

{{< /tab >}}

{{< /tabs >}}

{{< /tab >}}

{{< /tabs >}}

## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

{{< note >}}
If you are prototyping and already have a cluster to host your PostgreSQL
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](/sql/create-cluster/#resource-isolation).
{{< /note >}}

{{% postgres-direct/create-a-cluster %}}

### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.

{{< tabs >}}
{{< tab "Allow Materialize IPs">}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ips_cloud"
   example="create-secret" indent="true" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ips_cloud"
   example="create-connection" indent="true"%}}

   {{% include-example
   file="examples/ingest_data/postgres/create_connection_ips_cloud"
   example="create-connection-options-rds" indent="true"%}}

{{< /tab >}}

{{< tab "Use AWS PrivateLink (Cloud-only)" >}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_privatelink_cloud"
   example="create-privatelink-connection-rds-intro" indent="true" %}}

   {{% include-example
   file="examples/ingest_data/postgres/create_connection_privatelink_cloud"
   example="create-privatelink-connection-rds-in-region" indent="true" %}}

   {{% include-example
   file="examples/ingest_data/postgres/create_connection_privatelink_cloud"
   example="create-privatelink-connection-rds-in-region-options" indent="true"
   %}}

   {{% include-example
   file="examples/ingest_data/postgres/create_connection_privatelink_cloud"
   example="create-privatelink-connection-rds-cross-region" indent="true" %}}

   {{% include-example
   file="examples/ingest_data/postgres/create_connection_privatelink_cloud"
   example="create-privatelink-connection-rds-cross-region-options"
   indent="true" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_privatelink_cloud"
   example="get-principal-privatelink-connection" indent="true" %}}

   {{% include-example
   file="examples/ingest_data/postgres/create_connection_privatelink_cloud"
   example="get-principal-privatelink-connection-results" indent="true" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_privatelink_cloud"
   example="update-vpc-endpoint" indent="true" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_privatelink_cloud"
   example="approve-connection-request" indent="true" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_privatelink_cloud"
   example="validate-connection" indent="true" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_privatelink_cloud"
   example="create-secret" indent="true" %}}
1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_privatelink_cloud"
   example="create-connection" indent="true" %}}
   {{% include-example
   file="examples/ingest_data/postgres/create_connection_privatelink_cloud"
   example="create-connection-options-rds" indent="true" %}}
{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="create-ssh-tunnel-connection" indent="true" %}}

   {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="create-ssh-tunnel-connection-options" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="get-public-keys-aurora-rds-self-hosted" indent="true" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="login-to-ssh-bastion-host" indent="true" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="validate-ssh-tunnel-connection" indent="true" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="create-secret" indent="true" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="create-connection" indent="true" %}}

   {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="create-connection-options-rds" indent="true" %}}

{{< /tab >}}

{{< /tabs >}}

### 3. Start ingesting data

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="ingest-data-step" %}}

### 4. Monitor the ingestion status

{{% postgres-direct/check-the-ingestion-status %}}

### 5. Right-size the cluster

{{% postgres-direct/right-size-the-cluster %}}

## D. Explore your data

{{% postgres-direct/next-steps %}}

## Considerations

{{% include-from-yaml data="postgres_source_details"
name="postgres-considerations" %}}
