---
title: "Ingest data from Amazon Aurora"
description: "How to stream data from Amazon Aurora for PostgreSQL to Materialize"
aliases:
  - /ingest-data/postgres-amazon-aurora/
menu:
  main:
    parent: "postgresql"
    name: "Amazon Aurora"
    identifier: "pg-amazon-aurora"
    weight: 15
---

This page shows you how to stream data from [Amazon Aurora for PostgreSQL](https://aws.amazon.com/rds/aurora/)
to Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

{{% postgres-direct/before-you-begin %}}

{{< warning >}}
There is a known issue with Aurora PostgreSQL 16.1 that can cause logical replication to fail with the following error:
- `postgres: sql client error: db error: ERROR: could not map filenumber "base/16402/3147867235" to relation OID`

This is due to a bug in Aurora's implementation of logical replication in PostgreSQL 16.1, where the system fails to correctly fetch relation metadata from the catalogs. If you encounter these errors, you should upgrade your Aurora PostgreSQL instance to a newer minor version (16.2 or later).

For more information, see [this AWS discussion](https://repost.aws/questions/QU4RXUrLNQS_2oSwV34pmwww/error-could-not-map-filenumber-after-aurora-upgrade-to-16-1).
{{</ warning >}}

## A. Configure Amazon Aurora

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

To enable logical replication in Aurora, see the
[Aurora documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html#AuroraPostgreSQL.Replication.Logical.Configure).

{{< note >}}
Aurora Serverless (v1) [does **not** support](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless.html#aurora-serverless.limitations)
logical replication, so it's not possible to use this service with
Materialize.
{{</ note >}}

### 2. Create a publication and a replication user

{{% postgres-direct/create-a-publication-aws %}}

## B. (Optional) Configure network security

{{< note >}}
If you are prototyping and your Aurora instance is publicly accessible, **you can
skip this step**. For production scenarios, we recommend configuring one of the
network security options below.
{{< /note >}}

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

{{< /tab >}}

{{< tab "Use AWS PrivateLink">}}

[AWS PrivateLink](https://aws.amazon.com/privatelink/) lets you connect
Materialize to your Aurora instance without exposing traffic to the public
internet. To use AWS PrivateLink, you create a network load balancer in the
same VPC as your Aurora instance and a VPC endpoint service that Materialize
connects to. The VPC endpoint service then routes requests from Materialize to
Aurora via the network load balancer.

{{< note >}}
Materialize provides a Terraform module that automates the creation and
configuration of AWS resources for a PrivateLink connection. For more details,
see the [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-rds-privatelink).
{{</ note >}}

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
   example="create-connection" indent="true" %}}

   {{% include-example
   file="examples/ingest_data/postgres/create_connection_ips_cloud"
   example="create-connection-options-aurora" indent="true" %}}

{{< /tab >}}

{{< tab "Use AWS PrivateLink">}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_privatelink_cloud"
   example="create-privatelink-connection-aurora" indent="true" %}}

   {{% include-example
   file="examples/ingest_data/postgres/create_connection_privatelink_cloud"
   example="create-privatelink-connection-options-aurora" indent="true" %}}

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
   example="create-connection-options-aurora" indent="true" %}}
{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="create-ssh-tunnel-connection" indent="true"%}}

   {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="create-ssh-tunnel-connection-options" %}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="get-public-keys-aurora-rds-self-hosted" indent="true"%}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="login-to-ssh-bastion-host" indent="true"%}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="validate-ssh-tunnel-connection" indent="true" %}}

1. {{% include-example file="examples/ingest_data/postgres/create_connection_ssh_cloud" example="create-secret" indent="true"%}}

1. {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="create-connection" indent="true"%}}

   {{% include-example
   file="examples/ingest_data/postgres/create_connection_ssh_cloud"
   example="create-connection-options-aurora" indent="true"%}}

{{< /tab >}}

{{< /tabs >}}

### 3. Start ingesting data

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source" %}}

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source-options" %}}

{{% include-example file="examples/ingest_data/postgres/create_source_cloud"
example="schema-changes" %}}

### 4. Monitor the ingestion status

{{% postgres-direct/check-the-ingestion-status %}}

### 5. Right-size the cluster

{{% postgres-direct/right-size-the-cluster %}}

## D. Explore your data

{{% postgres-direct/next-steps %}}

## Considerations

{{< include-md file="shared-content/postgres-considerations.md" >}}
