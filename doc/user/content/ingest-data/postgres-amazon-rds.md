---
title: "Ingest data from Amazon RDS"
description: "How to stream data from Amazon RDS for PostgreSQL to Materialize"
aliases:
  - /guides/cdc-postgres/
  - /integrations/cdc-postgres/
  - /connect-sources/cdc-postgres-direct.md
menu:
  main:
    parent: "postgresql"
    name: "Amazon RDS"
    weight: 5
---

This page shows you how to stream data from [Amazon RDS for PostgreSQL](https://aws.amazon.com/rds/postgresql/) to Materialize using the [PostgreSQL source](/sql/create-source/postgres/).

## Before you begin

{{% postgres-direct/before-you-begin %}}

  {{% postgres-direct/postgres-schema-changes %}}

## Step 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html) protocol to track changes in your database and propagate them to Materialize.

As a first step, you need to make sure logical replication is enabled.

1. As a user with the `rds_superuser` role, use `psql` to connect to your database.

1. Check if logical replication is enabled:

    ``` sql
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

    - If logical replication is already on, skip to [Step 2. Create a publication](#step-2-create-a-publication).

1. [Create a custom RDS parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Creating).

    - Set **Parameter group family** to your PostgreSQL version.
    - Set **Type** to **DB Parameter Group**.

1. Edit the new parameter group and set the `rds.logical_replication` parameter to `1`.

1. [Associate the RDS parameter group to your database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Associating).

    Use the **Apply Immediately** option. The database must be rebooted in order for the parameter group association to take effect. Keep in mind that rebooting the RDS instance can affect database performance.

    Do not move on to the next step until the database **Status** is **Available** in the RDS Console.

1. Back in your `psql` shell, verify that replication is now enabled:

    ``` sql
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

## Step 2. Create a publication

{{% postgres-direct/create-a-publication-aws %}}

## Step 3. Configure network security

There are various ways to configure your database's network to allow Materialize to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can configure your database's security group to allow connections from a set of static Materialize IP addresses.

- **Use AWS PrivateLink** or **Use an SSH tunnel:** If your database is running in a private network, you can use either [AWS PrivateLink](https://aws.amazon.com/privatelink/) or an SSH tunnel to connect Materialize to the database.

Select the option that works best for you.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}

1. In the `psql` shell connected to Materialize, find the static egress IP addresses for the Materialize region you are running in:

    ```sql
    SELECT * FROM mz_egress_ips;
    ```

1. In the RDS Console, [add an inbound rule to your RDS security group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/working-with-security-groups.html#adding-security-group-rule) for each IP address from the previous step.

    In each rule:

    - Set **Type** to **PostgreSQL**.
    - Set **Source** to the IP address in CIDR notation.

{{< /tab >}}

{{< tab "Use AWS PrivateLink">}}

[AWS PrivateLink](https://aws.amazon.com/privatelink/) lets you connect Materialize to your RDS instance without exposing traffic to the public internet. To use AWS PrivateLink, you create a network load balancer in the same VPC as your RDS instance and a VPC endpoint service that Materialize connects to. The VPC endpoint service then routes requests from Materialize to RDS via the network load balancer.

1. Get the IP address of your RDS instance. You'll need this address to register your RDS instance as the target for the network load balancer in the next step.

    To get the IP address of your RDS instance:

    1. Select your database in the RDS Console.
    1. Find your RDS endpoint under **Connectivity & security**.
    1. Use the `dig` or `nslooklup` command to find the IP address that the endpoint resolves to:

        ```sh
        dig +short <RDS_ENDPOINT>
        ```

1. [Create a dedicated target group for your RDS instance](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html).

    - Choose the **IP addresses** type.

    - Set the protocol and port to **TCP** and **5432**.

    - Choose the same VPC as your RDS instance.

    - Use the IP address from the previous step to register your RDS instance as the target.

    **Warning:** The IP address of your RDS instance can change without notice. For this reason, it's best to set up automation to regularly check the IP of the instance and update your target group accordingly. You can use a lambda function to automate this process - see Materialize's [Terraform module for AWS PrivateLink](https://github.com/MaterializeInc/terraform-aws-rds-privatelink/blob/main/lambda_function.py) for an example. Another approach is to [configure an EC2 instance as an RDS router](https://aws.amazon.com/blogs/database/how-to-use-amazon-rds-and-amazon-aurora-with-a-static-ip-address/) for your network load balancer.

1. [Create a network load balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html).

    - For **Network mapping**, choose the same VPC as your RDS instance and select all of the availability zones and subnets that you RDS instance is in.

    - For **Listeners and routing**, set the protocol and port to **TCP** and **5432** and select the target group you created in the previous step.

1. In the security group of your RDS instance, [allow traffic from the the network load balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html).

    If [client IP preservation](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-target-groups.html#client-ip-preservation) is disabled, the easiest approach is to add an inbound rule with the VPC CIDR of the network load balancer. If you don't want to grant access to the entire VPC CIDR, you can add inbound rules for the private IP addresses of the load balancer subnets.

    - To find the VPC CIDR, go to your network load balancer and look under **Network mapping**.
    - To find the private IP addresses of the load balancer subnets, go to **Network Interfaces**, search for the name of the network load balancer, and look on the **Details** tab for each matching network interface.

1. [Create a VPC endpoint service](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html).

    - For **Load balancer type**, choose **Network** and then select the network load balancer you created in the previous step.

    - After creating the VPC endpoint service, note its **Service name**. You'll use this service name when connecting Materialize later.

1. Go back to the target group you created for the network load balancer and make sure that the [health checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html) are reporting the targets as healthy.

{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

To create an SSH tunnel from Materialize to your database, you launch an instance to serve as an SSH bastion host, configure the bastion host to allow traffic only from Materialize, and then configure your database's private network to allow traffic from the bastion host.

1. [Launch an EC2 instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/LaunchingAndUsingInstances.html) to serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your RDS instance.
    - Add a key pair and note the username. You'll use this username when connecting Materialize to your bastion host.

    **Warning:** Auto-assigned public IP addresses can change in [certain cases](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#concepts-public-addresses). For this reason, it's best to associate an [elastic IP address](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#ip-addressing-eips) to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

    1. In the `psql` shell connected to Materialize, get the static egress IP addresses for the Materialize region you are running in:

        ```sql
        SELECT * FROM mz_egress_ips;
        ```

    1. For each static egress IP, [add an inbound rule](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-security-groups.html) to your SSH bastion host's security group.

        In each rule:
        - Set **Type** to **PostgreSQL**.
        - Set **Source** to the IP address in CIDR notation.

1. In the security group of your RDS instance, [add an inbound rule](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html) to allow traffic from the SSH bastion host.

    - Set **Type** to **All TCP**.
    - Set **Source** to **Custom** and select the bastion host's security group.

{{< /tab >}}

{{< /tabs >}}

## Step 4. Create an ingestion cluster

{{% postgres-direct/create-an-ingestion-cluster %}}

## Step 5. Start ingesting data

Now that you've configured your database network and created an ingestion cluster, you can connect Materialize to your PostgreSQL database and start ingesting data. The exact steps depend on your networking configuration, so start by selecting the relevant option.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}

1. In the `psql` shell connected to Materialize, use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the password for the `materialize` PostgreSQL user you created [earlier](#step-2-create-a-publication):

    ```sql
    CREATE SECRET pgpass AS '<PASSWORD>';
    ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a connection object with access and authentication details for Materialize to use:

    ```sql
    CREATE CONNECTION pg_connection TO POSTGRES (
      HOST '<host>',
      PORT 5432,
      USER 'materialize',
      PASSWORD SECRET pgpass,
      SSL MODE 'require',
      DATABASE '<database>'
      );
    ```

    - Replace `<host>` with your RDS endpoint. To find your RDS endpoint, select your database in the RDS Console, and look under **Connectivity & security**.

    - Replace `<database>` with the name of the database containing the tables you want to replicate to Materialize.

1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize to your RDS instance and start ingesting data from the publication you created [earlier](#step-2-create-a-publication):

    ```sql
    CREATE SOURCE mz_source
      IN CLUSTER ingest_postgres
      FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
      FOR ALL TABLES;
    ```

    To ingest data from specific schemas or tables in your publication, use `FOR SCHEMAS (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` instead of `FOR ALL TABLES`.

{{< /tab >}}

{{< tab "Use AWS PrivateLink">}}

1. In the `psql` shell connected to Materialize, use the [`CREATE CONNECTION`](/sql/create-connection/#aws-privatelink) command to create an AWS PrivateLink connection:

    ```sql
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
      SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0356210a8a432d9e9',
      AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az3')
    );
    ```

    - Replace the `SERVICE NAME` value with the service name you noted [earlier](#step-3-configure-network-security).

    - Replace the `AVAILABILITY ZONES` list with the IDs of the availability zones in your AWS account.

      To find your availability zone IDs, select your database in the RDS Console and click the subnets under **Connectivity & security**. For each subnet, look for **Availability Zone ID** (e.g., `use1-az6`), not **Availability Zone** (e.g., `us-east-1d`).

1. Retrieve the AWS principal for the AWS PrivateLink connection you just created:

    ```sql
    SELECT principal
    FROM mz_aws_privatelink_connections plc
    JOIN mz_connections c ON plc.id = c.id
    WHERE c.name = 'privatelink_svc';
    ```
    <p></p>

    ```
       id   |                                 principal
    --------+---------------------------------------------------------------------------
     u1     | arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
    ```

1. [Update your VPC endpoint service to accept connections from the AWS principal](https://docs.aws.amazon.com/vpc/latest/privatelink/add-endpoint-service-permissions.html).

1. If your AWS PrivateLink service is configured to require acceptance of connection requests, [manually approve the connection request from Materialize](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).

    **Note:** It can take some time for the connection request to show up. Do not move on to the next step until you've approved the connection.

1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the password for the `materialize` PostgreSQL user you created [earlier](#step-2-create-a-publication):

    ```sql
    CREATE SECRET pgpass AS '<PASSWORD>';
    ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create another connection object, this time with database access and authentication details for Materialize to use:

    ```sql
    CREATE CONNECTION pg_connection TO POSTGRES (
      HOST '<host>',
      PORT 5432,
      USER 'materialize',
      PASSWORD SECRET pgpass,
      DATABASE '<database>',
      AWS PRIVATELINK privatelink_svc
      );
    ```

    - Replace `<host>` with your RDS endpoint. To find your RDS endpoint, select your database in the RDS Console, and look under **Connectivity & security**.

    - Replace `<database>` with the name of the database containing the tables you want to replicate to Materialize.

1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize to your RDS instance via AWS PrivateLink and start ingesting data from the publication you created [earlier](#step-2-create-a-publication):

    ```sql
    CREATE SOURCE mz_source
      IN CLUSTER ingest_postgres
      FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
      FOR ALL TABLES;
    ```

    To ingest data from specific schemas or tables in your publication, use `FOR SCHEMAS (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` instead of `FOR ALL TABLES`.

{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

1. In the `psql` shell connected to Materialize, use the [`CREATE CONNECTION`](/sql/create-connection/#ssh-tunnel) command to create an SSH tunnel connection:

    ```sql
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        PORT <SSH_BASTION_PORT>,
        USER '<SSH_BASTION_USER>'
    );
    ```

    - Replace `<SSH_BASTION_HOST>` and `<SSH_BASTION_PORT`> with the public IP address and port of the SSH bastion host you created [earlier](#step-3-configure-network-security).
    - Replace `<SSH_BASTION_USER>` with the username for the key pair you created for your SSH bastion host.

1. Get Materialize's public keys for the SSH tunnel connection:

    ```sql
    SELECT * FROM mz_ssh_tunnel_connections;
    ```

1. Log in to your SSH bastion host and add Materialize's public keys to the `authorized_keys` file, for example:

    ```sh
    # Command for Linux
    echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
    echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys
    ```

1. Back in the `psql` shell connected to Materialize, use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the password for the `materialize` PostgreSQL user you created [earlier](#step-2-create-a-publication):

    ```sql
    CREATE SECRET pgpass AS '<PASSWORD>';
    ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create another connection object, this time with database access and authentication details for Materialize to use:

    ```sql
    CREATE CONNECTION pg_connection TO POSTGRES (
      HOST '<host>',
      PORT 5432,
      USER 'materialize',
      PASSWORD SECRET pgpass,
      DATABASE '<database>',
      SSH TUNNEL ssh_connection
      );
    ```

    - Replace `<host>` with your RDS endpoint. To find your RDS endpoint, select your database in the RDS Console, and look under **Connectivity & security**.

    - Replace `<database>` with the name of the database containing the tables you want to replicate to Materialize.

1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize to your RDS instance and start ingesting data from the publication you created [earlier](#step-2-create-a-publication):

    ```sql
    CREATE SOURCE mz_source
      IN CLUSTER ingest_aurora_postgres
      FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
      FOR ALL TABLES;
    ```

    To ingest data from specific schemas or tables in your publication, use `FOR SCHEMAS (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` instead of `FOR ALL TABLES`.

{{< /tab >}}

{{< /tabs >}}

## Step 6. Check the ingestion status

{{% postgres-direct/check-the-ingestion-status %}}

## Step 7. Right-size the cluster

{{% postgres-direct/right-size-the-cluster %}}

## Next steps

{{% postgres-direct/next-steps %}}
