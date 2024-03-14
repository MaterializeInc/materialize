---
title: "Ingest data from Amazon Aurora MySQL"
description: "How to stream data from Amazon Aurora for MySQL to Materialize"
menu:
  main:
    parent: "mysql"
    name: "Amazon Aurora"
    identifier: "amazon-aurora-mysql"
    weight: 10
---

This page shows you how to stream data from [Amazon Aurora MySQL](https://aws.amazon.com/rds/aurora/)
to Materialize using the [MySQL source](/sql/create-source/mysql/).

## Before you begin

{{% mysql-direct/before-you-begin %}}

## Step 1. Enable GTID-based replication

{{< note >}}
GTID-based replication is supported for Amazon Aurora MySQL v2 and v3, as well
as Aurora Serverless v2.
{{</ note >}}

Before creating a source in Materialize, you **must** configure Amazon Aurora
MySQL for GTID-based binlog replication. This requires the following
configuration changes:

Configuration parameter          | Value  | Details
---------------------------------|--------| -------------------------------
`binlog_format`                  | `ROW`  | This configuration is [deprecated as of MySQL 8.0.34](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format). Newer versions of MySQL default to row-based logging.
`binlog_row_image`               | `FULL` |
`gtid_mode`                      | `ON`   | In the AWS console, this parameter appears as `gtid-mode`.
`enforce_gtid_consistency`       | `ON`   |
`binlog retention hours`         | 168    |
`replica_preserve_commit_order`  | `ON`   | Only required when connecting Materialize to a read-replica for replication, rather than the primary server.

For guidance on enabling GTID-based binlog replication in Aurora, see the
[Amazon Aurora MySQL documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/mysql-replication-gtid.html).

## Step 2. Create a user for replication

{{% mysql-direct/create-a-user-for-replication %}}

## Step 3. Configure network security

{{< note >}}
Support for AWS PrivateLink connections is planned for a future release.
{{< /note >}}

There are various ways to configure your database's network to allow Materialize
to connect:

- **Allow Materialize IPs:** If your database is publicly accessible, you can
    configure your database's security group to allow connections from a set of
    static Materialize IP addresses.

- **Use an SSH tunnel:** If your database is running in a private network, you
    can use an SSH tunnel to connect Materialize to the database.

Select the option that works best for you.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}

1. In the [SQL Shell](https://console.materialize.com/), or your preferred SQL
   client connected to Materialize, find the static egress IP addresses for the
   Materialize region you are running in:

    ```sql
    SELECT * FROM mz_egress_ips;
    ```

1. [Add an inbound rule to your Aurora security group](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Overview.RDSSecurityGroups.html)
    for each IP address from the previous step.

    In each rule:

    - Set **Type** to **MySQL**.
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
      Amazon Aurora MySQL instance.

    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.

    **Warning:** Auto-assigned public IP addresses can change in [certain cases](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#concepts-public-addresses).
      For this reason, it's best to associate an [elastic IP address](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-instance-addressing.html#ip-addressing-eips)
      to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

    1. In the [SQL Shell](https://console.materialize.com/), or your preferred
       SQL client connected to Materialize, get the static egress IP addresses for
       the Materialize region you are running in:

       ```sql
       SELECT * FROM mz_egress_ips;
       ```

    1. For each static egress IP, [add an inbound rule](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-security-groups.html)
       to your SSH bastion host's security group.

        In each rule:

        - Set **Type** to **MySQL**.
        - Set **Source** to the IP address in CIDR notation.

1. In the security group of your RDS instance, [add an inbound rule](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html)
   to allow traffic from the SSH bastion host.

    - Set **Type** to **All TCP**.
    - Set **Source** to **Custom** and select the bastion host's security
      group.

{{< /tab >}}

{{< /tabs >}}

## Step 4. (Optional) Create a cluster

{{< note >}}
If you are prototyping and already have a cluster to host your MySQL
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](https://materialize.com/docs/sql/create-cluster/#resource-isolation).
{{< /note >}}

{{% mysql-direct/create-a-cluster %}}

## Step 5. Start ingesting data

[//]: # "TODO(morsapaes) MySQL connections support multiple SSL modes. We should
adapt to that, rather than just state SSL MODE REQUIRED."

Now that you've configured your database network, you can connect Materialize to
your MySQL database and start ingesting data. The exact steps depend on your
networking configuration, so start by selecting the relevant option.

{{< tabs >}}

{{< tab "Allow Materialize IPs">}}

1. In the [SQL Shell](https://console.materialize.com/), or your preferred SQL
   client connected to Materialize, use the [`CREATE SECRET`](/sql/create-secret/)
   command to securely store the password for the `materialize` MySQL user you
   created [earlier](#step-2-create-a-publication):

    ```sql
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/#mysql) command to
   create a connection object with access and authentication details for
   Materialize to use:

    ```sql
    CREATE CONNECTION mysql_connection TO MYSQL (
        HOST <host>,
        PORT 3306,
        USER 'materialize',
        PASSWORD SECRET mysqlpass,
        SSL MODE REQUIRED
    );
    ```

    - Replace `<host>` with the **Writer** endpoint for your Aurora database. To
      find the endpoint, select your database in the RDS Console, then click
      the **Connectivity & security** tab and look for the endpoint with
      type **Writer**.

        <div class="warning">
            <strong class="gutter">WARNING!</strong>
            You must use the <strong>Writer</strong> endpoint for the database. Using a <strong>Reader</strong> endpoint will not work.
        </div>

1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize
   to your Aurora instance and start ingesting data.

    ```sql
    CREATE SOURCE mz_source
      FROM mysql CONNECTION mysql_connection
      FOR ALL TABLES;
    ```

    - By default, the source will be created in the active cluster; to use a
      different cluster, use the `IN CLUSTER` clause.

    - To ingest data from specific schemas or tables, use the `FOR SCHEMAS
      (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` options
      instead of `FOR ALL TABLES`.

    - To handle unsupported data types, use the `TEXT COLUMNS` or `IGNORE
      COLUMNS` options. Check out the [reference documentation](/sql/create-source/mysql/#supported-types)
      for guidance.

1. After source creation, you can handle upstream [schema changes](/sql/create-source/mysql/#schema-changes)
   by dropping and recreating the source.

{{< /tab >}}

{{< tab "Use an SSH tunnel">}}

1. In the [SQL Shell](https://console.materialize.com/), or your preferred SQL
   client connected to Materialize, use the [`CREATE CONNECTION`](/sql/create-connection/#ssh-tunnel)
   command to create an SSH tunnel connection:

    ```sql
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        PORT <SSH_BASTION_PORT>,
        USER '<SSH_BASTION_USER>'
    );
    ```

    - Replace `<SSH_BASTION_HOST>` and `<SSH_BASTION_PORT`> with the public IP
      address and port of the SSH bastion host you created [earlier](#step-3-configure-network-security).

    - Replace `<SSH_BASTION_USER>` with the username for the key pair you
      created for your SSH bastion host.

1. Get Materialize's public keys for the SSH tunnel connection you just
   created:

    ```sql
    SELECT
        mz_connections.name,
        mz_ssh_tunnel_connections.*
    FROM
        mz_connections
    JOIN
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

    ```sql
    VALIDATE CONNECTION ssh_connection;
    ```

    If no validation error is returned, move to the next step.

1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
   password for the `materialize` MySQL user you created [earlier](#step-2-create-a-user-for-replication):

    ```sql
    CREATE SECRET mysqlpass AS '<password>';
    ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create
   another connection object, this time with database access and authentication
   details for Materialize to use:

    ```sql
    CREATE CONNECTION mysql_connection TO MYSQL (
    HOST '<host>',
    SSH TUNNEL ssh_connection,
    );
    ```

    - Replace `<host>` with your Aurora endpoint. To find your Aurora endpoint,
      select your database in the RDS Console, and look under **Connectivity &
      security**.

1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize
   to your Aurora instance and start ingesting data:

    ```sql
    CREATE SOURCE mz_source
      FROM mysql CONNECTION mysql_connection
      FOR ALL TABLES;
    ```

    - By default, the source will be created in the active cluster; to use a
      different cluster, use the `IN CLUSTER` clause.

    - To ingest data from specific schemas or tables, use the `FOR SCHEMAS
      (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` options
      instead of `FOR ALL TABLES`.

    - To handle unsupported data types, use the `TEXT COLUMNS` or `IGNORE
      COLUMNS` options. Check out the [reference documentation](/sql/create-source/mysql/#supported-types)
      for guidance.

{{< /tab >}}

{{< /tabs >}}

[//]: # "TODO(morsapaes) Replace these Step 6. and 7. with guidance using the
new progress metrics in mz_source_statistics + console monitoring, when
available(also for PostgreSQL)."

## Step 6. Check the ingestion status

{{% mysql-direct/check-the-ingestion-status %}}

## Step 7. Right-size the cluster

{{% mysql-direct/right-size-the-cluster %}}

## Next steps

{{% mysql-direct/next-steps %}}
