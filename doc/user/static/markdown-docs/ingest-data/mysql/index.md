# MySQL

Connecting Materialize to a MySQL database for Change Data Capture (CDC).



## Change Data Capture (CDC)

Materialize supports MySQL as a real-time data source. The [MySQL source](/sql/create-source/mysql/)
uses MySQL's [binlog replication protocol](/sql/create-source/mysql/#change-data-capture)
to **continually ingest changes** resulting from CRUD operations in the upstream
database. The native support for MySQL Change Data Capture (CDC) in Materialize
gives you the following benefits:

* **No additional infrastructure:** Ingest MySQL change data into Materialize in
    real-time with no architectural changes or additional operational overhead.
    In particular, you **do not need to deploy Kafka and Debezium** for MySQL
    CDC.

* **Transactional consistency:** The MySQL source ensures that transactions in
    the upstream MySQL database are respected downstream. Materialize will
    **never show partial results** based on partially replicated transactions.

* **Incrementally updated materialized views:** Materialized views are **not
    supported in MySQL**, so you can use Materialize as a
    read-replica to build views on top of your MySQL data that are efficiently
    maintained and always up-to-date.

## Supported versions and services

> **Note:** MySQL-compatible database systems are not guaranteed to work with the MySQL
> source out-of-the-box. [MariaDB](https://mariadb.org/), [Vitess](https://vitess.io/)
> and [PlanetScale](https://planetscale.com/) are currently **not supported**.


The MySQL source requires **MySQL 5.7+** and is compatible with most common
MySQL hosted services.

| Integration guides                          |
| ------------------------------------------- |
| <ul><li>[Amazon Aurora for MySQL](/ingest-data/mysql/amazon-aurora/)</li><li>[Amazon RDS for MySQL](/ingest-data/mysql/amazon-rds/)</li><li>[Azure DB for MySQL](/ingest-data/mysql/azure-db/)</li><li>[Google Cloud SQL for MySQL](/ingest-data/mysql/google-cloud-sql/)</li><li>[Self-hosted MySQL](/ingest-data/mysql/self-hosted/)</li></ul>
    |

If there is a hosted service or MySQL distribution that is not listed above but
you would like to use with Materialize, please submit a [feature request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests&labels=A-integration)
or reach out in the Materialize [Community Slack](https://materialize.com/s/chat).

## Considerations

### Schema changes

> **Note:** Work to more smoothly support ddl changes to upstream tables is currently in
> progress. The work introduces the ability to re-ingest the same upstream table
> under a new schema and switch over without downtime.

Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes

<ul>
<li>
<p>Adding columns to tables. Materialize will <strong>not ingest</strong> new columns
added upstream unless you use <a href="/sql/alter-source/#context" ><code>DROP SOURCE</code></a> to
first drop the affected subsource, and then add the table back to the source
using <a href="/sql/alter-source/" ><code>ALTER SOURCE...ADD SUBSOURCE</code></a>.</p>
</li>
<li>
<p>Dropping columns that were added after the source was created. These
columns are never ingested, so you can drop them without issue.</p>
</li>
<li>
<p>Adding or removing <code>NOT NULL</code> constraints to tables that were nullable
when the source was created.</p>
</li>
</ul>


#### Incompatible schema changes

<p>All other schema changes to upstream tables will set the corresponding
subsource into an error state, which prevents you from reading from the
subsource.</p>
<p>To handle incompatible <a href="#schema-changes" >schema changes</a>, use <a href="/sql/alter-source/#context" ><code>DROP SOURCE</code></a> to first drop the affected subsource,
and then <a href="/sql/alter-source/" ><code>ALTER SOURCE...ADD SUBSOURCE</code></a> to add the
subsource back to the source. When you add the subsource, it will have the
updated schema from the corresponding upstream table.</p>


### Supported types

<p>Materialize natively supports the following MySQL types:</p>
<ul style="column-count: 3">
<li><code>bigint</code></li>
<li><code>binary</code></li>
<li><code>bit</code></li>
<li><code>blob</code></li>
<li><code>boolean</code></li>
<li><code>char</code></li>
<li><code>date</code></li>
<li><code>datetime</code></li>
<li><code>decimal</code></li>
<li><code>double</code></li>
<li><code>float</code></li>
<li><code>int</code></li>
<li><code>json</code></li>
<li><code>longblob</code></li>
<li><code>longtext</code></li>
<li><code>mediumblob</code></li>
<li><code>mediumint</code></li>
<li><code>mediumtext</code></li>
<li><code>numeric</code></li>
<li><code>real</code></li>
<li><code>smallint</code></li>
<li><code>text</code></li>
<li><code>time</code></li>
<li><code>timestamp</code></li>
<li><code>tinyblob</code></li>
<li><code>tinyint</code></li>
<li><code>tinytext</code></li>
<li><code>varbinary</code></li>
<li><code>varchar</code></li>
</ul>

<p>When replicating tables that contain the <strong>unsupported <a href="/sql/types/" >data
types</a></strong>, you can:</p>
<ul>
<li>
<p>Use <a href="/sql/create-source/mysql/#handling-unsupported-types" ><code>TEXT COLUMNS</code>
option</a> for the
following unsupported  MySQL types:</p>
<ul>
<li><code>enum</code></li>
<li><code>year</code></li>
</ul>
<p>The specified columns will be treated as <code>text</code> and will not offer the
expected MySQL type features.</p>
</li>
<li>
<p>Use the <a href="/sql/create-source/mysql/#excluding-columns" ><code>EXCLUDE COLUMNS</code></a>
option to exclude any columns that contain unsupported data types.</p>
</li>
</ul>


### Truncation

<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>

### Modifying an existing source

When you add a new subsource to an existing source ([`ALTER SOURCE ... ADD
SUBSOURCE ...`](/sql/alter-source/)), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.



---

## Ingest data from Amazon Aurora


This page shows you how to stream data from [Amazon Aurora MySQL](https://aws.amazon.com/rds/aurora/)
to Materialize using the [MySQL source](/sql/create-source/mysql/).

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).


## Before you begin

- Make sure you are running MySQL 5.7 or higher. Materialize uses
  [GTID-based binary log (binlog) replication](/sql/create-source/mysql/#change-data-capture),
  which is not available in older versions of MySQL.

- Ensure you have access to your MySQL instance via the [`mysql` client](https://dev.mysql.com/doc/refman/8.0/en/mysql.html),
  or your preferred SQL client.


## A. Configure Amazon Aurora

### 1. Enable GTID-based binlog replication

> **Note:** GTID-based replication is supported for Amazon Aurora MySQL v2 and v3 as well
> as Aurora Serverless v2.


1. Before creating a source in Materialize, you **must** configure Amazon Aurora
   MySQL for GTID-based binlog replication. Ensure the upstream MySQL database  has been configured for GTID-based binlog replication:




























   <table>
   <thead>
   <tr>

   <th>MySQL Configuration</th>


   <th>Value</th>


   <th>Notes</th>


   </tr>
   </thead>
   <tbody>







   <tr>











   <td>
   <code>log_bin</code>
   </td>











   <td>
   <code>ON</code>
   </td>











   <td>

   </td>

   </tr>








   <tr>











   <td>
   <code>binlog_format</code>
   </td>











   <td>
   <code>ROW</code>
   </td>











   <td>

   </td>

   </tr>








   <tr>











   <td>
   <code>binlog_row_image</code>
   </td>











   <td>
   <code>FULL</code>
   </td>











   <td>

   </td>

   </tr>








   <tr>











   <td>
   <code>gtid_mode</code>
   </td>











   <td>
   <code>ON</code>
   </td>











   <td>
   In the AWS console, this parameter appears as <code>gtid-mode</code>.
   </td>

   </tr>








   <tr>











   <td>
   <code>enforce_gtid_consistency</code>
   </td>











   <td>
   <code>ON</code>
   </td>











   <td>

   </td>

   </tr>








   <tr>











   <td>
   <code>replica_preserve_commit_order</code>
   </td>











   <td>
   <code>ON</code>
   </td>











   <td>
   Only required when connecting Materialize to a read-replica.
   </td>

   </tr>


   </tbody>
   </table>



   For guidance on enabling GTID-based binlog replication in Aurora, see the
   [Amazon Aurora MySQL
    documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/mysql-replication-gtid.html).

1. In addition to the step above, you **must** also ensure that
   [binlog retention](/sql/create-source/mysql/#binlog-retention) is set to a
   reasonable value. To check the current value of the `binlog retention hours`
   configuration parameter, connect to your RDS instance and run:

   ```mysql
   CALL mysql.rds_show_configuration;
   ```

   If the value returned is `NULL`, or less than `168` (i.e. 7 days), run:

   ```mysql
   CALL mysql.rds_set_configuration('binlog retention hours', 168);
   ```

   Although 7 days is a reasonable retention period, we recommend using the
   default MySQL retention period (30 days) in order to not compromise
   Materialize’s ability to resume replication in case of failures or
   restarts.

1. To validate that all configuration parameters are set to the expected values
   after the above configuration changes, run:

    ```mysql
    -- Validate "binlog retention hours" configuration parameter
    CALL mysql.rds_show_configuration;
    ```

    ```mysql
    -- Validate parameter group configuration parameters
    SHOW VARIABLES WHERE variable_name IN (
      'log_bin',
      'binlog_format',
      'binlog_row_image',
      'gtid_mode',
      'enforce_gtid_consistency',
      'replica_preserve_commit_order'
    );
    ```

### 2. Create a user for replication

Once GTID-based binlog replication is enabled, we recommend creating a dedicated
user for Materialize with sufficient privileges to manage replication.

1. As a _superuser_, use `mysql` (or your preferred SQL client) to connect to
   your database.

1. Create a dedicated user for Materialize, if you don't already have one:

   ```mysql
   CREATE USER 'materialize'@'%' IDENTIFIED BY '<password>';

   ALTER USER 'materialize'@'%' REQUIRE SSL;
   ```

   IAM authentication with AWS RDS for MySQL is also supported.  See the [Amazon RDS User Guide](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html) for instructions on enabling IAM database authentication, creating IAM policies, and creating a database account.

1. Grant the user permission to manage replication:

   ```mysql
   GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES ON *.* TO 'materialize'@'%';
   ```

   Once connected to your database, Materialize will take an initial snapshot of
   the tables in your MySQL server. `SELECT` privileges are required for this
   initial snapshot.

1. Apply the changes:

   ```mysql
   FLUSH PRIVILEGES;
   ```


## B. (Optional) Configure network security

> **Note:** If you are prototyping and your Aurora instance is publicly accessible, **you
> can skip this step**. For production scenarios, we recommend configuring one of
> the network security options below.




**Cloud:**

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



**Allow Materialize IPs:**

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, find the static egress IP addresses for the
   Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

1. [Add an inbound rule to your Aurora security group](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Overview.RDSSecurityGroups.html)
    for each IP address from the previous step.

    In each rule:

    - Set **Type** to **MySQL**.
    - Set **Source** to the IP address in CIDR notation.



**Use AWS PrivateLink:**

[AWS PrivateLink](https://aws.amazon.com/privatelink/) lets you connect
Materialize to your Aurora instance without exposing traffic to the public
internet. To use AWS PrivateLink, you create a network load balancer in the
same VPC as your Aurora instance and a VPC endpoint service that Materialize
connects to. The VPC endpoint service then routes requests from Materialize to
Aurora via the network load balancer.

> **Note:** Materialize provides a Terraform module that automates the creation and
> configuration of AWS resources for a PrivateLink connection. For more details,
> see the [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-rds-privatelink).


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

    - Set the protocol and port to **TCP** and **3306**.

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
      and **3306** and select the target group you created in the previous
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



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

> **Note:** Materialize provides a Terraform module that automates the creation and
> configuration of resources for an SSH tunnel. For more details, see the
> [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-ec2-ssh-bastion).


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

    1. In the [SQL Shell](/console/), or your preferred
       SQL client connected to Materialize, get the static egress IP addresses for
       the Materialize region you are running in:

       ```mzsql
       SELECT * FROM mz_egress_ips;
       ```

    1. For each static egress IP, [add an inbound rule](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-security-groups.html)
       to your SSH bastion host's security group.

        In each rule:

        - Set **Type** to **MySQL**.
        - Set **Source** to the IP address in CIDR notation.

1. In the security group of your RDS instance, [add an inbound rule](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html)
   to allow traffic from the SSH bastion host.

    - Set **Type** to **All TCP**.
    - Set **Source** to **Custom** and select the bastion host's security
      group.







**Self-Managed:**

<p>Configure your network to allow Materialize to connect to your database. For
example, you can:</p>
<ul>
<li>
<p><strong>Allow Materialize IPs:</strong> Configure your database&rsquo;s security group to allow
connections from Materialize.</p>
</li>
<li>
<p><strong>Use an SSH tunnel:</strong> Use an SSH tunnel to connect Materialize to the
database.</p>
</li>
</ul>
<div class="note">
  <strong class="gutter">NOTE:</strong>
  <p>The steps to allow Materialize to connect to your database  depends on your
  deployment setup. Refer to your company’s network/security policies and
  procedures.</p>
</div>




**Allow Materialize IPs:**

1. [Add an inbound rule to your Aurora security group](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Overview.RDSSecurityGroups.html)
    to allow traffic from Materialize IPs.

    In each rule:

    - Set **Type** to **MySQL**.
    - Set **Source** to the IP address in CIDR notation.


**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

> **Note:** Materialize provides a Terraform module that automates the creation and
> configuration of resources for an SSH tunnel. For more details, see the
> [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-ec2-ssh-bastion).


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

1. In the security group of your RDS instance, [add an inbound rule](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Overview.RDSSecurityGroups.html)
   to allow traffic from the SSH bastion host.

    - Set **Type** to **All TCP**.
    - Set **Source** to **Custom** and select the bastion host's security
      group.









## C. Ingest data in Materialize

### 1. (Optional) Create a source cluster

> **Note:** If you are prototyping and already have a cluster to host your MySQL
> source (e.g. `quickstart`), **you can skip this step**. For production
> scenarios, we recommend separating your workloads into multiple clusters for
> [resource isolation](/sql/create-cluster/#resource-isolation).


In Materialize, a [cluster](/concepts/clusters/) is an isolated
environment, similar to a virtual warehouse in Snowflake. When you create a
cluster, you choose the size of its compute resource allocation based on the
work you need the cluster to do, whether ingesting data from a source,
computing always-up-to-date query results, serving results to clients, or a
combination.

In this case, you'll create a dedicated cluster for ingesting source data from
your MySQL database.

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, use the [`CREATE CLUSTER`](/sql/create-cluster/)
   command to create the new cluster:

    ```mzsql
    CREATE CLUSTER ingest_mysql (SIZE = '200cc');

    SET CLUSTER = ingest_mysql;
    ```

    A cluster of [size](/sql/create-cluster/#size) `200cc` should be enough to
    process the initial snapshot of the tables in your MySQL database. For very
    large snapshots, consider using a larger size to speed up processing. Once
    the snapshot is finished, you can readjust the size of the cluster to fit
    the volume of changes being replicated from your upstream MySQL database.


### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.



**Allow Materialize IPs:**

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, use the [`CREATE SECRET`](/sql/create-secret/)
   command to securely store the password for the `materialize` MySQL user
   you created [earlier](#2-create-a-user-for-replication):

    ```mzsql
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

    For AWS IAM authentication, you must create a connection to AWS.  See the [`CREATE CONNECTION`](/sql/create-connection/#aws) command for details.

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
   connection object with access and authentication details for Materialize to
   use:

    ```mzsql
    CREATE CONNECTION mysql_connection TO MYSQL (
        HOST <host>,
        PORT 3306,
        USER 'materialize',
        PASSWORD SECRET mysqlpass,
        SSL MODE REQUIRED
    );
    ```

    - Replace `<host>` with your MySQL endpoint.

    AWS IAM authentication is also available, see the [`CREATE CONNECTION`](/sql/create-connection/#mysql) command for details.



**Use AWS PrivateLink (Cloud-only):**
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

1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
password for the `materialize` MySQL user you created [earlier](#2-create-a-user-for-replication):

    ```mzsql
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

    For AWS IAM authentication, you must create a connection to AWS.  See the [`CREATE CONNECTION`](/sql/create-connection/#aws) command for details.

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create
another connection object, this time with database access and authentication
details for Materialize to use:

    ```mzsql
    CREATE CONNECTION mysql_connection TO MYSQL (
    HOST <host>,
      PORT 3306,
      USER 'materialize',
      PASSWORD SECRET mysqlpass,
      SSL MODE REQUIRED,
      AWS PRIVATELINK privatelink_svc
    );
    ```

    - Replace `<host>` with your RDS endpoint. To find your RDS endpoint, select
      your database in the RDS Console, and look under **Connectivity &
      security**.

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

1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the password for the `materialize` MySQL user you created [earlier](#2-create-a-user-for-replication):

    ```mzsql
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

    For AWS IAM authentication, you must create a connection to AWS.  See the [`CREATE CONNECTION`](/sql/create-connection/#aws) command for details.

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create another connection object, this time with database access and authentication details for Materialize to use:

    ```mzsql
    CREATE CONNECTION mysql_connection TO MYSQL (
    HOST '<host>',
    SSH TUNNEL ssh_connection
    );
    ```

    - Replace `<host>` with your MySQL endpoint.

  AWS IAM authentication is also available, see the [`CREATE CONNECTION`](/sql/create-connection/#mysql)
  command for details.






### 3. Start ingesting data

Once you have created the connection, you can use the connection in the
[`CREATE SOURCE`](/sql/create-source/) command to connect to your MySQL instance and start ingesting
data:
```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR ALL TABLES;

```


- By default, the source will be created in the active cluster; to use a different cluster, use the `IN CLUSTER` clause.

- To ingest data from specific schemas or tables, use the `FOR SCHEMAS (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` options instead of `FOR ALL TABLES`.

- To handle [unsupported data types](#supported-types), use the `TEXT COLUMNS` or `EXCLUDE COLUMNS` options.


After source creation, refer to [schema changes
considerations](#schema-changes) for information on handling upstream schema changes.


[//]: # "TODO(morsapaes) Replace these Step 6. and 7. with guidance using the
new progress metrics in mz_source_statistics + console monitoring, when
available (also for PostgreSQL)."

### 4. Monitor the ingestion status

Before it starts consuming the replication stream, Materialize takes a snapshot
of the relevant tables. Until this snapshot is complete, Materialize won't have
the same view of your data as your MySQL database.

In this step, you'll first verify that the source is running and then check the
status of the snapshotting process.

1. Back in the SQL client connected to Materialize, use the
   [`mz_source_statuses`](/sql/system-catalog/mz_internal/#mz_source_statuses)
   table to check the overall status of your source:

    ```mzsql
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

    For each `subsource`, make sure the `status` is `running`. If you see
    `stalled` or `failed`, there's likely a configuration issue for you to fix.
    Check the `error` field for details and fix the issue before moving on.
    Also, if the `status` of any subsource is `starting` for more than a few
    minutes, [contact our team](/support/).

2. Once the source is running, use the [`mz_source_statistics`](/sql/system-catalog/mz_internal/#mz_source_statistics)
   table to check the status of the initial snapshot:

    ```mzsql
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
    <p></p>

    ```nofmt
    object_id | snapshot_committed
    ----------|------------------
     u144     | t
    (1 row)
    ```

    Once `snapshot_commited` is `t`, move on to the next step. Snapshotting can
    take between a few minutes to several hours, depending on the size of your
    dataset and the size of the cluster the source is running in.


### 5. Right-size the cluster

After the snapshotting phase, Materialize starts ingesting change events from
the MySQL replication stream. For this work, Materialize generally
performs well with a `100cc` replica, so you can resize the cluster
accordingly.

1. Still in a SQL client connected to Materialize, use the [`ALTER CLUSTER`](/sql/alter-cluster/)
   command to downsize the cluster to `100cc`:

    ```mzsql
    ALTER CLUSTER ingest_mysql SET (SIZE '100cc');
    ```

    Behind the scenes, this command adds a new `100cc` replica and removes the
    `200cc` replica.

1. Use the [`SHOW CLUSTER REPLICAS`](/sql/show-cluster-replicas/) command to
   check the status of the new replica:

    ```mzsql
    SHOW CLUSTER REPLICAS WHERE cluster = 'ingest_mysql';
    ```
    <p></p>

    ```nofmt
         cluster     | replica |  size  | ready
    -----------------+---------+--------+-------
     ingest_mysql    | r1      | 100cc  | t
    (1 row)
    ```


## D. Explore your data

With Materialize ingesting your MySQL data into durable storage, you can
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

### Schema changes

> **Note:** Work to more smoothly support ddl changes to upstream tables is currently in
> progress. The work introduces the ability to re-ingest the same upstream table
> under a new schema and switch over without downtime.

Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes

<ul>
<li>
<p>Adding columns to tables. Materialize will <strong>not ingest</strong> new columns
added upstream unless you use <a href="/sql/alter-source/#context" ><code>DROP SOURCE</code></a> to
first drop the affected subsource, and then add the table back to the source
using <a href="/sql/alter-source/" ><code>ALTER SOURCE...ADD SUBSOURCE</code></a>.</p>
</li>
<li>
<p>Dropping columns that were added after the source was created. These
columns are never ingested, so you can drop them without issue.</p>
</li>
<li>
<p>Adding or removing <code>NOT NULL</code> constraints to tables that were nullable
when the source was created.</p>
</li>
</ul>


#### Incompatible schema changes

<p>All other schema changes to upstream tables will set the corresponding
subsource into an error state, which prevents you from reading from the
subsource.</p>
<p>To handle incompatible <a href="#schema-changes" >schema changes</a>, use <a href="/sql/alter-source/#context" ><code>DROP SOURCE</code></a> to first drop the affected subsource,
and then <a href="/sql/alter-source/" ><code>ALTER SOURCE...ADD SUBSOURCE</code></a> to add the
subsource back to the source. When you add the subsource, it will have the
updated schema from the corresponding upstream table.</p>


### Supported types

<p>Materialize natively supports the following MySQL types:</p>
<ul style="column-count: 3">
<li><code>bigint</code></li>
<li><code>binary</code></li>
<li><code>bit</code></li>
<li><code>blob</code></li>
<li><code>boolean</code></li>
<li><code>char</code></li>
<li><code>date</code></li>
<li><code>datetime</code></li>
<li><code>decimal</code></li>
<li><code>double</code></li>
<li><code>float</code></li>
<li><code>int</code></li>
<li><code>json</code></li>
<li><code>longblob</code></li>
<li><code>longtext</code></li>
<li><code>mediumblob</code></li>
<li><code>mediumint</code></li>
<li><code>mediumtext</code></li>
<li><code>numeric</code></li>
<li><code>real</code></li>
<li><code>smallint</code></li>
<li><code>text</code></li>
<li><code>time</code></li>
<li><code>timestamp</code></li>
<li><code>tinyblob</code></li>
<li><code>tinyint</code></li>
<li><code>tinytext</code></li>
<li><code>varbinary</code></li>
<li><code>varchar</code></li>
</ul>

<p>When replicating tables that contain the <strong>unsupported <a href="/sql/types/" >data
types</a></strong>, you can:</p>
<ul>
<li>
<p>Use <a href="/sql/create-source/mysql/#handling-unsupported-types" ><code>TEXT COLUMNS</code>
option</a> for the
following unsupported  MySQL types:</p>
<ul>
<li><code>enum</code></li>
<li><code>year</code></li>
</ul>
<p>The specified columns will be treated as <code>text</code> and will not offer the
expected MySQL type features.</p>
</li>
<li>
<p>Use the <a href="/sql/create-source/mysql/#excluding-columns" ><code>EXCLUDE COLUMNS</code></a>
option to exclude any columns that contain unsupported data types.</p>
</li>
</ul>


### Truncation

<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>

### Modifying an existing source

When you add a new subsource to an existing source ([`ALTER SOURCE ... ADD
SUBSOURCE ...`](/sql/alter-source/)), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.


---

## Ingest data from Amazon RDS


This page shows you how to stream data from [Amazon RDS for MySQL](https://aws.amazon.com/rds/mysql/)
to Materialize using the [MySQL source](/sql/create-source/mysql).

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).


## Before you begin

- Make sure you are running MySQL 5.7 or higher. Materialize uses
  [GTID-based binary log (binlog) replication](/sql/create-source/mysql/#change-data-capture),
  which is not available in older versions of MySQL.

- Ensure you have access to your MySQL instance via the [`mysql` client](https://dev.mysql.com/doc/refman/8.0/en/mysql.html),
  or your preferred SQL client.


## A. Configure Amazon RDS

### 1. Enable GTID-based binlog replication

Before creating a source in Materialize, you **must** configure Amazon RDS for
GTID-based binlog replication. For guidance on enabling GTID-based
binlog replication in RDS, see the [Amazon RDS for MySQL documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/mysql-replication-gtid.html).

1. [Enable automated backups in your RDS instance](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithAutomatedBackups.html#USER_WorkingWithAutomatedBackups.Enabling)
by setting the backup retention period to a value greater than `0`.  This
enables binary logging (`log_bin`).

1. [Create a custom RDS parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Creating).

    - Set **Parameter group family** to your MySQL version.
    - Set **Type** to **DB Parameter Group**.

1. Edit the new parameter group to set the configuration parameters to the
   following values:


































   <table>
   <thead>
   <tr>

   <th>MySQL Configuration</th>


   <th>Value</th>


   <th>Notes</th>


   </tr>
   </thead>
   <tbody>






















   <tr>











   <td>
   <code>binlog_format</code>
   </td>











   <td>
   <code>ROW</code>
   </td>











   <td>
   <a href="https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format" >Deprecated as of MySQL 8.0.34</a>. Newer versions of MySQL default to row-based logging.
   </td>

   </tr>











   <tr>











   <td>
   <code>binlog_row_image</code>
   </td>











   <td>
   <code>FULL</code>
   </td>











   <td>

   </td>

   </tr>











   <tr>











   <td>
   <code>gtid_mode</code>
   </td>











   <td>
   <code>ON</code>
   </td>











   <td>
   In the AWS console, this parameter appears as <code>gtid-mode</code>.
   </td>

   </tr>











   <tr>











   <td>
   <code>enforce_gtid_consistency</code>
   </td>











   <td>
   <code>ON</code>
   </td>











   <td>

   </td>

   </tr>











   <tr>











   <td>
   <code>replica_preserve_commit_order</code>
   </td>











   <td>
   <code>ON</code>
   </td>











   <td>
   Only required when connecting Materialize to a read-replica.
   </td>

   </tr>


   </tbody>
   </table>




1. [Associate the RDS parameter group to your database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithDBInstanceParamGroups.html#USER_WorkingWithParamGroups.Associating).

    Use the **Apply Immediately** option. The database must be rebooted in order
    for the parameter group association to take effect. Keep in mind that
    rebooting the RDS instance can affect database performance.

    Do not move on to the next step until the database **Status**
    is **Available** in the RDS Console.

1. In addition to the step above, you **must** also ensure that
   [binlog retention](/sql/create-source/mysql/#binlog-retention) is set to a
   reasonable value. To check the current value of the [`binlog retention hours`](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/mysql-stored-proc-configuring.html#mysql_rds_set_configuration-usage-notes.binlog-retention-hours)
   configuration parameter, connect to your RDS instance and run:

   ```mysql
   CALL mysql.rds_show_configuration;
   ```

   If the value returned is `NULL`, or less than `168` (i.e. 7 days), run:

   ```mysql
   CALL mysql.rds_set_configuration('binlog retention hours', 168);
   ```

   Although 7 days is a reasonable retention period, we recommend using the
   default MySQL retention period (30 days) in order to not compromise
   Materialize’s ability to resume replication in case of failures or
   restarts.

1. To validate that all configuration parameters are set to the expected values
   after the above configuration changes, run:

    ```mysql
    -- Validate "binlog retention hours" configuration parameter
    CALL mysql.rds_show_configuration;
    ```

    ```mysql
    -- Validate parameter group configuration parameters
    SHOW VARIABLES WHERE variable_name IN (
      'log_bin',
      'binlog_format',
      'binlog_row_image',
      'gtid_mode',
      'enforce_gtid_consistency',
      'replica_preserve_commit_order'
    );
    ```

### 2. Create a user for replication

Once GTID-based binlog replication is enabled, we recommend creating a dedicated
user for Materialize with sufficient privileges to manage replication.

1. As a _superuser_, use `mysql` (or your preferred SQL client) to connect to
   your database.

1. Create a dedicated user for Materialize, if you don't already have one:

   ```mysql
   CREATE USER 'materialize'@'%' IDENTIFIED BY '<password>';

   ALTER USER 'materialize'@'%' REQUIRE SSL;
   ```

   IAM authentication with AWS RDS for MySQL is also supported.  See the [Amazon RDS User Guide](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html) for instructions on enabling IAM database authentication, creating IAM policies, and creating a database account.

1. Grant the user permission to manage replication:

   ```mysql
   GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES ON *.* TO 'materialize'@'%';
   ```

   Once connected to your database, Materialize will take an initial snapshot of
   the tables in your MySQL server. `SELECT` privileges are required for this
   initial snapshot.

1. Apply the changes:

   ```mysql
   FLUSH PRIVILEGES;
   ```


## B. (Optional) Configure network security

> **Note:** If you are prototyping and your RDS instance is publicly accessible, **you can
> skip this step**. For production scenarios, we recommend configuring one of the
> network security options below.




**Cloud:**

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



**Allow Materialize IPs:**

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, find the static egress IP addresses for the
   Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

1. In the RDS Console, [add an inbound rule to your RDS security group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/working-with-security-groups.html#adding-security-group-rule)
   for each IP address from the previous step.

    In each rule:

    - Set **Type** to **MySQL**.
    - Set **Source** to the IP address in CIDR notation.



**Use AWS PrivateLink:**

[AWS PrivateLink](https://aws.amazon.com/privatelink/) lets you connect
Materialize to your RDS instance without exposing traffic to the public
internet. To use AWS PrivateLink, you create a network load balancer in the
same VPC as your RDS instance and a VPC endpoint service that Materialize
connects to. The VPC endpoint service then routes requests from Materialize to
RDS via the network load balancer.

> **Note:** Materialize provides a Terraform module that automates the creation and
> configuration of AWS resources for a PrivateLink connection. For more details,
> see the [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-rds-privatelink).


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

    - Set the protocol and port to **TCP** and **3306**.

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
      and **3306** and select the target group you created in the previous
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



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

> **Note:** Materialize provides a Terraform module that automates the creation and
> configuration of resources for an SSH tunnel. For more details, see the
> [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-ec2-ssh-bastion).


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
        - Set **Type** to **MySQL**.
        - Set **Source** to the IP address in CIDR notation.

1. In the security group of your RDS instance, [add an inbound rule](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html)
   to allow traffic from the SSH bastion host.

    - Set **Type** to **All TCP**.
    - Set **Source** to **Custom** and select the bastion host's security
      group.







**Self-Managed:**

<p>Configure your network to allow Materialize to connect to your database. For
example, you can:</p>
<ul>
<li>
<p><strong>Allow Materialize IPs:</strong> Configure your database&rsquo;s security group to allow
connections from Materialize.</p>
</li>
<li>
<p><strong>Use an SSH tunnel:</strong> Use an SSH tunnel to connect Materialize to the
database.</p>
</li>
</ul>
<div class="note">
  <strong class="gutter">NOTE:</strong>
  <p>The steps to allow Materialize to connect to your database  depends on your
  deployment setup. Refer to your company’s network/security policies and
  procedures.</p>
</div>




**Allow Materialize IPs:**

1. In the RDS Console, [add an inbound rule to your RDS security group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/working-with-security-groups.html#adding-security-group-rule)
   to allow traffic from Materialize IPs.

    In each rule:

    - Set **Type** to **MySQL**.
    - Set **Source** to the IP address in CIDR notation.



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

> **Note:** Materialize provides a Terraform module that automates the creation and
> configuration of resources for an SSH tunnel. For more details, see the
> [Terraform module repository](https://github.com/MaterializeInc/terraform-aws-ec2-ssh-bastion).


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

1. In the security group of your RDS instance, [add an inbound rule](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html)
   to allow traffic from the SSH bastion host.

    - Set **Type** to **All TCP**.
    - Set **Source** to **Custom** and select the bastion host's security
      group.









## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

> **Note:** If you are prototyping and already have a cluster to host your MySQL
> source (e.g. `quickstart`), **you can skip this step**. For production
> scenarios, we recommend separating your workloads into multiple clusters for
> [resource isolation](/sql/create-cluster/#resource-isolation).


In Materialize, a [cluster](/concepts/clusters/) is an isolated
environment, similar to a virtual warehouse in Snowflake. When you create a
cluster, you choose the size of its compute resource allocation based on the
work you need the cluster to do, whether ingesting data from a source,
computing always-up-to-date query results, serving results to clients, or a
combination.

In this case, you'll create a dedicated cluster for ingesting source data from
your MySQL database.

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, use the [`CREATE CLUSTER`](/sql/create-cluster/)
   command to create the new cluster:

    ```mzsql
    CREATE CLUSTER ingest_mysql (SIZE = '200cc');

    SET CLUSTER = ingest_mysql;
    ```

    A cluster of [size](/sql/create-cluster/#size) `200cc` should be enough to
    process the initial snapshot of the tables in your MySQL database. For very
    large snapshots, consider using a larger size to speed up processing. Once
    the snapshot is finished, you can readjust the size of the cluster to fit
    the volume of changes being replicated from your upstream MySQL database.


### 2. Create a connection



Once you have configured your network, create a connection in Materialize per
your networking configuration.



**Allow Materialize IPs:**

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, use the [`CREATE SECRET`](/sql/create-secret/)
   command to securely store the password for the `materialize` MySQL user
   you created [earlier](#2-create-a-user-for-replication):

    ```mzsql
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

    For AWS IAM authentication, you must create a connection to AWS.  See the [`CREATE CONNECTION`](/sql/create-connection/#aws) command for details.

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
   connection object with access and authentication details for Materialize to
   use:

    ```mzsql
    CREATE CONNECTION mysql_connection TO MYSQL (
        HOST <host>,
        PORT 3306,
        USER 'materialize',
        PASSWORD SECRET mysqlpass,
        SSL MODE REQUIRED
    );
    ```

    - Replace `<host>` with your MySQL endpoint.

    AWS IAM authentication is also available, see the [`CREATE CONNECTION`](/sql/create-connection/#mysql) command for details.



**Use AWS PrivateLink (Cloud-only):**
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

1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
password for the `materialize` MySQL user you created [earlier](#2-create-a-user-for-replication):

    ```mzsql
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

    For AWS IAM authentication, you must create a connection to AWS.  See the [`CREATE CONNECTION`](/sql/create-connection/#aws) command for details.

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create
another connection object, this time with database access and authentication
details for Materialize to use:

    ```mzsql
    CREATE CONNECTION mysql_connection TO MYSQL (
    HOST <host>,
      PORT 3306,
      USER 'materialize',
      PASSWORD SECRET mysqlpass,
      SSL MODE REQUIRED,
      AWS PRIVATELINK privatelink_svc
    );
    ```

    - Replace `<host>` with your RDS endpoint. To find your RDS endpoint, select
      your database in the RDS Console, and look under **Connectivity &
      security**.

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

1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the password for the `materialize` MySQL user you created [earlier](#2-create-a-user-for-replication):

    ```mzsql
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

    For AWS IAM authentication, you must create a connection to AWS.  See the [`CREATE CONNECTION`](/sql/create-connection/#aws) command for details.

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create another connection object, this time with database access and authentication details for Materialize to use:

    ```mzsql
    CREATE CONNECTION mysql_connection TO MYSQL (
    HOST '<host>',
    SSH TUNNEL ssh_connection
    );
    ```

    - Replace `<host>` with your MySQL endpoint.

  AWS IAM authentication is also available, see the [`CREATE CONNECTION`](/sql/create-connection/#mysql)
  command for details.






### 3. Start ingesting data

Once you have created the connection, you can use the connection in the
[`CREATE SOURCE`](/sql/create-source/) command to connect to your MySQL instance and start ingesting
data:
```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR ALL TABLES;

```


- By default, the source will be created in the active cluster; to use a different cluster, use the `IN CLUSTER` clause.

- To ingest data from specific schemas or tables, use the `FOR SCHEMAS (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` options instead of `FOR ALL TABLES`.

- To handle [unsupported data types](#supported-types), use the `TEXT COLUMNS` or `EXCLUDE COLUMNS` options.


After source creation, refer to [schema changes
considerations](#schema-changes) for information on handling upstream schema changes.


[//]: # "TODO(morsapaes) Replace these Step 6. and 7. with guidance using the
new progress metrics in mz_source_statistics + console monitoring, when
available (also for PostgreSQL)."

### 4. Monitor the ingestion status

Before it starts consuming the replication stream, Materialize takes a snapshot
of the relevant tables. Until this snapshot is complete, Materialize won't have
the same view of your data as your MySQL database.

In this step, you'll first verify that the source is running and then check the
status of the snapshotting process.

1. Back in the SQL client connected to Materialize, use the
   [`mz_source_statuses`](/sql/system-catalog/mz_internal/#mz_source_statuses)
   table to check the overall status of your source:

    ```mzsql
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

    For each `subsource`, make sure the `status` is `running`. If you see
    `stalled` or `failed`, there's likely a configuration issue for you to fix.
    Check the `error` field for details and fix the issue before moving on.
    Also, if the `status` of any subsource is `starting` for more than a few
    minutes, [contact our team](/support/).

2. Once the source is running, use the [`mz_source_statistics`](/sql/system-catalog/mz_internal/#mz_source_statistics)
   table to check the status of the initial snapshot:

    ```mzsql
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
    <p></p>

    ```nofmt
    object_id | snapshot_committed
    ----------|------------------
     u144     | t
    (1 row)
    ```

    Once `snapshot_commited` is `t`, move on to the next step. Snapshotting can
    take between a few minutes to several hours, depending on the size of your
    dataset and the size of the cluster the source is running in.


### 5. Right-size the cluster

After the snapshotting phase, Materialize starts ingesting change events from
the MySQL replication stream. For this work, Materialize generally
performs well with a `100cc` replica, so you can resize the cluster
accordingly.

1. Still in a SQL client connected to Materialize, use the [`ALTER CLUSTER`](/sql/alter-cluster/)
   command to downsize the cluster to `100cc`:

    ```mzsql
    ALTER CLUSTER ingest_mysql SET (SIZE '100cc');
    ```

    Behind the scenes, this command adds a new `100cc` replica and removes the
    `200cc` replica.

1. Use the [`SHOW CLUSTER REPLICAS`](/sql/show-cluster-replicas/) command to
   check the status of the new replica:

    ```mzsql
    SHOW CLUSTER REPLICAS WHERE cluster = 'ingest_mysql';
    ```
    <p></p>

    ```nofmt
         cluster     | replica |  size  | ready
    -----------------+---------+--------+-------
     ingest_mysql    | r1      | 100cc  | t
    (1 row)
    ```


## D. Explore your data

With Materialize ingesting your MySQL data into durable storage, you can
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

### Schema changes

> **Note:** Work to more smoothly support ddl changes to upstream tables is currently in
> progress. The work introduces the ability to re-ingest the same upstream table
> under a new schema and switch over without downtime.

Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes

<ul>
<li>
<p>Adding columns to tables. Materialize will <strong>not ingest</strong> new columns
added upstream unless you use <a href="/sql/alter-source/#context" ><code>DROP SOURCE</code></a> to
first drop the affected subsource, and then add the table back to the source
using <a href="/sql/alter-source/" ><code>ALTER SOURCE...ADD SUBSOURCE</code></a>.</p>
</li>
<li>
<p>Dropping columns that were added after the source was created. These
columns are never ingested, so you can drop them without issue.</p>
</li>
<li>
<p>Adding or removing <code>NOT NULL</code> constraints to tables that were nullable
when the source was created.</p>
</li>
</ul>


#### Incompatible schema changes

<p>All other schema changes to upstream tables will set the corresponding
subsource into an error state, which prevents you from reading from the
subsource.</p>
<p>To handle incompatible <a href="#schema-changes" >schema changes</a>, use <a href="/sql/alter-source/#context" ><code>DROP SOURCE</code></a> to first drop the affected subsource,
and then <a href="/sql/alter-source/" ><code>ALTER SOURCE...ADD SUBSOURCE</code></a> to add the
subsource back to the source. When you add the subsource, it will have the
updated schema from the corresponding upstream table.</p>


### Supported types

<p>Materialize natively supports the following MySQL types:</p>
<ul style="column-count: 3">
<li><code>bigint</code></li>
<li><code>binary</code></li>
<li><code>bit</code></li>
<li><code>blob</code></li>
<li><code>boolean</code></li>
<li><code>char</code></li>
<li><code>date</code></li>
<li><code>datetime</code></li>
<li><code>decimal</code></li>
<li><code>double</code></li>
<li><code>float</code></li>
<li><code>int</code></li>
<li><code>json</code></li>
<li><code>longblob</code></li>
<li><code>longtext</code></li>
<li><code>mediumblob</code></li>
<li><code>mediumint</code></li>
<li><code>mediumtext</code></li>
<li><code>numeric</code></li>
<li><code>real</code></li>
<li><code>smallint</code></li>
<li><code>text</code></li>
<li><code>time</code></li>
<li><code>timestamp</code></li>
<li><code>tinyblob</code></li>
<li><code>tinyint</code></li>
<li><code>tinytext</code></li>
<li><code>varbinary</code></li>
<li><code>varchar</code></li>
</ul>

<p>When replicating tables that contain the <strong>unsupported <a href="/sql/types/" >data
types</a></strong>, you can:</p>
<ul>
<li>
<p>Use <a href="/sql/create-source/mysql/#handling-unsupported-types" ><code>TEXT COLUMNS</code>
option</a> for the
following unsupported  MySQL types:</p>
<ul>
<li><code>enum</code></li>
<li><code>year</code></li>
</ul>
<p>The specified columns will be treated as <code>text</code> and will not offer the
expected MySQL type features.</p>
</li>
<li>
<p>Use the <a href="/sql/create-source/mysql/#excluding-columns" ><code>EXCLUDE COLUMNS</code></a>
option to exclude any columns that contain unsupported data types.</p>
</li>
</ul>


### Truncation

<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>

### Modifying an existing source

When you add a new subsource to an existing source ([`ALTER SOURCE ... ADD
SUBSOURCE ...`](/sql/alter-source/)), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.


---

## Ingest data from Azure DB


This page shows you how to stream data from [Azure DB for MySQL](https://azure.microsoft.com/en-us/products/MySQL)
to Materialize using the [MySQL source](/sql/create-source/mysql/).

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).


## Before you begin

- Make sure you are running MySQL 5.7 or higher. Materialize uses
  [GTID-based binary log (binlog) replication](/sql/create-source/mysql/#change-data-capture),
  which is not available in older versions of MySQL.

- Ensure you have access to your MySQL instance via the [`mysql` client](https://dev.mysql.com/doc/refman/8.0/en/mysql.html),
  or your preferred SQL client.


## A. Configure Azure DB

### 1. Enable GTID-based binlog replication

> **Note:** GTID-based replication is supported for Azure DB for MySQL [flexible server](https://learn.microsoft.com/en-us/azure/mysql/flexible-server/overview-single).
> It is **not supported** for single server databases.


Before creating a source in Materialize, you **must** configure Azure DB for
MySQL for GTID-based binlog replication. Ensure the upstream MySQL database has
been configured for GTID-based binlog replication:




























<table>
<thead>
<tr>

<th>MySQL Configuration</th>


<th>Value</th>


<th>Notes</th>


</tr>
</thead>
<tbody>







<tr>











<td>
<code>log_bin</code>
</td>











<td>
<code>ON</code>
</td>











<td>

</td>

</tr>








<tr>











<td>
<code>binlog_format</code>
</td>











<td>
<code>ROW</code>
</td>











<td>
<a href="https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format" >Deprecated as of MySQL 8.0.34</a>. Newer versions of MySQL default to row-based logging.
</td>

</tr>








<tr>











<td>
<code>binlog_row_image</code>
</td>











<td>
<code>FULL</code>
</td>











<td>

</td>

</tr>








<tr>











<td>
<code>gtid_mode</code>
</td>











<td>
<code>ON</code>
</td>











<td>

</td>

</tr>








<tr>











<td>
<code>enforce_gtid_consistency</code>
</td>











<td>
<code>ON</code>
</td>











<td>

</td>

</tr>








<tr>











<td>
<code>replica_preserve_commit_order</code>
</td>











<td>
<code>ON</code>
</td>











<td>
Only required when connecting Materialize to a read-replica.
</td>

</tr>


</tbody>
</table>



For guidance on enabling GTID-based binlog replication in Azure DB, see the
[Azure documentation](https://learn.microsoft.com/en-us/azure/mysql/flexible-server/how-to-data-in-replication?tabs=shell%2Ccommand-line#configure-the-source-mysql-server).

### 2. Create a user for replication

Once GTID-based binlog replication is enabled, we recommend creating a dedicated
user for Materialize with sufficient privileges to manage replication.

1. As a _superuser_, use `mysql` (or your preferred SQL client) to connect to
   your database.

1. Create a dedicated user for Materialize, if you don't already have one:

   ```mysql
   CREATE USER 'materialize'@'%' IDENTIFIED BY '<password>';

   ALTER USER 'materialize'@'%' REQUIRE SSL;
   ```

   IAM authentication with AWS RDS for MySQL is also supported.  See the [Amazon RDS User Guide](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html) for instructions on enabling IAM database authentication, creating IAM policies, and creating a database account.

1. Grant the user permission to manage replication:

   ```mysql
   GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES ON *.* TO 'materialize'@'%';
   ```

   Once connected to your database, Materialize will take an initial snapshot of
   the tables in your MySQL server. `SELECT` privileges are required for this
   initial snapshot.

1. Apply the changes:

   ```mysql
   FLUSH PRIVILEGES;
   ```


## B. (Optional) Configure network security

> **Note:** If you are prototyping and your Azure DB instance is publicly accessible, **you
> can skip this step**. For production scenarios, we recommend configuring one of
> the network security options below.




**Cloud:**

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

1. Update your [Azure DB firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql)
   to allow traffic from each IP address from the previous step.



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

1. [Launch an Azure VM with a static public IP address](https://learn.microsoft.com/en-us/azure/virtual-network/ip-services/virtual-network-deploy-static-pip-arm-portal?toc=%2Fazure%2Fvirtual-machines%2Ftoc.json)
to serve as your SSH bastion host.

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

    1. Update your SSH bastion host's [firewall rules](https://learn.microsoft.com/en-us/azure/virtual-network/tutorial-filter-network-traffic?toc=%2Fazure%2Fvirtual-machines%2Ftoc.json)
    to allow traffic from each IP address from the previous step.

1. Update your [Azure DB firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql)
   to allow traffic from the SSH bastion host.







**Self-Managed:**

<p>Configure your network to allow Materialize to connect to your database. For
example, you can:</p>
<ul>
<li>
<p><strong>Allow Materialize IPs:</strong> Configure your database&rsquo;s security group to allow
connections from Materialize.</p>
</li>
<li>
<p><strong>Use an SSH tunnel:</strong> Use an SSH tunnel to connect Materialize to the
database.</p>
</li>
</ul>
<div class="note">
  <strong class="gutter">NOTE:</strong>
  <p>The steps to allow Materialize to connect to your database  depends on your
  deployment setup. Refer to your company’s network/security policies and
  procedures.</p>
</div>




**Allow Materialize IPs:**

1. Update your [Azure DB firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql)
   to allow traffic from Materialize IPs.



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

1. [Launch an Azure VM with a static public IP address](https://learn.microsoft.com/en-us/azure/virtual-network/ip-services/virtual-network-deploy-static-pip-arm-portal?toc=%2Fazure%2Fvirtual-machines%2Ftoc.json)
to serve as your SSH bastion host.

    - Make sure the VM is publicly accessible and in the same VPC as your
      database.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a static public IP address. You'll use this IP
      address when connecting Materialize to your bastion host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

1. Update your [Azure DB firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql)
   to allow traffic from the SSH bastion host.









## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

> **Note:** If you are prototyping and already have a cluster to host your MySQL
> source (e.g. `quickstart`), **you can skip this step**. For production
> scenarios, we recommend separating your workloads into multiple clusters for
> [resource isolation](/sql/create-cluster/#resource-isolation).


In Materialize, a [cluster](/concepts/clusters/) is an isolated
environment, similar to a virtual warehouse in Snowflake. When you create a
cluster, you choose the size of its compute resource allocation based on the
work you need the cluster to do, whether ingesting data from a source,
computing always-up-to-date query results, serving results to clients, or a
combination.

In this case, you'll create a dedicated cluster for ingesting source data from
your MySQL database.

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, use the [`CREATE CLUSTER`](/sql/create-cluster/)
   command to create the new cluster:

    ```mzsql
    CREATE CLUSTER ingest_mysql (SIZE = '200cc');

    SET CLUSTER = ingest_mysql;
    ```

    A cluster of [size](/sql/create-cluster/#size) `200cc` should be enough to
    process the initial snapshot of the tables in your MySQL database. For very
    large snapshots, consider using a larger size to speed up processing. Once
    the snapshot is finished, you can readjust the size of the cluster to fit
    the volume of changes being replicated from your upstream MySQL database.


### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.



**Allow Materialize IPs:**

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, use the [`CREATE SECRET`](/sql/create-secret/)
   command to securely store the password for the `materialize` MySQL user
   you created [earlier](#2-create-a-user-for-replication):

    ```mzsql
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

    For AWS IAM authentication, you must create a connection to AWS.  See the [`CREATE CONNECTION`](/sql/create-connection/#aws) command for details.

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
   connection object with access and authentication details for Materialize to
   use:

    ```mzsql
    CREATE CONNECTION mysql_connection TO MYSQL (
        HOST <host>,
        PORT 3306,
        USER 'materialize',
        PASSWORD SECRET mysqlpass,
        SSL MODE REQUIRED
    );
    ```

    - Replace `<host>` with your MySQL endpoint.

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

1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the password for the `materialize` MySQL user you created [earlier](#2-create-a-user-for-replication):

    ```mzsql
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

    For AWS IAM authentication, you must create a connection to AWS.  See the [`CREATE CONNECTION`](/sql/create-connection/#aws) command for details.

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create another connection object, this time with database access and authentication details for Materialize to use:

    ```mzsql
    CREATE CONNECTION mysql_connection TO MYSQL (
    HOST '<host>',
    SSH TUNNEL ssh_connection
    );
    ```

    - Replace `<host>` with your MySQL endpoint.

  AWS IAM authentication is also available, see the [`CREATE CONNECTION`](/sql/create-connection/#mysql)
  command for details.





### 3. Start ingesting data

Once you have created the connection, you can use the connection in the
[`CREATE SOURCE`](/sql/create-source/) command to connect to your MySQL instance and start ingesting
data:
```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR ALL TABLES;

```


- By default, the source will be created in the active cluster; to use a different cluster, use the `IN CLUSTER` clause.

- To ingest data from specific schemas or tables, use the `FOR SCHEMAS (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` options instead of `FOR ALL TABLES`.

- To handle [unsupported data types](#supported-types), use the `TEXT COLUMNS` or `EXCLUDE COLUMNS` options.


After source creation, refer to [schema changes
considerations](#schema-changes) for information on handling upstream schema changes.


### 4. Monitor the ingestion status

Before it starts consuming the replication stream, Materialize takes a snapshot
of the relevant tables. Until this snapshot is complete, Materialize won't have
the same view of your data as your MySQL database.

In this step, you'll first verify that the source is running and then check the
status of the snapshotting process.

1. Back in the SQL client connected to Materialize, use the
   [`mz_source_statuses`](/sql/system-catalog/mz_internal/#mz_source_statuses)
   table to check the overall status of your source:

    ```mzsql
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

    For each `subsource`, make sure the `status` is `running`. If you see
    `stalled` or `failed`, there's likely a configuration issue for you to fix.
    Check the `error` field for details and fix the issue before moving on.
    Also, if the `status` of any subsource is `starting` for more than a few
    minutes, [contact our team](/support/).

2. Once the source is running, use the [`mz_source_statistics`](/sql/system-catalog/mz_internal/#mz_source_statistics)
   table to check the status of the initial snapshot:

    ```mzsql
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
    <p></p>

    ```nofmt
    object_id | snapshot_committed
    ----------|------------------
     u144     | t
    (1 row)
    ```

    Once `snapshot_commited` is `t`, move on to the next step. Snapshotting can
    take between a few minutes to several hours, depending on the size of your
    dataset and the size of the cluster the source is running in.


### 5. Right-size the cluster

After the snapshotting phase, Materialize starts ingesting change events from
the MySQL replication stream. For this work, Materialize generally
performs well with a `100cc` replica, so you can resize the cluster
accordingly.

1. Still in a SQL client connected to Materialize, use the [`ALTER CLUSTER`](/sql/alter-cluster/)
   command to downsize the cluster to `100cc`:

    ```mzsql
    ALTER CLUSTER ingest_mysql SET (SIZE '100cc');
    ```

    Behind the scenes, this command adds a new `100cc` replica and removes the
    `200cc` replica.

1. Use the [`SHOW CLUSTER REPLICAS`](/sql/show-cluster-replicas/) command to
   check the status of the new replica:

    ```mzsql
    SHOW CLUSTER REPLICAS WHERE cluster = 'ingest_mysql';
    ```
    <p></p>

    ```nofmt
         cluster     | replica |  size  | ready
    -----------------+---------+--------+-------
     ingest_mysql    | r1      | 100cc  | t
    (1 row)
    ```


## D. Explore your data

With Materialize ingesting your MySQL data into durable storage, you can
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

### Schema changes

> **Note:** Work to more smoothly support ddl changes to upstream tables is currently in
> progress. The work introduces the ability to re-ingest the same upstream table
> under a new schema and switch over without downtime.

Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes

<ul>
<li>
<p>Adding columns to tables. Materialize will <strong>not ingest</strong> new columns
added upstream unless you use <a href="/sql/alter-source/#context" ><code>DROP SOURCE</code></a> to
first drop the affected subsource, and then add the table back to the source
using <a href="/sql/alter-source/" ><code>ALTER SOURCE...ADD SUBSOURCE</code></a>.</p>
</li>
<li>
<p>Dropping columns that were added after the source was created. These
columns are never ingested, so you can drop them without issue.</p>
</li>
<li>
<p>Adding or removing <code>NOT NULL</code> constraints to tables that were nullable
when the source was created.</p>
</li>
</ul>


#### Incompatible schema changes

<p>All other schema changes to upstream tables will set the corresponding
subsource into an error state, which prevents you from reading from the
subsource.</p>
<p>To handle incompatible <a href="#schema-changes" >schema changes</a>, use <a href="/sql/alter-source/#context" ><code>DROP SOURCE</code></a> to first drop the affected subsource,
and then <a href="/sql/alter-source/" ><code>ALTER SOURCE...ADD SUBSOURCE</code></a> to add the
subsource back to the source. When you add the subsource, it will have the
updated schema from the corresponding upstream table.</p>


### Supported types

<p>Materialize natively supports the following MySQL types:</p>
<ul style="column-count: 3">
<li><code>bigint</code></li>
<li><code>binary</code></li>
<li><code>bit</code></li>
<li><code>blob</code></li>
<li><code>boolean</code></li>
<li><code>char</code></li>
<li><code>date</code></li>
<li><code>datetime</code></li>
<li><code>decimal</code></li>
<li><code>double</code></li>
<li><code>float</code></li>
<li><code>int</code></li>
<li><code>json</code></li>
<li><code>longblob</code></li>
<li><code>longtext</code></li>
<li><code>mediumblob</code></li>
<li><code>mediumint</code></li>
<li><code>mediumtext</code></li>
<li><code>numeric</code></li>
<li><code>real</code></li>
<li><code>smallint</code></li>
<li><code>text</code></li>
<li><code>time</code></li>
<li><code>timestamp</code></li>
<li><code>tinyblob</code></li>
<li><code>tinyint</code></li>
<li><code>tinytext</code></li>
<li><code>varbinary</code></li>
<li><code>varchar</code></li>
</ul>

<p>When replicating tables that contain the <strong>unsupported <a href="/sql/types/" >data
types</a></strong>, you can:</p>
<ul>
<li>
<p>Use <a href="/sql/create-source/mysql/#handling-unsupported-types" ><code>TEXT COLUMNS</code>
option</a> for the
following unsupported  MySQL types:</p>
<ul>
<li><code>enum</code></li>
<li><code>year</code></li>
</ul>
<p>The specified columns will be treated as <code>text</code> and will not offer the
expected MySQL type features.</p>
</li>
<li>
<p>Use the <a href="/sql/create-source/mysql/#excluding-columns" ><code>EXCLUDE COLUMNS</code></a>
option to exclude any columns that contain unsupported data types.</p>
</li>
</ul>


### Truncation

<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>

### Modifying an existing source

When you add a new subsource to an existing source ([`ALTER SOURCE ... ADD
SUBSOURCE ...`](/sql/alter-source/)), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.


---

## Ingest data from Google Cloud SQL


This page shows you how to stream data from [Google Cloud SQL for MySQL](https://cloud.google.com/sql/MySQL)
to Materialize using the[MySQL source](/sql/create-source/mysql/).

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).


## Before you begin

- Make sure you are running MySQL 5.7 or higher. Materialize uses
  [GTID-based binary log (binlog) replication](/sql/create-source/mysql/#change-data-capture),
  which is not available in older versions of MySQL.

- Ensure you have access to your MySQL instance via the [`mysql` client](https://dev.mysql.com/doc/refman/8.0/en/mysql.html),
  or your preferred SQL client.


## A. Configure Google Cloud SQL

### 1. Enable GTID-based binlog replication

Before creating a source in Materialize, you **must** configure Google Cloud SQL
for MySQL for GTID-based binlog replication. Ensure the upstream MySQL database
has been configured for GTID-based binlog replication:




























<table>
<thead>
<tr>

<th>MySQL Configuration</th>


<th>Value</th>


<th>Notes</th>


</tr>
</thead>
<tbody>







<tr>











<td>
<code>log_bin</code>
</td>











<td>
<code>ON</code>
</td>











<td>

</td>

</tr>








<tr>











<td>
<code>binlog_format</code>
</td>











<td>
<code>ROW</code>
</td>











<td>
<a href="https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format" >Deprecated as of MySQL 8.0.34</a>. Newer versions of MySQL default to row-based logging.
</td>

</tr>








<tr>











<td>
<code>binlog_row_image</code>
</td>











<td>
<code>FULL</code>
</td>











<td>

</td>

</tr>








<tr>











<td>
<code>gtid_mode</code>
</td>











<td>
<code>ON</code>
</td>











<td>

</td>

</tr>








<tr>











<td>
<code>enforce_gtid_consistency</code>
</td>











<td>
<code>ON</code>
</td>











<td>

</td>

</tr>








<tr>











<td>
<code>replica_preserve_commit_order</code>
</td>











<td>
<code>ON</code>
</td>











<td>
Only required when connecting Materialize to a read-replica.
</td>

</tr>


</tbody>
</table>



For guidance on enabling GTID-based binlog replication in Cloud SQL, see the [Cloud SQL documentation](https://cloud.google.com/sql/docs/mysql/replication).

### 2. Create a user for replication

Once GTID-based binlog replication is enabled, we recommend creating a dedicated
user for Materialize with sufficient privileges to manage replication.

1. As a _superuser_, use `mysql` (or your preferred SQL client) to connect to
   your database.

1. Create a dedicated user for Materialize, if you don't already have one:

   ```mysql
   CREATE USER 'materialize'@'%' IDENTIFIED BY '<password>';

   ALTER USER 'materialize'@'%' REQUIRE SSL;
   ```

   IAM authentication with AWS RDS for MySQL is also supported.  See the [Amazon RDS User Guide](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html) for instructions on enabling IAM database authentication, creating IAM policies, and creating a database account.

1. Grant the user permission to manage replication:

   ```mysql
   GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES ON *.* TO 'materialize'@'%';
   ```

   Once connected to your database, Materialize will take an initial snapshot of
   the tables in your MySQL server. `SELECT` privileges are required for this
   initial snapshot.

1. Apply the changes:

   ```mysql
   FLUSH PRIVILEGES;
   ```


## B. (Optional) Configure network security

> **Note:** If you are prototyping and your Google Cloud SQL instance is publicly
> accessible, **you can skip this step**. For production scenarios, we recommend
> configuring one of the network security options below.




**Cloud:**

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

1. Update your Google Cloud SQL firewall rules to allow traffic from each IP
   address from the previous step.



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

1. [Launch a GCE instance](https://cloud.google.com/compute/docs/instances/create-start-instance) to serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your
      database.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a [static public IP address](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address).
      You'll use this IP address when connecting Materialize to your bastion
      host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

    1. In the [SQL Shell](/console/), or your preferred
       SQL client connected to Materialize, get the static egress IP addresses for
       the Materialize region you are running in:

       ```mzsql
       SELECT * FROM mz_egress_ips;
       ```

    1. Update your SSH bastion host's firewall rules to allow traffic from each
    IP address from the previous step.

1. Update your Google Cloud SQL firewall rules to allow traffic from the SSH
bastion host.







**Self-Managed:**

<p>Configure your network to allow Materialize to connect to your database. For
example, you can:</p>
<ul>
<li>
<p><strong>Allow Materialize IPs:</strong> Configure your database&rsquo;s security group to allow
connections from Materialize.</p>
</li>
<li>
<p><strong>Use an SSH tunnel:</strong> Use an SSH tunnel to connect Materialize to the
database.</p>
</li>
</ul>
<div class="note">
  <strong class="gutter">NOTE:</strong>
  <p>The steps to allow Materialize to connect to your database  depends on your
  deployment setup. Refer to your company’s network/security policies and
  procedures.</p>
</div>




**Allow Materialize IPs:**

1. Update your Google Cloud SQL to allow traffic from Materialize IPs.



**Use an SSH tunnel:**

To create an SSH tunnel from Materialize to your database, you launch an
instance to serve as an SSH bastion host, configure the bastion host to allow
traffic only from Materialize, and then configure your database's private
network to allow traffic from the bastion host.

1. [Launch a GCE instance](https://cloud.google.com/compute/docs/instances/create-start-instance) to serve as your SSH bastion host.

    - Make sure the instance is publicly accessible and in the same VPC as your
      database.
    - Add a key pair and note the username. You'll use this username when
      connecting Materialize to your bastion host.
    - Make sure the VM has a [static public IP address](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address).
      You'll use this IP address when connecting Materialize to your bastion
      host.

1. Configure the SSH bastion host to allow traffic only from Materialize.

1. Update your Google Cloud SQL firewall rules to allow traffic from the SSH
bastion host.









## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

> **Note:** If you are prototyping and already have a cluster to host your MySQL
> source (e.g. `quickstart`), **you can skip this step**. For production
> scenarios, we recommend separating your workloads into multiple clusters for
> [resource isolation](/sql/create-cluster/#resource-isolation).


In Materialize, a [cluster](/concepts/clusters/) is an isolated
environment, similar to a virtual warehouse in Snowflake. When you create a
cluster, you choose the size of its compute resource allocation based on the
work you need the cluster to do, whether ingesting data from a source,
computing always-up-to-date query results, serving results to clients, or a
combination.

In this case, you'll create a dedicated cluster for ingesting source data from
your MySQL database.

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, use the [`CREATE CLUSTER`](/sql/create-cluster/)
   command to create the new cluster:

    ```mzsql
    CREATE CLUSTER ingest_mysql (SIZE = '200cc');

    SET CLUSTER = ingest_mysql;
    ```

    A cluster of [size](/sql/create-cluster/#size) `200cc` should be enough to
    process the initial snapshot of the tables in your MySQL database. For very
    large snapshots, consider using a larger size to speed up processing. Once
    the snapshot is finished, you can readjust the size of the cluster to fit
    the volume of changes being replicated from your upstream MySQL database.


### 2. Create a connection


Once you have configured your network, create a connection in Materialize per
your networking configuration.



**Allow Materialize IPs:**

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, use the [`CREATE SECRET`](/sql/create-secret/)
   command to securely store the password for the `materialize` MySQL user
   you created [earlier](#2-create-a-user-for-replication):

    ```mzsql
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

    For AWS IAM authentication, you must create a connection to AWS.  See the [`CREATE CONNECTION`](/sql/create-connection/#aws) command for details.

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
   connection object with access and authentication details for Materialize to
   use:

    ```mzsql
    CREATE CONNECTION mysql_connection TO MYSQL (
        HOST <host>,
        PORT 3306,
        USER 'materialize',
        PASSWORD SECRET mysqlpass,
        SSL MODE REQUIRED
    );
    ```

    - Replace `<host>` with your MySQL endpoint.

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

1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the password for the `materialize` MySQL user you created [earlier](#2-create-a-user-for-replication):

    ```mzsql
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

    For AWS IAM authentication, you must create a connection to AWS.  See the [`CREATE CONNECTION`](/sql/create-connection/#aws) command for details.

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create another connection object, this time with database access and authentication details for Materialize to use:

    ```mzsql
    CREATE CONNECTION mysql_connection TO MYSQL (
    HOST '<host>',
    SSH TUNNEL ssh_connection
    );
    ```

    - Replace `<host>` with your MySQL endpoint.

  AWS IAM authentication is also available, see the [`CREATE CONNECTION`](/sql/create-connection/#mysql)
  command for details.





### 3. Start ingesting data

Once you have created the connection, you can use the connection in the
[`CREATE SOURCE`](/sql/create-source/) command to connect to your MySQL instance and start ingesting
data:
```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR ALL TABLES;

```


- By default, the source will be created in the active cluster; to use a different cluster, use the `IN CLUSTER` clause.

- To ingest data from specific schemas or tables, use the `FOR SCHEMAS (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` options instead of `FOR ALL TABLES`.

- To handle [unsupported data types](#supported-types), use the `TEXT COLUMNS` or `EXCLUDE COLUMNS` options.


After source creation, refer to [schema changes
considerations](#schema-changes) for information on handling upstream schema changes.


### 4. Monitor the ingestion status

Before it starts consuming the replication stream, Materialize takes a snapshot
of the relevant tables. Until this snapshot is complete, Materialize won't have
the same view of your data as your MySQL database.

In this step, you'll first verify that the source is running and then check the
status of the snapshotting process.

1. Back in the SQL client connected to Materialize, use the
   [`mz_source_statuses`](/sql/system-catalog/mz_internal/#mz_source_statuses)
   table to check the overall status of your source:

    ```mzsql
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

    For each `subsource`, make sure the `status` is `running`. If you see
    `stalled` or `failed`, there's likely a configuration issue for you to fix.
    Check the `error` field for details and fix the issue before moving on.
    Also, if the `status` of any subsource is `starting` for more than a few
    minutes, [contact our team](/support/).

2. Once the source is running, use the [`mz_source_statistics`](/sql/system-catalog/mz_internal/#mz_source_statistics)
   table to check the status of the initial snapshot:

    ```mzsql
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
    <p></p>

    ```nofmt
    object_id | snapshot_committed
    ----------|------------------
     u144     | t
    (1 row)
    ```

    Once `snapshot_commited` is `t`, move on to the next step. Snapshotting can
    take between a few minutes to several hours, depending on the size of your
    dataset and the size of the cluster the source is running in.


### 5. Right-size the cluster

After the snapshotting phase, Materialize starts ingesting change events from
the MySQL replication stream. For this work, Materialize generally
performs well with a `100cc` replica, so you can resize the cluster
accordingly.

1. Still in a SQL client connected to Materialize, use the [`ALTER CLUSTER`](/sql/alter-cluster/)
   command to downsize the cluster to `100cc`:

    ```mzsql
    ALTER CLUSTER ingest_mysql SET (SIZE '100cc');
    ```

    Behind the scenes, this command adds a new `100cc` replica and removes the
    `200cc` replica.

1. Use the [`SHOW CLUSTER REPLICAS`](/sql/show-cluster-replicas/) command to
   check the status of the new replica:

    ```mzsql
    SHOW CLUSTER REPLICAS WHERE cluster = 'ingest_mysql';
    ```
    <p></p>

    ```nofmt
         cluster     | replica |  size  | ready
    -----------------+---------+--------+-------
     ingest_mysql    | r1      | 100cc  | t
    (1 row)
    ```


## D. Explore your data

With Materialize ingesting your MySQL data into durable storage, you can
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

### Schema changes

> **Note:** Work to more smoothly support ddl changes to upstream tables is currently in
> progress. The work introduces the ability to re-ingest the same upstream table
> under a new schema and switch over without downtime.

Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes

<ul>
<li>
<p>Adding columns to tables. Materialize will <strong>not ingest</strong> new columns
added upstream unless you use <a href="/sql/alter-source/#context" ><code>DROP SOURCE</code></a> to
first drop the affected subsource, and then add the table back to the source
using <a href="/sql/alter-source/" ><code>ALTER SOURCE...ADD SUBSOURCE</code></a>.</p>
</li>
<li>
<p>Dropping columns that were added after the source was created. These
columns are never ingested, so you can drop them without issue.</p>
</li>
<li>
<p>Adding or removing <code>NOT NULL</code> constraints to tables that were nullable
when the source was created.</p>
</li>
</ul>


#### Incompatible schema changes

<p>All other schema changes to upstream tables will set the corresponding
subsource into an error state, which prevents you from reading from the
subsource.</p>
<p>To handle incompatible <a href="#schema-changes" >schema changes</a>, use <a href="/sql/alter-source/#context" ><code>DROP SOURCE</code></a> to first drop the affected subsource,
and then <a href="/sql/alter-source/" ><code>ALTER SOURCE...ADD SUBSOURCE</code></a> to add the
subsource back to the source. When you add the subsource, it will have the
updated schema from the corresponding upstream table.</p>


### Supported types

<p>Materialize natively supports the following MySQL types:</p>
<ul style="column-count: 3">
<li><code>bigint</code></li>
<li><code>binary</code></li>
<li><code>bit</code></li>
<li><code>blob</code></li>
<li><code>boolean</code></li>
<li><code>char</code></li>
<li><code>date</code></li>
<li><code>datetime</code></li>
<li><code>decimal</code></li>
<li><code>double</code></li>
<li><code>float</code></li>
<li><code>int</code></li>
<li><code>json</code></li>
<li><code>longblob</code></li>
<li><code>longtext</code></li>
<li><code>mediumblob</code></li>
<li><code>mediumint</code></li>
<li><code>mediumtext</code></li>
<li><code>numeric</code></li>
<li><code>real</code></li>
<li><code>smallint</code></li>
<li><code>text</code></li>
<li><code>time</code></li>
<li><code>timestamp</code></li>
<li><code>tinyblob</code></li>
<li><code>tinyint</code></li>
<li><code>tinytext</code></li>
<li><code>varbinary</code></li>
<li><code>varchar</code></li>
</ul>

<p>When replicating tables that contain the <strong>unsupported <a href="/sql/types/" >data
types</a></strong>, you can:</p>
<ul>
<li>
<p>Use <a href="/sql/create-source/mysql/#handling-unsupported-types" ><code>TEXT COLUMNS</code>
option</a> for the
following unsupported  MySQL types:</p>
<ul>
<li><code>enum</code></li>
<li><code>year</code></li>
</ul>
<p>The specified columns will be treated as <code>text</code> and will not offer the
expected MySQL type features.</p>
</li>
<li>
<p>Use the <a href="/sql/create-source/mysql/#excluding-columns" ><code>EXCLUDE COLUMNS</code></a>
option to exclude any columns that contain unsupported data types.</p>
</li>
</ul>


### Truncation

<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>

### Modifying an existing source

When you add a new subsource to an existing source ([`ALTER SOURCE ... ADD
SUBSOURCE ...`](/sql/alter-source/)), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.


---

## Ingest data from self-hosted MySQL


This page shows you how to stream data from a self-hosted MySQL database to
Materialize using the [MySQL source](/sql/create-source/mysql/).

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).


## Before you begin

- Make sure you are running MySQL 5.7 or higher. Materialize uses
  [GTID-based binary log (binlog) replication](/sql/create-source/mysql/#change-data-capture),
  which is not available in older versions of MySQL.

- Ensure you have access to your MySQL instance via the [`mysql` client](https://dev.mysql.com/doc/refman/8.0/en/mysql.html),
  or your preferred SQL client.


## A. Configure MySQL

### 1. Enable GTID-based binlog replication

Before creating a source in Materialize, you **must** configure your MySQL
database for GTID-based binlog replication. Ensure the upstream MySQL database
has been configured for GTID-based binlog replication:




























<table>
<thead>
<tr>

<th>MySQL Configuration</th>


<th>Value</th>


<th>Notes</th>


</tr>
</thead>
<tbody>







<tr>











<td>
<code>log_bin</code>
</td>











<td>
<code>ON</code>
</td>











<td>

</td>

</tr>








<tr>











<td>
<code>binlog_format</code>
</td>











<td>
<code>ROW</code>
</td>











<td>
<a href="https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format" >Deprecated as of MySQL 8.0.34</a>. Newer versions of MySQL default to row-based logging.
</td>

</tr>








<tr>











<td>
<code>binlog_row_image</code>
</td>











<td>
<code>FULL</code>
</td>











<td>

</td>

</tr>








<tr>











<td>
<code>gtid_mode</code>
</td>











<td>
<code>ON</code>
</td>











<td>

</td>

</tr>








<tr>











<td>
<code>enforce_gtid_consistency</code>
</td>











<td>
<code>ON</code>
</td>











<td>

</td>

</tr>








<tr>











<td>
<code>replica_preserve_commit_order</code>
</td>











<td>
<code>ON</code>
</td>











<td>
Only required when connecting Materialize to a read-replica.
</td>

</tr>


</tbody>
</table>



For guidance on enabling GTID-based binlog replication, see the
[MySQL documentation](https://dev.mysql.com/blog-archive/enabling-gtids-without-downtime-in-mysql-5-7-6/).

### 2. Create a user for replication

Once GTID-based binlog replication is enabled, we recommend creating a dedicated
user for Materialize with sufficient privileges to manage replication.

1. As a _superuser_, use `mysql` (or your preferred SQL client) to connect to
   your database.

1. Create a dedicated user for Materialize, if you don't already have one:

   ```mysql
   CREATE USER 'materialize'@'%' IDENTIFIED BY '<password>';

   ALTER USER 'materialize'@'%' REQUIRE SSL;
   ```

   IAM authentication with AWS RDS for MySQL is also supported.  See the [Amazon RDS User Guide](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html) for instructions on enabling IAM database authentication, creating IAM policies, and creating a database account.

1. Grant the user permission to manage replication:

   ```mysql
   GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES ON *.* TO 'materialize'@'%';
   ```

   Once connected to your database, Materialize will take an initial snapshot of
   the tables in your MySQL server. `SELECT` privileges are required for this
   initial snapshot.

1. Apply the changes:

   ```mysql
   FLUSH PRIVILEGES;
   ```


## B. (Optional) Configure network security

> **Note:** If you are prototyping and your MySQL instance is publicly accessible, **you can
> skip this step**. For production scenarios, we recommend configuring one of the
> network security options below.




**Cloud:**

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







**Self-Managed:**

<p>Configure your network to allow Materialize to connect to your database. For
example, you can:</p>
<ul>
<li>
<p><strong>Allow Materialize IPs:</strong> Configure your database&rsquo;s security group to allow
connections from Materialize.</p>
</li>
<li>
<p><strong>Use an SSH tunnel:</strong> Use an SSH tunnel to connect Materialize to the
database.</p>
</li>
</ul>
<div class="note">
  <strong class="gutter">NOTE:</strong>
  <p>The steps to allow Materialize to connect to your database  depends on your
  deployment setup. Refer to your company’s network/security policies and
  procedures.</p>
</div>




**Allow Materialize IPs:**

1. Update your database firewall rules to allow traffic from Materialize IPs.



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

1. Update your database firewall rules to allow traffic from the SSH bastion
   host.









## C. Ingest data in Materialize

### 1. (Optional) Create a cluster

> **Note:** If you are prototyping and already have a cluster to host your MySQL
> source (e.g. `quickstart`), **you can skip this step**. For production
> scenarios, we recommend separating your workloads into multiple clusters for
> [resource isolation](/sql/create-cluster/#resource-isolation).


In Materialize, a [cluster](/concepts/clusters/) is an isolated
environment, similar to a virtual warehouse in Snowflake. When you create a
cluster, you choose the size of its compute resource allocation based on the
work you need the cluster to do, whether ingesting data from a source,
computing always-up-to-date query results, serving results to clients, or a
combination.

In this case, you'll create a dedicated cluster for ingesting source data from
your MySQL database.

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, use the [`CREATE CLUSTER`](/sql/create-cluster/)
   command to create the new cluster:

    ```mzsql
    CREATE CLUSTER ingest_mysql (SIZE = '200cc');

    SET CLUSTER = ingest_mysql;
    ```

    A cluster of [size](/sql/create-cluster/#size) `200cc` should be enough to
    process the initial snapshot of the tables in your MySQL database. For very
    large snapshots, consider using a larger size to speed up processing. Once
    the snapshot is finished, you can readjust the size of the cluster to fit
    the volume of changes being replicated from your upstream MySQL database.


### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.



**Allow Materialize IPs:**

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, use the [`CREATE SECRET`](/sql/create-secret/)
   command to securely store the password for the `materialize` MySQL user
   you created [earlier](#2-create-a-user-for-replication):

    ```mzsql
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

    For AWS IAM authentication, you must create a connection to AWS.  See the [`CREATE CONNECTION`](/sql/create-connection/#aws) command for details.

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
   connection object with access and authentication details for Materialize to
   use:

    ```mzsql
    CREATE CONNECTION mysql_connection TO MYSQL (
        HOST <host>,
        PORT 3306,
        USER 'materialize',
        PASSWORD SECRET mysqlpass,
        SSL MODE REQUIRED
    );
    ```

    - Replace `<host>` with your MySQL endpoint.

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

1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the password for the `materialize` MySQL user you created [earlier](#2-create-a-user-for-replication):

    ```mzsql
    CREATE SECRET mysqlpass AS '<PASSWORD>';
    ```

    For AWS IAM authentication, you must create a connection to AWS.  See the [`CREATE CONNECTION`](/sql/create-connection/#aws) command for details.

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create another connection object, this time with database access and authentication details for Materialize to use:

    ```mzsql
    CREATE CONNECTION mysql_connection TO MYSQL (
    HOST '<host>',
    SSH TUNNEL ssh_connection
    );
    ```

    - Replace `<host>` with your MySQL endpoint.

  AWS IAM authentication is also available, see the [`CREATE CONNECTION`](/sql/create-connection/#mysql)
  command for details.





### 3. Start ingesting data

Once you have created the connection, you can use the connection in the
[`CREATE SOURCE`](/sql/create-source/) command to connect to your MySQL instance and start ingesting
data:
```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR ALL TABLES;

```


- By default, the source will be created in the active cluster; to use a different cluster, use the `IN CLUSTER` clause.

- To ingest data from specific schemas or tables, use the `FOR SCHEMAS (<schema1>,<schema2>)` or `FOR TABLES (<table1>, <table2>)` options instead of `FOR ALL TABLES`.

- To handle [unsupported data types](#supported-types), use the `TEXT COLUMNS` or `EXCLUDE COLUMNS` options.


After source creation, refer to [schema changes
considerations](#schema-changes) for information on handling upstream schema changes.


### 4. Monitor the ingestion status

Before it starts consuming the replication stream, Materialize takes a snapshot
of the relevant tables. Until this snapshot is complete, Materialize won't have
the same view of your data as your MySQL database.

In this step, you'll first verify that the source is running and then check the
status of the snapshotting process.

1. Back in the SQL client connected to Materialize, use the
   [`mz_source_statuses`](/sql/system-catalog/mz_internal/#mz_source_statuses)
   table to check the overall status of your source:

    ```mzsql
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

    For each `subsource`, make sure the `status` is `running`. If you see
    `stalled` or `failed`, there's likely a configuration issue for you to fix.
    Check the `error` field for details and fix the issue before moving on.
    Also, if the `status` of any subsource is `starting` for more than a few
    minutes, [contact our team](/support/).

2. Once the source is running, use the [`mz_source_statistics`](/sql/system-catalog/mz_internal/#mz_source_statistics)
   table to check the status of the initial snapshot:

    ```mzsql
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
    <p></p>

    ```nofmt
    object_id | snapshot_committed
    ----------|------------------
     u144     | t
    (1 row)
    ```

    Once `snapshot_commited` is `t`, move on to the next step. Snapshotting can
    take between a few minutes to several hours, depending on the size of your
    dataset and the size of the cluster the source is running in.


### 5. Right-size the cluster

After the snapshotting phase, Materialize starts ingesting change events from
the MySQL replication stream. For this work, Materialize generally
performs well with a `100cc` replica, so you can resize the cluster
accordingly.

1. Still in a SQL client connected to Materialize, use the [`ALTER CLUSTER`](/sql/alter-cluster/)
   command to downsize the cluster to `100cc`:

    ```mzsql
    ALTER CLUSTER ingest_mysql SET (SIZE '100cc');
    ```

    Behind the scenes, this command adds a new `100cc` replica and removes the
    `200cc` replica.

1. Use the [`SHOW CLUSTER REPLICAS`](/sql/show-cluster-replicas/) command to
   check the status of the new replica:

    ```mzsql
    SHOW CLUSTER REPLICAS WHERE cluster = 'ingest_mysql';
    ```
    <p></p>

    ```nofmt
         cluster     | replica |  size  | ready
    -----------------+---------+--------+-------
     ingest_mysql    | r1      | 100cc  | t
    (1 row)
    ```


## D. Explore your data

With Materialize ingesting your MySQL data into durable storage, you can
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

### Schema changes

> **Note:** Work to more smoothly support ddl changes to upstream tables is currently in
> progress. The work introduces the ability to re-ingest the same upstream table
> under a new schema and switch over without downtime.

Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes

<ul>
<li>
<p>Adding columns to tables. Materialize will <strong>not ingest</strong> new columns
added upstream unless you use <a href="/sql/alter-source/#context" ><code>DROP SOURCE</code></a> to
first drop the affected subsource, and then add the table back to the source
using <a href="/sql/alter-source/" ><code>ALTER SOURCE...ADD SUBSOURCE</code></a>.</p>
</li>
<li>
<p>Dropping columns that were added after the source was created. These
columns are never ingested, so you can drop them without issue.</p>
</li>
<li>
<p>Adding or removing <code>NOT NULL</code> constraints to tables that were nullable
when the source was created.</p>
</li>
</ul>


#### Incompatible schema changes

<p>All other schema changes to upstream tables will set the corresponding
subsource into an error state, which prevents you from reading from the
subsource.</p>
<p>To handle incompatible <a href="#schema-changes" >schema changes</a>, use <a href="/sql/alter-source/#context" ><code>DROP SOURCE</code></a> to first drop the affected subsource,
and then <a href="/sql/alter-source/" ><code>ALTER SOURCE...ADD SUBSOURCE</code></a> to add the
subsource back to the source. When you add the subsource, it will have the
updated schema from the corresponding upstream table.</p>


### Supported types

<p>Materialize natively supports the following MySQL types:</p>
<ul style="column-count: 3">
<li><code>bigint</code></li>
<li><code>binary</code></li>
<li><code>bit</code></li>
<li><code>blob</code></li>
<li><code>boolean</code></li>
<li><code>char</code></li>
<li><code>date</code></li>
<li><code>datetime</code></li>
<li><code>decimal</code></li>
<li><code>double</code></li>
<li><code>float</code></li>
<li><code>int</code></li>
<li><code>json</code></li>
<li><code>longblob</code></li>
<li><code>longtext</code></li>
<li><code>mediumblob</code></li>
<li><code>mediumint</code></li>
<li><code>mediumtext</code></li>
<li><code>numeric</code></li>
<li><code>real</code></li>
<li><code>smallint</code></li>
<li><code>text</code></li>
<li><code>time</code></li>
<li><code>timestamp</code></li>
<li><code>tinyblob</code></li>
<li><code>tinyint</code></li>
<li><code>tinytext</code></li>
<li><code>varbinary</code></li>
<li><code>varchar</code></li>
</ul>

<p>When replicating tables that contain the <strong>unsupported <a href="/sql/types/" >data
types</a></strong>, you can:</p>
<ul>
<li>
<p>Use <a href="/sql/create-source/mysql/#handling-unsupported-types" ><code>TEXT COLUMNS</code>
option</a> for the
following unsupported  MySQL types:</p>
<ul>
<li><code>enum</code></li>
<li><code>year</code></li>
</ul>
<p>The specified columns will be treated as <code>text</code> and will not offer the
expected MySQL type features.</p>
</li>
<li>
<p>Use the <a href="/sql/create-source/mysql/#excluding-columns" ><code>EXCLUDE COLUMNS</code></a>
option to exclude any columns that contain unsupported data types.</p>
</li>
</ul>


### Truncation

<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>

### Modifying an existing source

When you add a new subsource to an existing source ([`ALTER SOURCE ... ADD
SUBSOURCE ...`](/sql/alter-source/)), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.


---

## MySQL CDC using Kafka and Debezium


> **Warning:** You can use [Debezium](https://debezium.io/) to propagate Change
> Data Capture(CDC) data to Materialize from a MySQL database, but
> we **strongly recommend** using the native [MySQL](/sql/create-source/mysql/)
> source instead.


For help getting started with your own data, you can schedule a [free guided
trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).


Change Data Capture (CDC) allows you to track and propagate changes in a MySQL
database to downstream consumers based on its binary log (`binlog`). In this
guide, we’ll cover how to use Materialize to create and efficiently maintain
real-time views with incrementally updated results on top of CDC data.

## Kafka + Debezium

You can use [Debezium](https://debezium.io/) and the [Kafka source](/sql/create-source/kafka/#debezium-envelope)
to propagate CDC data from MySQL to Materialize in the unlikely event that using
the [native MySQL source](/sql/create-source/mysql/) is not an option. Debezium
captures row-level changes resulting from `INSERT`, `UPDATE` and `DELETE`
operations in the upstream database and publishes them as events to Kafka using
Kafka Connect-compatible connectors.

### A. Configure database

Before deploying a Debezium connector, you need to ensure that the upstream
database is configured to support [row-based replication](https://dev.mysql.com/doc/refman/8.0/en/replication-rbr-usage.html).
As _root_:

1. Check the `log_bin` and `binlog_format` settings:

    ```mysql
    SHOW VARIABLES
    WHERE variable_name IN ('log_bin', 'binlog_format');
    ```

    For CDC, binary logging must be enabled and use the `row` format. If your
    settings differ, you can adjust the database configuration file
    (`/etc/mysql/my.cnf`) to use `log_bin=mysql-bin` and `binlog_format=row`.
    Keep in mind that changing these settings requires a restart of the MySQL
    instance and can [affect database performance](https://dev.mysql.com/doc/refman/8.0/en/replication-sbr-rbr.html#replication-sbr-rbr-rbr-disadvantages).

    **Note:** Additional steps may be required if you're using MySQL on
      [Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_LogAccess.MySQL.BinaryFormat.html).

1. Grant enough privileges to the replication user to ensure Debezium can
   operate in the database:

    ```mysql
    GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO "user";

    FLUSH PRIVILEGES;
    ```

### B. Deploy Debezium

**Minimum requirements:** Debezium 1.5+

Debezium is deployed as a set of Kafka Connect-compatible connectors, so you
first need to define a MySQL connector configuration and then start the
connector by adding it to Kafka Connect.

> **Warning:** If you deploy the MySQL Debezium connector in [Confluent Cloud](https://docs.confluent.io/cloud/current/connectors/cc-mysql-source-cdc-debezium.html),
> you **must** override the default value of `After-state only` to `false`.



**Debezium 1.5+:**

1. Create a connector configuration file and save it as `register-mysql.json`:

    ```json
    {
      "name": "your-connector",
      "config": {
          "connector.class": "io.debezium.connector.mysql.MySqlConnector",
          "tasks.max": "1",
          "database.hostname": "mysql",
          "database.port": "3306",
          "database.user": "user",
          "database.password": "mysqlpwd",
          "database.server.id":"223344",
          "database.server.name": "dbserver1",
          "database.history.kafka.bootstrap.servers":"kafka:9092",
          "database.history.kafka.topic":"dbserver1.history",
          "database.include.list": "db1",
          "table.include.list": "table1",
          "include.schema.changes": false
        }
    }
    ```

    You can read more about each configuration property in the
    [Debezium documentation](https://debezium.io/documentation/reference/connectors/mysql.html#mysql-connector-properties).


**Debezium 2.0+:**

1. From Debezium 2.0, Confluent Schema Registry (CSR) support is not bundled in
   Debezium containers. To enable CSR, you must install the following Confluent
   Avro converter JAR files into the Kafka Connect plugin directory (by default,
   `/kafka/connect`):

    * `kafka-connect-avro-converter`
    * `kafka-connect-avro-data`
    * `kafka-avro-serializer`
    * `kafka-schema-serializer`
    * `kafka-schema-registry-client`
    * `common-config`
    * `common-utils`

    You can read more about this in the [Debezium documentation](https://debezium.io/documentation/reference/stable/configuration/avro.html#deploying-confluent-schema-registry-with-debezium-containers).

1. Create a connector configuration file and save it as `register-mysql.json`:

    ```json
    {
      "name": "your-connector",
      "config": {
          "connector.class": "io.debezium.connector.mysql.MySqlConnector",
          "tasks.max": "1",
          "database.hostname": "mysql",
          "database.port": "3306",
          "database.user": "user",
          "database.password": "mysqlpwd",
          "database.server.id":"223344",
          "topic.prefix": "dbserver1",
          "database.include.list": "db1",
          "database.history.kafka.topic":"dbserver1.history",
          "database.history.kafka.bootstrap.servers":"kafka:9092",
          "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
          "schema.history.internal.kafka.topic": "dbserver1.internal.history",
          "table.include.list": "table1",
          "key.converter": "io.confluent.connect.avro.AvroConverter",
          "value.converter": "io.confluent.connect.avro.AvroConverter",
          "key.converter.schema.registry.url": "http://<scheme-registry>:8081",
          "value.converter.schema.registry.url": "http://<scheme-registry>:8081",
          "include.schema.changes": false
        }
    }
    ```

    You can read more about each configuration property in the
    [Debezium documentation](https://debezium.io/documentation/reference/2.4/connectors/mysql.html).
    By default, the connector writes events for each table to a Kafka topic
    named `serverName.databaseName.tableName`.




1. Start the Debezium MySQL connector using the configuration file:

    ```bash
    export CURRENT_HOST='<your-host>'
    curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
    http://$CURRENT_HOST:8083/connectors/ -d @register-mysql.json
    ```

1. Check that the connector is running:

    ```bash
    curl http://$CURRENT_HOST:8083/connectors/your-connector/status
    ```

    The first time it connects to a MySQL server, Debezium takes a
    [consistent snapshot](https://debezium.io/documentation/reference/connectors/mysql.html#mysql-snapshots)
    of the tables selected for replication, so you should see that the
    pre-existing records in the replicated table are initially pushed into your
    Kafka topic:

    ```bash
    /usr/bin/kafka-avro-console-consumer \
      --bootstrap-server kafka:9092 \
      --from-beginning \
      --topic dbserver1.db1.table1
    ```

### C. Create a source

<div class="note">
  <strong class="gutter">NOTE:</strong> Currently, Materialize only supports Avro-encoded Debezium records. If you're interested in JSON support, please reach out in the community Slack or submit a <a href="https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests">feature request</a>.
</div>


Debezium emits change events using an envelope that contains detailed
information about upstream database operations, like the `before` and `after`
values for each record. To create a source that interprets the
[Debezium envelope](/sql/create-source/kafka/#debezium-envelope) in Materialize:

```mzsql
CREATE SOURCE kafka_repl
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'dbserver1.db1.table1')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
    ENVELOPE DEBEZIUM;
```

By default, the source will be created in the active cluster; to use a different
cluster, use the `IN CLUSTER` clause.

### D. Create a view on the source

A [view](/concepts/views/) saves a query under a name to provide a shorthand for
referencing the query. During view creation, the underlying query is not
executed.

```mzsql
CREATE VIEW cnt_table1 AS
    SELECT field1,
           COUNT(*) AS cnt
    FROM kafka_repl
    GROUP BY field1;
```


### E. Create an index on the view

In Materialize, [indexes](/concepts/indexes) on views compute and, as new data
arrives, incrementally update view results in memory within a
[cluster](/concepts/clusters/) instead of recomputing the results from scratch.

Create an index on `cnt_table1` view. Then, as new change events stream in
through Kafka (as the result of `INSERT`, `UPDATE` and `DELETE` operations in
the upstream database), the index incrementally updates the view
results in memory, such that the in-memory up-to-date results are immediately
available and computationally free to query.

```mzsql
CREATE INDEX idx_cnt_table1_field1 ON cnt_table1(field1);
```

For best practices on when to index a view, see
[Indexes](/concepts/indexes/) and [Views](/concepts/views/).
