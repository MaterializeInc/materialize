# Ingest data from Azure DB
How to stream data from Azure DB for MySQL to Materialize
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
