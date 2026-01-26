# Ingest data from Google Cloud SQL

How to stream data from Google Cloud SQL for PostgreSQL to Materialize



This page shows you how to stream data from [Google Cloud SQL for PostgreSQL](https://cloud.google.com/sql/postgresql)
to Materialize using the[PostgreSQL source](/sql/create-source/postgres/).

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).
>
>



## Before you begin

<ul>
<li>
<p>Make sure you are running PostgreSQL 11 or higher.</p>
</li>
<li>
<p>Make sure you have access to your PostgreSQL instance via <a href="https://www.postgresql.org/docs/current/app-psql.html" ><code>psql</code></a>,
or your preferred SQL client.</p>
</li>
</ul>


## A. Configure Google Cloud SQL

### 1. Enable logical replication

Materialize uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html)
protocol to track changes in your database and propagate them to Materialize.

To enable logical replication in Cloud SQL, see the [Cloud SQL
documentation](https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication#configuring-your-postgresql-instance).

### 2. Create a publication and a replication user

<p>Once logical replication is enabled, the next step is to create a publication
with the tables that you want to replicate to Materialize. You&rsquo;ll also need a
user for Materialize with sufficient privileges to manage replication.</p>
<ol>
<li>
<p>For each table that you want to replicate to Materialize, set the
<a href="https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY" >replica identity</a>
to <code>FULL</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span> <span class="k">REPLICA</span> <span class="k">IDENTITY</span> <span class="k">FULL</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span> <span class="k">REPLICA</span> <span class="k">IDENTITY</span> <span class="k">FULL</span><span class="p">;</span>
</span></span></code></pre></div><p><code>REPLICA IDENTITY FULL</code> ensures that the replication stream includes the
previous data of changed rows, in the case of <code>UPDATE</code> and <code>DELETE</code>
operations. This setting enables Materialize to ingest PostgreSQL data with
minimal in-memory state. However, you should expect increased disk usage in
your PostgreSQL database.</p>
</li>
<li>
<p>Create a <a href="https://www.postgresql.org/docs/current/logical-replication-publication.html" >publication</a>
with the tables you want to replicate:</p>
<p><em>For specific tables:</em></p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="n">PUBLICATION</span> <span class="n">mz_source</span> <span class="k">FOR</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span><span class="p">,</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span><span class="p">;</span>
</span></span></code></pre></div><p><em>For all tables in the database:</em></p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="n">PUBLICATION</span> <span class="n">mz_source</span> <span class="k">FOR</span> <span class="k">ALL</span> <span class="k">TABLES</span><span class="p">;</span>
</span></span></code></pre></div><p>The <code>mz_source</code> publication will contain the set of change events generated
from the specified tables, and will later be used to ingest the replication
stream.</p>
<p>Be sure to include only the tables you need. If the publication includes
additional tables, Materialize will waste resources on ingesting and then
immediately discarding the data.</p>
</li>
<li>
<p>Create a user for Materialize, if you don&rsquo;t already have one:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">USER</span> <span class="n">materialize</span> <span class="k">PASSWORD</span> <span class="s1">&#39;&lt;password&gt;&#39;</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>Grant the user permission to manage replication:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">ROLE</span> <span class="n">materialize</span> <span class="k">WITH</span> <span class="n">REPLICATION</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>Grant the user the required permissions on the tables you want to replicate:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="n">CONNECT</span> <span class="k">ON</span> <span class="k">DATABASE</span> <span class="o">&lt;</span><span class="n">dbname</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="n">USAGE</span> <span class="k">ON</span> <span class="k">SCHEMA</span> <span class="o">&lt;</span><span class="k">schema</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="o">&lt;</span><span class="n">table1</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="o">&lt;</span><span class="n">table2</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div><p>Once connected to your database, Materialize will take an initial snapshot
of the tables in your publication. <code>SELECT</code> privileges are required for
this initial snapshot.</p>
<p>If you expect to add tables to your publication, you can grant <code>SELECT</code> on
all tables in the schema instead of naming the specific tables:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="k">ALL</span> <span class="k">TABLES</span> <span class="k">IN</span> <span class="k">SCHEMA</span> <span class="o">&lt;</span><span class="k">schema</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">materialize</span><span class="p">;</span>
</span></span></code></pre></div></li>
</ol>


## B. (Optional) Configure network security

> **Note:** If you are prototyping and your Google Cloud SQL instance is publicly
> accessible, **you can skip this step**. For production scenarios, we recommend
> configuring one of the network security options below.
>




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

1. In the [Materialize console's SQL Shell](/console/),
   or your preferred SQL client connected to Materialize, find the static egress
   IP addresses for the Materialize region you are running in:

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

    1. In the [Materialize console's SQL
       Shell](/console/), or your preferred SQL client
       connected to Materialize, get the static egress IP addresses for the
       Materialize region you are running in:

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

1. Update your Google Cloud SQL firewall rules to allow traffic from Materialize
   IPs.



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

> **Note:** If you are prototyping and already have a cluster to host your PostgreSQL
> source (e.g. `quickstart`), **you can skip this step**. For production
> scenarios, we recommend separating your workloads into multiple clusters for
> [resource isolation](/sql/create-cluster/#resource-isolation).
>


<p>In Materialize, a <a href="/concepts/clusters/" >cluster</a> is an isolated environment,
similar to a virtual warehouse in Snowflake. When you create a cluster, you
choose the size of its compute resource allocation based on the work you need
the cluster to do, whether ingesting data from a source, computing
always-up-to-date query results, serving results to external clients, or a
combination.</p>
<p>In this step, you&rsquo;ll create a dedicated cluster for ingesting source data from
your PostgreSQL database.</p>
<ol>
<li>
<p>In the <a href="/console/" >SQL Shell</a>, or your preferred SQL
client connected to Materialize, use the <a href="/sql/create-cluster/" ><code>CREATE CLUSTER</code></a>
command to create the new cluster:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">CLUSTER</span> <span class="n">ingest_postgres</span> <span class="p">(</span><span class="k">SIZE</span> <span class="o">=</span> <span class="s1">&#39;50cc&#39;</span><span class="p">);</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">SET</span> <span class="k">CLUSTER</span> <span class="o">=</span> <span class="n">ingest_postgres</span><span class="p">;</span>
</span></span></code></pre></div><p>A cluster of <a href="/sql/create-cluster/#size" >size</a> <code>50cc</code> should be enough to
accommodate multiple PostgreSQL sources, depending on the source
characteristics (e.g., sources with <a href="/sql/create-source/kafka/#upsert-envelope" ><code>ENVELOPE UPSERT</code></a>
or <a href="/sql/create-source/kafka/#debezium-envelope" ><code>ENVELOPE DEBEZIUM</code></a> will be more
memory-intensive) and the upstream traffic patterns. You can readjust the
size of the cluster at any time using the <a href="/sql/alter-cluster" ><code>ALTER CLUSTER</code></a> command:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">CLUSTER</span> <span class="o">&lt;</span><span class="n">cluster_name</span><span class="o">&gt;</span> <span class="k">SET</span> <span class="p">(</span> <span class="k">SIZE</span> <span class="o">=</span> <span class="o">&lt;</span><span class="n">new_size</span><span class="o">&gt;</span> <span class="p">);</span>
</span></span></code></pre></div></li>
</ol>


### 2. Create a connection

Once you have configured your network, create a connection in Materialize per
your networking configuration.



**Allow Materialize IPs:**
1. In the [Materialize Console's SQL Shell](/console/), or your preferred SQL
client connected to Materialize, use the [`CREATE
SECRET`](/sql/create-secret/) command to securely store the password for the
`materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):

   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```

1. Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create a
connection object with access and authentication details for Materialize to
use:


   ```mzsql
   CREATE CONNECTION pg_connection TO POSTGRES (
     HOST '<host>',
     PORT 5432,
     USER materialize,
     PASSWORD SECRET pgpass,
     SSL MODE 'require',
     DATABASE '<database>'
   );

   ```


   - Replace `<host>` with your PostgreSQL endpoint.

   - Replace `<database>` with the name of the database containing the tables
     you want to replicate to Materialize.



**Use an SSH tunnel:**

1. In the [Materialize Console's SQL Shell](/console/), or your preferred SQL
client connected to Materialize, use the [`CREATE
CONNECTION`](/sql/create-connection/#ssh-tunnel) command to create an SSH
tunnel connection:

   ```mzsql
   CREATE CONNECTION ssh_connection TO SSH TUNNEL (
       HOST '<SSH_BASTION_HOST>',
       PORT <SSH_BASTION_PORT>,
       USER '<SSH_BASTION_USER>'
   );

   ```

   - Replace `<SSH_BASTION_HOST>` and `<SSH_BASTION_PORT>` with the public IP
   address and port of the SSH bastion host you created
   [earlier](#b-optional-configure-network-security).

   - Replace `<SSH_BASTION_USER>` with the username for the key pair you
   created for your SSH bastion host.

1. Get Materialize's public keys for the SSH tunnel connection:


   ```mzsql
   SELECT * FROM mz_ssh_tunnel_connections;

   ```

1. Log in to your SSH bastion host and add Materialize's public keys to the
`authorized_keys` file, for example:


   ```mzsql
   echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
   echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys

   ```

1. Back in the SQL client connected to Materialize, validate the SSH tunnel
connection you created using the [`VALIDATE
CONNECTION`](/sql/validate-connection) command:


   ```mzsql
   VALIDATE CONNECTION ssh_connection;

   ```   If no validation error is returned, move to the next step.


1. Use the [`CREATE SECRET`](/sql/create-secret/) command to securely store the
password for the `materialize` PostgreSQL user you created
[earlier](#2-create-a-publication-and-a-replication-user):


   ```mzsql
   CREATE SECRET pgpass AS '<PASSWORD>';

   ```

1.
Use the [`CREATE CONNECTION`](/sql/create-connection/) command to create another connection object, this time with database access and authentication details for Materialize to use:


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

   - Replace `<host>` with your PostgreSQL endpoint.

   - Replace `<database>` with the name of the database containing the tables
   you want to replicate to Materialize.






### 3. Start ingesting data

{{< tabs >}}
{{< tab "Legacy Syntax" >}}
#### Legacy syntax

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source-legacy" %}}
{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="schema-changes" %}}
{{< /tab >}}

{{< tab "New Syntax" >}}
#### New syntax

{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="create-source" %}}
{{% include-example file="examples/ingest_data/postgres/create_source_cloud" example="schema-changes" %}}
{{< /tab >}}
{{< /tabs >}}


### 4. Monitor the ingestion status

<p>Before it starts consuming the replication stream, Materialize takes a snapshot
of the relevant tables in your publication. Until this snapshot is complete,
Materialize won&rsquo;t have the same view of your data as your PostgreSQL database.</p>
<p>In this step, you&rsquo;ll first verify that the source is running and then check the
status of the snapshotting process.</p>
<ol>
<li>
<p>Back in the SQL client connected to Materialize, use the
<a href="/sql/system-catalog/mz_internal/#mz_source_statuses" ><code>mz_source_statuses</code></a>
table to check the overall status of your source:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">WITH</span>
</span></span><span class="line"><span class="cl">  <span class="n">source_ids</span> <span class="k">AS</span>
</span></span><span class="line"><span class="cl">  <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">mz_sources</span> <span class="k">WHERE</span> <span class="k">name</span> <span class="o">=</span> <span class="s1">&#39;mz_source&#39;</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">  <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_source_statuses</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span>
</span></span><span class="line"><span class="cl">      <span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="k">SELECT</span> <span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">        <span class="k">FROM</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_object_dependencies</span>
</span></span><span class="line"><span class="cl">        <span class="k">WHERE</span>
</span></span><span class="line"><span class="cl">          <span class="n">object_id</span> <span class="k">IN</span> <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">UNION</span> <span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span>
</span></span><span class="line"><span class="cl">      <span class="p">)</span>
</span></span><span class="line"><span class="cl">      <span class="k">AS</span> <span class="k">sources</span>
</span></span><span class="line"><span class="cl">    <span class="k">ON</span> <span class="n">mz_source_statuses</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span><span class="p">;</span>
</span></span></code></pre></div><p>For each <code>subsource</code>, make sure the <code>status</code> is <code>running</code>. If you see
<code>stalled</code> or <code>failed</code>, there&rsquo;s likely a configuration issue for you to fix.
Check the <code>error</code> field for details and fix the issue before moving on.
Also, if the <code>status</code> of any subsource is <code>starting</code> for more than a few
minutes, <a href="/support/" >contact our team</a>.</p>
</li>
<li>
<p>Once the source is running, use the <a href="/sql/system-catalog/mz_internal/#mz_source_statistics" ><code>mz_source_statistics</code></a>
table to check the status of the initial snapshot:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">WITH</span>
</span></span><span class="line"><span class="cl">  <span class="n">source_ids</span> <span class="k">AS</span>
</span></span><span class="line"><span class="cl">  <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">mz_sources</span> <span class="k">WHERE</span> <span class="k">name</span> <span class="o">=</span> <span class="s1">&#39;mz_source&#39;</span><span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span> <span class="k">AS</span> <span class="k">id</span><span class="p">,</span> <span class="n">mz_sources</span><span class="mf">.</span><span class="k">name</span><span class="p">,</span> <span class="n">snapshot_committed</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">  <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_source_statistics</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span>
</span></span><span class="line"><span class="cl">      <span class="p">(</span>
</span></span><span class="line"><span class="cl">        <span class="k">SELECT</span> <span class="n">object_id</span><span class="p">,</span> <span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">        <span class="k">FROM</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_object_dependencies</span>
</span></span><span class="line"><span class="cl">        <span class="k">WHERE</span>
</span></span><span class="line"><span class="cl">          <span class="n">object_id</span> <span class="k">IN</span> <span class="p">(</span><span class="k">SELECT</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">        <span class="k">UNION</span> <span class="k">SELECT</span> <span class="k">id</span><span class="p">,</span> <span class="k">id</span> <span class="k">FROM</span> <span class="n">source_ids</span>
</span></span><span class="line"><span class="cl">      <span class="p">)</span>
</span></span><span class="line"><span class="cl">      <span class="k">AS</span> <span class="k">sources</span>
</span></span><span class="line"><span class="cl">    <span class="k">ON</span> <span class="n">mz_source_statistics</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_sources</span> <span class="k">ON</span> <span class="n">mz_sources</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="k">sources</span><span class="mf">.</span><span class="n">referenced_object_id</span><span class="p">;</span>
</span></span></code></pre></div> <p></p>
<pre tabindex="0"><code class="language-nofmt" data-lang="nofmt">object_id | snapshot_committed
----------|------------------
 u144     | t
(1 row)
</code></pre><p>Once <code>snapshot_commited</code> is <code>t</code>, move on to the next step. Snapshotting can
take between a few minutes to several hours, depending on the size of your
dataset and the size of the cluster the source is running in.</p>
</li>
</ol>


### 5. Right-size the cluster

<p>After the snapshotting phase, Materialize starts ingesting change events from
the PostgreSQL replication stream. For this work, Materialize generally
performs well with an <code>100cc</code> replica, so you can resize the cluster
accordingly.</p>
<ol>
<li>
<p>Still in a SQL client connected to Materialize, use the <a href="/sql/alter-cluster/" ><code>ALTER CLUSTER</code></a>
command to downsize the cluster to <code>100cc</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">CLUSTER</span> <span class="n">ingest_postgres</span> <span class="k">SET</span> <span class="p">(</span><span class="k">SIZE</span> <span class="s1">&#39;100cc&#39;</span><span class="p">);</span>
</span></span></code></pre></div><p>Behind the scenes, this command adds a new <code>100cc</code> replica and removes the
<code>50cc</code> replica.</p>
</li>
<li>
<p>Use the <a href="/sql/show-cluster-replicas/" ><code>SHOW CLUSTER REPLICAS</code></a> command to
check the status of the new replica:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SHOW</span> <span class="k">CLUSTER</span> <span class="k">REPLICAS</span> <span class="k">WHERE</span> <span class="k">cluster</span> <span class="o">=</span> <span class="s1">&#39;ingest_postgres&#39;</span><span class="p">;</span>
</span></span></code></pre></div> <p></p>
<pre tabindex="0"><code class="language-nofmt" data-lang="nofmt">     cluster     | replica |  size  | ready
-----------------+---------+--------+-------
 ingest_postgres | r1      | 100cc  | t
(1 row)
</code></pre></li>
<li>
<p>Going forward, you can verify that your new cluster size is sufficient as
follows:</p>
<ol>
<li>
<p>In Materialize, get the replication slot name associated with your
PostgreSQL source from the <a href="/sql/system-catalog/mz_internal/#mz_postgres_sources" ><code>mz_internal.mz_postgres_sources</code></a>
table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span>
</span></span><span class="line"><span class="cl">    <span class="n">d</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">database_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">n</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">schema_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">s</span><span class="mf">.</span><span class="k">name</span> <span class="k">AS</span> <span class="n">source_name</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="n">pgs</span><span class="mf">.</span><span class="n">replication_slot</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span>
</span></span><span class="line"><span class="cl">    <span class="n">mz_sources</span> <span class="k">AS</span> <span class="n">s</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_internal</span><span class="mf">.</span><span class="n">mz_postgres_sources</span> <span class="k">AS</span> <span class="n">pgs</span> <span class="k">ON</span> <span class="n">s</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">pgs</span><span class="mf">.</span><span class="k">id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_schemas</span> <span class="k">AS</span> <span class="n">n</span> <span class="k">ON</span> <span class="n">n</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">s</span><span class="mf">.</span><span class="n">schema_id</span>
</span></span><span class="line"><span class="cl">    <span class="k">JOIN</span> <span class="n">mz_databases</span> <span class="k">AS</span> <span class="n">d</span> <span class="k">ON</span> <span class="n">d</span><span class="mf">.</span><span class="k">id</span> <span class="o">=</span> <span class="n">n</span><span class="mf">.</span><span class="n">database_id</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>In PostgreSQL, check the replication slot lag, using the replication slot
name from the previous step:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-postgres" data-lang="postgres"><span class="line"><span class="cl"><span class="k">SELECT</span>
</span></span><span class="line"><span class="cl">    <span class="n">pg_size_pretty</span><span class="p">(</span><span class="n">pg_current_wal_lsn</span><span class="p">()</span> <span class="o">-</span> <span class="n">confirmed_flush_lsn</span><span class="p">)</span>
</span></span><span class="line"><span class="cl">    <span class="k">AS</span> <span class="n">replication_lag_bytes</span>
</span></span><span class="line"><span class="cl"><span class="k">FROM</span> <span class="n">pg_replication_slots</span>
</span></span><span class="line"><span class="cl"><span class="k">WHERE</span> <span class="n">slot_name</span> <span class="o">=</span> <span class="s1">&#39;&lt;slot_name&gt;&#39;</span><span class="p">;</span>
</span></span></code></pre></div><p>The result of this query is the amount of data your PostgreSQL cluster
must retain in its replication log because of this replication slot.
Typically, this means Materialize has not yet communicated back to
PostgreSQL that it has committed this data. A high value can indicate
that the source has fallen behind and that you might need to scale up
your ingestion cluster.</p>
</li>
</ol>
</li>
</ol>


## D. Explore your data

<p>With Materialize ingesting your PostgreSQL data into durable storage, you can
start exploring the data, computing real-time results that stay up-to-date as
new data arrives, and serving results efficiently.</p>
<ul>
<li>
<p>Explore your data with <a href="/sql/show-sources" ><code>SHOW SOURCES</code></a> and <a href="/sql/select/" ><code>SELECT</code></a>.</p>
</li>
<li>
<p>Compute real-time results in memory with <a href="/sql/create-view/" ><code>CREATE VIEW</code></a>
and <a href="/sql/create-index/" ><code>CREATE INDEX</code></a> or in durable
storage with <a href="/sql/create-materialized-view/" ><code>CREATE MATERIALIZED VIEW</code></a>.</p>
</li>
<li>
<p>Serve results to a PostgreSQL-compatible SQL client or driver with <a href="/sql/select/" ><code>SELECT</code></a>
or <a href="/sql/subscribe/" ><code>SUBSCRIBE</code></a> or to an external message broker with
<a href="/sql/create-sink/" ><code>CREATE SINK</code></a>.</p>
</li>
<li>
<p>Check out the <a href="/integrations/" >tools and integrations</a> supported by
Materialize.</p>
</li>
</ul>


## Considerations

<h3 id="schema-changes">Schema changes</h3>
<p>Materialize supports schema changes in the upstream database as follows:</p>
<h4 id="compatible-schema-changes-legacy-syntax">Compatible schema changes (Legacy syntax)</h4>
<blockquote>
<p><strong>Note:</strong> This section refer to the legacy <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a> that creates subsources as part of the
<code>CREATE SOURCE</code> operation.  To be able to handle the upstream column
additions and drops, see <a href="/sql/create-source/postgres-v2/" ><code>CREATE SOURCE (New Syntax)</code></a> and <a href="/sql/create-table" ><code>CREATE TABLE FROM SOURCE</code></a>.</p>
</blockquote>
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
<h4 id="incompatible-schema-changes">Incompatible schema changes</h4>
<p>All other schema changes to upstream tables will set the corresponding
Materialize tables into an error state, preventing reads from these tables.</p>
<p>To handle <a href="#incompatible-schema-changes" >incompatible schema changes</a>, drop
the affected table <a href="/sql/drop-table/" ><code>DROP TABLE</code></a> , and then, <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> to recreate the table with the
updated schema.</p>
<h3 id="publication-membership">Publication membership</h3>
<p>PostgreSQL&rsquo;s logical replication API does not provide a signal when users
remove tables from publications. Because of this, Materialize relies on
periodic checks to determine if a table has been removed from a publication,
at which time it generates an irrevocable error, preventing any values from
being read from the table.</p>
<p>However, it is possible to remove a table from a publication and then re-add
it before Materialize notices that the table was removed. In this case,
Materialize can no longer provide any consistency guarantees about the data
we present from the table and, unfortunately, is wholly unaware that this
occurred.</p>
<p>To mitigate this issue, if you need to drop and re-add a table to a
publication, ensure that you remove the table/subsource from the source
<em>before</em> re-adding it using the <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> command.</p>
<h3 id="supported-types">Supported types</h3>
<p>Materialize natively supports the following PostgreSQL types (including the
array type for each of the types):</p>
<ul style="column-count: 3">
<li><code>bool</code></li>
<li><code>bpchar</code></li>
<li><code>bytea</code></li>
<li><code>char</code></li>
<li><code>date</code></li>
<li><code>daterange</code></li>
<li><code>float4</code></li>
<li><code>float8</code></li>
<li><code>int2</code></li>
<li><code>int2vector</code></li>
<li><code>int4</code></li>
<li><code>int4range</code></li>
<li><code>int8</code></li>
<li><code>int8range</code></li>
<li><code>interval</code></li>
<li><code>json</code></li>
<li><code>jsonb</code></li>
<li><code>numeric</code></li>
<li><code>numrange</code></li>
<li><code>oid</code></li>
<li><code>text</code></li>
<li><code>time</code></li>
<li><code>timestamp</code></li>
<li><code>timestamptz</code></li>
<li><code>tsrange</code></li>
<li><code>tstzrange</code></li>
<li><code>uuid</code></li>
<li><code>varchar</code></li>
</ul>
<p>Replicating tables that contain <strong>unsupported <a href="/sql/types/" >data types</a></strong> is
possible via the <code>TEXT COLUMNS</code> option. The specified columns will be
treated as <code>text</code>; i.e., will not have the expected PostgreSQL type
features. For example:</p>
<ul>
<li>
<p><a href="https://www.postgresql.org/docs/current/datatype-enum.html" ><code>enum</code></a>: When decoded as <code>text</code>, the implicit ordering of the original
PostgreSQL <code>enum</code> type is not preserved; instead, Materialize will sort values
as <code>text</code>.</p>
</li>
<li>
<p><a href="https://www.postgresql.org/docs/current/datatype-money.html" ><code>money</code></a>: When decoded as <code>text</code>, resulting <code>text</code> value cannot be cast
back to <code>numeric</code>, since PostgreSQL adds typical currency formatting to the
output.</p>
</li>
</ul>
<h3 id="truncation">Truncation</h3>
<p>Avoid truncating upstream tables that are being replicated into Materialize.
If a replicated upstream table is truncated, the corresponding
subsource(s)/table(s) in Materialize becomes inaccessible and will not
produce any data until it is recreated.</p>
<p>Instead of truncating, use an unqualified <code>DELETE</code> to remove all rows from
the upstream table:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DELETE</span> <span class="k">FROM</span> <span class="n">t</span><span class="p">;</span>
</span></span></code></pre></div>
<h3 id="inherited-tables">Inherited tables</h3>
<p>When using <a href="https://www.postgresql.org/docs/current/tutorial-inheritance.html" >PostgreSQL table inheritance</a>,
PostgreSQL serves data from <code>SELECT</code>s as if the inheriting tables&rsquo; data is
also present in the inherited table. However, both PostgreSQL&rsquo;s logical
replication and <code>COPY</code> only present data written to the tables themselves,
i.e. the inheriting data is <em>not</em> treated as part of the inherited table.</p>
<p>PostgreSQL sources use logical replication and <code>COPY</code> to ingest table data,
so inheriting tables&rsquo; data will only be ingested as part of the inheriting
table, i.e. in Materialize, the data will not be returned when serving
<code>SELECT</code>s from the inherited table.</p>
<ul>
<li>
<p>If using legacy syntax <a href="/sql/create-source/postgres/" ><code>CREATE SOURCE ... FOR ...</code></a>:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>ADD SUBSOURCE</code> and
create a new view (materialized or non-) that unions the new table.</p>
</li>
<li>
<p>If using new <a href="/sql/create-table/" ><code>CREATE TABLE FROM SOURCE</code></a> syntax:</p>
<p>You can mimic PostgreSQL’s <code>SELECT</code> behavior with inherited tables by
creating a materialized view that unions data from the inherited and
inheriting tables (using <code>UNION ALL</code>). However, if new tables inherit from
the table, data from the inheriting tables will not be available in the
view. You will need to add the inheriting tables via <code>CREATE TABLE .. FROM SOURCE</code> and create a new view (materialized or non-) that unions the new
table.</p>
</li>
</ul>
<h3 id="replication-slots">Replication slots</h3>
<p>Each source ingests the raw replication stream data for all tables in the
specified publication using <strong>a single</strong> replication slot. To manage
replication slots:</p>
<ul>
<li>
<p>For PostgreSQL 13+, set a reasonable value
for <a href="https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE" ><code>max_slot_wal_keep_size</code></a>
to limit the amount of storage used by replication slots.</p>
</li>
<li>
<p>If you stop using Materialize, or if either the Materialize instance or
the PostgreSQL instance crash, delete any replication slots. You can query
the <code>mz_internal.mz_postgres_sources</code> table to look up the name of the
replication slot created for each source.</p>
</li>
<li>
<p>If you delete all objects that depend on a source without also dropping
the source, the upstream replication slot remains and will continue to
accumulate data so that the source can resume in the future. To avoid
unbounded disk space usage, make sure to use <a href="/sql/drop-source/" ><code>DROP SOURCE</code></a> or manually delete the replication slot.</p>
</li>
</ul>
<h3 id="modifying-an-existing-source">Modifying an existing source</h3>
<p>When you add a new subsource to an existing source (<a href="/sql/alter-source/" ><code>ALTER SOURCE ... ADD SUBSOURCE ...</code></a>), Materialize starts the snapshotting
process for the new subsource. During this snapshotting, the data ingestion for
the existing subsources for the same source is temporarily blocked. As such, if
possible, you can resize the cluster to speed up the snapshotting process and
once the process finishes, resize the cluster for steady-state.
</p>
