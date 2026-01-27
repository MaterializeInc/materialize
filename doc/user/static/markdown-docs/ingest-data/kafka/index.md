# Kafka

Connecting Materialize to a Kafka source.



Materialize provides native connector for Kafka message broker. To ingest data
from Kafka, you need to

1. Create a connection that specifies access and authentication parameters.
2. Create a source that specifies the format of the data you want to ingest.

## Supported versions

The Kafka source supports **Kafka 3.2+** and is compatible with most common Kafka hosted services, including all supported versions of the [Confluent Platform](https://docs.confluent.io/platform/current/installation/versions-interoperability.html).

## Formats

Materialize can decode incoming bytes of data from several formats:

- Avro
- JSON
- CSV
- Protobuf
- Plain text
- Raw bytes

## Envelopes

What Materialize actually does with the data it receives depends on the
"envelope" your data provides:

Envelope | Action
---------|-------
**Append-only** | Inserts all received data; does not support updates or deletes.
**Debezium** | Treats data as wrapped in a "diff envelope" that indicates whether the record is an insertion, deletion, or update. The Debezium envelope is only supported by sources published to Kafka by Debezium.<br/><br/>For more information, see [`CREATE SOURCE`: Kafka - Debezium envelope](/sql/create-source/kafka/#debezium-envelope).
**Upsert** | Treats data as having a key and a value. New records with non-null value that have the same key as a preexisting record in the dataflow will replace the preexisting record. New records with null value that have the same key as preexisting record will cause the preexisting record to be deleted. <br/><br/>For more information, see [`CREATE SOURCE`: Kafka - Upsert envelope](/sql/create-source/kafka/#upsert-envelope).


## Integration guides

- [Amazon MSK](/ingest-data/kafka/amazon-msk/)
- [Confluent Cloud](/ingest-data/kafka/confluent-cloud/)
- [Self-hosted Kafka](/ingest-data/kafka/kafka-self-hosted/)
- [Warpstream](/ingest-data/kafka/warpstream/)

## See also

- [Redpanda Cloud](/ingest-data/redpanda/redpanda-cloud/)
- [Redpanda Self-hosted](/ingest-data/redpanda/)



---

## Amazon Managed Streaming for Apache Kafka (Amazon MSK)


[//]: # "TODO(morsapaes) The Kafka guides need to be rewritten for consistency
with the PostgreSQL ones. We should add information about using AWS IAM
authentication then."

This guide goes through the required steps to connect Materialize to an Amazon
MSK cluster.

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).


## Before you begin

Before you begin, you must have:

- An Amazon MSK cluster running on AWS.
- A client machine that can interact with your cluster.

## Creating a connection



**Cloud:**

There are various ways to configure your Kafka network to allow Materialize to
connect:

- **Allow Materialize IPs:** If your Kafka cluster is publicly accessible, you
    can configure your firewall to allow connections from a set of static
    Materialize IP addresses.

- **Use AWS PrivateLink**: If your Kafka cluster is running in a private network, you
    can use [AWS PrivateLink](/ingest-data/network-security/privatelink/) to
    connect Materialize to the cluster. For details, see [AWS PrivateLink](/ingest-data/network-security/privatelink/).

- **Use an SSH tunnel:** If your Kafka cluster is running in a private network,
    you can use an SSH tunnel to connect Materialize to the cluster.



**PrivateLink:**

> **Note:** Materialize provides a Terraform module that automates the creation and
> configuration of AWS resources for a PrivateLink connection. For more details,
> see the Terraform module repositories for [Amazon MSK](https://github.com/MaterializeInc/terraform-aws-msk-privatelink)
> and [self-managed Kafka clusters](https://github.com/MaterializeInc/terraform-aws-kafka-privatelink).


This section covers how to create AWS PrivateLink connections
and retrieve the AWS principal needed to configure the AWS PrivateLink service.

1. Create target groups. Create a dedicated [target
   group](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html)
   **for each broker** with the following details:

    a. Target type as **IP address**.

    b. Protocol as **TCP**.

    c. Port as **9092**, or the port that you are using in case it is not 9092 (e.g. 9094 for TLS or 9096 for SASL).

    d. Make sure that the target group is in the same VPC as the Kafka cluster.

    e. Click next, and register the respective Kafka broker to each target group using its IP address.

1. Create a [Network Load
    Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html)
    that is **enabled for the same subnets** that the Kafka brokers are in.

1. Create a [TCP
   listener](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-listener.html)
   for every Kafka broker that forwards to the corresponding target group you
   created (e.g. `b-1`, `b-2`, `b-3`).

    The listener port needs to be unique, and will be used later on in the `CREATE CONNECTION` statement.

    For example, you can create a listener for:

    a. Port `9001` → broker `b-1...`.

    b. Port `9002` → broker `b-2...`.

    c. Port `9003` → broker `b-3...`.

1. Verify security groups and health checks. Once the TCP listeners have been
   created, make sure that the [health
   checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html)
   for each target group are passing and that the targets are reported as
   healthy.

    If you have set up a security group for your Kafka cluster, you must ensure that it allows traffic on both the listener port and the health check port.

    **Remarks**:

    a. Network Load Balancers do not have associated security groups. Therefore, the security groups for your targets must use IP addresses to allow traffic.

    b. You can't use the security groups for the clients as a source in the security groups for the targets. Therefore, the security groups for your targets must use the IP addresses of the clients to allow traffic. For more details, check the [AWS documentation](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html).

1. Create a VPC [endpoint service](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html) and associate it with the **Network Load Balancer** that you’ve just created.

    Note the **service name** that is generated for the endpoint service.

1. Create an AWS PrivateLink connection. In Materialize, create an [AWS
    PrivateLink connection](/sql/create-connection/#aws-privatelink) that
    references the endpoint service that you created in the previous step.

    ↕️ **In-region connections**

    To connect to an AWS PrivateLink endpoint service in the **same region** as your
    Materialize environment:

      ```mzsql
      CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
        SERVICE NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
        AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az4')
      );
      ```

    - Replace the `SERVICE NAME` value with the service name you noted earlier.

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

    - Replace the `SERVICE NAME` value with the service name you noted earlier.

    - The service name region refers to where the endpoint service was created.
      You **do not need** to specify `AVAILABILITY ZONES` manually — these will
      be optimally auto-assigned when none are provided.

    - For Kafka connections, it is required for [cross-zone load balancing](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html) to be
      enabled on the VPC endpoint service's NLB when using cross-region Privatelink.

1. Configure the AWS PrivateLink service. Retrieve the AWS principal for the AWS
    PrivateLink connection you just created:

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

1. If your AWS PrivateLink service is configured to require acceptance of connection requests, you must manually approve the connection request from Materialize after executing `CREATE CONNECTION`. For more details, check the [AWS PrivateLink documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).

    **Note:** It might take some time for the endpoint service connection to show up, so you would need to wait for the endpoint service connection to be ready before you create a source.

1. Validate the AWS PrivateLink connection you created using the [`VALIDATE CONNECTION`](/sql/validate-connection) command:

   ```mzsql
   VALIDATE CONNECTION privatelink_svc;
   ```

   If no validation error is returned, move to the next step.

1. Create a source connection

   In Materialize, create a source connection that uses the AWS PrivateLink
connection you just configured:

   ```mzsql
   CREATE CONNECTION kafka_connection TO KAFKA (
       BROKERS (
           -- The port **must exactly match** the port assigned to the broker in
           -- the TCP listerner of the NLB.
           'b-1.hostname-1:9096' USING AWS PRIVATELINK privatelink_svc (PORT  9001, AVAILABILITY ZONE 'use1-az2'),
           'b-2.hostname-2:9096' USING AWS PRIVATELINK privatelink_svc (PORT  9002, AVAILABILITY ZONE 'use1-az1'),
           'b-3.hostname-3:9096' USING AWS PRIVATELINK privatelink_svc (PORT  9003, AVAILABILITY ZONE 'use1-az4')
       ),
       -- Authentication details
       -- Depending on the authentication method the Kafka cluster is using
       SASL MECHANISMS = 'SCRAM-SHA-512',
       SASL USERNAME = 'foo',
       SASL PASSWORD = SECRET kafka_password
   );
   ```

   If you run into connectivity issues during source creation, make sure that:

   * The `(PORT <port_number>)` value **exactly matches** the port assigned to
      the corresponding broker in the **TCP listener** of the Network Load
      Balancer. Misalignment between ports and broker addresses is the most
      common cause for connectivity issues.

   * For **in-region connections**, the correct availability zone is specified
      for each broker.




**SSH Tunnel:**

Materialize can connect to a Kafka broker, a Confluent Schema Registry server, a
PostgreSQL database, or a MySQL database through an SSH tunnel connection. In
this guide, you will create an SSH tunnel connection, configure your
Materialize authentication settings, and create a source connection.

Before you begin, make sure you have access to a bastion host. You will need:

* The bastion host IP address and port number
* The bastion host username

1. Create an SSH tunnel connection. In Materialize, create an [SSH tunnel
   connection](/sql/create-connection/#ssh-tunnel) to the bastion host:

    ```mzsql
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        USER '<SSH_BASTION_USER>',
        PORT <SSH_BASTION_PORT>
    );
    ```

1. Configure the SSH bastion host. The bastion host needs a **public key** to
connect to the Materialize tunnel you created in the previous step. Materialize
stores public keys for SSH tunnels in the system catalog. Query
[`mz_ssh_tunnel_connections`](/sql/system-catalog/mz_catalog/#mz_ssh_tunnel_connections)
to retrieve the public keys for the SSH tunnel connection you just created:

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

    ```
    | id    | public_key_1                          | public_key_2                          |
    |-------|---------------------------------------|---------------------------------------|
    | u75   | ssh-ed25519 AAAA...76RH materialize   | ssh-ed25519 AAAA...hLYV materialize   |
    ```


    > Materialize provides two public keys to allow you to rotate keys without
    connection downtime. Review the [`ALTER CONNECTION`](/sql/alter-connection) documentation for
    more information on how to rotate your keys.

1. Log in to your SSH bastion server and add each key to the bastion `authorized_keys` file:

    ```bash
    # Command for Linux
    echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
    echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys
    ```

1. Configure your internal firewall to allow the SSH bastion host to connect to your Kafka cluster or PostgreSQL instance.

    If you are using a cloud provider like AWS or GCP, update the security group or firewall rules for your PostgreSQL instance or Kafka brokers.

    Allow incoming traffic from the SSH bastion host IP address on the necessary ports.

    For example, use port `5432` for PostgreSQL and ports `9092`, `9094`, and `9096` for Kafka.

    Test the connection from the bastion host to the Kafka cluster or PostgreSQL instance.

    ```bash
    telnet <KAFKA_BROKER_HOST> <KAFKA_BROKER_PORT>
    telnet <POSTGRES_HOST> <POSTGRES_PORT>
    ```

    If the command hangs, double-check your security group and firewall settings. If the connection is successful, you can proceed to the next step.

1. Verify the SSH tunnel connection from your source to your bastion host:

    ```bash
    # Command for Linux
    ssh -L 9092:kafka-broker:9092 <SSH_BASTION_USER>@<SSH_BASTION_HOST>
    ```

    Verify that you can connect to the Kafka broker or PostgreSQL instance via the SSH tunnel:

    ```bash
    telnet localhost 9092
    ```

    If you are unable to connect using the `telnet` command, enable `AllowTcpForwarding` and `PermitTunnel` on your bastion host SSH configuration file.

    On your SSH bastion host, open the SSH config file (usually located at `/etc/ssh/sshd_config`) using a text editor:

    ```bash
    sudo nano /etc/ssh/sshd_config
    ```

    Add or uncomment the following lines:

    ```bash
    AllowTcpForwarding yes
    PermitTunnel yes
    ```

    Save the changes and restart the SSH service:

    ```bash
    sudo systemctl restart sshd
    ```

1. Retrieve the static egress IPs from Materialize and configure the firewall rules (e.g. AWS Security Groups) for your bastion host to allow SSH traffic for those IP addresses only.

    ```mzsql
    SELECT * FROM mz_catalog.mz_egress_ips;
    ```

    ```
    XXX.140.90.33
    XXX.198.159.213
    XXX.100.27.23
    ```

1. To confirm that the SSH tunnel connection is correctly configured, use the
   [`VALIDATE CONNECTION`](/sql/validate-connection) command:

   ```mzsql
   VALIDATE CONNECTION ssh_connection;
   ```

   If no validation errors are returned, the connection can be used to create a
   source connection.


1. In Materialize, create a source connection that uses the SSH tunnel
   connection you configured in the previous section:

   ```mzsql
   CREATE CONNECTION kafka_connection TO KAFKA (
     BROKER 'broker1:9092',
     SSH TUNNEL ssh_connection
   );
   ```



**Public cluster:**

<p>This section goes through the required steps to connect Materialize to an Amazon MSK cluster, including some of the more complicated bits around configuring security settings in Amazon MSK.</p>
<p>If you already have an Amazon MSK cluster, you can skip step 1 and directly move
on to <strong>Make the cluster public and enable SASL</strong> step. You can also skip steps
3 and 4 if you already have Apache Kafka installed and running, and have created
a topic that you want to create a source for.</p>
<p>The process to connect Materialize to Amazon MSK consists of the following steps:</p>
<ol>
<li>
<p>Create an Amazon MSK cluster. If you already have an Amazon MSK cluster set
up, then you can skip this step.</p>
<p>a. Sign in to the AWS Management Console and open the <a href="https://console.aws.amazon.com/msk/" >Amazon MSK console</a></p>
<p>b. Choose <strong>Create cluster</strong></p>
<p>c. Enter a cluster name, and leave all other settings unchanged</p>
<p>d. From the table under <strong>All cluster settings</strong>, copy the values of the following settings and save them because you need them later in this tutorial: <strong>VPC</strong>, <strong>Subnets</strong>, <strong>Security groups associated with VPC</strong></p>
<p>e. Choose <strong>Create cluster</strong></p>
<p><strong>Note:</strong> This creation can take about 15 minutes.</p>
</li>
<li>
<p>Make the cluster public and enable SASL.</p>
<h5 id="turn-on-sasl">Turn on SASL</h5>
<p>a. Navigate to the <a href="https://console.aws.amazon.com/msk/" >Amazon MSK console</a></p>
<p>b. Choose the MSK cluster you just created in Step 1</p>
<p>c. Click on the <strong>Properties</strong> tab</p>
<p>d. In the <strong>Security settings</strong> section, choose <strong>Edit</strong></p>
<p>e. Check the checkbox next to <strong>SASL/SCRAM authentication</strong></p>
<p>f. Click <strong>Save changes</strong></p>
<p>You can find more details about updating a cluster&rsquo;s security configurations <a href="https://docs.aws.amazon.com/msk/latest/developerguide/msk-update-security.html" >here</a>.</p>
<h5 id="create-a-symmetric-key">Create a symmetric key</h5>
<p>a. Now go to the <a href="https://console.aws.amazon.com/kms" >AWS Key Management Service (AWS KMS) console</a></p>
<p>b. Click <strong>Create Key</strong></p>
<p>c. Choose <strong>Symmetric</strong> and click <strong>Next</strong></p>
<p>d. Give the key and <strong>Alias</strong> and click <strong>Next</strong></p>
<p>e. Under Administrative permissions, check the checkbox next to the <strong>AWSServiceRoleForKafka</strong> and click <strong>Next</strong></p>
<p>f. Under Key usage permissions, again check the checkbox next to the <strong>AWSServiceRoleForKafka</strong> and click <strong>Next</strong></p>
<p>g. Click on <strong>Create secret</strong></p>
<p>h. Review the details and click <strong>Finish</strong></p>
<p>You can find more details about creating a symmetric key <a href="https://docs.aws.amazon.com/kms/latest/developerguide/create-keys.html#create-symmetric-cmk" >here</a>.</p>
<h5 id="store-a-new-secret">Store a new Secret</h5>
<p>a. Go to the <a href="https://console.aws.amazon.com/secretsmanager/" >AWS Secrets Manager console</a></p>
<p>b. Click <strong>Store a new secret</strong></p>
<p>c. Choose <strong>Other type of secret</strong> (e.g. API key) for the secret type</p>
<p>d. Under <strong>Key/value pairs</strong> click on <strong>Plaintext</strong></p>
<p>e. Paste the following in the space below it and replace <code>&lt;your-username&gt;</code> and <code>&lt;your-password&gt;</code> with the username and password you want to set for the cluster</p>
<pre tabindex="0"><code>  {
    &#34;username&#34;: &#34;&lt;your-username&gt;&#34;,
    &#34;password&#34;: &#34;&lt;your-password&gt;&#34;
  }
</code></pre><p>f. On the next page, give a <strong>Secret name</strong> that starts with <code>AmazonMSK_</code></p>
<p>g. Under <strong>Encryption Key</strong>, select the symmetric key you just created in the previous sub-section from the dropdown</p>
<p>h. Go forward to the next steps and finish creating the secret. Once created, record the ARN (Amazon Resource Name) value for your secret</p>
<p>You can find more details about creating a secret using AWS Secrets Manager <a href="https://docs.aws.amazon.com/msk/latest/developerguide/msk-password.html" >here</a>.</p>
<h5 id="associate-secret-with-msk-cluster">Associate secret with MSK cluster</h5>
<p>a. Navigate back to the <a href="https://console.aws.amazon.com/msk/" >Amazon MSK console</a> and click on the cluster you created in Step 1</p>
<p>b. Click on the <strong>Properties</strong> tab</p>
<p>c. In the <strong>Security settings</strong> section, under <strong>SASL/SCRAM authentication</strong>, click on <strong>Associate secrets</strong></p>
<p>d. Paste the ARN you recorded in the previous subsection and click <strong>Associate secrets</strong></p>
<h5 id="create-the-clusters-configuration">Create the cluster&rsquo;s configuration</h5>
<p>a. Go to the <a href="https://console.aws.amazon.com/cloudshell/" >Amazon CloudShell console</a></p>
<p>b. Create a file (eg. <em>msk-config.txt</em>) with the following line</p>
<pre tabindex="0"><code>  allow.everyone.if.no.acl.found = false
</code></pre><p>c. Run the following AWS CLI command, replacing <code>&lt;config-file-path&gt;</code> with the path to the file where you saved your configuration in the previous step</p>
<pre tabindex="0"><code>  aws kafka create-configuration --name &#34;MakePublic&#34; \
  --description &#34;Set allow.everyone.if.no.acl.found = false&#34; \
  --kafka-versions &#34;2.6.2&#34; \
  --server-properties fileb://&lt;config-file-path&gt;/msk-config.txt
</code></pre><p>You can find more information about making your cluster public <a href="https://docs.aws.amazon.com/msk/latest/developerguide/public-access.html" >here</a>.</p>
</li>
<li>
<p>If you already have a client machine set up that can interact with your
cluster, then you can skip this step.</p>
<p>If not, you can create an EC2 client machine and then add the security group of the client to the inbound rules of the cluster&rsquo;s security group from the VPC console. You can find more details about how to do that <a href="https://docs.aws.amazon.com/msk/latest/developerguide/create-client-machine.html" >here</a>.</p>
</li>
<li>
<p>Install Apache Kafka and create a topic. To start using Materialize with
Apache Kafka, you need to create a Materialize source over an Apache Kafka
topic. If you already have Apache Kafka installed and a topic created, you
can skip this step.</p>
<p>Otherwise, you can install Apache Kafka on your client machine from the previous step and create a topic. You can find more information about how to do that <a href="https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html" >here</a>.</p>
</li>
<li>
<p>Create ACLs. As <code>allow.everyone.if.no.acl.found</code> is set to <code>false</code>, you must
create ACLs for the cluster and topics configured in the previous step to
set appropriate access permissions. For more information, see the <a href="https://docs.aws.amazon.com/msk/latest/developerguide/msk-acls.html" >Amazon
MSK</a>
documentation.</p>
</li>
<li>
<p>Create a connection in Materialize.</p>
<p>a. Open the <a href="https://console.aws.amazon.com/msk/" >Amazon MSK console</a> and select your cluster</p>
<p>b. Click on <strong>View client information</strong></p>
<p>c. Copy the url under <strong>Private endpoint</strong> and against <strong>SASL/SCRAM</strong>. This will be your <code>&lt;broker-url&gt;</code> going forward.</p>
<p>d. Connect to Materialize using the <a href="/console/" >SQL Shell</a>,
or your preferred SQL client.</p>
<p>e. Create a connection using the command below. The broker URL is what you copied in step c of this subsection. The <code>&lt;topic-name&gt;</code> is the name of the topic you created in Step 4. The <code>&lt;your-username&gt;</code> and <code>&lt;your-password&gt;</code> is from <em>Store a new secret</em> under Step 2.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">SECRET</span> <span class="n">msk_password</span> <span class="k">AS</span> <span class="s1">&#39;&lt;your-password&gt;&#39;</span><span class="p">;</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">CONNECTION</span> <span class="n">kafka_connection</span> <span class="k">TO</span> <span class="k">KAFKA</span> <span class="p">(</span>
</span></span><span class="line"><span class="cl">    <span class="k">BROKER</span> <span class="s1">&#39;&lt;broker-url&gt;&#39;</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="k">SASL</span> <span class="k">MECHANISMS</span> <span class="o">=</span> <span class="s1">&#39;SCRAM-SHA-512&#39;</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="k">SASL</span> <span class="k">USERNAME</span> <span class="o">=</span> <span class="s1">&#39;&lt;your-username&gt;&#39;</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="k">SASL</span> <span class="k">PASSWORD</span> <span class="o">=</span> <span class="k">SECRET</span> <span class="n">msk_password</span>
</span></span><span class="line"><span class="cl">  <span class="p">);</span>
</span></span></code></pre></div></li>
</ol>





**Self-Managed:**
Configure your Kafka network to allow Materialize to connect:

- **Use an SSH tunnel**: If your Kafka cluster is running in a private network, you can use an SSH tunnel to connect Materialize to the cluster.

- **Allow Materialize IPs**: If your Kafka cluster is publicly accessible, you can configure your firewall to allow connections from a set of static Materialize IP addresses.


**SSH Tunnel:**

Materialize can connect to a Kafka broker, a Confluent Schema Registry server, a
PostgreSQL database, or a MySQL database through an SSH tunnel connection. In
this guide, you will create an SSH tunnel connection, configure your
Materialize authentication settings, and create a source connection.

Before you begin, make sure you have access to a bastion host. You will need:

* The bastion host IP address and port number
* The bastion host username

1. Create an SSH tunnel connection. In Materialize, create an [SSH tunnel
   connection](/sql/create-connection/#ssh-tunnel) to the bastion host:

    ```mzsql
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        USER '<SSH_BASTION_USER>',
        PORT <SSH_BASTION_PORT>
    );
    ```

1. Configure the SSH bastion host. The bastion host needs a **public key** to
connect to the Materialize tunnel you created in the previous step. Materialize
stores public keys for SSH tunnels in the system catalog. Query
[`mz_ssh_tunnel_connections`](/sql/system-catalog/mz_catalog/#mz_ssh_tunnel_connections)
to retrieve the public keys for the SSH tunnel connection you just created:

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

    ```
    | id    | public_key_1                          | public_key_2                          |
    |-------|---------------------------------------|---------------------------------------|
    | u75   | ssh-ed25519 AAAA...76RH materialize   | ssh-ed25519 AAAA...hLYV materialize   |
    ```


    > Materialize provides two public keys to allow you to rotate keys without
    connection downtime. Review the [`ALTER CONNECTION`](/sql/alter-connection) documentation for
    more information on how to rotate your keys.

1. Log in to your SSH bastion server and add each key to the bastion `authorized_keys` file:

    ```bash
    # Command for Linux
    echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
    echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys
    ```

1. Configure your internal firewall to allow the SSH bastion host to connect to your Kafka cluster or PostgreSQL instance.

    If you are using a cloud provider like AWS or GCP, update the security group or firewall rules for your PostgreSQL instance or Kafka brokers.

    Allow incoming traffic from the SSH bastion host IP address on the necessary ports.

    For example, use port `5432` for PostgreSQL and ports `9092`, `9094`, and `9096` for Kafka.

    Test the connection from the bastion host to the Kafka cluster or PostgreSQL instance.

    ```bash
    telnet <KAFKA_BROKER_HOST> <KAFKA_BROKER_PORT>
    telnet <POSTGRES_HOST> <POSTGRES_PORT>
    ```

    If the command hangs, double-check your security group and firewall settings. If the connection is successful, you can proceed to the next step.

1. Verify the SSH tunnel connection from your source to your bastion host:

    ```bash
    # Command for Linux
    ssh -L 9092:kafka-broker:9092 <SSH_BASTION_USER>@<SSH_BASTION_HOST>
    ```

    Verify that you can connect to the Kafka broker or PostgreSQL instance via the SSH tunnel:

    ```bash
    telnet localhost 9092
    ```

    If you are unable to connect using the `telnet` command, enable `AllowTcpForwarding` and `PermitTunnel` on your bastion host SSH configuration file.

    On your SSH bastion host, open the SSH config file (usually located at `/etc/ssh/sshd_config`) using a text editor:

    ```bash
    sudo nano /etc/ssh/sshd_config
    ```

    Add or uncomment the following lines:

    ```bash
    AllowTcpForwarding yes
    PermitTunnel yes
    ```

    Save the changes and restart the SSH service:

    ```bash
    sudo systemctl restart sshd
    ```

1. Ensure materialize cluster pods have network access to your SSH bastion host.

1. Validate the SSH tunnel connection

   To confirm that the SSH tunnel connection is correctly configured, use the [`VALIDATE CONNECTION`](/sql/validate-connection) command:

    ```mzsql
    VALIDATE CONNECTION ssh_connection;
    ```

    If no validation errors are returned, the connection can be used to create a source connection.


1. In Materialize, create a source connection that uses the SSH tunnel
   connection you configured in the previous section:

   ```mzsql
   CREATE CONNECTION kafka_connection TO KAFKA (
     BROKER 'broker1:9092',
     SSH TUNNEL ssh_connection
   );
   ```



**Public cluster:**

<p>This section goes through the required steps to connect Materialize to an Amazon MSK cluster, including some of the more complicated bits around configuring security settings in Amazon MSK.</p>
<p>If you already have an Amazon MSK cluster, you can skip step 1 and directly move
on to <strong>Make the cluster public and enable SASL</strong> step. You can also skip steps
3 and 4 if you already have Apache Kafka installed and running, and have created
a topic that you want to create a source for.</p>
<p>The process to connect Materialize to Amazon MSK consists of the following steps:</p>
<ol>
<li>
<p>Create an Amazon MSK cluster. If you already have an Amazon MSK cluster set
up, then you can skip this step.</p>
<p>a. Sign in to the AWS Management Console and open the <a href="https://console.aws.amazon.com/msk/" >Amazon MSK console</a></p>
<p>b. Choose <strong>Create cluster</strong></p>
<p>c. Enter a cluster name, and leave all other settings unchanged</p>
<p>d. From the table under <strong>All cluster settings</strong>, copy the values of the following settings and save them because you need them later in this tutorial: <strong>VPC</strong>, <strong>Subnets</strong>, <strong>Security groups associated with VPC</strong></p>
<p>e. Choose <strong>Create cluster</strong></p>
<p><strong>Note:</strong> This creation can take about 15 minutes.</p>
</li>
<li>
<p>Make the cluster public and enable SASL.</p>
<h5 id="turn-on-sasl">Turn on SASL</h5>
<p>a. Navigate to the <a href="https://console.aws.amazon.com/msk/" >Amazon MSK console</a></p>
<p>b. Choose the MSK cluster you just created in Step 1</p>
<p>c. Click on the <strong>Properties</strong> tab</p>
<p>d. In the <strong>Security settings</strong> section, choose <strong>Edit</strong></p>
<p>e. Check the checkbox next to <strong>SASL/SCRAM authentication</strong></p>
<p>f. Click <strong>Save changes</strong></p>
<p>You can find more details about updating a cluster&rsquo;s security configurations <a href="https://docs.aws.amazon.com/msk/latest/developerguide/msk-update-security.html" >here</a>.</p>
<h5 id="create-a-symmetric-key">Create a symmetric key</h5>
<p>a. Now go to the <a href="https://console.aws.amazon.com/kms" >AWS Key Management Service (AWS KMS) console</a></p>
<p>b. Click <strong>Create Key</strong></p>
<p>c. Choose <strong>Symmetric</strong> and click <strong>Next</strong></p>
<p>d. Give the key and <strong>Alias</strong> and click <strong>Next</strong></p>
<p>e. Under Administrative permissions, check the checkbox next to the <strong>AWSServiceRoleForKafka</strong> and click <strong>Next</strong></p>
<p>f. Under Key usage permissions, again check the checkbox next to the <strong>AWSServiceRoleForKafka</strong> and click <strong>Next</strong></p>
<p>g. Click on <strong>Create secret</strong></p>
<p>h. Review the details and click <strong>Finish</strong></p>
<p>You can find more details about creating a symmetric key <a href="https://docs.aws.amazon.com/kms/latest/developerguide/create-keys.html#create-symmetric-cmk" >here</a>.</p>
<h5 id="store-a-new-secret">Store a new Secret</h5>
<p>a. Go to the <a href="https://console.aws.amazon.com/secretsmanager/" >AWS Secrets Manager console</a></p>
<p>b. Click <strong>Store a new secret</strong></p>
<p>c. Choose <strong>Other type of secret</strong> (e.g. API key) for the secret type</p>
<p>d. Under <strong>Key/value pairs</strong> click on <strong>Plaintext</strong></p>
<p>e. Paste the following in the space below it and replace <code>&lt;your-username&gt;</code> and <code>&lt;your-password&gt;</code> with the username and password you want to set for the cluster</p>
<pre tabindex="0"><code>  {
    &#34;username&#34;: &#34;&lt;your-username&gt;&#34;,
    &#34;password&#34;: &#34;&lt;your-password&gt;&#34;
  }
</code></pre><p>f. On the next page, give a <strong>Secret name</strong> that starts with <code>AmazonMSK_</code></p>
<p>g. Under <strong>Encryption Key</strong>, select the symmetric key you just created in the previous sub-section from the dropdown</p>
<p>h. Go forward to the next steps and finish creating the secret. Once created, record the ARN (Amazon Resource Name) value for your secret</p>
<p>You can find more details about creating a secret using AWS Secrets Manager <a href="https://docs.aws.amazon.com/msk/latest/developerguide/msk-password.html" >here</a>.</p>
<h5 id="associate-secret-with-msk-cluster">Associate secret with MSK cluster</h5>
<p>a. Navigate back to the <a href="https://console.aws.amazon.com/msk/" >Amazon MSK console</a> and click on the cluster you created in Step 1</p>
<p>b. Click on the <strong>Properties</strong> tab</p>
<p>c. In the <strong>Security settings</strong> section, under <strong>SASL/SCRAM authentication</strong>, click on <strong>Associate secrets</strong></p>
<p>d. Paste the ARN you recorded in the previous subsection and click <strong>Associate secrets</strong></p>
<h5 id="create-the-clusters-configuration">Create the cluster&rsquo;s configuration</h5>
<p>a. Go to the <a href="https://console.aws.amazon.com/cloudshell/" >Amazon CloudShell console</a></p>
<p>b. Create a file (eg. <em>msk-config.txt</em>) with the following line</p>
<pre tabindex="0"><code>  allow.everyone.if.no.acl.found = false
</code></pre><p>c. Run the following AWS CLI command, replacing <code>&lt;config-file-path&gt;</code> with the path to the file where you saved your configuration in the previous step</p>
<pre tabindex="0"><code>  aws kafka create-configuration --name &#34;MakePublic&#34; \
  --description &#34;Set allow.everyone.if.no.acl.found = false&#34; \
  --kafka-versions &#34;2.6.2&#34; \
  --server-properties fileb://&lt;config-file-path&gt;/msk-config.txt
</code></pre><p>You can find more information about making your cluster public <a href="https://docs.aws.amazon.com/msk/latest/developerguide/public-access.html" >here</a>.</p>
</li>
<li>
<p>If you already have a client machine set up that can interact with your
cluster, then you can skip this step.</p>
<p>If not, you can create an EC2 client machine and then add the security group of the client to the inbound rules of the cluster&rsquo;s security group from the VPC console. You can find more details about how to do that <a href="https://docs.aws.amazon.com/msk/latest/developerguide/create-client-machine.html" >here</a>.</p>
</li>
<li>
<p>Install Apache Kafka and create a topic. To start using Materialize with
Apache Kafka, you need to create a Materialize source over an Apache Kafka
topic. If you already have Apache Kafka installed and a topic created, you
can skip this step.</p>
<p>Otherwise, you can install Apache Kafka on your client machine from the previous step and create a topic. You can find more information about how to do that <a href="https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html" >here</a>.</p>
</li>
<li>
<p>Create ACLs. As <code>allow.everyone.if.no.acl.found</code> is set to <code>false</code>, you must
create ACLs for the cluster and topics configured in the previous step to
set appropriate access permissions. For more information, see the <a href="https://docs.aws.amazon.com/msk/latest/developerguide/msk-acls.html" >Amazon
MSK</a>
documentation.</p>
</li>
<li>
<p>Create a connection in Materialize.</p>
<p>a. Open the <a href="https://console.aws.amazon.com/msk/" >Amazon MSK console</a> and select your cluster</p>
<p>b. Click on <strong>View client information</strong></p>
<p>c. Copy the url under <strong>Private endpoint</strong> and against <strong>SASL/SCRAM</strong>. This will be your <code>&lt;broker-url&gt;</code> going forward.</p>
<p>d. Connect to Materialize using the <a href="/console/" >SQL Shell</a>,
or your preferred SQL client.</p>
<p>e. Create a connection using the command below. The broker URL is what you copied in step c of this subsection. The <code>&lt;topic-name&gt;</code> is the name of the topic you created in Step 4. The <code>&lt;your-username&gt;</code> and <code>&lt;your-password&gt;</code> is from <em>Store a new secret</em> under Step 2.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">SECRET</span> <span class="n">msk_password</span> <span class="k">AS</span> <span class="s1">&#39;&lt;your-password&gt;&#39;</span><span class="p">;</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">CONNECTION</span> <span class="n">kafka_connection</span> <span class="k">TO</span> <span class="k">KAFKA</span> <span class="p">(</span>
</span></span><span class="line"><span class="cl">    <span class="k">BROKER</span> <span class="s1">&#39;&lt;broker-url&gt;&#39;</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="k">SASL</span> <span class="k">MECHANISMS</span> <span class="o">=</span> <span class="s1">&#39;SCRAM-SHA-512&#39;</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="k">SASL</span> <span class="k">USERNAME</span> <span class="o">=</span> <span class="s1">&#39;&lt;your-username&gt;&#39;</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="k">SASL</span> <span class="k">PASSWORD</span> <span class="o">=</span> <span class="k">SECRET</span> <span class="n">msk_password</span>
</span></span><span class="line"><span class="cl">  <span class="p">);</span>
</span></span></code></pre></div></li>
</ol>








## Creating a source

The Kafka connection created in the previous section can then be reused across
multiple [`CREATE SOURCE`](/sql/create-source/kafka/) statements. By default,
the source will be created in the active cluster; to use a different cluster,
use the `IN CLUSTER` clause.

```mzsql
CREATE SOURCE json_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT JSON;
```

If the command executes without an error and outputs _CREATE SOURCE_, it means
that you have successfully connected Materialize to your cluster.

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`: Kafka](/sql/create-source/kafka)


---

## Confluent Cloud


[//]: # "TODO(morsapaes) The Kafka guides need to be rewritten for consistency
with the Postgres ones. We should include spill to disk in the guidance then."

This guide goes through the required steps to connect Materialize to a Confluent
Cloud Kafka cluster.

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).


If you already have a Confluent Cloud Kafka cluster, you can skip step 1 and
directly move on to [Create an API Key](#create-an-api-key). You can also skip
step 3 if you already have a Confluent Cloud Kafka cluster up and running, and
have created a topic that you want to create a source for.

The process to connect Materialize to a Confluent Cloud Kafka cluster consists
of the following steps:

1. #### Create a Confluent Cloud Kafka cluster

    If you already have a Confluent Cloud Kafka cluster set up, then you can
    skip this step.

    a. Sign in to [Confluent Cloud](https://confluent.cloud/)

    b. Choose **Create a new cluster**

    c. Select the cluster type, and specify the rest of the settings based on
    your needs

    d. Choose **Create cluster**

    **Note:** This creation can take about 10 minutes. For more information on the cluster creation, see [Confluent Cloud documentation](https://docs.confluent.io/cloud/current/get-started/index.html#step-1-create-a-ak-cluster-in-ccloud).

2. #### Create an API Key

    ##### API Key

    a. Navigate to the [Confluent Cloud dashboard](https://confluent.cloud/)

    b. Choose the Confluent Cloud Kafka cluster you just created in Step 1

    c. Click on the **API Keys** tab

    d. In the **API Keys** section, choose **Add Key**

    e. Specify the scope for the API key and then click **Create Key**. If you
    choose to create a _granular access_ API key, make sure to create a
    [service account](https://docs.confluent.io/cloud/current/access-management/identity/service-accounts.html#create-a-service-account-using-the-ccloud-console)
    and add an [ACL](https://docs.confluent.io/cloud/current/access-management/access-control/acl.html#use-access-control-lists-acls-for-ccloud)
    with `Read` access to the topic you want to create a source for.

    Take note of the API Key you just created, as well as the API Key secret
    key; you'll need them later on. Keep in mind that the API Key secret key
    contains sensitive information, and you should store it somewhere safe!

3. #### Create a topic

    To start using Materialize with Confluent Cloud, you need to point it to an
    existing Kafka topic you want to read data from.

    If you already have a topic created, you can skip this step.

    Otherwise, you can find more information about how to do that [here](https://docs.confluent.io/cloud/current/get-started/index.html#step-2-create-a-ak-topic).

4. #### Create a source in Materialize

    a. Open the [Confluent Cloud dashboard](https://confluent.cloud/) and select your cluster.

    b. Click on **Overview** and select **Cluster settings**.

    c. Copy the URL under **Bootstrap server**. This will be your `<broker-url>` going forward.

    d. Connect to Materialize using the [SQL Shell](/console/),
       or your preferred SQL client.

    e. Run the following command. Replace `<confluent_cloud>` with whatever you
    want to name your source. The broker URL is what you copied in step c of
    this subsection. The `<topic-name>` is the name of the topic you created in
    Step 4. The `<your-api-key>` and `<your-api-secret>` are from the _Create
    an API Key_ step.

    ```mzsql
      CREATE SECRET confluent_username AS '<your-api-key>';
      CREATE SECRET confluent_password AS '<your-api-secret>';

      CREATE CONNECTION <confluent_cloud> TO KAFKA (
        BROKER '<confluent-broker-url>',
        SASL MECHANISMS = 'PLAIN',
        SASL USERNAME = SECRET confluent_username,
        SASL PASSWORD = SECRET confluent_password
      );

      CREATE SOURCE <source-name>
        FROM KAFKA CONNECTION confluent_cloud (TOPIC '<topic-name>')
        FORMAT JSON;
    ```
    By default, the source will be created in the active cluster; to use a different
    cluster, use the `IN CLUSTER` clause.

    f. If the command executes without an error and outputs _CREATE SOURCE_, it
    means that you have successfully connected Materialize to your Confluent
    Cloud Kafka cluster.

    **Note:** The example above walked through creating a source, which is a way
    of connecting Materialize to an external data source. We created a
    connection to Confluent Cloud Kafka using SASL authentication and credentials
    securely stored as secrets in Materialize's secret management system. For
    input formats, we used `JSON`, but you can also ingest Kafka messages
    formatted in other supported formats; e.g., [Avro and CSV](/sql/create-source/kafka/#syntax).
    You can find more details about the various different supported formats and
    possible configurations in the [reference documentation](/sql/create-source/kafka/).


---

## Ingest data from Self-hosted Kafka


[//]: # "TODO(morsapaes) The Kafka guides need to be rewritten for consistency
with the Postgres ones. We should include spill to disk in the guidance then."

This guide goes through the required steps to connect Materialize to a
self-hosted Kafka cluster.

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).


## Before you begin

Before you begin, you must have:

- A Kafka cluster running Kafka 3.2 or later.
- A client machine that can interact with your cluster.

## Configure network security

There are various ways to configure your Kafka network to allow Materialize to
connect:

- **Use AWS PrivateLink**: If your Kafka cluster is running on AWS, you can use
    AWS PrivateLink to connect Materialize to the cluster.

- **Use an SSH tunnel**: If your Kafka cluster is running in a private network,
    you can use an SSH tunnel to connect Materialize to the cluster.

- **Allow Materialize IPs**: If your Kafka cluster is publicly accessible, you
    can configure your firewall to allow connections from a set of static
    Materialize IP addresses.

Select the option that works best for you.



**Cloud:**


**Privatelink:**

> **Note:** Materialize provides Terraform modules for both [Amazon MSK clusters](https://github.com/MaterializeInc/terraform-aws-msk-privatelink)
> and [self-managed Kafka clusters](https://github.com/MaterializeInc/terraform-aws-kafka-privatelink)
> which can be used to create the target groups for each Kafka broker (step 1),
> the network load balancer (step 2), the TCP listeners (step 3) and the VPC
> endpoint service (step 5).


This section covers how to create AWS PrivateLink connections
and retrieve the AWS principal needed to configure the AWS PrivateLink service.

1. Create target groups. Create a dedicated [target
   group](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html)
   **for each broker** with the following details:

    a. Target type as **IP address**.

    b. Protocol as **TCP**.

    c. Port as **9092**, or the port that you are using in case it is not 9092 (e.g. 9094 for TLS or 9096 for SASL).

    d. Make sure that the target group is in the same VPC as the Kafka cluster.

    e. Click next, and register the respective Kafka broker to each target group using its IP address.

1. Create a [Network Load
    Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html)
    that is **enabled for the same subnets** that the Kafka brokers are in.

1. Create a [TCP
   listener](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-listener.html)
   for every Kafka broker that forwards to the corresponding target group you
   created (e.g. `b-1`, `b-2`, `b-3`).

    The listener port needs to be unique, and will be used later on in the `CREATE CONNECTION` statement.

    For example, you can create a listener for:

    a. Port `9001` → broker `b-1...`.

    b. Port `9002` → broker `b-2...`.

    c. Port `9003` → broker `b-3...`.

1. Verify security groups and health checks. Once the TCP listeners have been
   created, make sure that the [health
   checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html)
   for each target group are passing and that the targets are reported as
   healthy.

    If you have set up a security group for your Kafka cluster, you must ensure that it allows traffic on both the listener port and the health check port.

    **Remarks**:

    a. Network Load Balancers do not have associated security groups. Therefore, the security groups for your targets must use IP addresses to allow traffic.

    b. You can't use the security groups for the clients as a source in the security groups for the targets. Therefore, the security groups for your targets must use the IP addresses of the clients to allow traffic. For more details, check the [AWS documentation](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html).

1. Create a VPC [endpoint service](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html) and associate it with the **Network Load Balancer** that you’ve just created.

    Note the **service name** that is generated for the endpoint service.

1. Create an AWS PrivateLink connection. In Materialize, create an [AWS
    PrivateLink connection](/sql/create-connection/#aws-privatelink) that
    references the endpoint service that you created in the previous step.

    ↕️ **In-region connections**

    To connect to an AWS PrivateLink endpoint service in the **same region** as your
    Materialize environment:

      ```mzsql
      CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
        SERVICE NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
        AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az4')
      );
      ```

    - Replace the `SERVICE NAME` value with the service name you noted earlier.

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

    - Replace the `SERVICE NAME` value with the service name you noted earlier.

    - The service name region refers to where the endpoint service was created.
      You **do not need** to specify `AVAILABILITY ZONES` manually — these will
      be optimally auto-assigned when none are provided.

    - For Kafka connections, it is required for [cross-zone load balancing](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html) to be
      enabled on the VPC endpoint service's NLB when using cross-region Privatelink.

1. Configure the AWS PrivateLink service. Retrieve the AWS principal for the AWS
    PrivateLink connection you just created:

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

1. If your AWS PrivateLink service is configured to require acceptance of connection requests, you must manually approve the connection request from Materialize after executing `CREATE CONNECTION`. For more details, check the [AWS PrivateLink documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).

    **Note:** It might take some time for the endpoint service connection to show up, so you would need to wait for the endpoint service connection to be ready before you create a source.

1. Validate the AWS PrivateLink connection you created using the [`VALIDATE CONNECTION`](/sql/validate-connection) command:

   ```mzsql
   VALIDATE CONNECTION privatelink_svc;
   ```

   If no validation error is returned, move to the next step.

1. Create a source connection

   In Materialize, create a source connection that uses the AWS PrivateLink
connection you just configured:

   ```mzsql
   CREATE CONNECTION kafka_connection TO KAFKA (
       BROKERS (
           -- The port **must exactly match** the port assigned to the broker in
           -- the TCP listerner of the NLB.
           'b-1.hostname-1:9096' USING AWS PRIVATELINK privatelink_svc (PORT  9001, AVAILABILITY ZONE 'use1-az2'),
           'b-2.hostname-2:9096' USING AWS PRIVATELINK privatelink_svc (PORT  9002, AVAILABILITY ZONE 'use1-az1'),
           'b-3.hostname-3:9096' USING AWS PRIVATELINK privatelink_svc (PORT  9003, AVAILABILITY ZONE 'use1-az4')
       ),
       -- Authentication details
       -- Depending on the authentication method the Kafka cluster is using
       SASL MECHANISMS = 'SCRAM-SHA-512',
       SASL USERNAME = 'foo',
       SASL PASSWORD = SECRET kafka_password
   );
   ```

   If you run into connectivity issues during source creation, make sure that:

   * The `(PORT <port_number>)` value **exactly matches** the port assigned to
      the corresponding broker in the **TCP listener** of the Network Load
      Balancer. Misalignment between ports and broker addresses is the most
      common cause for connectivity issues.

   * For **in-region connections**, the correct availability zone is specified
      for each broker.




**SSH Tunnel:**

Materialize can connect to a Kafka broker, a Confluent Schema Registry server, a
PostgreSQL database, or a MySQL database through an SSH tunnel connection. In
this guide, you will create an SSH tunnel connection, configure your
Materialize authentication settings, and create a source connection.

Before you begin, make sure you have access to a bastion host. You will need:

* The bastion host IP address and port number
* The bastion host username

1. Create an SSH tunnel connection. In Materialize, create an [SSH tunnel
   connection](/sql/create-connection/#ssh-tunnel) to the bastion host:

    ```mzsql
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        USER '<SSH_BASTION_USER>',
        PORT <SSH_BASTION_PORT>
    );
    ```

1. Configure the SSH bastion host. The bastion host needs a **public key** to
connect to the Materialize tunnel you created in the previous step. Materialize
stores public keys for SSH tunnels in the system catalog. Query
[`mz_ssh_tunnel_connections`](/sql/system-catalog/mz_catalog/#mz_ssh_tunnel_connections)
to retrieve the public keys for the SSH tunnel connection you just created:

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

    ```
    | id    | public_key_1                          | public_key_2                          |
    |-------|---------------------------------------|---------------------------------------|
    | u75   | ssh-ed25519 AAAA...76RH materialize   | ssh-ed25519 AAAA...hLYV materialize   |
    ```


    > Materialize provides two public keys to allow you to rotate keys without
    connection downtime. Review the [`ALTER CONNECTION`](/sql/alter-connection) documentation for
    more information on how to rotate your keys.

1. Log in to your SSH bastion server and add each key to the bastion `authorized_keys` file:

    ```bash
    # Command for Linux
    echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
    echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys
    ```

1. Configure your internal firewall to allow the SSH bastion host to connect to your Kafka cluster or PostgreSQL instance.

    If you are using a cloud provider like AWS or GCP, update the security group or firewall rules for your PostgreSQL instance or Kafka brokers.

    Allow incoming traffic from the SSH bastion host IP address on the necessary ports.

    For example, use port `5432` for PostgreSQL and ports `9092`, `9094`, and `9096` for Kafka.

    Test the connection from the bastion host to the Kafka cluster or PostgreSQL instance.

    ```bash
    telnet <KAFKA_BROKER_HOST> <KAFKA_BROKER_PORT>
    telnet <POSTGRES_HOST> <POSTGRES_PORT>
    ```

    If the command hangs, double-check your security group and firewall settings. If the connection is successful, you can proceed to the next step.

1. Verify the SSH tunnel connection from your source to your bastion host:

    ```bash
    # Command for Linux
    ssh -L 9092:kafka-broker:9092 <SSH_BASTION_USER>@<SSH_BASTION_HOST>
    ```

    Verify that you can connect to the Kafka broker or PostgreSQL instance via the SSH tunnel:

    ```bash
    telnet localhost 9092
    ```

    If you are unable to connect using the `telnet` command, enable `AllowTcpForwarding` and `PermitTunnel` on your bastion host SSH configuration file.

    On your SSH bastion host, open the SSH config file (usually located at `/etc/ssh/sshd_config`) using a text editor:

    ```bash
    sudo nano /etc/ssh/sshd_config
    ```

    Add or uncomment the following lines:

    ```bash
    AllowTcpForwarding yes
    PermitTunnel yes
    ```

    Save the changes and restart the SSH service:

    ```bash
    sudo systemctl restart sshd
    ```

1. Retrieve the static egress IPs from Materialize and configure the firewall rules (e.g. AWS Security Groups) for your bastion host to allow SSH traffic for those IP addresses only.

    ```mzsql
    SELECT * FROM mz_catalog.mz_egress_ips;
    ```

    ```
    XXX.140.90.33
    XXX.198.159.213
    XXX.100.27.23
    ```

1. To confirm that the SSH tunnel connection is correctly configured, use the
   [`VALIDATE CONNECTION`](/sql/validate-connection) command:

   ```mzsql
   VALIDATE CONNECTION ssh_connection;
   ```

   If no validation errors are returned, the connection can be used to create a
   source connection.


1. In Materialize, create a source connection that uses the SSH tunnel
connection you configured in the previous section:

  ```mzsql
  CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'broker1:9092',
    SSH TUNNEL ssh_connection
  );
```



**Allow Materialize IPs:**

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, find the static egress IP addresses for the
   Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```

1. Update your Kafka cluster firewall rules to allow traffic from each IP
   address from the previous step.

1. Create a [Kafka connection](/sql/create-connection/#kafka) that references
   your Kafka cluster:

    ```mzsql
    CREATE SECRET kafka_password AS '<your-password>';

    CREATE CONNECTION kafka_connection TO KAFKA (
        BROKER '<broker-url>',
        SASL MECHANISMS = 'SCRAM-SHA-512',
        SASL USERNAME = '<your-username>',
        SASL PASSWORD = SECRET kafka_password
    );
    ```





**Self-Managed:**

There are various ways to configure your Kafka network to allow Materialize to
connect:

- **Use an SSH tunnel**: If your Kafka cluster is running in a private network,
    you can use an SSH tunnel to connect Materialize to the cluster.

- **Allow Materialize IPs**: If your Kafka cluster is publicly accessible, you
    can configure your firewall to allow connections from a set of static
    Materialize IP addresses.

Select the option that works best for you.



**SSH Tunnel:**

Materialize can connect to a Kafka broker, a Confluent Schema Registry server, a
PostgreSQL database, or a MySQL database through an SSH tunnel connection. In
this guide, you will create an SSH tunnel connection, configure your
Materialize authentication settings, and create a source connection.

Before you begin, make sure you have access to a bastion host. You will need:

* The bastion host IP address and port number
* The bastion host username

1. Create an SSH tunnel connection. In Materialize, create an [SSH tunnel
   connection](/sql/create-connection/#ssh-tunnel) to the bastion host:

    ```mzsql
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        USER '<SSH_BASTION_USER>',
        PORT <SSH_BASTION_PORT>
    );
    ```

1. Configure the SSH bastion host. The bastion host needs a **public key** to
connect to the Materialize tunnel you created in the previous step. Materialize
stores public keys for SSH tunnels in the system catalog. Query
[`mz_ssh_tunnel_connections`](/sql/system-catalog/mz_catalog/#mz_ssh_tunnel_connections)
to retrieve the public keys for the SSH tunnel connection you just created:

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

    ```
    | id    | public_key_1                          | public_key_2                          |
    |-------|---------------------------------------|---------------------------------------|
    | u75   | ssh-ed25519 AAAA...76RH materialize   | ssh-ed25519 AAAA...hLYV materialize   |
    ```


    > Materialize provides two public keys to allow you to rotate keys without
    connection downtime. Review the [`ALTER CONNECTION`](/sql/alter-connection) documentation for
    more information on how to rotate your keys.

1. Log in to your SSH bastion server and add each key to the bastion `authorized_keys` file:

    ```bash
    # Command for Linux
    echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
    echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys
    ```

1. Configure your internal firewall to allow the SSH bastion host to connect to your Kafka cluster or PostgreSQL instance.

    If you are using a cloud provider like AWS or GCP, update the security group or firewall rules for your PostgreSQL instance or Kafka brokers.

    Allow incoming traffic from the SSH bastion host IP address on the necessary ports.

    For example, use port `5432` for PostgreSQL and ports `9092`, `9094`, and `9096` for Kafka.

    Test the connection from the bastion host to the Kafka cluster or PostgreSQL instance.

    ```bash
    telnet <KAFKA_BROKER_HOST> <KAFKA_BROKER_PORT>
    telnet <POSTGRES_HOST> <POSTGRES_PORT>
    ```

    If the command hangs, double-check your security group and firewall settings. If the connection is successful, you can proceed to the next step.

1. Verify the SSH tunnel connection from your source to your bastion host:

    ```bash
    # Command for Linux
    ssh -L 9092:kafka-broker:9092 <SSH_BASTION_USER>@<SSH_BASTION_HOST>
    ```

    Verify that you can connect to the Kafka broker or PostgreSQL instance via the SSH tunnel:

    ```bash
    telnet localhost 9092
    ```

    If you are unable to connect using the `telnet` command, enable `AllowTcpForwarding` and `PermitTunnel` on your bastion host SSH configuration file.

    On your SSH bastion host, open the SSH config file (usually located at `/etc/ssh/sshd_config`) using a text editor:

    ```bash
    sudo nano /etc/ssh/sshd_config
    ```

    Add or uncomment the following lines:

    ```bash
    AllowTcpForwarding yes
    PermitTunnel yes
    ```

    Save the changes and restart the SSH service:

    ```bash
    sudo systemctl restart sshd
    ```

1. Ensure materialize cluster pods have network access to your SSH bastion host.

1. Validate the SSH tunnel connection

   To confirm that the SSH tunnel connection is correctly configured, use the [`VALIDATE CONNECTION`](/sql/validate-connection) command:

    ```mzsql
    VALIDATE CONNECTION ssh_connection;
    ```

    If no validation errors are returned, the connection can be used to create a source connection.



1. In Materialize, create a source connection that uses the SSH tunnel
connection you configured in the previous section:

```mzsql
CREATE CONNECTION kafka_connection TO KAFKA (
  BROKER 'broker1:9092',
  SSH TUNNEL ssh_connection
);
```



**Allow Materialize IPs:**

1. Update your Kafka cluster firewall rules to allow traffic from Materialize.

1. Create a [Kafka connection](/sql/create-connection/#kafka) that references
   your Kafka cluster:

    ```mzsql
    CREATE SECRET kafka_password AS '<your-password>';

    CREATE CONNECTION kafka_connection TO KAFKA (
        BROKER '<broker-url>',
        SASL MECHANISMS = 'SCRAM-SHA-512',
        SASL USERNAME = '<your-username>',
        SASL PASSWORD = SECRET kafka_password
    );
    ```







## Creating a source

The Kafka connection created in the previous section can then be reused across
multiple [`CREATE SOURCE`](/sql/create-source/kafka/) statements:

```mzsql
CREATE SOURCE json_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT JSON;
```

By default, the source will be created in the active cluster; to use a different
cluster, use the `IN CLUSTER` clause.

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`: Kafka](/sql/create-source/kafka)


---

## WarpStream


[//]: # "TODO(morsapaes) The Kafka guides need to be rewritten for consistency
with the Postgres ones. We should include spill to disk in the guidance then."

This guide goes through the necessary steps to connect Materialize to
[WarpStream](https://www.warpstream.com/), an Apache Kafka® protocol compatible
data streaming platform.

WarpStream runs on commodity object stores (e.g., Amazon S3, Google Cloud
Storage, Azure Blob Storage) and offers benefits such as no inter-AZ bandwidth
costs and no local disks management. This guide highlights its integration with
Materialize using [Fly.io](https://fly.io/).

> **Tip:** For help getting started with your own data, you can schedule a [free guided
> trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).


#### Before you begin

Ensure you have the following:

-   [A WarpStream account](https://console.warpstream.com/signup)
-   A Fly.io account: used for deploying a WarpStream cluster with TLS termination
    and SASL authentication.

1. #### Set up WarpStream

    If you already have a WarpStream cluster, you can skip this step.

    a. Begin by registering for a WarpStream account or logging in to your
    existing account.

    b. Follow [this guide](https://github.com/warpstreamlabs/warpstream-fly-io-template)
    to deploy your WarpStream cluster on Fly.io.

    c. Post deployment, [create credentials](https://docs.warpstream.com/warpstream/how-to/configure-the-warpstream-agent-for-production/configure-authentication-for-the-warpstream-agent#sasl-authentication)
    for connecting to your WarpStream cluster.

    d. Use the provided credentials to connect to the WarpStream cluster on
    Fly.io. Test this connection using [the WarpStream CLI](https://docs.warpstream.com/warpstream/install-the-warpstream-agent):

    ```bash
    warpstream kcmd -type diagnose-connection \
                    -bootstrap-host $CLUSTER_NAME.fly.dev \
                    -tls -username ccun_XXXXXXXXXX \
                    -password ccp_XXXXXXXXXX
    ```

    Change the `bootstrap-host` to the name of your WarpStream cluster on
    Fly.io.

    e. Create the `materialize_click_streams` topic:

    ```bash
    warpstream kcmd -bootstrap-host $CLUSTER_NAME.fly.dev \
                    -tls -username ccun_XXXXXXXXX \
                    -password ccp_XXXXXXXXXX \
                    -type create-topic \
                    -topic materialize_click_streams
    ```

    f. Generate and push sample records for testing:

    ```bash
    warpstream kcmd -bootstrap-host $CLUSTER_NAME.fly.dev \
                    -tls -username ccun_XXXXXXXXXX \
                    -password ccp_XXXXXXXXXX \
                    -type produce \
                    -topic materialize_click_streams \
                    --records '{"action": "click", "user_id": "user_0", "page_id": "home"},,{"action": "hover", "user_id": "user_0", "page_id": "home"},,{"action": "scroll", "user_id": "user_0", "page_id": "home"}'
    ```

    > **Note:** The WarpStream CLI uses `,,` as a delimiter between JSON records.

2. #### Integrate with Materialize

    To integrate WarpStream with Materialize, you need to set up a connection to
    the WarpStream broker and create a source in Materialize to consume the
    data.

    Head over to the Materialize console and follow the steps below:

    a. Save WarpStream credentials:

    ```mzsql
    CREATE SECRET warpstream_username AS '<username>';
    CREATE SECRET warpstream_password AS '<password>';
    ```

    b. Set up a connection to the WarpStream broker:

    ```mzsql
    CREATE CONNECTION warpstream_kafka TO KAFKA (
        BROKER '<CLUSTER_NAME>.fly.dev:9092',
        SASL MECHANISMS = "PLAIN",
        SASL USERNAME = SECRET warpstream_username,
        SASL PASSWORD = SECRET warpstream_password
    );
    ```

    c. Create a source in Materialize to consume messages. By default, the
    source will be created in the active cluster; to use a different cluster,
    use the `IN CLUSTER` clause.

    ```mzsql
    CREATE SOURCE warpstream_click_stream_source
        FROM KAFKA CONNECTION warpstream_kafka (TOPIC 'materialize_click_streams')
        FORMAT JSON;
    ```

    d. Verify the ingestion and query the data in Materialize:

    ```mzsql
    SELECT * FROM warpstream_click_stream_source LIMIT 10;
    ```

    e. Furthermore, create a materialized view to aggregate the data:

    ```mzsql
    CREATE MATERIALIZED VIEW warpstream_click_stream_aggregate AS
        SELECT
            user_id,
            page_id,
            COUNT(*) AS count
        FROM warpstream_click_stream_source
        GROUP BY user_id, page_id;
    ```

    f. Produce additional records to monitor real-time updates:

    ```bash
    warpstream kcmd -bootstrap-host $CLUSTER_NAME.fly.dev \
                    -tls -username ccun_XXXXXXXXXX \
                    -password ccp_XXXXXXXXXX \
                    -type produce \
                    -topic materialize_click_streams \
                    --records '{"action": "click", "user_id": "user_1", "page_id": "home"}'
    ```

    g. Query the materialized view to monitor the real-time updates:

    ```mzsql
    SELECT * FROM warpstream_click_stream_aggregate;
    ```

---

By following the steps outlined above, you will have successfully set up a
connection between WarpStream and Materialize. You can now use Materialize to
query the data ingested from WarpStream.
