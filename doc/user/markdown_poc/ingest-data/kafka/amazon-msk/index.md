<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [Ingest data](/docs/ingest-data/)
 /  [Kafka](/docs/ingest-data/kafka/)

</div>

# Amazon Managed Streaming for Apache Kafka (Amazon MSK)

This guide goes through the required steps to connect Materialize to an
Amazon MSK cluster.

<div class="tip">

**💡 Tip:** For help getting started with your own data, you can
schedule a [free guided
trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).

</div>

## Before you begin

Before you begin, you must have:

- An Amazon MSK cluster running on AWS.
- A client machine that can interact with your cluster.

## Creating a connection

<div class="code-tabs">

<div class="tab-content">

<div id="tab-cloud" class="tab-pane" title="Cloud">

There are various ways to configure your Kafka network to allow
Materialize to connect:

- **Allow Materialize IPs:** If your Kafka cluster is publicly
  accessible, you can configure your firewall to allow connections from
  a set of static Materialize IP addresses.

- **Use AWS PrivateLink**: If your Kafka cluster is running in a private
  network, you can use [AWS
  PrivateLink](/docs/ingest-data/network-security/privatelink/) to
  connect Materialize to the cluster. For details, see [AWS
  PrivateLink](/docs/ingest-data/network-security/privatelink/).

- **Use an SSH tunnel:** If your Kafka cluster is running in a private
  network, you can use an SSH tunnel to connect Materialize to the
  cluster.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-privatelink" class="tab-pane" title="PrivateLink">

<div class="note">

**NOTE:** Materialize provides a Terraform module that automates the
creation and configuration of AWS resources for a PrivateLink
connection. For more details, see the Terraform module repositories for
[Amazon
MSK](https://github.com/MaterializeInc/terraform-aws-msk-privatelink)
and [self-managed Kafka
clusters](https://github.com/MaterializeInc/terraform-aws-kafka-privatelink).

</div>

This section covers how to create AWS PrivateLink connections and
retrieve the AWS principal needed to configure the AWS PrivateLink
service.

1.  Create target groups. Create a dedicated [target
    group](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html)
    **for each broker** with the following details:

    a\. Target type as **IP address**.

    b\. Protocol as **TCP**.

    c\. Port as **9092**, or the port that you are using in case it is
    not 9092 (e.g. 9094 for TLS or 9096 for SASL).

    d\. Make sure that the target group is in the same VPC as the Kafka
    cluster.

    e\. Click next, and register the respective Kafka broker to each
    target group using its IP address.

2.  Create a [Network Load
    Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html)
    that is **enabled for the same subnets** that the Kafka brokers are
    in.

3.  Create a [TCP
    listener](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-listener.html)
    for every Kafka broker that forwards to the corresponding target
    group you created (e.g. `b-1`, `b-2`, `b-3`).

    The listener port needs to be unique, and will be used later on in
    the `CREATE CONNECTION` statement.

    For example, you can create a listener for:

    a\. Port `9001` → broker `b-1...`.

    b\. Port `9002` → broker `b-2...`.

    c\. Port `9003` → broker `b-3...`.

4.  Verify security groups and health checks. Once the TCP listeners
    have been created, make sure that the [health
    checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html)
    for each target group are passing and that the targets are reported
    as healthy.

    If you have set up a security group for your Kafka cluster, you must
    ensure that it allows traffic on both the listener port and the
    health check port.

    **Remarks**:

    a\. Network Load Balancers do not have associated security groups.
    Therefore, the security groups for your targets must use IP
    addresses to allow traffic.

    b\. You can’t use the security groups for the clients as a source in
    the security groups for the targets. Therefore, the security groups
    for your targets must use the IP addresses of the clients to allow
    traffic. For more details, check the [AWS
    documentation](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html).

5.  Create a VPC [endpoint
    service](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html)
    and associate it with the **Network Load Balancer** that you’ve just
    created.

    Note the **service name** that is generated for the endpoint
    service.

6.  Create an AWS PrivateLink connection. In Materialize, create an [AWS
    PrivateLink
    connection](/docs/sql/create-connection/#aws-privatelink) that
    references the endpoint service that you created in the previous
    step.

    ↕️ **In-region connections**

    To connect to an AWS PrivateLink endpoint service in the **same
    region** as your Materialize environment:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
      SERVICE NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
      AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az4')
    );
    ```

    </div>

    - Replace the `SERVICE NAME` value with the service name you noted
      earlier.

    - Replace the `AVAILABILITY ZONES` list with the IDs of the
      availability zones in your AWS account. For in-region connections
      the availability zones of the NLB and the consumer VPC **must
      match**.

      To find your availability zone IDs, select your database in the
      RDS Console and click the subnets under **Connectivity &
      security**. For each subnet, look for **Availability Zone ID**
      (e.g., `use1-az6`), not **Availability Zone** (e.g.,
      `us-east-1d`).

    ↔️ **Cross-region connections**

    To connect to an AWS PrivateLink endpoint service in a **different
    region** to the one where your Materialize environment is deployed:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
      SERVICE NAME 'com.amazonaws.vpce.us-west-1.vpce-svc-<endpoint_service_id>',
      -- For now, the AVAILABILITY ZONES clause **is** required, but will be
      -- made optional in a future release.
      AVAILABILITY ZONES ()
    );
    ```

    </div>

    - Replace the `SERVICE NAME` value with the service name you noted
      earlier.

    - The service name region refers to where the endpoint service was
      created. You **do not need** to specify `AVAILABILITY ZONES`
      manually — these will be optimally auto-assigned when none are
      provided.

    - For Kafka connections, it is required for [cross-zone load
      balancing](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html)
      to be enabled on the VPC endpoint service’s NLB when using
      cross-region Privatelink.

7.  Configure the AWS PrivateLink service. Retrieve the AWS principal
    for the AWS PrivateLink connection you just created:

    <div class="highlight">

    ``` chroma
    SELECT principal
    FROM mz_aws_privatelink_connections plc
    JOIN mz_connections c ON plc.id = c.id
    WHERE c.name = 'privatelink_svc';
    ```

    </div>

    ```
                                     principal
    ---------------------------------------------------------------------------
     arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
    ```

    Follow the instructions in the [AWS PrivateLink
    documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/add-endpoint-service-permissions.html)
    to configure your VPC endpoint service to accept connections from
    the provided AWS principal.

8.  If your AWS PrivateLink service is configured to require acceptance
    of connection requests, you must manually approve the connection
    request from Materialize after executing `CREATE CONNECTION`. For
    more details, check the [AWS PrivateLink
    documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).

    **Note:** It might take some time for the endpoint service
    connection to show up, so you would need to wait for the endpoint
    service connection to be ready before you create a source.

9.  Validate the AWS PrivateLink connection you created using the
    [`VALIDATE CONNECTION`](/docs/sql/validate-connection) command:

    <div class="highlight">

    ``` chroma
    VALIDATE CONNECTION privatelink_svc;
    ```

    </div>

    If no validation error is returned, move to the next step.

10. Create a source connection

    In Materialize, create a source connection that uses the AWS
    PrivateLink connection you just configured:

    <div class="highlight">

    ``` chroma
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

    </div>

    If you run into connectivity issues during source creation, make
    sure that:

    - The `(PORT <port_number>)` value **exactly matches** the port
      assigned to the corresponding broker in the **TCP listener** of
      the Network Load Balancer. Misalignment between ports and broker
      addresses is the most common cause for connectivity issues.

    - For **in-region connections**, the correct availability zone is
      specified for each broker.

</div>

<div id="tab-ssh-tunnel" class="tab-pane" title="SSH Tunnel">

Materialize can connect to a Kafka broker, a Confluent Schema Registry
server, a PostgreSQL database, or a MySQL database through an SSH tunnel
connection. In this guide, you will create an SSH tunnel connection,
configure your Materialize authentication settings, and create a source
connection.

Before you begin, make sure you have access to a bastion host. You will
need:

- The bastion host IP address and port number
- The bastion host username

1.  Create an SSH tunnel connection. In Materialize, create an [SSH
    tunnel connection](/docs/sql/create-connection/#ssh-tunnel) to the
    bastion host:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        USER '<SSH_BASTION_USER>',
        PORT <SSH_BASTION_PORT>
    );
    ```

    </div>

2.  Configure the SSH bastion host. The bastion host needs a **public
    key** to connect to the Materialize tunnel you created in the
    previous step. Materialize stores public keys for SSH tunnels in the
    system catalog. Query
    [`mz_ssh_tunnel_connections`](/docs/sql/system-catalog/mz_catalog/#mz_ssh_tunnel_connections)
    to retrieve the public keys for the SSH tunnel connection you just
    created:

    <div class="highlight">

    ``` chroma
    SELECT
        mz_connections.name,
        mz_ssh_tunnel_connections.*
    FROM
        mz_connections JOIN
        mz_ssh_tunnel_connections USING(id)
    WHERE
        mz_connections.name = 'ssh_connection';
    ```

    </div>

    ```
    | id    | public_key_1                          | public_key_2                          |
    |-------|---------------------------------------|---------------------------------------|
    | u75   | ssh-ed25519 AAAA...76RH materialize   | ssh-ed25519 AAAA...hLYV materialize   |
    ```

    > Materialize provides two public keys to allow you to rotate keys
    > without connection downtime. Review the
    > [`ALTER CONNECTION`](/docs/sql/alter-connection) documentation for
    > more information on how to rotate your keys.

3.  Log in to your SSH bastion server and add each key to the bastion
    `authorized_keys` file:

    <div class="highlight">

    ``` chroma
    # Command for Linux
    echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
    echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys
    ```

    </div>

4.  Configure your internal firewall to allow the SSH bastion host to
    connect to your Kafka cluster or PostgreSQL instance.

    If you are using a cloud provider like AWS or GCP, update the
    security group or firewall rules for your PostgreSQL instance or
    Kafka brokers.

    Allow incoming traffic from the SSH bastion host IP address on the
    necessary ports.

    For example, use port `5432` for PostgreSQL and ports `9092`,
    `9094`, and `9096` for Kafka.

    Test the connection from the bastion host to the Kafka cluster or
    PostgreSQL instance.

    <div class="highlight">

    ``` chroma
    telnet <KAFKA_BROKER_HOST> <KAFKA_BROKER_PORT>
    telnet <POSTGRES_HOST> <POSTGRES_PORT>
    ```

    </div>

    If the command hangs, double-check your security group and firewall
    settings. If the connection is successful, you can proceed to the
    next step.

5.  Verify the SSH tunnel connection from your source to your bastion
    host:

    <div class="highlight">

    ``` chroma
    # Command for Linux
    ssh -L 9092:kafka-broker:9092 <SSH_BASTION_USER>@<SSH_BASTION_HOST>
    ```

    </div>

    Verify that you can connect to the Kafka broker or PostgreSQL
    instance via the SSH tunnel:

    <div class="highlight">

    ``` chroma
    telnet localhost 9092
    ```

    </div>

    If you are unable to connect using the `telnet` command, enable
    `AllowTcpForwarding` and `PermitTunnel` on your bastion host SSH
    configuration file.

    On your SSH bastion host, open the SSH config file (usually located
    at `/etc/ssh/sshd_config`) using a text editor:

    <div class="highlight">

    ``` chroma
    sudo nano /etc/ssh/sshd_config
    ```

    </div>

    Add or uncomment the following lines:

    <div class="highlight">

    ``` chroma
    AllowTcpForwarding yes
    PermitTunnel yes
    ```

    </div>

    Save the changes and restart the SSH service:

    <div class="highlight">

    ``` chroma
    sudo systemctl restart sshd
    ```

    </div>

6.  Retrieve the static egress IPs from Materialize and configure the
    firewall rules (e.g. AWS Security Groups) for your bastion host to
    allow SSH traffic for those IP addresses only.

    <div class="highlight">

    ``` chroma
    SELECT * FROM mz_catalog.mz_egress_ips;
    ```

    </div>

    ```
    XXX.140.90.33
    XXX.198.159.213
    XXX.100.27.23
    ```

7.  To confirm that the SSH tunnel connection is correctly configured,
    use the [`VALIDATE CONNECTION`](/docs/sql/validate-connection)
    command:

    <div class="highlight">

    ``` chroma
    VALIDATE CONNECTION ssh_connection;
    ```

    </div>

    If no validation errors are returned, the connection can be used to
    create a source connection.

8.  In Materialize, create a source connection that uses the SSH tunnel
    connection you configured in the previous section:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION kafka_connection TO KAFKA (
      BROKER 'broker1:9092',
      SSH TUNNEL ssh_connection
    );
    ```

    </div>

</div>

<div id="tab-public-cluster" class="tab-pane" title="Public cluster">

This section goes through the required steps to connect Materialize to
an Amazon MSK cluster, including some of the more complicated bits
around configuring security settings in Amazon MSK.

If you already have an Amazon MSK cluster, you can skip step 1 and
directly move on to **Make the cluster public and enable SASL** step.
You can also skip steps 3 and 4 if you already have Apache Kafka
installed and running, and have created a topic that you want to create
a source for.

The process to connect Materialize to Amazon MSK consists of the
following steps:

1.  Create an Amazon MSK cluster. If you already have an Amazon MSK
    cluster set up, then you can skip this step.

    a\. Sign in to the AWS Management Console and open the [Amazon MSK
    console](https://console.aws.amazon.com/msk/)

    b\. Choose **Create cluster**

    c\. Enter a cluster name, and leave all other settings unchanged

    d\. From the table under **All cluster settings**, copy the values
    of the following settings and save them because you need them later
    in this tutorial: **VPC**, **Subnets**, **Security groups associated
    with VPC**

    e\. Choose **Create cluster**

    **Note:** This creation can take about 15 minutes.

2.  Make the cluster public and enable SASL.

    ##### Turn on SASL

    a\. Navigate to the [Amazon MSK
    console](https://console.aws.amazon.com/msk/)

    b\. Choose the MSK cluster you just created in Step 1

    c\. Click on the **Properties** tab

    d\. In the **Security settings** section, choose **Edit**

    e\. Check the checkbox next to **SASL/SCRAM authentication**

    f\. Click **Save changes**

    You can find more details about updating a cluster’s security
    configurations
    [here](https://docs.aws.amazon.com/msk/latest/developerguide/msk-update-security.html).

    ##### Create a symmetric key

    a\. Now go to the [AWS Key Management Service (AWS KMS)
    console](https://console.aws.amazon.com/kms)

    b\. Click **Create Key**

    c\. Choose **Symmetric** and click **Next**

    d\. Give the key and **Alias** and click **Next**

    e\. Under Administrative permissions, check the checkbox next to the
    **AWSServiceRoleForKafka** and click **Next**

    f\. Under Key usage permissions, again check the checkbox next to
    the **AWSServiceRoleForKafka** and click **Next**

    g\. Click on **Create secret**

    h\. Review the details and click **Finish**

    You can find more details about creating a symmetric key
    [here](https://docs.aws.amazon.com/kms/latest/developerguide/create-keys.html#create-symmetric-cmk).

    ##### Store a new Secret

    a\. Go to the [AWS Secrets Manager
    console](https://console.aws.amazon.com/secretsmanager/)

    b\. Click **Store a new secret**

    c\. Choose **Other type of secret** (e.g. API key) for the secret
    type

    d\. Under **Key/value pairs** click on **Plaintext**

    e\. Paste the following in the space below it and replace
    `<your-username>` and `<your-password>` with the username and
    password you want to set for the cluster

    ```
      {
        "username": "<your-username>",
        "password": "<your-password>"
      }
    ```

    f\. On the next page, give a **Secret name** that starts with
    `AmazonMSK_`

    g\. Under **Encryption Key**, select the symmetric key you just
    created in the previous sub-section from the dropdown

    h\. Go forward to the next steps and finish creating the secret.
    Once created, record the ARN (Amazon Resource Name) value for your
    secret

    You can find more details about creating a secret using AWS Secrets
    Manager
    [here](https://docs.aws.amazon.com/msk/latest/developerguide/msk-password.html).

    ##### Associate secret with MSK cluster

    a\. Navigate back to the [Amazon MSK
    console](https://console.aws.amazon.com/msk/) and click on the
    cluster you created in Step 1

    b\. Click on the **Properties** tab

    c\. In the **Security settings** section, under **SASL/SCRAM
    authentication**, click on **Associate secrets**

    d\. Paste the ARN you recorded in the previous subsection and click
    **Associate secrets**

    ##### Create the cluster’s configuration

    a\. Go to the [Amazon CloudShell
    console](https://console.aws.amazon.com/cloudshell/)

    b\. Create a file (eg. *msk-config.txt*) with the following line

    ```
      allow.everyone.if.no.acl.found = false
    ```

    c\. Run the following AWS CLI command, replacing
    `<config-file-path>` with the path to the file where you saved your
    configuration in the previous step

    ```
      aws kafka create-configuration --name "MakePublic" \
      --description "Set allow.everyone.if.no.acl.found = false" \
      --kafka-versions "2.6.2" \
      --server-properties fileb://<config-file-path>/msk-config.txt
    ```

    You can find more information about making your cluster public
    [here](https://docs.aws.amazon.com/msk/latest/developerguide/public-access.html).

3.  If you already have a client machine set up that can interact with
    your cluster, then you can skip this step.

    If not, you can create an EC2 client machine and then add the
    security group of the client to the inbound rules of the cluster’s
    security group from the VPC console. You can find more details about
    how to do that
    [here](https://docs.aws.amazon.com/msk/latest/developerguide/create-client-machine.html).

4.  Install Apache Kafka and create a topic. To start using Materialize
    with Apache Kafka, you need to create a Materialize source over an
    Apache Kafka topic. If you already have Apache Kafka installed and a
    topic created, you can skip this step.

    Otherwise, you can install Apache Kafka on your client machine from
    the previous step and create a topic. You can find more information
    about how to do that
    [here](https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html).

5.  Create ACLs. As `allow.everyone.if.no.acl.found` is set to `false`,
    you must create ACLs for the cluster and topics configured in the
    previous step to set appropriate access permissions. For more
    information, see the [Amazon
    MSK](https://docs.aws.amazon.com/msk/latest/developerguide/msk-acls.html)
    documentation.

6.  Create a connection in Materialize.

    a\. Open the [Amazon MSK
    console](https://console.aws.amazon.com/msk/) and select your
    cluster

    b\. Click on **View client information**

    c\. Copy the url under **Private endpoint** and against
    **SASL/SCRAM**. This will be your `<broker-url>` going forward.

    d\. Connect to Materialize using the [SQL Shell](/docs/console/), or
    your preferred SQL client.

    e\. Create a connection using the command below. The broker URL is
    what you copied in step c of this subsection. The `<topic-name>` is
    the name of the topic you created in Step 4. The `<your-username>`
    and `<your-password>` is from *Store a new secret* under Step 2.

    <div class="highlight">

    ``` chroma
    CREATE SECRET msk_password AS '<your-password>';

    CREATE CONNECTION kafka_connection TO KAFKA (
        BROKER '<broker-url>',
        SASL MECHANISMS = 'SCRAM-SHA-512',
        SASL USERNAME = '<your-username>',
        SASL PASSWORD = SECRET msk_password
      );
    ```

    </div>

</div>

</div>

</div>

</div>

<div id="tab-self-managed" class="tab-pane" title="Self-Managed">

Configure your Kafka network to allow Materialize to connect:

- **Use an SSH tunnel**: If your Kafka cluster is running in a private
  network, you can use an SSH tunnel to connect Materialize to the
  cluster.

- **Allow Materialize IPs**: If your Kafka cluster is publicly
  accessible, you can configure your firewall to allow connections from
  a set of static Materialize IP addresses.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-ssh-tunnel" class="tab-pane" title="SSH Tunnel">

Materialize can connect to a Kafka broker, a Confluent Schema Registry
server, a PostgreSQL database, or a MySQL database through an SSH tunnel
connection. In this guide, you will create an SSH tunnel connection,
configure your Materialize authentication settings, and create a source
connection.

Before you begin, make sure you have access to a bastion host. You will
need:

- The bastion host IP address and port number
- The bastion host username

1.  Create an SSH tunnel connection. In Materialize, create an [SSH
    tunnel connection](/docs/sql/create-connection/#ssh-tunnel) to the
    bastion host:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION ssh_connection TO SSH TUNNEL (
        HOST '<SSH_BASTION_HOST>',
        USER '<SSH_BASTION_USER>',
        PORT <SSH_BASTION_PORT>
    );
    ```

    </div>

2.  Configure the SSH bastion host. The bastion host needs a **public
    key** to connect to the Materialize tunnel you created in the
    previous step. Materialize stores public keys for SSH tunnels in the
    system catalog. Query
    [`mz_ssh_tunnel_connections`](/docs/sql/system-catalog/mz_catalog/#mz_ssh_tunnel_connections)
    to retrieve the public keys for the SSH tunnel connection you just
    created:

    <div class="highlight">

    ``` chroma
    SELECT
        mz_connections.name,
        mz_ssh_tunnel_connections.*
    FROM
        mz_connections JOIN
        mz_ssh_tunnel_connections USING(id)
    WHERE
        mz_connections.name = 'ssh_connection';
    ```

    </div>

    ```
    | id    | public_key_1                          | public_key_2                          |
    |-------|---------------------------------------|---------------------------------------|
    | u75   | ssh-ed25519 AAAA...76RH materialize   | ssh-ed25519 AAAA...hLYV materialize   |
    ```

    > Materialize provides two public keys to allow you to rotate keys
    > without connection downtime. Review the
    > [`ALTER CONNECTION`](/docs/sql/alter-connection) documentation for
    > more information on how to rotate your keys.

3.  Log in to your SSH bastion server and add each key to the bastion
    `authorized_keys` file:

    <div class="highlight">

    ``` chroma
    # Command for Linux
    echo "ssh-ed25519 AAAA...76RH materialize" >> ~/.ssh/authorized_keys
    echo "ssh-ed25519 AAAA...hLYV materialize" >> ~/.ssh/authorized_keys
    ```

    </div>

4.  Configure your internal firewall to allow the SSH bastion host to
    connect to your Kafka cluster or PostgreSQL instance.

    If you are using a cloud provider like AWS or GCP, update the
    security group or firewall rules for your PostgreSQL instance or
    Kafka brokers.

    Allow incoming traffic from the SSH bastion host IP address on the
    necessary ports.

    For example, use port `5432` for PostgreSQL and ports `9092`,
    `9094`, and `9096` for Kafka.

    Test the connection from the bastion host to the Kafka cluster or
    PostgreSQL instance.

    <div class="highlight">

    ``` chroma
    telnet <KAFKA_BROKER_HOST> <KAFKA_BROKER_PORT>
    telnet <POSTGRES_HOST> <POSTGRES_PORT>
    ```

    </div>

    If the command hangs, double-check your security group and firewall
    settings. If the connection is successful, you can proceed to the
    next step.

5.  Verify the SSH tunnel connection from your source to your bastion
    host:

    <div class="highlight">

    ``` chroma
    # Command for Linux
    ssh -L 9092:kafka-broker:9092 <SSH_BASTION_USER>@<SSH_BASTION_HOST>
    ```

    </div>

    Verify that you can connect to the Kafka broker or PostgreSQL
    instance via the SSH tunnel:

    <div class="highlight">

    ``` chroma
    telnet localhost 9092
    ```

    </div>

    If you are unable to connect using the `telnet` command, enable
    `AllowTcpForwarding` and `PermitTunnel` on your bastion host SSH
    configuration file.

    On your SSH bastion host, open the SSH config file (usually located
    at `/etc/ssh/sshd_config`) using a text editor:

    <div class="highlight">

    ``` chroma
    sudo nano /etc/ssh/sshd_config
    ```

    </div>

    Add or uncomment the following lines:

    <div class="highlight">

    ``` chroma
    AllowTcpForwarding yes
    PermitTunnel yes
    ```

    </div>

    Save the changes and restart the SSH service:

    <div class="highlight">

    ``` chroma
    sudo systemctl restart sshd
    ```

    </div>

6.  Ensure materialize cluster pods have network access to your SSH
    bastion host.

7.  Validate the SSH tunnel connection

    To confirm that the SSH tunnel connection is correctly configured,
    use the [`VALIDATE CONNECTION`](/docs/sql/validate-connection)
    command:

    <div class="highlight">

    ``` chroma
    VALIDATE CONNECTION ssh_connection;
    ```

    </div>

    If no validation errors are returned, the connection can be used to
    create a source connection.

8.  In Materialize, create a source connection that uses the SSH tunnel
    connection you configured in the previous section:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION kafka_connection TO KAFKA (
      BROKER 'broker1:9092',
      SSH TUNNEL ssh_connection
    );
    ```

    </div>

</div>

<div id="tab-public-cluster" class="tab-pane" title="Public cluster">

This section goes through the required steps to connect Materialize to
an Amazon MSK cluster, including some of the more complicated bits
around configuring security settings in Amazon MSK.

If you already have an Amazon MSK cluster, you can skip step 1 and
directly move on to **Make the cluster public and enable SASL** step.
You can also skip steps 3 and 4 if you already have Apache Kafka
installed and running, and have created a topic that you want to create
a source for.

The process to connect Materialize to Amazon MSK consists of the
following steps:

1.  Create an Amazon MSK cluster. If you already have an Amazon MSK
    cluster set up, then you can skip this step.

    a\. Sign in to the AWS Management Console and open the [Amazon MSK
    console](https://console.aws.amazon.com/msk/)

    b\. Choose **Create cluster**

    c\. Enter a cluster name, and leave all other settings unchanged

    d\. From the table under **All cluster settings**, copy the values
    of the following settings and save them because you need them later
    in this tutorial: **VPC**, **Subnets**, **Security groups associated
    with VPC**

    e\. Choose **Create cluster**

    **Note:** This creation can take about 15 minutes.

2.  Make the cluster public and enable SASL.

    ##### Turn on SASL

    a\. Navigate to the [Amazon MSK
    console](https://console.aws.amazon.com/msk/)

    b\. Choose the MSK cluster you just created in Step 1

    c\. Click on the **Properties** tab

    d\. In the **Security settings** section, choose **Edit**

    e\. Check the checkbox next to **SASL/SCRAM authentication**

    f\. Click **Save changes**

    You can find more details about updating a cluster’s security
    configurations
    [here](https://docs.aws.amazon.com/msk/latest/developerguide/msk-update-security.html).

    ##### Create a symmetric key

    a\. Now go to the [AWS Key Management Service (AWS KMS)
    console](https://console.aws.amazon.com/kms)

    b\. Click **Create Key**

    c\. Choose **Symmetric** and click **Next**

    d\. Give the key and **Alias** and click **Next**

    e\. Under Administrative permissions, check the checkbox next to the
    **AWSServiceRoleForKafka** and click **Next**

    f\. Under Key usage permissions, again check the checkbox next to
    the **AWSServiceRoleForKafka** and click **Next**

    g\. Click on **Create secret**

    h\. Review the details and click **Finish**

    You can find more details about creating a symmetric key
    [here](https://docs.aws.amazon.com/kms/latest/developerguide/create-keys.html#create-symmetric-cmk).

    ##### Store a new Secret

    a\. Go to the [AWS Secrets Manager
    console](https://console.aws.amazon.com/secretsmanager/)

    b\. Click **Store a new secret**

    c\. Choose **Other type of secret** (e.g. API key) for the secret
    type

    d\. Under **Key/value pairs** click on **Plaintext**

    e\. Paste the following in the space below it and replace
    `<your-username>` and `<your-password>` with the username and
    password you want to set for the cluster

    ```
      {
        "username": "<your-username>",
        "password": "<your-password>"
      }
    ```

    f\. On the next page, give a **Secret name** that starts with
    `AmazonMSK_`

    g\. Under **Encryption Key**, select the symmetric key you just
    created in the previous sub-section from the dropdown

    h\. Go forward to the next steps and finish creating the secret.
    Once created, record the ARN (Amazon Resource Name) value for your
    secret

    You can find more details about creating a secret using AWS Secrets
    Manager
    [here](https://docs.aws.amazon.com/msk/latest/developerguide/msk-password.html).

    ##### Associate secret with MSK cluster

    a\. Navigate back to the [Amazon MSK
    console](https://console.aws.amazon.com/msk/) and click on the
    cluster you created in Step 1

    b\. Click on the **Properties** tab

    c\. In the **Security settings** section, under **SASL/SCRAM
    authentication**, click on **Associate secrets**

    d\. Paste the ARN you recorded in the previous subsection and click
    **Associate secrets**

    ##### Create the cluster’s configuration

    a\. Go to the [Amazon CloudShell
    console](https://console.aws.amazon.com/cloudshell/)

    b\. Create a file (eg. *msk-config.txt*) with the following line

    ```
      allow.everyone.if.no.acl.found = false
    ```

    c\. Run the following AWS CLI command, replacing
    `<config-file-path>` with the path to the file where you saved your
    configuration in the previous step

    ```
      aws kafka create-configuration --name "MakePublic" \
      --description "Set allow.everyone.if.no.acl.found = false" \
      --kafka-versions "2.6.2" \
      --server-properties fileb://<config-file-path>/msk-config.txt
    ```

    You can find more information about making your cluster public
    [here](https://docs.aws.amazon.com/msk/latest/developerguide/public-access.html).

3.  If you already have a client machine set up that can interact with
    your cluster, then you can skip this step.

    If not, you can create an EC2 client machine and then add the
    security group of the client to the inbound rules of the cluster’s
    security group from the VPC console. You can find more details about
    how to do that
    [here](https://docs.aws.amazon.com/msk/latest/developerguide/create-client-machine.html).

4.  Install Apache Kafka and create a topic. To start using Materialize
    with Apache Kafka, you need to create a Materialize source over an
    Apache Kafka topic. If you already have Apache Kafka installed and a
    topic created, you can skip this step.

    Otherwise, you can install Apache Kafka on your client machine from
    the previous step and create a topic. You can find more information
    about how to do that
    [here](https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html).

5.  Create ACLs. As `allow.everyone.if.no.acl.found` is set to `false`,
    you must create ACLs for the cluster and topics configured in the
    previous step to set appropriate access permissions. For more
    information, see the [Amazon
    MSK](https://docs.aws.amazon.com/msk/latest/developerguide/msk-acls.html)
    documentation.

6.  Create a connection in Materialize.

    a\. Open the [Amazon MSK
    console](https://console.aws.amazon.com/msk/) and select your
    cluster

    b\. Click on **View client information**

    c\. Copy the url under **Private endpoint** and against
    **SASL/SCRAM**. This will be your `<broker-url>` going forward.

    d\. Connect to Materialize using the [SQL Shell](/docs/console/), or
    your preferred SQL client.

    e\. Create a connection using the command below. The broker URL is
    what you copied in step c of this subsection. The `<topic-name>` is
    the name of the topic you created in Step 4. The `<your-username>`
    and `<your-password>` is from *Store a new secret* under Step 2.

    <div class="highlight">

    ``` chroma
    CREATE SECRET msk_password AS '<your-password>';

    CREATE CONNECTION kafka_connection TO KAFKA (
        BROKER '<broker-url>',
        SASL MECHANISMS = 'SCRAM-SHA-512',
        SASL USERNAME = '<your-username>',
        SASL PASSWORD = SECRET msk_password
      );
    ```

    </div>

</div>

</div>

</div>

</div>

</div>

</div>

## Creating a source

The Kafka connection created in the previous section can then be reused
across multiple [`CREATE SOURCE`](/docs/sql/create-source/kafka/)
statements. By default, the source will be created in the active
cluster; to use a different cluster, use the `IN CLUSTER` clause.

<div class="highlight">

``` chroma
CREATE SOURCE json_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT JSON;
```

</div>

If the command executes without an error and outputs *CREATE SOURCE*, it
means that you have successfully connected Materialize to your cluster.

## Related pages

- [`CREATE SECRET`](/docs/sql/create-secret)
- [`CREATE CONNECTION`](/docs/sql/create-connection)
- [`CREATE SOURCE`: Kafka](/docs/sql/create-source/kafka)

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/ingest-data/kafka/amazon-msk.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
