---
title: "Ingest data from Amazon Kafka"
description: "How to connect Materialize to a Kafka broker an AWS"
aliases:
  - /integrations/aws-kafka/
  - /integrations/amazon-kafka/
  - /connect-sources/amazon-kafka/
menu:
  main:
    parent: "kafka"
    name: "Amazon Kafka"
---

This guide goes through the required steps to connect Materialize to an Amazon Kafka cluster, including MSK and self-managed Kafka clusters.


## Before you begin

Before you begin, you must have:

- Kafka Cluster running on AWS. Both [MSK](https://aws.amazon.com/msk/) and self-managed Kafka clusters are supported.
- TODO

## Creating a connection

There are three ways to connect to a Kafka cluster running on AWS:

- AWS PrivateLink
- SSH Tunnel
- Public Cluster

Follow the instructions in the section that best matches your use case.

{{< tabs tabID="1" >}}

# Privatelink
{{< tab "Privatelink">}}

This section covers how to create `AWS PRIVATELINK` connections
and retrieve the AWS principal needed to configure the AWS PrivateLink service.

{{< note >}}
Materialize provides Terraform modules for both [MSK cluster](https://github.com/MaterializeInc/terraform-aws-msk-privatelink) and [self-managed Kafka clusters](https://github.com/MaterializeInc/terraform-aws-kafka-privatelink) which can be used to create the target groups for each Kafka broker (step 1), the network load balancer (step 2),
the TCP listeners (step 3) and the VPC endpoint service (step 5).
{{< /note >}}

1. #### Create target groups
    Create a dedicated [target group](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html) **for each broker** with the following details:

    a. Target type as **IP address**.

    b. Protocol as **TCP**.

    c. Port as **9092**, or the port that you are using in case it is not 9092 (e.g. 9094 for TLS or 9096 for SASL).

    d. Make sure that the target group is in the same VPC as the Kafka cluster.

    e. Click next, and register the respective Kafka broker to each target group using its IP address.

1. #### Create a Network Load Balancer (NLB)
    Create a [Network Load Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html) that is **enabled for the same subnets** that the Kafka brokers are in.

1. #### Create TCP listeners

    Create a [TCP listener](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-listener.html) for every Kafka broker that forwards to the corresponding target group you created (e.g. `b-1`, `b-2`, `b-3`).

    The listener port needs to be unique, and will be used later on in the `CREATE CONNECTION` statement.

    For example, you can create a listener for:

    a. Port `9001` → broker `b-1...`.

    b. Port `9002` → broker `b-2...`.

    c. Port `9003` → broker `b-3...`.

1. #### Verify security groups and health checks

    Once the TCP listeners have been created, make sure that the [health checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html) for each target group are passing and that the targets are reported as healthy.

    If you have set up a security group for your Kafka cluster, you must ensure that it allows traffic on both the listener port and the health check port.

    **Remarks**:

    a. Network Load Balancers do not have associated security groups. Therefore, the security groups for your targets must use IP addresses to allow traffic.

    b. You can't use the security groups for the clients as a source in the security groups for the targets. Therefore, the security groups for your targets must use the IP addresses of the clients to allow traffic. For more details, check the [AWS documentation](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html).

1. #### Create a VPC endpoint service

    Create a VPC [endpoint service](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html) and associate it with the **Network Load Balancer** that you’ve just created.

    Note the **service name** that is generated for the endpoint service.

1. #### Create an AWS PrivateLink Connection
     In Materialize, create a [`AWS PRIVATELINK`](/sql/create-connection/#aws-privatelink) connection that references the endpoint service that you created in the previous step.

     ```sql
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
        SERVICE NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
        AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az3')
    );
    ```

    Update the list of the availability zones to match the ones in your AWS account.

## Configure the AWS PrivateLink service

1. Retrieve the AWS principal for the AWS PrivateLink connection you just created:

    ```sql
    SELECT principal
    FROM mz_aws_privatelink_connections plc
    JOIN mz_connections c ON plc.id = c.id
    WHERE c.name = 'privatelink_svc';
    ```

    ```
       id   |                                 principal
    --------+---------------------------------------------------------------------------
     u1     | arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
    ```

    Follow the instructions in the [AWS PrivateLink documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/add-endpoint-service-permissions.html)
    to configure your VPC endpoint service to accept connections from the
    provided AWS principal.

1. If your AWS PrivateLink service is configured to require acceptance of connection requests, you must manually approve the connection request from Materialize after executing `CREATE CONNECTION`. For more details, check the [AWS PrivateLink documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).

    **Note:** It might take some time for the endpoint service connection to show up, so you would need to wait for the endpoint service connection to be ready before you create a source.

## Create a source connection

In Materialize, create a source connection that uses the AWS PrivateLink connection you just configured:

```sql
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKERS (
        'b-1.hostname-1:9096' USING AWS PRIVATELINK privatelink_svc (PORT 9001, AVAILABILITY ZONE 'use1-az2'),
        'b-2.hostname-2:9096' USING AWS PRIVATELINK privatelink_svc (PORT 9002, AVAILABILITY ZONE 'use1-az1'),
        'b-3.hostname-3:9096' USING AWS PRIVATELINK privatelink_svc (PORT 9003, AVAILABILITY ZONE 'use1-az3')
    ),
    -- Authentication details
    -- Depending on the authentication method the Kafka cluster is using
    SASL MECHANISMS = 'SCRAM-SHA-512',
    SASL USERNAME = 'foo',
    SASL PASSWORD = SECRET kafka_password
);
```

The `(PORT <port_number>)` value must match the port that you used when creating the **TCP listener** in the Network Load Balancer. Be sure to specify the correct availability zone for each broker.


{{< /tab >}}

# SSH
{{< tab "SSH Tunnel">}}

Materialize can connect to data sources like Kafka, Confluent, and PostgreSQL with a
secure SSH bastion server. This section covers how to create an `SSH TUNNEL`
connection and configure your Materialize authentication settings.

## Prerequisites

Before you begin, make sure you have access to a bastion host. You will need:

* The bastion host IP address and port number
* The bastion host username

## Create an SSH tunnel connection

In Materialize, use a `CREATE CONNECTION` statement to create an [SSH tunnel connection](/sql/create-connection/#ssh-tunnel) to the bastion server:

```sql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);
```

## Configure the SSH bastion server

The bastion host needs a **public key** to connect to the Materialize tunnel you
created in the previous step.

1. Materialize stores public keys for SSH tunnels. Use a `SELECT` statement to
   return two public keys:

    ```sql
    SELECT * FROM mz_ssh_tunnel_connections;
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

3. Configure your internal firewall to allow the SSH bastion host to connect to your Kafka cluster.

    If you are using a cloud provider like AWS or GCP, update the security group or firewall rules for your Kafka brokers.

    Allow incoming traffic from the SSH bastion host IP address on the necessary ports.

    For example, use ports `9092`, `9094`, and `9096` for Kafka.

    Test the connection from the bastion host to the Kafka cluster.

    ```bash
    telnet <KAFKA_BROKER_HOST> <KAFKA_BROKER_PORT>
    ```

    If the command hangs, double-check your security group and firewall settings. If the connection is successful, you can proceed to the next step.

4. Verify the tunnel connection from your source to your SSH bastion host

    ```bash
    # Command for Linux
    ssh -L 9092:kafka-broker:9092 <SSH_BASTION_USER>@<SSH_BASTION_HOST>
    ```

    Verify that you can connect to the Kafka broker via the tunnel:

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

5. Retrieve the static egress IPs from Materialize and configure the firewall rules (e.g. AWS Security Groups) for your SSH bastion to allow SSH traffic for those IP addresses only.

    ```sql
    SELECT * FROM mz_catalog.mz_egress_ips;
    ```

    ```
    XXX.140.90.33
    XXX.198.159.213
    XXX.100.27.23
    ```

## Create a source connection

In Materialize, create a source connection that uses the SSH tunnel connection you configured in the previous section:

```sql
CREATE CONNECTION kafka_connection TO KAFKA (
BROKERS (
    'broker1:9092' USING SSH TUNNEL ssh_connection,
    'broker2:9092' USING SSH TUNNEL ssh_connection
    -- Add all Kafka brokers
    )
);
```

{{< /tab >}}

# Public Cluster
{{< tab "Public Cluster">}}

This section goes through the required steps to connect Materialize to an Amazon MSK cluster, including some of the more complicated bits around configuring security settings in Amazon MSK.

If you already have an Amazon MSK cluster, you can skip step 1 and directly move on to [Make the cluster public and enable SASL](#make-the-cluster-public-and-enable-sasl). You can also skip steps 3 and 4 if you already have Apache Kafka installed and running, and have created a topic that you want to create a source for.

The process to connect Materialize to Amazon MSK consists of the following steps:
1. #### Create an Amazon MSK cluster
    If you already have an Amazon MSK cluster set up, then you can skip this step.

    a. Sign in to the AWS Management Console and open the [Amazon MSK console](https://console.aws.amazon.com/msk/)

    b. Choose **Create cluster**

    c. Enter a cluster name, and leave all other settings unchanged

    d. From the table under **All cluster settings**, copy the values of the following settings and save them because you need them later in this tutorial: **VPC**, **Subnets**, **Security groups associated with VPC**

    e. Choose **Create cluster**

    **Note:** This creation can take about 15 minutes.

2. #### Make the cluster public and enable SASL
    ##### Turn on SASL
    a. Navigate to the [Amazon MSK console](https://console.aws.amazon.com/msk/)

    b. Choose the MSK cluster you just created in Step 1

    c. Click on the **Properties** tab

    d. In the **Security settings** section, choose **Edit**

    e. Check the checkbox next to **SASL/SCRAM authentication**

    f. Click **Save changes**

    You can find more details about updating a cluster's security configurations [here](https://docs.aws.amazon.com/msk/latest/developerguide/msk-update-security.html).

    ##### Create a symmetric key
    a. Now go to the [AWS Key Management Service (AWS KMS) console](https://console.aws.amazon.com/kms)

    b. Click **Create Key**

    c. Choose **Symmetric** and click **Next**

    d. Give the key and **Alias** and click **Next**

    e. Under Administrative permissions, check the checkbox next to the **AWSServiceRoleForKafka** and click **Next**

    f. Under Key usage permissions, again check the checkbox next to the **AWSServiceRoleForKafka** and click **Next**

    g. Click on **Create secret**

    h. Review the details and click **Finish**

    You can find more details about creating a symmetric key [here](https://docs.aws.amazon.com/kms/latest/developerguide/create-keys.html#create-symmetric-cmk).

    ##### Store a new Secret
    a. Go to the [AWS Secrets Manager console](https://console.aws.amazon.com/secretsmanager/)

    b. Click **Store a new secret**

    c. Choose **Other type of secret** (e.g. API key) for the secret type

    d. Under **Key/value pairs** click on **Plaintext**

    e. Paste the following in the space below it and replace `<your-username>` and `<your-password>` with the username and password you want to set for the cluster
      ```
        {
          "username": "<your-username>",
          "password": "<your-password>"
        }
      ```

    f. On the next page, give a **Secret name** that starts with `AmazonMSK_`

    g. Under **Encryption Key**, select the symmetric key you just created in the previous sub-section from the dropdown

    h. Go forward to the next steps and finish creating the secret. Once created, record the ARN (Amazon Resource Name) value for your secret

    You can find more details about creating a secret using AWS Secrets Manager [here](https://docs.aws.amazon.com/msk/latest/developerguide/msk-password.html).

    ##### Associate secret with MSK cluster
    a. Navigate back to the [Amazon MSK console](https://console.aws.amazon.com/msk/) and click on the cluster you created in Step 1

    b. Click on the **Properties** tab

    c. In the **Security settings** section, under **SASL/SCRAM authentication**, click on **Associate secrets**

    d. Paste the ARN you recorded in the previous subsection and click **Associate secrets**

    ##### Create the cluster's configuration
    a. Go to the [Amazon CloudShell console](https://console.aws.amazon.com/cloudshell/)

    b. Create a file (eg. _msk-config.txt_) with the following line
      ```
        allow.everyone.if.no.acl.found = false
      ```

    c. Run the following AWS CLI command, replacing `<config-file-path>` with the path to the file where you saved your configuration in the previous step
    ```
      aws kafka create-configuration --name "MakePublic" \
      --description "Set allow.everyone.if.no.acl.found = false" \
      --kafka-versions "2.6.2" \
      --server-properties fileb://<config-file-path>/msk-config.txt
    ```


    You can find more information about making your cluster public [here](https://docs.aws.amazon.com/msk/latest/developerguide/public-access.html).

3. #### Create a client machine
    If you already have a client machine set up that can interact with your cluster, then you can skip this step.

    If not, you can create an EC2 client machine and then add the security group of the client to the inbound rules of the cluster's security group from the VPC console. You can find more details about how to do that [here](https://docs.aws.amazon.com/msk/latest/developerguide/create-client-machine.html).

4. #### Install Apache Kafka and create a topic
    To start using Materialize with Apache Kafka, you need to create a Materialize source over an Apache Kafka topic. If you already have Apache Kafka installed and a topic created, you can skip this step.

    Otherwise, you can install Apache Kafka on your client machine from the previous step and create a topic. You can find more information about how to do that [here](https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html).

5. #### Create ACLs
    As `allow.everyone.if.no.acl.found` is set to `false`, you must create ACLs for the cluster and topics configured in the previous step to set appropriate access permissions. For more information, see the [Amazon MSK](https://docs.aws.amazon.com/msk/latest/developerguide/msk-acls.html) documentation.


6. #### Create a source in Materialize
    a. Open the [Amazon MSK console](https://console.aws.amazon.com/msk/) and select your cluster

    b. Click on **View client information**

    c. Copy the url under **Private endpoint** and against **SASL/SCRAM**. This will be your `<broker-url>` going forward.

    d. From a `psql` terminal, connect to Materialize.

    e. Create a connection using the command below. The broker URL is what you copied in step c of this subsection. The `<topic-name>` is the name of the topic you created in Step 4. The `<your-username>` and `<your-password>` is from _Store a new secret_ under Step 2.

      ```sql
      CREATE SECRET msk_password AS '<your-password>';

      CREATE CONNECTION kafka_connection TO KAFKA (
          BROKER '<broker-url>',
          SASL MECHANISMS = 'SCRAM-SHA-512',
          SASL USERNAME = '<your-username>',
          SASL PASSWORD = SECRET msk_password
        );
      ```

    f. If the command executes without an error and outputs _CREATE SOURCE_, it means that you have successfully connected Materialize to your cluster.

    **Note:** The example above walked through creating a source which is a way of connecting Materialize to an external data source. We created a connection to Amazon MSK using SASL authentication, using credentials securely stored as secrets in Materialize's secret management system. For input formats, we used `text`, however, Materialize supports various other options as well. For example, you can ingest messages formatted in [JSON, Avro and Protobuf](/sql/create-source/kafka/#supported-formats). You can find more details about the various different supported formats and possible configurations [here](/sql/create-source/kafka/).

{{< /tab >}}
{{< /tabs >}}

#

## Creating a source

The Kafka connection created in the previous section can then be reused across multiple [`CREATE SOURCE`](/sql/create-source/kafka/)
statements:

```sql
CREATE SOURCE json_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT BYTES
  WITH (SIZE = '3xsmall');
```

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`: Kafka](/sql/create-source/kafka)
