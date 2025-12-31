---
audience: developer
canonical_url: https://materialize.com/docs/ingest-data/kafka/
complexity: advanced
description: Connecting Materialize to a Kafka source.
doc_type: reference
keywords:
- Kafka 3.2+
- Upsert
- 'Tip:'
- CREATE A
- CREATE SOURCE
- Append-only
- Debezium
- Kafka
product_area: Sources
status: stable
title: Kafka
---

# Kafka

## Purpose
Connecting Materialize to a Kafka source.

If you need to understand the syntax and options for this command, you're in the right place.


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
- Protobuf
- CSV
- Plain text
- Raw bytes
- JSON

## Envelopes

What Materialize actually does with the data it receives depends on the
"envelope" your data provides:

Envelope | Action
---------|-------
**Append-only** | Inserts all received data; does not support updates or deletes.
**Debezium** | Treats data as wrapped in a "diff envelope" that indicates whether the record is an insertion, deletion, or update. The Debezium envelope is only supported by sources published to Kafka by [Debezium].<br/><br/>For more information, see [`CREATE SOURCE`: Kafka - Using Debezium](/sql/create-source/kafka/#using-debezium).
**Upsert** | Treats data as having a key and a value. New records with non-null value that have the same key as a preexisting record in the dataflow will replace the preexisting record. New records with null value that have the same key as preexisting record will cause the preexisting record to be deleted. <br/><br/>For more information, see [`CREATE SOURCE`: Kafka - Handling upserts](/sql/create-source/kafka/#handling-upserts).


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

> **Tip:** 


## Before you begin

Before you begin, you must have:

- An Amazon MSK cluster running on AWS.
- A client machine that can interact with your cluster.

## Creating a connection


This section covers creating a connection.

#### Cloud

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


#### PrivateLink

> **Note:** 
Materialize provides a Terraform module that automates the creation and
configuration of AWS resources for a PrivateLink connection. For more details,
see the Terraform module repositories for [Amazon MSK](https://github.com/MaterializeInc/terraform-aws-msk-privatelink)
and [self-managed Kafka clusters](https://github.com/MaterializeInc/terraform-aws-kafka-privatelink).


<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: network-security/privatelink-kafka --> --> -->

#### SSH Tunnel

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: network-security/ssh-tunnel --> --> -->

1. In Materialize, create a source connection that uses the SSH tunnel
   connection you configured in the previous section:

   ```mzsql
   CREATE CONNECTION kafka_connection TO KAFKA (
     BROKER 'broker1:9092',
     SSH TUNNEL ssh_connection
   );
   ```bash

#### Public cluster

This section goes through the required steps to connect Materialize to an Amazon MSK cluster, including some of the more complicated bits around configuring security settings in Amazon MSK.

If you already have an Amazon MSK cluster, you can skip step 1 and directly move
on to **Make the cluster public and enable SASL** step. You can also skip steps
3 and 4 if you already have Apache Kafka installed and running, and have created
a topic that you want to create a source for.

The process to connect Materialize to Amazon MSK consists of the following steps:
1. Create an Amazon MSK cluster. If you already have an Amazon MSK cluster set
   up, then you can skip this step.

    a. Sign in to the AWS Management Console and open the [Amazon MSK console](https://console.aws.amazon.com/msk/)

    b. Choose **Create cluster**

    c. Enter a cluster name, and leave all other settings unchanged

    d. From the table under **All cluster settings**, copy the values of the following settings and save them because you need them later in this tutorial: **VPC**, **Subnets**, **Security groups associated with VPC**

    e. Choose **Create cluster**

    **Note:** This creation can take about 15 minutes.

1. Make the cluster public and enable SASL.
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
      ```json
        {
          "username": "<your-username>",
          "password": "<your-password>"
        }
      ```text

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
      ```text
        allow.everyone.if.no.acl.found = false
      ```text

    c. Run the following AWS CLI command, replacing `<config-file-path>` with the path to the file where you saved your configuration in the previous step
    ```text
      aws kafka create-configuration --name "MakePublic" \
      --description "Set allow.everyone.if.no.acl.found = false" \
      --kafka-versions "2.6.2" \
      --server-properties fileb://<config-file-path>/msk-config.txt
    ```text

    You can find more information about making your cluster public [here](https://docs.aws.amazon.com/msk/latest/developerguide/public-access.html).

1. If you already have a client machine set up that can interact with your
   cluster, then you can skip this step.

    If not, you can create an EC2 client machine and then add the security group of the client to the inbound rules of the cluster's security group from the VPC console. You can find more details about how to do that [here](https://docs.aws.amazon.com/msk/latest/developerguide/create-client-machine.html).

1. Install Apache Kafka and create a topic. To start using Materialize with
    Apache Kafka, you need to create a Materialize source over an Apache Kafka
    topic. If you already have Apache Kafka installed and a topic created, you
    can skip this step.

    Otherwise, you can install Apache Kafka on your client machine from the previous step and create a topic. You can find more information about how to do that [here](https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html).

1. Create ACLs. As `allow.everyone.if.no.acl.found` is set to `false`, you must
    create ACLs for the cluster and topics configured in the previous step to
    set appropriate access permissions. For more information, see the [Amazon
    MSK](https://docs.aws.amazon.com/msk/latest/developerguide/msk-acls.html)
    documentation.


1. Create a connection in Materialize.

    a. Open the [Amazon MSK console](https://console.aws.amazon.com/msk/) and select your cluster

    b. Click on **View client information**

    c. Copy the url under **Private endpoint** and against **SASL/SCRAM**. This will be your `<broker-url>` going forward.

    d. Connect to Materialize using the [SQL Shell](/console/),
       or your preferred SQL client.

    e. Create a connection using the command below. The broker URL is what you copied in step c of this subsection. The `<topic-name>` is the name of the topic you created in Step 4. The `<your-username>` and `<your-password>` is from _Store a new secret_ under Step 2.

      ```mzsql
      CREATE SECRET msk_password AS '<your-password>';

      CREATE CONNECTION kafka_connection TO KAFKA (
          BROKER '<broker-url>',
          SASL MECHANISMS = 'SCRAM-SHA-512',
          SASL USERNAME = '<your-username>',
          SASL PASSWORD = SECRET msk_password
        );
      ```json

#### Self-Managed

Configure your Kafka network to allow Materialize to connect:

- **Use an SSH tunnel**: If your Kafka cluster is running in a private network, you can use an SSH tunnel to connect Materialize to the cluster.

- **Allow Materialize IPs**: If your Kafka cluster is publicly accessible, you can configure your firewall to allow connections from a set of static Materialize IP addresses.

#### SSH Tunnel

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: network-security/ssh-tunnel-sm --> --> -->

1. In Materialize, create a source connection that uses the SSH tunnel
   connection you configured in the previous section:

   ```mzsql
   CREATE CONNECTION kafka_connection TO KAFKA (
     BROKER 'broker1:9092',
     SSH TUNNEL ssh_connection
   );
   ```bash

#### Public cluster

This section goes through the required steps to connect Materialize to an Amazon MSK cluster, including some of the more complicated bits around configuring security settings in Amazon MSK.

If you already have an Amazon MSK cluster, you can skip step 1 and directly move
on to **Make the cluster public and enable SASL** step. You can also skip steps
3 and 4 if you already have Apache Kafka installed and running, and have created
a topic that you want to create a source for.

The process to connect Materialize to Amazon MSK consists of the following steps:
1. Create an Amazon MSK cluster. If you already have an Amazon MSK cluster set
   up, then you can skip this step.

    a. Sign in to the AWS Management Console and open the [Amazon MSK console](https://console.aws.amazon.com/msk/)

    b. Choose **Create cluster**

    c. Enter a cluster name, and leave all other settings unchanged

    d. From the table under **All cluster settings**, copy the values of the following settings and save them because you need them later in this tutorial: **VPC**, **Subnets**, **Security groups associated with VPC**

    e. Choose **Create cluster**

    **Note:** This creation can take about 15 minutes.

1. Make the cluster public and enable SASL.
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
      ```json
        {
          "username": "<your-username>",
          "password": "<your-password>"
        }
      ```text

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
      ```text
        allow.everyone.if.no.acl.found = false
      ```text

    c. Run the following AWS CLI command, replacing `<config-file-path>` with the path to the file where you saved your configuration in the previous step
    ```text
      aws kafka create-configuration --name "MakePublic" \
      --description "Set allow.everyone.if.no.acl.found = false" \
      --kafka-versions "2.6.2" \
      --server-properties fileb://<config-file-path>/msk-config.txt
    ```text

    You can find more information about making your cluster public [here](https://docs.aws.amazon.com/msk/latest/developerguide/public-access.html).

1. If you already have a client machine set up that can interact with your
   cluster, then you can skip this step.

    If not, you can create an EC2 client machine and then add the security group of the client to the inbound rules of the cluster's security group from the VPC console. You can find more details about how to do that [here](https://docs.aws.amazon.com/msk/latest/developerguide/create-client-machine.html).

1. Install Apache Kafka and create a topic. To start using Materialize with
    Apache Kafka, you need to create a Materialize source over an Apache Kafka
    topic. If you already have Apache Kafka installed and a topic created, you
    can skip this step.

    Otherwise, you can install Apache Kafka on your client machine from the previous step and create a topic. You can find more information about how to do that [here](https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html).

1. Create ACLs. As `allow.everyone.if.no.acl.found` is set to `false`, you must
    create ACLs for the cluster and topics configured in the previous step to
    set appropriate access permissions. For more information, see the [Amazon
    MSK](https://docs.aws.amazon.com/msk/latest/developerguide/msk-acls.html)
    documentation.


1. Create a connection in Materialize.

    a. Open the [Amazon MSK console](https://console.aws.amazon.com/msk/) and select your cluster

    b. Click on **View client information**

    c. Copy the url under **Private endpoint** and against **SASL/SCRAM**. This will be your `<broker-url>` going forward.

    d. Connect to Materialize using the [SQL Shell](/console/),
       or your preferred SQL client.

    e. Create a connection using the command below. The broker URL is what you copied in step c of this subsection. The `<topic-name>` is the name of the topic you created in Step 4. The `<your-username>` and `<your-password>` is from _Store a new secret_ under Step 2.

      ```mzsql
      CREATE SECRET msk_password AS '<your-password>';

      CREATE CONNECTION kafka_connection TO KAFKA (
          BROKER '<broker-url>',
          SASL MECHANISMS = 'SCRAM-SHA-512',
          SASL USERNAME = '<your-username>',
          SASL PASSWORD = SECRET msk_password
        );
      ```json


## Creating a source

The Kafka connection created in the previous section can then be reused across
multiple [`CREATE SOURCE`](/sql/create-source/kafka/) statements. By default,
the source will be created in the active cluster; to use a different cluster,
use the `IN CLUSTER` clause.

```mzsql
CREATE SOURCE json_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT JSON;
```text

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

> **Tip:** 


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
    ```text
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
    formatted in e.g. [Avro and Protobuf](/sql/create-source/kafka/#supported-formats).
    You can find more details about the various different supported formats and
    possible configurations in the [reference documentation](/sql/create-source/kafka/).


---

## Ingest data from Self-hosted Kafka


[//]: # "TODO(morsapaes) The Kafka guides need to be rewritten for consistency
with the Postgres ones. We should include spill to disk in the guidance then."

This guide goes through the required steps to connect Materialize to a
self-hosted Kafka cluster.

> **Tip:** 


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

#### Cloud


#### Privatelink

> **Note:** 
Materialize provides Terraform modules for both [Amazon MSK clusters](https://github.com/MaterializeInc/terraform-aws-msk-privatelink)
and [self-managed Kafka clusters](https://github.com/MaterializeInc/terraform-aws-kafka-privatelink)
which can be used to create the target groups for each Kafka broker (step 1),
the network load balancer (step 2), the TCP listeners (step 3) and the VPC
endpoint service (step 5).


<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: network-security/privatelink-kafka --> --> -->

#### SSH Tunnel

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: network-security/ssh-tunnel --> --> -->

1. In Materialize, create a source connection that uses the SSH tunnel
connection you configured in the previous section:

  ```mzsql
  CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'broker1:9092',
    SSH TUNNEL ssh_connection
  );
```bash

#### Allow Materialize IPs

1. In the [SQL Shell](/console/), or your preferred SQL
   client connected to Materialize, find the static egress IP addresses for the
   Materialize region you are running in:

    ```mzsql
    SELECT * FROM mz_egress_ips;
    ```text

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
    ```json

#### Self-Managed


There are various ways to configure your Kafka network to allow Materialize to
connect:

- **Use an SSH tunnel**: If your Kafka cluster is running in a private network,
    you can use an SSH tunnel to connect Materialize to the cluster.

- **Allow Materialize IPs**: If your Kafka cluster is publicly accessible, you
    can configure your firewall to allow connections from a set of static
    Materialize IP addresses.

Select the option that works best for you.

#### SSH Tunnel

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: network-security/ssh-tunnel-sm --> --> -->


1. In Materialize, create a source connection that uses the SSH tunnel
connection you configured in the previous section:

```mzsql
CREATE CONNECTION kafka_connection TO KAFKA (
  BROKER 'broker1:9092',
  SSH TUNNEL ssh_connection
);
```bash

#### Allow Materialize IPs

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
    ```json


## Creating a source

The Kafka connection created in the previous section can then be reused across
multiple [`CREATE SOURCE`](/sql/create-source/kafka/) statements:

```mzsql
CREATE SOURCE json_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT JSON;
```text

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

> **Tip:** 


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
    ```text

    Change the `bootstrap-host` to the name of your WarpStream cluster on
    Fly.io.

    e. Create the `materialize_click_streams` topic:

    ```bash
    warpstream kcmd -bootstrap-host $CLUSTER_NAME.fly.dev \
                    -tls -username ccun_XXXXXXXXX \
                    -password ccp_XXXXXXXXXX \
                    -type create-topic \
                    -topic materialize_click_streams
    ```text

    f. Generate and push sample records for testing:

    ```bash
    warpstream kcmd -bootstrap-host $CLUSTER_NAME.fly.dev \
                    -tls -username ccun_XXXXXXXXXX \
                    -password ccp_XXXXXXXXXX \
                    -type produce \
                    -topic materialize_click_streams \
                    --records '{"action": "click", "user_id": "user_0", "page_id": "home"},,{"action": "hover", "user_id": "user_0", "page_id": "home"},,{"action": "scroll", "user_id": "user_0", "page_id": "home"}'
    ```text

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
    ```text

    b. Set up a connection to the WarpStream broker:

    ```mzsql
    CREATE CONNECTION warpstream_kafka TO KAFKA (
        BROKER '<CLUSTER_NAME>.fly.dev:9092',
        SASL MECHANISMS = "PLAIN",
        SASL USERNAME = SECRET warpstream_username,
        SASL PASSWORD = SECRET warpstream_password
    );
    ```text

    c. Create a source in Materialize to consume messages. By default, the
    source will be created in the active cluster; to use a different cluster,
    use the `IN CLUSTER` clause.

    ```mzsql
    CREATE SOURCE warpstream_click_stream_source
        FROM KAFKA CONNECTION warpstream_kafka (TOPIC 'materialize_click_streams')
        FORMAT JSON;
    ```text

    d. Verify the ingestion and query the data in Materialize:

    ```mzsql
    SELECT * FROM warpstream_click_stream_source LIMIT 10;
    ```text

    e. Furthermore, create a materialized view to aggregate the data:

    ```mzsql
    CREATE MATERIALIZED VIEW warpstream_click_stream_aggregate AS
        SELECT
            user_id,
            page_id,
            COUNT(*) AS count
        FROM warpstream_click_stream_source
        GROUP BY user_id, page_id;
    ```text

    f. Produce additional records to monitor real-time updates:

    ```bash
    warpstream kcmd -bootstrap-host $CLUSTER_NAME.fly.dev \
                    -tls -username ccun_XXXXXXXXXX \
                    -password ccp_XXXXXXXXXX \
                    -type produce \
                    -topic materialize_click_streams \
                    --records '{"action": "click", "user_id": "user_1", "page_id": "home"}'
    ```text

    g. Query the materialized view to monitor the real-time updates:

    ```mzsql
    SELECT * FROM warpstream_click_stream_aggregate;
    ```

---

By following the steps outlined above, you will have successfully set up a
connection between WarpStream and Materialize. You can now use Materialize to
query the data ingested from WarpStream.