---
title: "Confluent Cloud"
description: "How to securely connect a Confluent Cloud Kafka cluster as a source to Materialize."
aliases:
  - /integrations/confluent-cloud/
  - /connect-sources/confuent-cloud/
  - /ingest-data/confluent-cloud/
menu:
  main:
    parent: "kafka"
    name: "Confluent Cloud"
---

[//]: # "TODO(morsapaes) The Kafka guides need to be rewritten for consistency
with the Postgres ones. We should include spill to disk in the guidance then."

This guide goes through the required steps to connect Materialize to a Confluent
Cloud Kafka cluster.

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

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

4. #### Create a connection in Materialize

    a. Open the [Confluent Cloud dashboard](https://confluent.cloud/) and select your cluster.

    b. Click on **Overview** and select **Cluster settings**.

    c. Copy the URL under **Bootstrap server**. This will be your `<broker-url>` going forward.

    d. Connect to Materialize using the [SQL Shell](/console/),
       or your preferred SQL client.

    e. Create the connection. The exact steps depend on your networking
    configuration, so start by selecting the relevant option.

{{< tabs >}}
{{< tab "Public">}}

```mzsql
CREATE SECRET confluent_username AS '<your-api-key>';
CREATE SECRET confluent_password AS '<your-api-secret>';

CREATE CONNECTION confluent_cloud TO KAFKA (
    BROKER '<confluent-broker-url>',
    SASL MECHANISMS = 'PLAIN',
    SASL USERNAME = SECRET confluent_username,
    SASL PASSWORD = SECRET confluent_password
);
```

{{< /tab >}}

{{< tab "PrivateLink">}}

[AWS PrivateLink](https://aws.amazon.com/privatelink/) lets you connect
Materialize to your Confluent Cloud cluster without exposing traffic to the
public internet.

1. In the [Confluent Cloud console](https://confluent.cloud/), navigate to
your cluster's **Networking** settings and set up a PrivateLink endpoint.
Record the **VPC Endpoint Service Name** and the **DNS domain**.

1. In the Materialize [SQL shell](/console/), create a
[PrivateLink connection](/ingest-data/network-security/privatelink/) using
the service name from the previous step. Be sure to specify **all
availability zones** of your Confluent Cloud cluster.

    ```mzsql
    CREATE CONNECTION confluent_privatelink TO AWS PRIVATELINK (
        SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
        AVAILABILITY ZONES ('use1-az1', 'use1-az4', 'use1-az6')
    );
    ```

1. Retrieve the AWS principal for the PrivateLink connection:

    ```mzsql
    SELECT principal
    FROM mz_aws_privatelink_connections plc
    JOIN mz_connections c ON plc.id = c.id
    WHERE c.name = 'confluent_privatelink';
    ```

1. In the Confluent Cloud console, add the AWS principal to the PrivateLink
access list.

1. In Materialize, validate the PrivateLink connection:

    ```mzsql
    VALIDATE CONNECTION confluent_privatelink;
    ```

    If no validation error is returned, move to the next step.

1. Create the Kafka connection. The static broker (used for bootstrapping)
does not need an `AVAILABILITY ZONE` — Materialize will find it across
availability zones. The `MATCHING` rules should specify `AVAILABILITY ZONE`
to route discovered brokers through their specific AZ endpoint. The
availability zones in the `MATCHING` rules must match the AZs where Confluent
has deployed brokers. For best results, deploy brokers across 3 AZs and
select those same AZs during the Confluent PrivateLink ingress setup.

    ```mzsql
    CREATE SECRET confluent_username AS '<your-api-key>';
    CREATE SECRET confluent_password AS '<your-api-secret>';

    CREATE CONNECTION confluent_cloud TO KAFKA (
        BROKERS (
            '<confluent-broker-url>' USING AWS PRIVATELINK confluent_privatelink,
            MATCHING '*.use1-az1.*' USING AWS PRIVATELINK confluent_privatelink (AVAILABILITY ZONE = 'use1-az1'),
            MATCHING '*.use1-az4.*' USING AWS PRIVATELINK confluent_privatelink (AVAILABILITY ZONE = 'use1-az4'),
            MATCHING '*.use1-az6.*' USING AWS PRIVATELINK confluent_privatelink (AVAILABILITY ZONE = 'use1-az6')
        ),
        SASL MECHANISMS = 'PLAIN',
        SASL USERNAME = SECRET confluent_username,
        SASL PASSWORD = SECRET confluent_password
    );
    ```

    The `MATCHING` patterns correspond to the AZ-specific DNS subdomains
    from your Confluent Cloud networking settings. Adjust the patterns and
    availability zones to match your cluster's configuration.

{{< /tab >}}
{{< /tabs >}}

5. #### Start ingesting data

    Once you have created the connection, create a source and start ingesting
    data from your topic. By default, the source will be created in the active
    cluster; to use a different cluster, use the `IN CLUSTER` clause.

    ```mzsql
    CREATE SOURCE confluent_source
        FROM KAFKA CONNECTION confluent_cloud (TOPIC '<topic-name>')
        FORMAT JSON;
    ```

    If the command executes without an error and outputs _CREATE SOURCE_, it
    means that you have successfully connected Materialize to your Confluent
    Cloud Kafka cluster.

    **Note:** The example above used `JSON`, but you can also ingest Kafka messages
    formatted in other supported formats; e.g., [Avro and CSV](/sql/create-source/kafka/#syntax).
    You can find more details about the various different supported formats and
    possible configurations in the [reference documentation](/sql/create-source/kafka/).
