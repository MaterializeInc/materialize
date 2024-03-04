---
title: "Redpanda Cloud"
description: "How to securely connect a Redpanda Cloud cluster as a source to Materialize."
aliases:
  - /integrations/redpanda-cloud/
  - /connect-sources/redpanda-cloud/
menu:
  main:
    parent: "redpanda"
    name: "Redpanda Cloud"
---

[//]: # "TODO(morsapaes) The Kafka guides need to be rewritten for consistency
with the Postgres ones."

This guide goes through the required steps to connect Materialize to a Redpanda
Cloud cluster. If you already have a Redpanda Cloud cluster up and running,
skip straight to [Step 4. Download Redpanda Broker CA certificate](#step-4-download-the-broker-ca-certificate).

## Step 1. Create a Redpanda Cloud cluster

{{< note >}}
Once created, provisioning a Redpanda Cloud cluster can take up to 15 minutes.
{{</ note >}}

1. Sign in to the [Redpanda Cloud console](https://vectorized.cloud/).

1. Choose **Add cluster**.

1. Enter a cluster name, and specify the rest of the settings based on your
needs.

1. Choose **Create cluster**.

## Step 2. Create a service account

1. Navigate to the [Redpanda Cloud console](https://vectorized.cloud/).

1. Choose the Redpanda cluster you created in **Step 1**.

1. Click on the **Security** tab.

1. In the **Security** section, choose **Add service account**.

1. Specify a name for the service account and click **Create**.

Take note of the name of the service account you just created, as well as
the service account secret key; you'll need them later on. Keep in mind
that the service account secret key contains sensitive information, and you
should store it somewhere safe!

## Step 3. Create an Access Control List

1. Next, still in the **Security** section, click on the **ACL** tab.

1. Click **Add ACL**,

1. Choose the **Service Account** you just created.

1. Choose the **Read** and **Write** permissions for the **Topics** section.

1. You can grant access on a per-topic basis, but you can also choose to grant
access to all topics by adding a `*` in the **Topics ID** input field.

1. Click **Create**.

## Step 4. Download the broker CA certificate

1. Go to the [Redpanda Cloud console](https://vectorized.cloud/).

1. Click on the **Overview** tab.

1. Click on the **Download** button next to the **Kafka API TLS**.

1. You'll need to `base64`-encode the certificate. One way to do this is to open
up a terminal and run the following command:

    ```bash
    cat <redpanda-broker-ca>.crt | base64
    ```

    Store the encoded certificate somewhere safe; you'll need it to securely
    connect to Materialize.

## Step 5. Create a topic

To start using Materialize with Redpanda, you need to point it to an existing
Redpanda topic you want to read data from. If the topic you want to ingest
already exists, you can skip this step.

Otherwise, you can install Redpanda Keeper (aka `rpk`) on the client machine
from the previous step to create a topic. For guidance on how to use `rpk`,
check the [Redpanda documentation](https://docs.redpanda.com/docs/reference/rpk-commands/#rpk-topic-create).

## Step 6. Start ingesting data

Now that you’ve configured your Redpanda cluster, you can start ingesting data
into Materialize. The exact steps depend on your networking configuration, so
start by selecting the relevant option.

{{< tabs >}}
{{< tab "Public cluster">}}

1. Open the [Redpanda Cloud console](https://vectorized.cloud/) and select your cluster.

1. Click on **Overview**

1. Copy the URL under **Cluster hosts**. This will be your
`<redpanda-broker-url>` going forward.

1. In the Materialize [SQL shell](https://console.materialize.com/), or your
preferred SQL client, create a connection with your Redpanda Cloud cluster
access and authentication details using the [`CREATE CONNECTION`](/sql/create-connection/)
command:

    ```sql
      -- The credentials of your service account.
      CREATE SECRET redpanda_username AS '<your-username>';
      CREATE SECRET redpanda_password AS '<your-password>';
      -- The base64 encoded certificate from Step 4.
      CREATE SECRET redpanda_ca_cert AS  decode('<redpanda-broker-ca-cert>', 'base64');

      CREATE CONNECTION rp_connection TO KAFKA (
          BROKER '<redpanda-broker-url>',
          SASL MECHANISMS = 'SCRAM-SHA-256',
          SASL USERNAME = SECRET redpanda_username,
          SASL PASSWORD = SECRET redpanda_password,
          SSL CERTIFICATE AUTHORITY = SECRET redpanda_ca_cert
      );
    ```

1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize
   to your Redpanda Cloud cluster and start ingesting data from your target topic.
   By default, the source will be created in the active cluster; to use a
   different cluster, use the `IN CLUSTER` clause.

    ```sql
    CREATE SOURCE rp_source
      -- The topic you want to read from.
      FROM KAFKA CONNECTION redpanda_cloud (TOPIC '<topic-name>')
      FORMAT JSON;
    ```

    If the command executes without an error and outputs _CREATE SOURCE_, it
    means that you have successfully connected Materialize to your Redpanda
    cluster.

This example walked through creating a source, which is a way of connecting
Materialize to an external data source. We created a connection to Redpanda
Cloud using SASL authentication and credentials securely stored as secrets in
Materialize's secret management system. For input formats, we used `JSON`, but
you can also ingest Kafka messages formatted in e.g. [Avro and Protobuf](/sql/create-source/kafka/#supported-formats).
You can find more details about the various different supported formats and
possible configurations in the [reference documentation](/sql/create-source/kafka/).

{{< /tab >}}

{{< tab "Use AWS PrivateLink">}}

{{< private-preview />}}

[AWS PrivateLink](https://aws.amazon.com/privatelink/) lets you connect
Materialize to your Redpanda Cloud instance without exposing traffic to the
public internet. To use AWS PrivateLink, your Redpanda cluster must be deployed
in a region supported by Materialize: `us-east-1`,`us-west-2`, or `eu-west-1`.

1. **Enable PrivateLink in your Redpanda Cloud cluster.**

    Alter your cluster to enable PrivateLink with an empty `allowed_principals`
    field using the template script below. Make sure to record the PrivateLink
    service name.

    **Note:** This can take up to 15 minutes. You can re-run the script (or just
      the `GET` request) periodically until it resolves.

    ```bash
    PUBLIC_API_ENDPOINT="https://api.cloud.redpanda.com"
    REGION=US-EAST-1
    CLOUD_CLIENT_ID="<your-redpanda-client-id>"
    CLOUD_CLIENT_SECRET="<your-redpanda-client-secret>"
    CLUSTER_ID="<your-redpanda-cluster-id>"
    NAMESPACE_ID="<your-redpanda-namespace-id>"

    AUTH_TOKEN=$(
      curl -s -X POST 'https://auth.prd.cloud.redpanda.com/oauth/token' \
             -H 'content-type: application/x-www-form-urlencoded' \
             -d grant_type=client_credentials \
             -d client_id=$CLOUD_CLIENT_ID \
             -d client_secret=$CLOUD_CLIENT_SECRET \
             -d audience=cloudv2-production.redpanda.cloud | jq .access_token | sed 's/"//g'
    )

    CLUSTER_PATCH_BODY="$(cat <<EOF
     {
       "private_link": {
         "enabled": true,
         "aws": {
           "allowed_principals": [ ]
         }
       }
     }
    EOF
    )"

    curl -X PATCH \
       -H "Content-Type: application/json" \
       -H "Authorization: Bearer $AUTH_TOKEN" \
       -d "$CLUSTER_PATCH_BODY" \
        $PUBLIC_API_ENDPOINT/v1beta1/clusters/$CLUSTER_ID

    curl -X GET \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $AUTH_TOKEN" \
        $PUBLIC_API_ENDPOINT/v1beta1/clusters/$CLUSTER_ID | jq \
        '.private_link'
    ```

1. In the Materialize [SQL shell](https://console.materialize.com/), or your
preferred SQL client, create a [PrivateLink connection](/ingest-data/network-security/privatelink/)
using the service name from the previous step. Be sure to specify **all
availability zones** of your Redpanda Cloud cluster.

    ```sql
    CREATE CONNECTION rp_privatelink TO AWS PRIVATELINK (
      SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-abcdefghijk',
      AVAILABILITY ZONES ('use1-az4','use1-az1','use1-az2')
    );
    ```

1. Retrieve the AWS principal for the AWS PrivateLink connection you just
created:

    ```sql
    SELECT principal
    FROM mz_aws_privatelink_connections plc
    JOIN mz_connections c ON plc.id = c.id
    WHERE c.name = 'rp_privatelink';
    ```
    <p></p>

    ```
       id   |                                 principal
    --------+---------------------------------------------------------------------------
     u1     | arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
    ```

1. Patch your Redpanda Cloud cluster to accept connections from the AWS
principal:

    ```bash
    PUBLIC_API_ENDPOINT="https://api.cloud.redpanda.com"
    REGION=US-EAST-1
    CLOUD_CLIENT_ID="<your-redpanda-client-id>"
    CLOUD_CLIENT_SECRET="<your-redpanda-client-secret>"
    CLUSTER_ID="<your-redpanda-cluster-id>"
    NAMESPACE_ID="<your-redpanda-namespace-id>"

    MATERIALIZE_CONNECTION_ARN="<arn-created-for-materialize-aws-privatelink-connection>"

    AUTH_TOKEN=$(
    curl -s -X POST 'https://auth.prd.cloud.redpanda.com/oauth/token' \
           -H 'content-type: application/x-www-form-urlencoded' \
           -d grant_type=client_credentials \
           -d client_id=$CLOUD_CLIENT_ID \
           -d client_secret=$CLOUD_CLIENT_SECRET \
           -d audience=cloudv2-production.redpanda.cloud | jq .access_token | sed 's/"//g'
    )

    CLUSTER_PATCH_BODY="$(cat <<EOF
     {
       "private_link": {
         "enabled": true,
         "aws": { "allowed_principals": ["$MATERIALIZE_CONNECTION_ARN"] }
       }
     }
    EOF
    )"

    curl -X PATCH \
       -H "Content-Type: application/json" \
       -H "Authorization: Bearer $AUTH_TOKEN" \
       -d "$CLUSTER_PATCH_BODY" \
       $PUBLIC_API_ENDPOINT/v1beta1/clusters/$CLUSTER_ID
    ```

1. In Materialize, validate the AWS PrivateLink connection you created using the
[`VALIDATE CONNECTION`](/sql/validate-connection) command:

    ```sql
    VALIDATE CONNECTION rp_privatelink;
    ```

    If no validation error is returned, move to the next step.

    This can take several minutes, and report errors like `Error: Endpoint cannot be
    discovered` while the policies are being updated and the endpoint resources are
    being re-evaluated. If the validation errors persist for longer than 10
    minutes, double-check the ARNs and service names and [contact our team](https://materialize.com/docs/support/).

1. Finally, create a connection to your Redpanda Cloud cluster using the AWS
Privatelink connection you created earlier:

    ```sql
    -- The credentials of your service account.
    CREATE SECRET redpanda_username AS '<your-username>';
    CREATE SECRET redpanda_password AS '<your-password>';
    -- The base64 encoded certificate from Step 4.
    CREATE SECRET redpanda_ca_cert AS  decode('<redpanda-broker-ca-cert>', 'base64');

    CREATE CONNECTION rp_connection TO KAFKA (
        AWS PRIVATELINK rp_privatelink (PORT 30292)
        SASL MECHANISMS = 'SCRAM-SHA-256',
        SASL USERNAME = SECRET redpanda_username,
        SASL PASSWORD = SECRET redpanda_password,
        SSL CERTIFICATE AUTHORITY = SECRET redpanda_ca_cert
    );

    CREATE SOURCE rp_source
      -- The topic you want to read from.
      FROM KAFKA CONNECTION redpanda_cloud (TOPIC '<topic-name>')
      FORMAT JSON;
    ```

This example walked through creating a source, which is a way of connecting
Materialize to an external data source. We created a connection to Redpanda
Cloud using AWS PrivateLink and credentials securely stored as secrets in
Materialize's secret management system. For input formats, we used `JSON`, but
you can also ingest Redpanda messages formatted in e.g. [Avro and Protobuf](/sql/create-source/kafka/#supported-formats).
You can find more details about the various different supported formats and
possible configurations in the [reference documentation](/sql/create-source/kafka/).

{{< /tab >}}
{{< /tabs >}}
