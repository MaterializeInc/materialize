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
with the Postgres ones. We should include spill to disk in the guidance then."

This guide goes through the required steps to connect Materialize to a Redpanda Cloud cluster.

If you already have a Redpanda Cloud cluster, you can skip steps 1 and 2 and directly move on to [Download Redpanda Broker CA certificate](#download-the-redpanda-broker-ca-certificate). You can also skip step 4 if you already have a Redpanda Cloud cluster up and running, and have created a topic that you want to create a source for.

The process to connect Materialize to Redpanda Cloud consists of the following steps:
1. #### Create a Redpanda Cloud cluster
    If you already have a Redpanda Cloud cluster set up, then you can skip this step.

    a. Sign in to the [Redpanda Cloud console](https://vectorized.cloud/)

    b. Choose **Add cluster**

    c. Enter a cluster name, and specify the rest of the settings based on your needs

    d. Choose **Create cluster**

    **Note:** This creation can take about 15 minutes.

2. #### Create a Service Account
    ##### Service Account
    a. Navigate to the [Redpanda Cloud console](https://vectorized.cloud/)

    b. Choose the Redpanda cluster you just created in Step 1

    c. Click on the **Security** tab

    d. In the **Security** section, choose **Add service account**

    e. Specify a name for the service account and click **Create**

    Take note of the name of the service account you just created, as well as the service account secret key; you'll need them later on. Keep in mind that the service account secret key contains sensitive information, and you should store it somewhere safe!

    ##### Create an Access Control List (ACL)
    a. Next, while you're at the **Security** section, click on the **ACL** tab

    b. Click **Add ACL**

    c. Choose the **Service Account** that you've just created

    d. Choose the **Read** and **Write** permissions for the **Topics** section

    e. You can grant access on a per-topic basis, but you can also choose to grant access to all topics by adding a `*` in the **Topics ID** input field.

    f. Click on **Create**

3. #### Download the Redpanda Broker CA certificate
    a. Go to the [Redpanda Cloud console](https://vectorized.cloud/)

    b. Click on the **Overview** tab

    c. Click on the **Download** button next to the **Kafka API TLS**

    d. You'll need to `base64`-encode the certificate, and store it somewhere safe; you'll need this to securely connect to Materialize.


    One way of doing this is to open up a terminal and run the following command:
    ```shell
    cat <redpanda-broker-ca>.crt | base64
    ```

4. #### Create a topic
    To start using Materialize with Redpanda, you need to point it to an existing Redpanda topic you want to read data from.

    If you already have a topic created, you can skip this step.

    Otherwise, you can install Redpanda on your client machine from the previous step and create a topic. You can find more information about how to do that [here](https://docs.redpanda.com/docs/reference/rpk-commands/#rpk-topic-create).


5. #### Create a source in Materialize
    a. Open the [Redpanda Cloud console](https://vectorized.cloud/) and select your cluster

    b. Click on **Overview**

    c. Copy the URL under **Cluster hosts**. This will be your `<broker-url>` going forward

    d. From the _psql_ terminal, run the following command. Replace `<redpanda_cloud>` with whatever you want to name your source. The broker URL is what you copied in step c of this subsection. The `<topic-name>` is the name of the topic you created in Step 4. The `<your-username>` and `<your-password>` are from the _Create a Service Account_ step.

    ```sql
      CREATE SECRET redpanda_username AS '<your-username>';
      CREATE SECRET redpanda_password AS '<your-password>';
      CREATE SECRET redpanda_ca_cert AS  decode('<redpanda-broker-ca-cert>', 'base64'); -- The base64 encoded certificate

      CREATE CONNECTION <redpanda_cloud> TO KAFKA (
          BROKER '<redpanda-broker-url>',
          SASL MECHANISMS = 'SCRAM-SHA-256',
          SASL USERNAME = SECRET redpanda_username,
          SASL PASSWORD = SECRET redpanda_password,
          SSL CERTIFICATE AUTHORITY = SECRET redpanda_ca_cert
      );

      CREATE SOURCE <topic-name>
        FROM KAFKA CONNECTION redpanda_cloud (TOPIC '<topic-name>')
        FORMAT JSON;
    ```

    e. If the command executes without an error and outputs _CREATE SOURCE_, it
    means that you have successfully connected Materialize to your Redpanda
    cluster.

    **Note:** The example above walked through creating a source, which is a way
    of connecting Materialize to an external data source. We created a
    connection to Redpanda Cloud using SASL authentication and credentials
    securely stored as secrets in Materialize's secret management system. For
    input formats, we used `JSON`, but you can also ingest Kafka messages
    formatted in e.g. [Avro and Protobuf](/sql/create-source/kafka/#supported-formats).
    You can find more details about the various different supported formats and
    possible configurations in the [reference documentation](/sql/create-source/kafka/).

6. #### Connecting via AWS Privatelink
    It is also possible to connect Redpanda and Materialize via AWS Privatelink.
    This requires that you are using Redpanda Cloud, have deployed Redpanda in an region
    supported by Materialize, and have the AWS Privatelink feature enabled on your
    Redpanda cloud account.

    The following is an example of this setup in AWS us-east-1:

    a. Alter your Redpanda cluster to enable privatelink with an empty `allowed_principals` field
     using the following script template. Be sure to record the privatelink service name.
    **Note:** This creation can take about 15 minutes. You can re-run this script, or just the GET request, periodically until it resolves.

    Example script:
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

    b. Create a Privatelink connection in Materialize using the service name
    recorded above. Be sure to specify all availability zones of your
    Redpanda Cloud cluster.
    ```sql
    CREATE CONNECTION rp_privatelink TO AWS PRIVATELINK (
      SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-abcdefghijk',
      AVAILABILITY ZONES ('use1-az4','use1-az1','use1-az2')
    );
    ```

    c. Find the connection ARN created by Materialize for the privatelink connection.
    ```
    SELECT principal
    FROM mz_aws_privatelink_connections plc
    JOIN mz_connections c ON plc.id = c.id
    WHERE c.name = 'rp_privatelink';
    ```

    d. Patch your Redpanda cluster to allow the Materialize connection ARN.
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
    e. Wait for the Materialize AWS Privatelink connection to become valid.
    **Note:** This could take several minutes and may report errors like:
    `Error: Endpoint cannot be discovered` while the policies are being updated
    and the endpoint resources are being re-evaluated. If the validation errors persist
    for longer than 10 minutes, double check the ARNs and service names and reach out
    to Materialize Support.
    ```sql
    VALIDATE CONNECTION rp_privatelink;
    ```
    f. Finally, using the Materialize AWS Privatelink connectiopn you created above,
    create a kafka connection to Redpanda and a source for your topics.

    ```sql
    CREATE SECRET redpanda_username AS '<your-username>';
    CREATE SECRET redpanda_password AS '<your-password>';
    CREATE SECRET redpanda_ca_cert AS  decode('<redpanda-broker-ca-cert>', 'base64'); -- The base64 encoded certificate

    CREATE CONNECTION <redpanda_cloud> TO KAFKA (
        AWS PRIVATELINK rp_privatleink (PORT 30292)
        SASL MECHANISMS = 'SCRAM-SHA-256',
        SASL USERNAME = SECRET redpanda_username,
        SASL PASSWORD = SECRET redpanda_password,
        SSL CERTIFICATE AUTHORITY = SECRET redpanda_ca_cert
    );

    CREATE SOURCE <topic-name>
      FROM KAFKA CONNECTION redpanda_cloud (TOPIC '<topic-name>')
      FORMAT JSON;
    ```
