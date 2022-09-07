---
title: "How to connect Redpanda Cloud to Materialize"
description: "How to securely connect a Redpanda Cloud cluster as a source to Materialize."
menu:
  main:
    parent: "integration-guides"
    name: "Redpanda Cloud"
    weight: 11
---

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
      CREATE CONNECTION <redpanda_cloud>
        FOR KAFKA
          BROKER '<redpanda-broker-url>',
          SASL MECHANISMS = 'SCRAM-SHA-256',
          SASL USERNAME = SECRET redpanda_username,
          SASL PASSWORD = SECRET redpanda_password,
          SSL CERTIFICATE AUTHORITY = SECRET redpanda_ca_cert;
      CREATE SOURCE <topic-name>
        FROM KAFKA CONNECTION redpanda_cloud TOPIC '<topic-name>'
        FORMAT BYTES;
    ```

    e. If the command executes without an error and outputs _CREATE SOURCE_, it means that you have successfully connected Materialize to your Redpanda cluster. You can quickly test your connection by running the following statement:
    ```sql
      SELECT convert_from(data, 'utf8') from topic_name;
    ```

    **Note:** The example above walked through creating a source, which is a way of connecting Materialize to an external data source. We created a connection to Redpanda using SASL authentication, using credentials securely stored as secrets in Materialize's secret management system. For input formats, we used `bytes`, however, Materialize supports various other options as well. For example, you can ingest Redpanda messages formatted in [JSON, Avro and Protobuf](/sql/create-source/kafka/#supported-formats). You can find more details about the various different supported formats and possible configurations [here](/sql/create-source/kafka/).
