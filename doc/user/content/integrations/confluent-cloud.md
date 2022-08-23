---
title: "How to connect Confluent Cloud Kafka cluster to Materialize"
description: "How to securely connect a Confluent Cloud Kafka cluster as a source to Materialize."
menu:
  main:
    parent: "integration-guides"
    name: "Confluent Cloud"
    weight: 12
---

This guide goes through the required steps to connect Materialize to a Confluent Cloud Kafka cluster.

If you already have a Confluent Cloud Kafka cluster, you can skip step 1 and directly move on to [Create an API Key](#create-an-api-key). You can also skip step 3 if you already have a Confluent Cloud Kafka cluster up and running, and have created a topic that you want to create a source for.

The process to connect Materialize to a Confluent Cloud Kafka cluster consists of the following steps:
1. #### Create a Confluent Cloud Kafka cluster
    If you already have a Confluent Cloud Kafka cluster set up, then you can skip this step.

    a. Sign in to [Confluent Cloud](https://confluent.cloud/)

    b. Choose **Create a new cluster**

    c. Select the cluster type, and specify the rest of the settings based on your needs

    d. Choose **Create cluster**

    **Note:** This creation can take about 10 minutes. For more information on the cluster creation, see [Confluent Cloud documentation](https://docs.confluent.io/cloud/current/get-started/index.html#step-1-create-a-ak-cluster-in-ccloud).

2. #### Create an API Key
    ##### API Key
    a. Navigate to the [Confluent Cloud](https://confluent.cloud/)

    b. Choose the Confluent Cloud Kafka cluster you just created in Step 1

    c. Click on the **Data Integration** tab and then choose **API Keys**

    d. In the **API Keys** section, choose **Add Key**

    e. Specify the scope for the **API key** and then click **Create Key**. If you choose to create a **Granular access** API key, make sure to give the **API key** **Read** and **Write** access to the topic you want to create a source for.

    Take note of the API Key you just created, as well as the API Key secret key; you'll need them later on. Keep in mind that the API Key secret key contains sensitive information, and you should store it somewhere safe!

3. #### Create a topic
    To start using Materialize with Confluent Cloud, you need to point it to an existing Kafka topic you want to read data from.

    If you already have a topic created, you can skip this step.

    Otherwise, you can find more information about how to do that [here](https://docs.confluent.io/cloud/current/get-started/index.html#step-2-create-a-ak-topic).

4. #### Create a source in Materialize
    a. Open the [Confluent Cloud Dashboard](https://confluent.cloud/) and select your cluster

    b. Click on **Overview** and select **Cluster settings**

    c. Copy the URL under **Bootstrap server**. This will be your `<broker-url>` going forward

    d. From the _psql_ terminal, run the following command. Replace `<confluent_cloud>` with whatever you want to name your source. The broker URL is what you copied in step c of this subsection. The `<topic-name>` is the name of the topic you created in Step 4. The `<your-username>` and `<your-password>` are from the _Create a API Key_ step.
    ```sql
      CREATE SECRET confluent_username AS '<your-username>';
      CREATE SECRET confluent_password AS '<your-password>';
      CREATE CONNECTION <confluent_cloud>
        FOR KAFKA
          BROKER '<confluent-broker-url>',
          SASL MECHANISMS = 'PLAIN',
          SASL USERNAME = SECRET confluent_username,
          SASL PASSWORD = SECRET confluent_password;
      CREATE SOURCE <topic-name>
        FROM KAFKA CONNECTION confluent_cloud TOPIC '<topic-name>'
        FORMAT BYTES;
    ```

    e. If the command executes without an error and outputs _CREATE SOURCE_, it means that you have successfully connected Materialize to your Confluent Cloud Kafka cluster. You can quickly test your connection by running the following statement:
    ```sql
      SELECT convert_from(data, 'utf8') from topic_name;
    ```

    **Note:** The example above walked through creating a source, which is a way of connecting Materialize to an external data source. We created a connection to Confluent Cloud Kafka using SASL authentication, using credentials securely stored as secrets in Materialize's secret management system. For input formats, we used `bytes`, however, Materialize supports various other options as well. For example, you can ingest Kafka messages formatted in [JSON, Avro and Protobuf](/sql/create-source/kafka/#supported-formats). You can find more details about the various different supported formats and possible configurations [here](/sql/create-source/kafka/).
