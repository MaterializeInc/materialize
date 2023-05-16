---
title: "Upstash Kafka"
description: "How to securely connect an Upstash Kafka cluster to Materialize as a source or sink."
aliases:
  - /integrations/upstash-kafka/
menu:
  main:
    parent: "kafka"
    name: "Upstash Kafka"
---

This guide goes through the required steps to connect Materialize to an Upstash Kafka cluster.

If you already have an Upstash Kafka cluster, you can skip steps 1 and 2 and directly move on to [Create a topic](#create-a-topic). You can also skip step 3 if you already have an Upstash Kafka cluster up and running, and have created a topic that you want to create a source for.

The process to connect Materialize to Upstash Kafka consists of the following steps:
1. #### Create an Upstash Kafka cluster
    If you already have an Upstash Kafka cluster set up, then you can skip this step.

    a. Sign in to the [Upstash console](https://console.upstash.com/login)

    b. Choose **Kafka** and then click the **Create Cluster** button

    c. Enter a cluster name, and specify the rest of the settings based on your needs

    d. Choose **Create**

    e. Create your first topic by specifying a topic name and the number of partitions

    f. Click **Create**

    **Note:** This creation process should take only a few seconds.

2. #### Create new credentials

    By default, Upstash provides a default credential that you can use to connect to your cluster. It's important to note that this default has full access to all topics in your cluster. For a more restricted setup, you can create a new credential with limited access to specific topics. If you want to use the default credential, skip this step.

    a. Navigate to the [Upstash console](https://console.upstash.com/login)

    b. Choose the Upstash Kafka cluster you just created in Step 1

    c. Click on the **Credentials** tab

    d. In the **Credentials** section, choose **New Credentials**

    e. Specify a name for the credentials

    f. List the topics you want to grant access to

    g. Choose the **Permissions** you want to grant to the credentials

    h. Click **Create**

    If you want to create a [source](/sql/create-source/kafka/) in Materialize, you'll need to grant the **Read** permission. To create a [sink](/sql/create-sink/kafka/), you'll need to grant the **Read** and **Write** permissions.

    Take note of the credentials you just created, you'll need them later on. Keep in mind that the credentials contain sensitive information, and you should store them somewhere safe!

3. #### Create a topic
    To start using Materialize with Upstash, you need to point it to an existing Upstash Kafka topic you want to read data from.

    If you already have a topic created, you can skip this step.

    Otherwise, you can follow the steps [here](https://docs.upstash.com/kafka#create-a-topic) to create a topic.

    Once you have a topic, you can produce a message to it by clicking on the topic name and then clicking on the **Messages** tab, and then clicking on the **Produce** button.

4. #### Create a source in Materialize
    a. Open the [Upstash console](https://console.upstash.com/login) and select your cluster

    b. Click on **Details**

    c. Copy the URL under **Endpoint**. This will be your `<upstash-broker-url>` going forward

    d. From the _psql_ terminal, run the following command. Replace `<upstash_kafka>` with whatever you want to name your source. The broker URL is what you copied in step c of this subsection. The `<topic-name>` is the name of the topic you created in Step 3. The `<your-username>` and `<your-password>` are from the _Create new credentials_ step.

    ```sql
      CREATE SECRET upstash_username AS '<your-username>';
      CREATE SECRET upstash_password AS '<your-password>';

      CREATE CONNECTION <upstash_kafka> TO KAFKA (
          BROKER '<upstash-broker-url>',
          SASL MECHANISMS = 'SCRAM-SHA-256',
          SASL USERNAME = SECRET upstash_username,
          SASL PASSWORD = SECRET upstash_password
      );

      CREATE SOURCE <source-name>
        FROM KAFKA CONNECTION upstash_kafka (TOPIC '<topic-name>')
        FORMAT BYTES
        WITH (SIZE = '3xsmall');
    ```

    e. If the command executes without an error and outputs _CREATE SOURCE_, it means that you have successfully connected Materialize to your Upstash Kafka cluster. You can quickly test your connection by running the following statement:
    ```sql
      SELECT convert_from(data, 'utf8') from <source-name>;
    ```

    **Note:** The example above walked through creating a source, which is a way of connecting Materialize to an external data source. We created a connection to Upstash Kafka using SASL authentication, using credentials securely stored as secrets in Materialize's secret management system. For input formats, we used `BYTES` to deserialize JSON-formatted data; however, Materialize supports other formats as well, like [Avro, and Protobuf](/sql/create-source/kafka/#supported-formats).

5. #### Create a sink in Materialize

    A [sink](/sql/create-sink) is a way to write data from Materialize to an external system like a Kafka cluster.

    To create a sink, from the _psql_ terminal, run the following command:

    ```sql
      CREATE SINK <sink-name>
        FROM <source, table or mview>
        INTO KAFKA CONNECTION <upstash_kafka> (TOPIC '<sink-topic-name>')
        FORMAT JSON
        ENVELOPE DEBEZIUM
        WITH (SIZE = '3xsmall');
    ```

    The generated schema will have a Debezium-style diff envelope to capture changes in the input view or source as we defined in the `ENVELOPE` clause. You can find more details about the various different supported formats and possible configurations [here](/sql/create-sink/kafka/).

    Once you have created the sink, you can go to the [Upstash console](https://console.upstash.com/login) and click on the topic you defined in the `CREATE SINK` statement. You will see the messages that were written to the topic. Materialize will produce messages to the topic every time the underlying source, table, or materialized view has new changes.
