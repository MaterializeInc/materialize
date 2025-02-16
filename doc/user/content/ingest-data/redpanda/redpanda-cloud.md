---
title: "Redpanda Cloud"
description: "How to securely connect a Redpanda Cloud cluster as a source to Materialize."
aliases:
  - /integrations/redpanda-cloud/
  - /connect-sources/redpanda-cloud/
  - /ingest-data/redpanda-cloud/
menu:
  main:
    parent: "redpanda"
    name: "Redpanda Cloud"
---

[//]: # "TODO(morsapaes) The Kafka guides need to be rewritten for consistency
with the Postgres ones."

This guide goes through the required steps to connect Materialize to a Redpanda
Cloud cluster. If you already have a Redpanda Cloud cluster up and running,
skip straight to [Step 2](#step-2-create-a-user).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Step 1. Create a Redpanda Cloud cluster

{{< note >}}
Once created, provisioning a Dedicated Redpanda Cloud cluster can take up to 40 minutes.

Serverless clusters are provisioned in a few minutes.
{{</ note >}}

1. Sign in to the [Redpanda Cloud console](https://cloud.redpanda.com/).

1. Create a new namespace or select an existing one where you want to create the cluster.

1. Choose the type of cluster you want to create.

1. Enter a cluster name, and specify the rest of the settings based on your
needs.

1. Click **Next** and select the network settings for your cluster.

1. Next, click **Create** to create the cluster.

## Step 2. Create a user

1. Navigate to the [Redpanda Cloud console](https://cloud.redpanda.com/).

1. Choose the Redpanda cluster you created in **Step 1**.

1. Click on the **Security** tab.

1. In the **Security** section, choose **Create User**.

1. Specify a name and a password along with the SASL Mechanism
for the user and click **Create**.

Take note of the user you just created, as well as the password and the SASL Mechanism you chose;
you'll need them later on. Keep in mind that the password contains sensitive information, and you
should store it somewhere safe!

## Step 3. Create an Access Control List

1. Next, still in the **Security** section, click on the **ACL** tab.

1. Click **Add ACL**,

1. Choose the **User** you just created.

1. Choose the **Read** and **Write** permissions for the **Topics** section.

1. You can grant access on a per-topic basis, but you can also choose to grant
access to all topics by adding a `*` in the **Topics ID** input field.

1. Click **Create**.

## Step 5. Create a topic

To start using Materialize with Redpanda, you need to point it to an existing
Redpanda topic you want to read data from. If the topic you want to ingest
already exists, you can skip this step.

Otherwise, you can install Redpanda Keeper (aka `rpk`) on the client machine
from the previous step to create a topic. For guidance on how to use `rpk`,
check the [Redpanda documentation](https://docs.redpanda.com/docs/reference/rpk-commands/#rpk-topic-create).

## Step 6. Start ingesting data

Now that you’ve configured your Redpanda cluster, you can start ingesting data
into Materialize. The exact steps depend on your networking configuration.
Below is an example of how to connect Materialize to a Redpanda Cloud
cluster using credentials securely stored as secrets.

1. Open the [Redpanda Cloud console](https://cloud.redpanda.com/) and select your cluster.

1. Click on **Overview**

1. Copy the URL under **Cluster hosts**. This will be your
`<redpanda-broker-url>` going forward.

1. In the Materialize [SQL shell](/console/), or your
preferred SQL client, create a connection with your Redpanda Cloud cluster
access and authentication details using the [`CREATE CONNECTION`](/sql/create-connection/)
command:

    ```mzsql
      -- The credentials of your Redpanda Cloud user.
      CREATE SECRET redpanda_username AS '<your-username>';
      CREATE SECRET redpanda_password AS '<your-password>';

      CREATE CONNECTION rp_connection TO KAFKA (
          BROKER '<redpanda-broker-url>',
          SASL MECHANISMS = 'SCRAM-SHA-256', -- The SASL mechanism you chose in Step 2.
          SASL USERNAME = SECRET redpanda_username,
          SASL PASSWORD = SECRET redpanda_password
      );
    ```

1. Use the [`CREATE SOURCE`](/sql/create-source/) command to connect Materialize
   to your Redpanda Cloud cluster and start ingesting data from your target topic.
   By default, the source will be created in the active cluster; to use a
   different cluster, use the `IN CLUSTER` clause.

    ```mzsql
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


This example walked through creating a source, which is a way of connecting
Materialize to an external data source. We created a connection to Redpanda
Cloud using credentials securely stored as secrets in
Materialize's secret management system. For input formats, we used `JSON`, but
you can also ingest Redpanda messages formatted in e.g. [Avro and Protobuf](/sql/create-source/kafka/#supported-formats).
You can find more details about the various different supported formats and
possible configurations in the [reference documentation](/sql/create-source/kafka/).
