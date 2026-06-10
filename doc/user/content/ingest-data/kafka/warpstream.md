---
title: "WarpStream"
description: "How to securely connect WarpStream to Materialize for efficient data streaming."
aliases:
  - /integrations/warpstream/
  - /connect-sources/warpstream/
  - /ingest-data/warpstream/
menu:
  main:
    parent: "kafka"
    name: "WarpStream"
---

[//]: # "TODO(morsapaes) The Kafka guides need to be rewritten for consistency
with the Postgres ones. We should include spill to disk in the guidance then."

This guide goes through the necessary steps to connect Materialize to
[WarpStream](https://www.warpstream.com/), an Apache Kafka® protocol compatible
data streaming platform.

WarpStream runs on commodity object stores (e.g., Amazon S3, Google Cloud
Storage, Azure Blob Storage) and offers benefits such as no inter-AZ bandwidth
costs and no local disks management. This guide highlights its integration with
Materialize using [Fly.io](https://fly.io/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

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
    ```

    Change the `bootstrap-host` to the name of your WarpStream cluster on
    Fly.io.

    e. Create the `materialize_click_streams` topic:

    ```bash
    warpstream kcmd -bootstrap-host $CLUSTER_NAME.fly.dev \
                    -tls -username ccun_XXXXXXXXX \
                    -password ccp_XXXXXXXXXX \
                    -type create-topic \
                    -topic materialize_click_streams
    ```

    f. Generate and push sample records for testing:

    ```bash
    warpstream kcmd -bootstrap-host $CLUSTER_NAME.fly.dev \
                    -tls -username ccun_XXXXXXXXXX \
                    -password ccp_XXXXXXXXXX \
                    -type produce \
                    -topic materialize_click_streams \
                    --records '{"action": "click", "user_id": "user_0", "page_id": "home"},,{"action": "hover", "user_id": "user_0", "page_id": "home"},,{"action": "scroll", "user_id": "user_0", "page_id": "home"}'
    ```

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
    ```

    b. Set up a connection to the WarpStream broker:

    ```mzsql
    CREATE CONNECTION warpstream_kafka TO KAFKA (
        BROKER '<CLUSTER_NAME>.fly.dev:9092',
        SASL MECHANISMS = "PLAIN",
        SASL USERNAME = SECRET warpstream_username,
        SASL PASSWORD = SECRET warpstream_password
    );
    ```

    c. Create a source in Materialize to consume messages. By default, the
    source will be created in the active cluster; to use a different cluster,
    use the `IN CLUSTER` clause.

    ```mzsql
    CREATE SOURCE warpstream_click_stream_source
        FROM KAFKA CONNECTION warpstream_kafka (TOPIC 'materialize_click_streams')
        FORMAT JSON;
    ```

    d. Verify the ingestion and query the data in Materialize:

    ```mzsql
    SELECT * FROM warpstream_click_stream_source LIMIT 10;
    ```

    e. Furthermore, create a materialized view to aggregate the data:

    ```mzsql
    CREATE MATERIALIZED VIEW warpstream_click_stream_aggregate AS
        SELECT
            user_id,
            page_id,
            COUNT(*) AS count
        FROM warpstream_click_stream_source
        GROUP BY user_id, page_id;
    ```

    f. Produce additional records to monitor real-time updates:

    ```bash
    warpstream kcmd -bootstrap-host $CLUSTER_NAME.fly.dev \
                    -tls -username ccun_XXXXXXXXXX \
                    -password ccp_XXXXXXXXXX \
                    -type produce \
                    -topic materialize_click_streams \
                    --records '{"action": "click", "user_id": "user_1", "page_id": "home"}'
    ```

    g. Query the materialized view to monitor the real-time updates:

    ```mzsql
    SELECT * FROM warpstream_click_stream_aggregate;
    ```

---

By following the steps outlined above, you will have successfully set up a
connection between WarpStream and Materialize. You can now use Materialize to
query the data ingested from WarpStream.
