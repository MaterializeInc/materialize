---
title: "WarpStream"
description: "How to securely connect WarpStream to Materialize for efficient data streaming."
aliases:
  - /integrations/warpstream/
  - /connect-sources/warpstream/
menu:
  main:
    parent: "kafka"
    name: "WarpStream"
---

This guide goes through the necessary steps to connect Materialize to [WarpStream](https://www.warpstream.com/), an Apache Kafka® protocol compatible data streaming platform.

WarpStream runs on commodity object stores (e.g., AWS S3, GCP GCS, Azure Blob Storage) and offers benefits such as no inter-AZ bandwidth costs and no local disks management. This guide highlights its integration with Materialize using [Fly.io](https://fly.io/).

#### Before you begin

Ensure you have the following:

-   WarpStream account: [Register here](https://console.warpstream.com/signup)
-   Fly.io account: Used for deploying a WarpStream cluster with TLS termination and SASL authentication.

1. #### Set up WarpStream

    If you already have a WarpStream cluster, you can skip this step.

    a. **Sign Up**: Begin by registering for a WarpStream account.

    b. **Deploy on Fly.io**: Follow [this guide](https://github.com/warpstreamlabs/warpstream-fly-io-template) to deploy your WarpStream cluster on Fly.io.

    c. **Generate Credentials**: Post deployment, [create credentials](https://docs.warpstream.com/warpstream/how-to/configure-the-warpstream-agent-for-production/configure-authentication-for-the-warpstream-agent#sasl-authentication) for connecting to your WarpStream cluster.

    d. **Test Connection**: Use the provided credentials to connect to the WarpStream cluster on Fly.io. Test this connection using [the WarpStream CLI](https://docs.warpstream.com/warpstream/install-the-warpstream-agent):

    ```bash
    warpstream kcmd -type diagnose-connection \
                    -bootstrap-host $CLUSTER_NAME.fly.dev \
                    -tls -username ccun_XXXXXXXXXX \
                    -password ccp_XXXXXXXXXX
    ```

    Change the `bootstrap-host` to the name of your WarpStream cluster on Fly.io.

    e. **Topic Creation**: Establish the `materialize_click_streams` topic:

    ```bash
    warpstream kcmd -bootstrap-host $CLUSTER_NAME.fly.dev \
                    -tls -username ccun_XXXXXXXXX \
                    -password ccp_XXXXXXXXXX \
                    -type create-topic \
                    -topic materialize_click_streams
    ```

    f. **Produce Sample Records**: Generate and push sample records for testing:

    ```bash
    warpstream kcmd -bootstrap-host $CLUSTER_NAME.fly.dev \
                    -tls -username ccun_XXXXXXXXXX \
                    -password ccp_XXXXXXXXXX \
                    -type produce \
                    -topic materialize_click_streams_1 \
                    --records '{"action": "click", "user_id": "user_0", "page_id": "home"},,{"action": "hover", "user_id": "user_0", "page_id": "home"},,{"action": "scroll", "user_id": "user_0", "page_id": "home"}'
    ```

    > **Note:** The WarpStream CLI uses `,,` as a delimiter between JSON records.

2. #### Integrate with Materialize

    To integrate WarpStream with Materialize, you need to set up a connection to the WarpStream broker and create a source in Materialize to consume the data.

    Head over to the Materialize console and follow the steps below:

    a. Save WarpStream credentials:

    ```sql
    CREATE SECRET warpstream_username AS '<username>';
    CREATE SECRET warpstream_password AS '<password>';
    ```

    b. Set up a connection to the WarpStream broker:

    ```sql
    CREATE CONNECTION warpstream_kafka TO KAFKA (
        BROKER '<CLUSTER_NAME>.fly.dev:9092',
        SASL MECHANISMS = "PLAIN",
        SASL USERNAME = SECRET warpstream_username,
        SASL PASSWORD = SECRET warpstream_password
    );
    ```

    c. Create a source in Materialize to consume messages:

    ```sql
    CREATE SOURCE warpstream_click_stream_source
        FROM KAFKA CONNECTION warpstream_kafka (TOPIC 'materialize_click_streams')
        FORMAT JSON
        WITH (SIZE = '3xsmall');
    ```

    d. Verify the ingestion and query the data in Materialize:

    ```sql
    SELECT * FROM warpstream_click_stream_source LIMIT 10;
    ```

    e. Furthermore, create a materialized view to aggregate the data:

    ```sql
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
    warpstream kcmd -bootstrap-host warpstream-agent-demo.fly.dev \
                    -tls -username ccun_XXXXXXXXXX \
                    -password ccp_XXXXXXXXXX \
                    -type produce \
                    -topic materialize_click_streams_1 \
                    --records '{"action": "click", "user_id": "user_1", "page_id": "home"}'
    ```

    g. Query the materialized view to monitor the real-time updates:

    ```sql
    SELECT * FROM warpstream_click_stream_aggregate;
    ```

---

By following the steps outlined above, you will have successfully set up a connection between WarpStream and Materialize. You can now use Materialize to query the data ingested from WarpStream.
