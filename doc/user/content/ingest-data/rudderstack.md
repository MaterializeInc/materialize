---
title: "RudderStack"
description: "How to stream data from RudderStack to Materialize using webhooks"
menu:
  main:
    parent: "webhooks"
    name: "RudderStack"
    weight: 10
---

[RudderStack](https://rudderstack.com/) is a Customer Data Platform (CDP) for gathering and routing data from your applications.

This guide will walk you through the steps to ingest data from RudderStack to Materialize.

## Before you begin

Ensure that you have:

- [A RudderStack account](https://app.rudderstack.com/signup)
- A RudderStack [source](https://www.rudderstack.com/docs/sources/overview/) set up and running.

If you don't have a RudderStack source set up, follow the steps outlined in their [Getting Started](https://www.rudderstack.com/docs/dashboard-guides/sources/) guide.

## Step 1. Create a Materialize Cluster

To create a Materialize cluster, follow the steps outlined in our
create a [cluster](/sql/create-cluster) guide,
or create a managed cluster using the following SQL statement:

```sql
CREATE CLUSTER rudderstack_cluster SIZE = '3xsmall', REPLICATION FACTOR = 2;
```

## Step 2. Create a Shared Secret

To ensure data authenticity between RudderStack and Materialize, establish a shared secret:

```sql
CREATE SECRET rudderstack_shared_secret AS '<secret_value>';
```

This shared secret allows RudderStack to authenticate each incoming request, ensuring the data's integrity.

Change the `<secret_value>` to a unique value that only you know and store it in a secure location.

## Step 3. Set Up a Webhook Source

Using the secret from the previous step, create a [webhook source](/sql/create-source/webhook/) in Materialize to ingest data from RudderStack:

```sql
CREATE SOURCE rudderstack_source IN CLUSTER rudderstack_cluster
  FROM WEBHOOK
    BODY FORMAT JSON
    CHECK (
      WITH (
        HEADERS,
        BODY AS request_body,
        SECRET rudderstack_shared_secret
      )
      headers->'authorization' = rudderstack_shared_secret
);
```

The above webhook source uses [basic authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme).
This enables a simple and rudimentary way to grant authorization to your webhook source.

## Step 4. Add a Webhook Destination in RudderStack

To configure Webhook as a destination in RudderStack, follow the steps outlined below:

1.  **Select Your RudderStack Source**: Identify the source you wish to add a webhook endpoint to. If you don't have a source set up, follow the steps outlined in their [Getting Started](https://www.rudderstack.com/docs/dashboard-guides/sources/) guide.

1.  **Choose Webhook as Destination**:
    1. Navigate to the destination section.
    1. Select the **Webhook** option.
    1. Assign a name to your destination.
    1. Click **Continue**.

### Configuring Connection Settings

On the Connection Settings page:

- **Webhook URL**: Define the endpoint where events will be dispatched by RudderStack.

    [The webhook URL structure](/sql/create-source/webhook/#webhook-url) is:

    ```
    https://<HOST>/api/webhook/<database>/<schema>/<src_name>
    ```

- **URL Method**: Use the `POST` method to send events to Materialize.

- **Headers**: These headers get added to the RudderStack request sent to your webhook. For this setup, ensure that the following headers are added:

    - `Content-Type`: `application/json`
    - `Authorization`: `<secret_value>` (the same value used in [Step 2](#step-2-create-a-shared-secret))

## Step 5. Monitor Incoming Data

With the source set up in Materialize, you can now monitor the incoming data from RudderStack:

1. [Login to the Materialize console](https://console.materialize.com/).

1. Go to the **SQL Shell**.

1. Select a cluster.

1. Use SQL queries to inspect and analyze the incoming data:

    ```sql
    SELECT * FROM rudderstack_source LIMIT 10;
    ```

This will show you the last ten records ingested from RudderStack.

If you don't see any data, head over to the [RudderStack console](https://app.rudderstack.com/) and try to sync your source to trigger a new data ingestion.

## Step 6. Parse Incoming Data

Depending on your use case, you can create a materialized view to parse the incoming data from RudderStack:

```sql
CREATE VIEW json_parsed AS
WITH parse AS (
    SELECT
        (body -> '_metadata' ->> 'nodeVersion')::text AS nodeVersion,
        (body ->> 'channel')::text AS channel,
        (body ->> 'event')::text AS event,
        (body ->> 'userId')::text AS userId
    FROM my_webhook_source
);
```

This `json_parsed` view parses the incoming data, transforming the nested JSON structure into discernible columns, such as `nodeVersion`, `channel`, `event`, and `userId`.

Furthermore, with the vast amount of data processed and potential network issues, it's not uncommon to receive duplicate records. You can use the
`DISTINCT` clause to remove duplicates, for more details, refer to the webhook source [documentation](/sql/create-source/webhook/#duplicated-and-partial-events).

{{< note >}} For comprehensive details regarding RudderStack's Webhook Destination, refer to their [official documentation](https://rudderstack.com/docs/). {{< /note >}}

---

By following the steps outlined above, you will have successfully set up a seamless integration between RudderStack and Materialize, allowing for real-time data ingestion using webhooks.
