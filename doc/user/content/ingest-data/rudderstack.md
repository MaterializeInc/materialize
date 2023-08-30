---
title: "Ingest data RudderStack using webhooks"
description: "How to stream data from RudderStack to Materialize using webhooks"
menu:
  main:
    parent: "webhooks"
    name: "RudderStack"
    weight: 10
---

[RudderStack](https://rudderstack.com/) is a platform for gathering and routing data from your applications.

This guide will walk you through the steps to ingest data from RudderStack and Materialize.

## Before you begin

Ensure that you have:

- [A RudderStack account](https://app.rudderstack.com/signup)
- A RudderStack [source](https://www.rudderstack.com/docs/sources/overview/) set up and running.

If you don't have a RudderStack source set up, follow the steps outlined in their [Getting Started](https://www.rudderstack.com/docs/dashboard-guides/sources/) guide.

## Step 1. Create a Materialize Cluster

To create a Materialize cluster, follow the steps outlined in our
[Create a Cluster](/docs/get-started/create-cluster/) guide,
or create a managed cluster with two medium replicas using the following SQL statement:

```sql
CREATE CLUSTER rudderstack_cluster SIZE = 'medium', REPLICATION FACTOR = 2;
```

## Step 2. Create a Shared Secret

To ensure data authenticity between RudderStack and Materialize, establish a shared secret:

```sql
CREATE SECRET rudderstack_shared_secret AS '<secret_value>';
```

This shared secret allows RudderStack to authenticate each incoming request, ensuring the data's integrity.

Change the `<secret_value>` to a unique value that only you know and store it in a secure location.

## Step 3. Set Up a Webhook Source in Materialize

Using the secret, define the Materialize source to ingest data from RudderStack:

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

## Step 4. Configure Webhook as a Destination in RudderStack

To configure Webhook as a destination in RudderStack, follow the steps outlined below:

1.  **Select Your RudderStack Source**: Identify the source you wish to add a webhook endpoint to. If you don't have a source set up, follow the steps outlined in their [Getting Started](https://www.rudderstack.com/docs/dashboard-guides/sources/) guide.
2.  **Choose Webhook as Destination**:
    - Navigate to the destination section.
    - Opt for the "Webhook" option.
    - Assign a name to your destination.
    - Click "Continue".

### Configuring Connection Settings

On the Connection Settings page:

- **Webhook URL**: Define the endpoint where events will be dispatched by RudderStack.

    The webhook URL structure is:

    ```
    https://<HOST>/api/webhook/<database>/<schema>/<src_name>
    ```

    - `<HOST>`: The hostname of your Materialize cluster.
    - `<database>`: The name of the database where the source is created (default is `materialize`).
    - `<schema>`: The schema name where the source gets created (default is `public`).
    - `<src_name>`: The source's name. For this guide, use `rudderstack_source`.
- **URL Method**: Use the `POST` method to send events to Materialize.

- **Headers**: These headers get added to the RudderStack request sent to your webhook. For this setup, ensure that the following headers are added:

    - `Content-Type`: `application/json`
    - `Authorization`: [Your Secret Value] (the same value used in Step 2)

## Step 5. Monitor Incoming Data

With the source set up in Materialize, you can now monitor the incoming data from RudderStack:

1. [Login to the Materialize console](https://console.materialize.com/).
1. Go to the **SQL Shell**.
1. Select the `default` cluster.
1. Use SQL queries to inspect and analyze the incoming data:

```sql
SELECT * FROM rudderstack_source LIMIT 10;
```

This will show you the last ten records ingested from RudderStack.

If you don't see any data, head over to the [RudderStack console](https://app.rudderstack.com/) and try to scync your source to trigger a new data ingestion.

## Step 6. Create a Materialized View

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
