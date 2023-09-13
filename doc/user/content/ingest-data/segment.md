---
title: "Segment"
description: "How to stream data from Segment to Materialize using webhooks"
menu:
  main:
    parent: "webhooks"
    name: "Segment"
    weight: 10
---

[Segment](https://Segment.com/) is a Customer Data Platform (CDP) that collects and routes events
from your web and mobile app. You can use Materialize to process, sink or serve each event.

This guide will walk you through the steps to ingest data from Segment to Materialize.

### Before you begin

Ensure that you have a Segment source.

## Step 1. (Optional) Create a Materialize Cluster

If you already have a cluster for your webhook sources, you can skip this step.

To create a Materialize cluster, follow the steps outlined in our
create a [cluster guide](/sql/create-cluster),
or create a managed cluster for all your webhooks using the following SQL statement:

```sql
CREATE CLUSTER webhooks_cluster SIZE = '3xsmall';
```

## Step 2. Create a Shared Secret

To validate requests between Segment and Materialize, create a shared secret:

```sql
CREATE SECRET segment_shared_secret AS '<secret_value>';
```

Change the `<secret_value>` to a unique value that only you know and store it in a secure location.

## Step 3. Set Up a Webhook Source

Using the secret from the previous step, create a [webhook source](/sql/create-source/webhook/)
in Materialize to ingest data from Segment:

```sql
CREATE SOURCE my_segment_source IN CLUSTER webhooks_cluster FROM WEBHOOK
  BODY FORMAT JSON
  INCLUDE HEADER 'event-type' AS event_type
  INCLUDE HEADERS
  CHECK (
    WITH ( BODY BYTES, HEADERS, SECRET segment_shared_secret AS secret BYTES)
    decode(headers->'x-signature', 'hex') = hmac(body, secret, 'sha1')
  );
```

After a successful run, the command returns a `NOTICE` message containing the [webhook URL](https://materialize.com/docs/sql/create-source/webhook/#webhook-url).
Copy and store it temporarily for the following steps. Otherwise, you can query it here: [`mz_internal.mz_webhook_sources`](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_webhook_sources).

In the code, you will notice a `CHECK` statement. It defines how to validate each request. At the time of writing, Segment
validates requests by signing them with an HMAC in the `X-Signature` request header. The HMAC is a
hex-encoded SHA1 hash using the shared secret from the previous step and request body. Materialize decodes the signature using
the [`decode`](/sql/functions/#decode) function, getting the raw bytes, and generate our own HMAC
using the [`hmac`](/sql/functions/#hmac) function. If the two values are equal, then the request is
legitimate!

## Step 4. Create a Webhook Destination in Segment

1. In Segment, go to **Connections > Catalog**.

2. Search and click **Webhooks (Actions)**.

3. Click **Add destination**.

4. Select a data source and click **Next**.

5. Jot a *Destination Name* and click **Create Destination**.

## Step 5. Configure the Mappings in Segment

A webhook destination in Segment requires a [data mapping](https://segment.com/blog/data-mapping/) to send events from the source to the destination. For this guide, the destination is the Materialize source. Follow these steps to create the correct mapping:

1. Go to your webhook destination created in the previous step.

2. Click on **Mappings > New Mapping**.

3. Click **Send** and fill the configuration as follows:
   1. In **Select events to map and send**, fill the conditions as you wish.
   2. In **Add test**, select any of the events.
   3. In **Select mappings**, fill the fields as follows:
    Field               | Value
    ------------------- | ----------------------
    Url                 | Use the URL from the [Step 3](`/`).
    Method              | `POST`
    Batch Size          | `0`
    Headers             | -
    Data                | `$event`
    Enable Batching     | `No`.
4. Click **Test Mapping** and validate the webhook is working.
5. After a succesful test, click **Save**.

  {{< note >}}
  If **Test Mapping** fails in and throws a *"failed to validate the request"* error, it means te shared secret is not right. To fix the error, follow this steps:
 1. In **Segment**, go to your webhook destination created in the **Step 4**.
 2. Click **Settings**.
 3. In **Shared Secret**, enter the secret created in the **Step 2**.
 4. Click **Save Changes**.
  {{< /note >}}

## Step 5. Parse Incoming Data

Create a materialized view to parse the incoming events from Segment:

```sql
CREATE VIEW segment_source_parsed AS
WITH parse AS (
    SELECT
        body->>'email' AS email,
        body->>'event' AS event,
        body->>'messageId' AS message_id,
        body->>'properties' AS properties,
        body->>'timestamp' AS ts,
        body->>'type' AS event_type,
        body->>'userId' AS user_id
    FROM my_segment_source
);
```

This view parses the incoming data, transforming the nested JSON structure into discernible columns, such as `email`, `event`, `type`, and `userId`.

Furthermore, with the vast amount of data processed and potential network issues, it's not uncommon to receive duplicate records. You can use the
`DISTINCT` clause to remove duplicates, for more details, refer to the webhook source [documentation](/sql/create-source/webhook/#duplicated-and-partial-events).

{{< note >}} For comprehensive details regarding Segment's webhook destination, refer to their [official documentation](https://segment.com/docs/connections/destinations/catalog/actions-webhook/). {{< /note >}}

---

By following the steps outlined above, you will have successfully set up a seamless integration between Segment and Materialize, allowing for real-time data ingestion using webhooks.
