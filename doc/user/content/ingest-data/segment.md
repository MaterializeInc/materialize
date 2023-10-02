---
title: "Segment"
description: "How to stream data from Segment to Materialize using webhooks"
menu:
  main:
    parent: "webhooks"
    name: "Segment"
    weight: 10
aliases:
  - /sql/create-source/webhook/#connecting-with-segment
---

[Segment](https://segment.com/) is a Customer Data Platform (CDP) that collects and routes events
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
Copy and store it. You will need it for the next steps. Otherwise, you can query it here: [`mz_internal.mz_webhook_sources`](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_webhook_sources).

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
    Url                 | Use the URL from the [Step 3](#step-3-set-up-a-webhook-source).
    Method              | `POST`
    Batch Size          | `0`
    Headers             | -
    Data                | `$event`
    Enable Batching     | `No`.

4. Click **Test Mapping** and validate the webhook is working.

5. After a succesful test, click **Save**.

  {{< note >}}
  If **Test Mapping** fails in and throws a *"failed to validate the request"* error, it means the shared secret is not right. To fix this, follow this steps:
 1. In **Segment**, go to your webhook destination created in the **Step 4**.
 2. Click **Settings**.
 3. In **Shared Secret**, enter the secret created in the **Step 2**.
 4. Click **Save Changes**.
  {{< /note >}}

## Step 5. Parse Incoming Data

Create a view to parse incoming events from Segment. You have the option to either build your own parser view by [pasting your event JSON here](/transform-data/json/), or use any of the following templates for the most common types of Segment:

{{< tabs >}}

{{< tab "Page">}}
```sql
CREATE VIEW parse_segment AS SELECT
    body->>'anonymousId' AS anonymousId,
    body->>'channel' AS channel,
    body->'context'->>'ip' AS context_ip,
    body->'context'->>'userAgent' AS context_userAgent,
    (body->'integrations'->>'All')::bool AS integrations_All,
    (body->'integrations'->>'Mixpanel')::bool AS integrations_Mixpanel,
    (body->'integrations'->>'Salesforce')::bool AS integrations_Salesforce,
    body->>'messageId' AS messageId,
    body->>'name' AS name,
    body->'properties'->>'title' AS properties_title,
    body->'properties'->>'url' AS properties_url,
    (body->>'receivedAt')::timestamp AS receivedAt,
    (body->>'sentAt')::timestamp AS sentAt,
    (body->>'timestamp')::timestamp AS timestamp,
    body->>'type' AS type,
    body->>'userId' AS userId,
    (body->>'version')::timestamp AS version
FROM my_segment_source;
```
{{< /tab >}}

{{< tab "Track">}}

```sql
CREATE VIEW parse_segment AS SELECT
    body->>'anonymousId' AS anonymousId,
    body->'context'->'library'->>'name' AS context_library_name,
    (body->'context'->'library'->>'version')::timestamp AS context_library_version,
    body->'context'->'page'->>'path' AS context_page_path,
    body->'context'->'page'->>'referrer' AS context_page_referrer,
    body->'context'->'page'->>'search' AS context_page_search,
    body->'context'->'page'->>'title' AS context_page_title,
    body->'context'->'page'->>'url' AS context_page_url,
    body->'context'->>'userAgent' AS context_userAgent,
    body->'context'->>'ip' AS context_ip,
    body->>'event' AS event,
    body->>'messageId' AS messageId,
    body->'properties'->>'title' AS properties_title,
    (body->>'receivedAt')::timestamp AS receivedAt,
    (body->>'sentAt')::timestamp AS sentAt,
    (body->>'timestamp')::timestamp AS timestamp,
    body->>'type' AS type,
    body->>'userId' AS userId,
    (body->>'originalTimestamp')::timestamp AS originalTimestamp
FROM my_segment_source;
```

{{< /tab >}}

{{< tab "Identity">}}
```sql
CREATE VIEW parse_segment AS SELECT
    body->>'anonymousId' AS anonymousId,
    body->>'channel' AS channel,
    body->'context'->>'ip' AS context_ip,
    body->'context'->>'userAgent' AS context_userAgent,
    (body->'integrations'->>'All')::bool AS integrations_All,
    (body->'integrations'->>'Mixpanel')::bool AS integrations_Mixpanel,
    (body->'integrations'->>'Salesforce')::bool AS integrations_Salesforce,
    body->>'messageId' AS messageId,
    (body->>'receivedAt')::timestamp AS receivedAt,
    (body->>'sentAt')::timestamp AS sentAt,
    (body->>'timestamp')::timestamp AS timestamp,
    body->'traits'->>'name' AS traits_name,
    body->'traits'->>'email' AS traits_email,
    body->'traits'->>'plan' AS traits_plan,
    (body->'traits'->>'logins')::numeric AS traits_logins,
    body->'traits'->'address'->>'street' AS traits_address_street,
    body->'traits'->'address'->>'city' AS traits_address_city,
    body->'traits'->'address'->>'state' AS traits_address_state,
    (body->'traits'->'address'->>'postalCode')::timestamp AS traits_address_postalCode,
    body->'traits'->'address'->>'country' AS traits_address_country,
    body->>'type' AS type,
    body->>'userId' AS userId,
    (body->>'version')::timestamp AS version
FROM my_segment_source;
```
{{< /tab >}}
{{< /tabs >}}


This view parses the incoming data, transforming the nested JSON structure into discernible columns, such as `type` or `userId`.

Furthermore, with the vast amount of data processed and potential network issues, it's not uncommon to receive duplicate records. You can use the
`DISTINCT` clause to remove duplicates, for more details, refer to the webhook source [documentation](/sql/create-source/webhook/#handling-duplicated-and-partial-events).

{{< note >}} For comprehensive details regarding Segment's webhook destination, refer to their [official documentation](https://segment.com/docs/connections/destinations/catalog/actions-webhook/). {{< /note >}}

---

By following the steps outlined above, you will have successfully set up a seamless integration between Segment and Materialize, allowing for real-time data ingestion using webhooks.
