---
title: "RudderStack"
description: "How to stream data from RudderStack to Materialize using webhooks"
menu:
  main:
    parent: "webhooks"
    name: "RudderStack"
aliases:
  - /ingest-data/rudderstack/
---

This guide walks through the steps to ingest data from [RudderStack](https://rudderstack.com/)
into Materialize using the [Webhook source](/sql/create-source/webhook/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

Ensure that you have:

- A RudderStack [account](https://app.rudderstack.com/signup)
- A RudderStack [source](https://www.rudderstack.com/docs/sources/overview/) set up and running.

## Step 1. (Optional) Create a cluster

{{< note >}}
If you are prototyping and already have a cluster to host your webhook
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](/sql/create-cluster/#resource-isolation).
{{< /note >}}

To create a cluster in Materialize, use the [`CREATE CLUSTER` command](/sql/create-cluster):

```mzsql
CREATE CLUSTER webhooks_cluster (SIZE = '25cc');

SET CLUSTER = webhooks_cluster;
```

## Step 2. Create a secret

To validate requests between Rudderstack and Materialize, you must create a [secret](/sql/create-secret/):

```mzsql
CREATE SECRET rudderstack_webhook_secret AS '<secret_value>';
```

Change the `<secret_value>` to a unique value that only you know and store it in a secure location.

## Step 3. Set up a webhook source

Using the secret from the previous step, create a [webhook source](/sql/create-source/webhook/)
in Materialize to ingest data from RudderStack. By default, the source will be
created in the active cluster; to use a different cluster, use the `IN
CLUSTER` clause.

```mzsql
CREATE SOURCE rudderstack_source
  FROM WEBHOOK
    BODY FORMAT JSON
    CHECK (
      WITH (
        HEADERS,
        BODY AS request_body,
        SECRET rudderstack_webhook_secret AS validation_secret
      )
      -- The constant_time_eq validation function **does not support** fully
      -- qualified secret names. We recommend always aliasing the secret name
      -- for ease of use.
      constant_time_eq(headers->'authorization', validation_secret)
);
```

After a successful run, the command returns a `NOTICE` message containing the
unique [webhook URL](/sql/create-source/webhook/#webhook-url)
that allows you to `POST` events to the source. Copy and store it. You will need
it for the next step.

The URL will have the following format:

```
https://<HOST>/api/webhook/<database>/<schema>/<src_name>
```

If you missed the notice, you can find the URLs for all webhook sources in the
[`mz_internal.mz_webhook_sources`](/sql/system-catalog/mz_internal/#mz_webhook_sources)
system table.

### Access and authentication

{{< warning >}}
Without a `CHECK` statement, **all requests will be accepted**. To prevent bad
actors from injecting data into your source, it is **strongly encouraged** that
you define a `CHECK` statement with your webhook sources.
{{< /warning >}}

The above webhook source uses [basic authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme).
This enables a simple and rudimentary way to grant authorization to your webhook source.

## Step 4. Create a webhook destination in RudderStack

To configure the webhook endpoint as a destination in RudderStack, follow the
steps outlined below:

1.  **Select your RudderStack source**

    Identify the source you wish to add a webhook endpoint to. If you don't have
    a source set up, follow the steps outlined in the Rudderstack
    [Getting Started](https://www.rudderstack.com/docs/dashboard-guides/sources/) guide.

1.  **Add a webhook destination and connect it to the Rudderstack source**
    1. Navigate to the **Add Destination** menu.
    1. Select the **Webhook** option.
    1. Assign a name to your destination and click **Continue**.

#### Connection settings

On the **Connection Settings** page:

- **Webhook URL**: Define the endpoint where events will be dispatched by RudderStack. Use the URL from **Step 3.**.

- **URL method**: Use the `POST` method to send events to Materialize.

- **Headers**: These headers get added to the RudderStack request sent to your webhook. For this setup, ensure that the following headers are added:

    - `Content-Type`: `application/json`
    - `Authorization`: Use the secret created in **Step 2.**.

## Step 5. Validate incoming data

With the source set up in Materialize and the webhook destination configured in
Rudderstack, you can now query the incoming data:

1. [In the Materialize console](/console/), navigate to
   the **SQL Shell**.

1. Use SQL queries to inspect and analyze the incoming data:

    ```mzsql
    SELECT * FROM rudderstack_source LIMIT 10;
    ```

    If you don't see any data, head over to the [RudderStack console](https://app.rudderstack.com/)
    and try to sync your source to trigger a new data ingestion.

## Step 6. Transform incoming data

### JSON parsing

Webhook data is ingested as a JSON blob. We recommend creating a parsing view on
top of your webhook source that uses [`jsonb` operators](/sql/types/jsonb/#operators)
to map the individual fields to columns with the required data types.

```mzsql
CREATE VIEW json_parsed AS
  SELECT
    (body -> '_metadata' ->> 'nodeVersion')::text AS nodeVersion,
    (body ->> 'channel')::text AS channel,
    (body ->> 'event')::text AS event,
    (body ->> 'userId')::text AS userId
  FROM rudderstack_source;
```

{{< json-parser >}}

### Timestamp handling

We highly recommend using the [`try_parse_monotonic_iso8601_timestamp`](/transform-data/patterns/temporal-filters/#temporal-filter-pushdown)
function when casting from `text` to `timestamp`, which enables [temporal filter
pushdown](/transform-data/patterns/temporal-filters/#temporal-filter-pushdown).

### Deduplication

With the vast amount of data processed and potential network issues, it's not
uncommon to receive duplicate records. You can use the `DISTINCT ON` clause to
efficiently remove duplicates. For more details, refer to the webhook source
[reference documentation](/sql/create-source/webhook/#handling-duplicated-and-partial-events).

## Next steps

With Materialize ingesting your Rudderstack data, you can start exploring it,
computing real-time results that stay up-to-date as new data arrives, and
serving results efficiently. For more details, check out the
[Rudderstack documentation](https://rudderstack.com/docs/) and the
[webhook source reference documentation](/sql/create-source/webhook/).
