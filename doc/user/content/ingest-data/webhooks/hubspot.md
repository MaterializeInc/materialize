---
title: "HubSpot"
description: "How to stream data from HubSpot to Materialize using webhooks"
menu:
  main:
    parent: "webhooks"
    name: "HubSpot"
aliases:
  - /ingest-data/hubspot/
---

This guide walks through the steps to ingest data from [HubSpot](https://www.hubspot.com/)
into Materialize using the [Webhook source](/sql/create-source/webhook/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

### Before you begin

Ensure that you have:

- A HubSpot account with an [Operations Hub subscription](https://www.hubspot.com/pricing/operations).

## Step 1. (Optional) Create a cluster

{{< note >}}
If you are prototyping and already have a cluster to host your webhook
source (e.g. `quickstart`), **you can skip this step**. For production
scenarios, we recommend separating your workloads into multiple clusters for
[resource isolation](https://materialize.com/docs/sql/create-cluster/#resource-isolation).
{{< /note >}}

To create a cluster in Materialize, use the [`CREATE CLUSTER` command](/sql/create-cluster):

```mzsql
CREATE CLUSTER webhooks_cluster (SIZE = '25cc');

SET CLUSTER = webhooks_cluster;
```

## Step 2. Create a secret

To validate requests between HubSpot and Materialize, you must create a [secret](/sql/create-secret/):

```mzsql
CREATE SECRET hubspot_webhook_secret AS '<secret_value>';
```

Change the `<secret_value>` to a unique value that only you know and store it in
a secure location.

## Step 3. Set up a webhook source

Using the secret the previous step, create a [webhook source](/sql/create-source/webhook/)
in Materialize to ingest data from HubSpot. By default, the source will be
created in the active cluster; to use a different cluster, use the `IN
CLUSTER` clause.

```mzsql
CREATE SOURCE hubspot_source
  FROM WEBHOOK
    BODY FORMAT JSON
    CHECK (
      WITH (
        HEADERS,
        BODY AS body,
        SECRET hubspot_webhook_secret AS validation_secret
      )
      -- The constant_time_eq validation function **does not support** fully
      -- qualified secret names. We recommend always aliasing the secret name
      -- for ease of use.
      constant_time_eq(headers->'authorization', validation_secret)
);
```

After a successful run, the command returns a `NOTICE` message containing the
unique [webhook URL](https://materialize.com/docs/sql/create-source/webhook/#webhook-url)
that allows you to `POST` events to the source. Copy and store it. You will need
it for the next step.

The URL will have the following format:

```
https://<HOST>/api/webhook/<database>/<schema>/<src_name>
```

If you missed the notice, you can find the URLs for all webhook sources in the
[`mz_internal.mz_webhook_sources`](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_webhook_sources)
system table.

### Access and authentication

{{< warning >}}
Without a `CHECK` statement, **all requests will be accepted**. To prevent bad
actors from injecting data into your source, it is **strongly encouraged** that
you define a `CHECK` statement with your webhook sources.
{{< /warning >}}

The `CHECK` clause defines how to validate each request. At the time of writing,
HubSpot supports API key authentication, which you can use to validate
requests.

The above webhook source uses [basic authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme).
This enables a simple and rudimentary way to grant authorization to your webhook
source.

## Step 4. Create a webhook workflow in HubSpot

A [webhook in HubSpot](https://knowledge.hubspot.com/workflows/how-do-i-use-webhooks-with-hubspot-workflows)
is a workflow action that sends data to a webhook URL. You can create a webhook
workflow in HubSpot to send data to the webhook source you created in the
previous step.

1. In HubSpot, go to **Automation > Workflows**.

1. Click the **Name** of the workflow you want to add the webhook to, or create a new one.

1. Click the **+** icon to add an action.

1. In the right panel, search for **Send a webhook**.

1. Click the **Method** dropdown menu, then select `POST`.

1. Enter the URL from **Step 3.**.

1. Authenticate the request using the **API key** option. Use the secret created in **Step 2.**.

1. For the **API Key Name**, enter `authorization`. This is the key used in the `CHECK` clause of the webhook source.

1. Click **Save**.

## Step 5. Configure the request body in HubSpot

The request body is the data that HubSpot sends to the webhook URL. You can
configure the request body to send the data you want to ingest into
Materialize.

1. In HubSpot, go to the webhook workflow created in **Step 4.**.

1. Go to the **Request body** section, and click **Customize request body**.

1. In the **Request body** section, click **Add property**.

1. From the dropdown menu, select the property you want to send to Materialize.
   Repeat this step for each property you want to send to Materialize.

1. Click **Test Mapping** to validate that the webhook is working. If **Test Mapping** fails and throws a `failed to validate the request` error, this means that the secret is not correct. To fix this:

    1. In **HubSpot**, go to the webhook workflow.
    1. Go to the **Authentication** section.
    1. Enter the secret created in **Step 2.**.
    1. Verify that the **API Key Name** is `authorization`.
    1. Click **Save**.

1. After a succesful test, click **Save**.

## Step 6. Validate incoming data

With the source set up in Materialize and the webhook workflow configured in
HubSpot, you can now query the incoming data:

1. [In the Materialize console](https://console.materialize.com/), navigate to
   the **SQL Shell**.

1. Use SQL queries to inspect and analyze the incoming data:

    ```mzsql
    SELECT * FROM hubspot_source LIMIT 10;
    ```

## Step 7. Transform incoming data

### JSON parsing

Webhook data is ingested as a JSON blob. We recommend creating a parsing view on
top of your webhook source that uses [`jsonb` operators](/sql/types/jsonb/#operators)
to map the individual fields to columns with the required data types.

```mzsql
CREATE VIEW parse_hubspot AS SELECT
    body->>'city' AS city,
    body->>'firstname' AS firstname,
    body->>'ip_city' AS ip_city,
    -- Add all of the fields you want to ingest
FROM hubspot_source;
```

{{< json-parser >}}

### Timestamp handling

We highly recommend using the [`try_parse_monotonic_iso8601_timestamp`](/transform-data/patterns/temporal-filters/#temporal-filter-pushdown)
function when casting from `text` to `timestamp`, which enables [temporal filter
pushdown](https://materialize.com/docs/transform-data/patterns/temporal-filters/#temporal-filter-pushdown).

### Deduplication

With the vast amount of data processed and potential network issues, it's not
uncommon to receive duplicate records. You can use the `DISTINCT ON` clause to
efficiently remove duplicates. For more details, refer to the webhook source
[reference documentation](/sql/create-source/webhook/#handling-duplicated-and-partial-events).

## Next steps

With Materialize ingesting your HubSpot data, you can start exploring it,
computing real-time results that stay up-to-date as new data arrives, and
serving results efficiently. For more details, check out the
[HubSpot documentation](https://knowledge.hubspot.com/workflows/how-do-i-use-webhooks-with-hubspot-workflows) and the
[webhook source reference documentation](/sql/create-source/webhook/).
