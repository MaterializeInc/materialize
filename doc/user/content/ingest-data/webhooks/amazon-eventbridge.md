---
title: "Amazon EventBridge"
description: "How to stream data from Amazon EventBridge to Materialize using webhooks"
menu:
  main:
    parent: "webhooks"
    name: "Amazon EventBridge"
aliases:
  - /sql/create-source/webhook/#connecting-with-amazon-eventbridge
  - /ingest-data/amazon-eventbridge/
---

This guide walks through the steps to ingest data from [Amazon EventBridge](https://aws.amazon.com/eventbridge/)
into Materialize using the [Webhook source](/sql/create-source/webhook/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

Ensure that you have:

- An [EventBridge-enabled Amazon Simple Storage Service (S3) bucket](https://docs.aws.amazon.com/AmazonS3/latest/user-guide/Welcome.html).

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

To validate requests between Amazon EventBridge and Materialize, you must create
a [secret](/sql/create-secret/):

```mzsql
CREATE SECRET eventbridge_webhook_secret AS '<secret_value>';
```

Change the `<secret_value>` to a unique value that only you know and store it in
a secure location.

## Step 3. Set up a webhook source

Using the secret from the previous step, create a [webhook source](/sql/create-source/webhook/)
in Materialize to ingest data from Amazon EventBridge. By default, the source
will be created in the active cluster; to use a different cluster, use the `IN
CLUSTER` clause.

```mzsql
CREATE SOURCE eventbridge_source
FROM WEBHOOK
  BODY FORMAT JSON
  -- Include all headers, but filter out the secret.
  INCLUDE HEADERS ( NOT 'x-mz-api-key' )
  CHECK (
    WITH ( HEADERS, SECRET eventbridge_webhook_secret AS validation_secret)
    -- The constant_time_eq validation function **does not support** fully
    -- qualified secret names. We recommend always aliasing the secret name
    -- for ease of use.
    constant_time_eq(headers->'x-mz-api-key', validation_secret)
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

### Throughput

If your throughput exceeds the [maximum request rate](/sql/create-source/webhook/#request-limits),
we recommend [batching multiple events](/sql/create-source/webhook/#handling-batch-events)
into a single request, for example using [EventBridge Pipes](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-pipes-batching-concurrency.html).

## Step 4. Create an API destination in Amazon EventBridge

[//]: # "TODO(morsapaes) This needs to be broken down into instructions, same as
the other guides."

For guidance on creating an API destination in Amazon EventBridge to connect to
Materialize, check out [this guide](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-tutorial-datadog.html).
Use the secret created in **Step 2.** as the **API key name** for request
validation.

## Step 5. Validate incoming data

With the source set up in Materialize and the API destination configured in
Amazon EventBridge, you can now query the incoming data:

1. [In the Materialize console](/console/), navigate to
   the **SQL Shell**.

1. Use SQL queries to inspect and analyze the incoming data:

    ```mzsql
    SELECT * FROM eventbridge_source LIMIT 10;
    ```

## Step 6. Transform incoming data

### JSON parsing

Webhook data is ingested as a JSON blob. We recommend creating a parsing view on
top of your webhook source that uses [`jsonb` operators](/sql/types/jsonb/#operators)
to map the individual fields to columns with the required data types.

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

With Materialize ingesting your Amazon EventBridge data, you can start exploring it,
computing real-time results that stay up-to-date as new data arrives, and
serving results efficiently. For more details, check out the
[Amazon EventBridge documentation](https://docs.aws.amazon.com/eventbridge/) and the
[webhook source reference documentation](/sql/create-source/webhook/).
