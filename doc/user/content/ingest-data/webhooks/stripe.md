---
title: "Stripe"
description: "How to stream data from Stripe to Materialize using webhooks"
menu:
  main:
    parent: "webhooks"
    name: "Stripe"
aliases:
  - /sql/create-source/webhook/#connecting-with-stripe
  - /ingest-data/stripe/
---

This guide walks through the steps to ingest data from [Stripe](https://stripe.com/)
into Materialize using the [Webhook source](/sql/create-source/webhook/).

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

### Before you begin

Ensure that you have a Stripe account.

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

To validate requests between Stripe and Materialize, you must create a [secret](/sql/create-secret/):

```mzsql
CREATE SECRET stripe_webhook_secret AS '<secret_value>';
```

Change the `<secret_value>` to a unique value that only you know and store it in a secure location.

## Step 3. Set up a webhook source

Using the secret from the previous step, create a [webhook source](/sql/create-source/webhook/)
in Materialize to ingest data from Stripe. By default, the source will be
created in the active cluster; to use a different cluster, use the `IN
CLUSTER` clause.

```mzsql
CREATE SOURCE stripe_source IN CLUSTER webhooks_cluster
FROM WEBHOOK
    BODY FORMAT JSON;
    CHECK (
        WITH (BODY, HEADERS, SECRET stripe_webhook_secret AS validation_secret)
        (
            -- The constant_time_eq validation function **does not support** fully
            -- qualified secret names. We recommend always aliasing the secret name
            -- for ease of use.
            constant_time_eq(
                -- Sign the timestamp and body.
                encode(hmac(
                    (
                        -- Extract the `t` component from the `Stripe-Signature` header.
                        regexp_split_to_array(headers->'stripe-signature', ',|=')[
                            array_position(regexp_split_to_array(headers->'stripe-signature', ',|='), 't')
                            + 1
                        ]
                        || '.' ||
                        body
                    ),
                    validation_secret,
                    'sha256'
                ), 'hex'),
                -- Extract the `v1` component from the `Stripe-Signature` header.
                regexp_split_to_array(headers->'stripe-signature', ',|=')[
                    array_position(regexp_split_to_array(headers->'stripe-signature', ',|='), 'v1')
                    + 1
                ],
            )
        )
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

The `CHECK` clause defines how to validate each request. For details on the
Stripe signing scheme, check out the [Stripe documentation](https://stripe.com/docs/webhooks#verify-manually).

## Step 4. Create a webhook endpoint in Stripe

1. In Stripe, go to **Developers > Webhooks**.

2. Click **Add endpoint**.

3. Enter the webhook URL from the previous step in the *Endpoint URL* field.

4. Configure the events you'd like to receive.

5. Create the endpoint.

6. Copy the signing secret and save it for the next step.

## Step 5. Validate incoming data

1. [In the Materialize console](/console/), navigate to
   the **SQL Shell**.

1. Use SQL queries to inspect and analyze the incoming data:

    ```mzsql
    SELECT * FROM stripe_source LIMIT 10;
    ```

    You may need to wait for webhook-generating events in Stripe to occur.

## Step 6. Transform incoming data

### JSON parsing

Webhook data is ingested as a JSON blob. We recommend creating a parsing view on
top of your webhook source that uses [`jsonb` operators](/sql/types/jsonb/#operators)
to map the individual fields to columns with the required data types.

```mzsql
CREATE VIEW parse_stripe AS SELECT
    body->>'api_version' AS api_version,
    to_timestamp((body->'created')::int) AS created,
    body->'data' AS data,
    body->'id' AS id,
    (body->'livemode')::boolean AS livemode,
    body->'object' AS object,
    body->>'pending_webhooks' AS pending_webhooks,
    body->'request'->'idempotency_key' AS idempotency_key,
    body->>'type' AS type
FROM stripe_source;
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

With Materialize ingesting your Stripe data, you can start exploring it,
computing real-time results that stay up-to-date as new data arrives, and
serving results efficiently. For more details, check out the
[Stripe documentation](https://stripe.com/docs/webhooks) and the
[webhook source reference documentation](/sql/create-source/webhook/).
