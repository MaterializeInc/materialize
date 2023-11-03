---
title: "Stripe"
description: "How to stream data from Stripe to Materialize using webhooks"
menu:
  main:
    parent: "webhooks"
    name: "Stripe"
    weight: 15
aliases:
  - /sql/create-source/webhook/#connecting-with-stripe
---

[Stripe](https://stripe.com/) is a developer-focused payments platform. You can
use Materialize's webhook source to ingest data from Stripe in real time.

This guide will walk you through the steps to ingest data from Stripe to
Materialize.

### Before you begin

Ensure that you have a Stripe account.

## Step 1. (Optional) Create a Materialize cluster

If you already have a cluster for your webhook sources, you can skip this step.

To create a Materialize cluster, follow the steps outlined in our
create a [cluster guide](/sql/create-cluster),
or create a managed cluster for all your webhooks using the following SQL statement:

```sql
CREATE CLUSTER webhooks_cluster SIZE = '3xsmall';
```

## Step 2. Set up a webhook source

Create a [webhook source](/sql/create-source/webhook/) in Materialize to ingest
data from Stripe:

```sql
CREATE SOURCE stripe_source
IN CLUSTER webhooks_cluster
FROM WEBHOOK
    BODY FORMAT JSON;
```

After a successful run, the command returns a `NOTICE` message containing the [webhook URL](https://materialize.com/docs/sql/create-source/webhook/#webhook-url).
Copy and store it. You will need it for the next step.

If you missed the notice, you can find the URLs for all webhook sources in the
[`mz_internal.mz_webhook_sources`](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_webhook_sources)
system table.

## Step 3. Create a webhook endpoint in Stripe

1. In Stripe, go to **Developers > Webhooks**.

2. Click **Add endpoint**.

3. Enter the webhook URL from the previous step in the *Endpoint URL* field.

4. Configure the events you'd like to receive.

5. Create the endpoint.

6. Copy the signing secret and save it for the next step.

## Step 4. Recreate the webhook source with validation

To ensure that only your Stripe account can write to your webhook source, add
[validation with the `CHECK` clause](/sql/create-source/webhook/#validating-requests)
to your webhook.

First, add the signing secret from the last step to Materialize:

```sql
CREATE SECRET stripe_webhook_secret AS '<signing-secret>';
```

Then, drop the webhook source you created without validation:

```sql
DROP SOURCE stripe_source;
```

Finally, recreate the webhook source with the following `CHECK` clause:

```sql
CREATE SOURCE stripe_source
IN CLUSTER webhooks_cluster
FROM WEBHOOK
    BODY FORMAT JSON;
    CHECK (
        WITH (BODY, HEADERS, SECRET stripe_webhook_secret)
        (
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
                stripe_webhook_secret,
                'sha256'
            ), 'hex')
            =
            -- Extract the `v1` component from the `Stripe-Signature` header.
            regexp_split_to_array(headers->'stripe-signature', ',|=')[
                array_position(regexp_split_to_array(headers->'stripe-signature', ',|='), 'v1')
                + 1
            ]
        )
    );
```

For details about the Stripe signing scheme, consult the
[webhook signature validation](https://stripe.com/docs/webhooks#verify-manually)
section of the Stripe documentation.

## Step 5. Monitor incoming data

With the source set up in Materialize, you can now monitor the incoming data
from Stripe:

```sql
SELECT * FROM stripe_source LIMIT 10;
```

This will show you the last ten records ingested from Stripe. You may need
to wait for webhook-generating events in Stripe to occur.

## Step 6. Parse incoming data

Create a view to parse incoming events from Stripe. You have the option to
either build your own parser view by [pasting your event JSON
here](/transform-data/json/), or use the following starter template:

```sql
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

This view parses the incoming data, transforming the nested JSON structure into
discernible columns, such as `created` and `type`.

With the vast amount of data processed and potential network issues, it's not
uncommon to receive duplicate records. You can use the `DISTINCT` clause to
remove duplicates. For more details, refer to the webhook source
[documentation](/sql/create-source/webhook/#handling-duplicated-and-partial-events).

{{< note >}}
For comprehensive details regarding Stripe's webhook destination, refer to their
[official documentation](https://stripe.com/docs/webhooks).
{{< /note >}}

---

By following the steps outlined above, you will have successfully set up a seamless integration between Stripe and Materialize, allowing for real-time data ingestion using webhooks.
