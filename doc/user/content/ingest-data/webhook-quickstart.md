---
title: "Webhooks quickstart"
description: "Learn and prototype with the webhook source without external deoendencies"
menu:
  main:
    parent: "webhooks"
    weight: 1
    name: "Quickstart"
    identifier: "quickstart-webhooks"
---

Webhook sources let your applications push webhook events into Materialize. This
quickstart uses an embedded **webhook event generator** that makes it easier for
you to learn and prototype with no external dependencies.

## Before you begin

All you need is a Materialize account. If you already have one —
great! If not, [sign up for a playground account](https://materialize.com/register/?utm_campaign=General&utm_source=documentation) first.

When you're ready, head over to the [Materialize console](https://console.materialize.com/),
and pop open the SQL Shell.

## Step 1. Create a secret

To validate requests between the webhook event generator and Materialize, you
need a [secret](/sql/create-secret/):

```sql
CREATE SECRET demo_webhook AS '<secret_value>';
```

Change the `<secret_value>` to a unique value that only you know and store it in
a secure location.

## Step 2. Set up a webhook source

Using the secret from the previous step, create a webhook source to ingest data from
the webhook event generator. Replace `my_cluster` with the cluster where you want the source to run.

```sql
CREATE SOURCE webhook_demo IN CLUSTER my_cluster FROM WEBHOOK
  BODY FORMAT JSON
  CHECK (
    WITH (
      HEADERS,
      BODY AS request_body,
      SECRET demo_webhook
    )
    constant_time_eq(headers->'x-api-key', demo_webhook)
  );
```

After a successful run, the command returns a `NOTICE` message containing the
unique [webhook URL](/sql/create-source/webhook/#webhook-url)
that allows you to `POST` events to the source. Copy and store it. You will need
it for the next step.

## Step 3. Generate webhook events

The webhook event generator uses [Faker.js](https://fakerjs.dev/) under the
covers, which means you can use any of the [supported modules](https://fakerjs.dev/api/)
to shape the events.

{{% plugins/webhooks-datagen %}}

In the SQL shell, select from the source to see one of the records.

```sql
SELECT jsonb_pretty(body) AS body FROM webhook_demo LIMIT 1;
```

Here is an example payload:
```json
{
  "location": {
    "latitude": 6,
    "longitude": 0
  },
  "sensor_id": 48,
  "temperature": 89.38,
  "timestamp": "2029-10-07T10:44:13.456Z"
}
```

## Step 4. Parse JSON

{{< json-parser >}}

Paste a sample payload into the widget above. Then generate a `CREATE VIEW` statement using the view name `webhook_demo_parsed`, the source name `webhook_demo`, and JSON column name `body`. Execute the resulting `CREATE VIEW` statement in the SQL shell.

For example, here is the `CREATE VIEW` statement if you are using the sensor data generator:
```sql
CREATE VIEW webhook_demo_parsed AS SELECT
    (body->'location'->>'latitude')::numeric AS location_latitude,
    (body->'location'->>'longitude')::numeric AS location_longitude,
    (body->>'sensor_id')::numeric AS sensor_id,
    (body->>'temperature')::numeric AS temperature,
    try_parse_monotonic_iso8601_timestamp(body->>'timestamp') AS timestamp
FROM webhook_demo;
```

## Step 5. Subscribe to see the output

To see your data stream into Materialize, you can [subscribe](/sql/subscribe/) to the view you made in the previous step. Execute this statement in the SQL shell:

```sql
SUBSCRIBE(SELECT * FROM webhook_demo_parsed) WITH (SNAPSHOT = FALSE);
```
Press the "Stop Streaming" button when you want to cancel the subscription.

## Step 6. Clean up

Be kind and rewind. Execute this statement to drop all resources created in this tutorial.

```sql
DROP SECRET demo_webhook CASCADE;
```

## Next steps

To get started with your own data, [check out the reference documentation for the webhook source](/sql/create-source/webhook/).
