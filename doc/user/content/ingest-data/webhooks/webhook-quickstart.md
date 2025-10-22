---
title: "Webhooks quickstart"
description: "Learn and prototype with the webhook source without external deoendencies"
menu:
  main:
    parent: "webhooks"
    weight: 1
    name: "Quickstart"
    identifier: "quickstart-webhooks"
aliases:
  - /ingest-data/webhook-quickstart/
---

Webhook sources let your applications push webhook events into Materialize. This
quickstart uses an embedded **webhook event generator** that makes it easier for
you to learn and prototype with no external dependencies.

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Before you begin

All you need is a Materialize account. If you already have one —
great! If not, [sign up for a free trial account](https://materialize.com/register/?utm_campaign=General&utm_source=documentation) first.

When you're ready, head over to the [Materialize console](/console/),
and pop open the SQL Shell.

## Step 1. Create a secret

To validate requests between the webhook event generator and Materialize, you
need a [secret](/sql/create-secret/):

```mzsql
CREATE SECRET demo_webhook AS '<secret_value>';
```

Change the `<secret_value>` to a unique value that only you know and store it in
a secure location.

## Step 2. Set up a webhook source

Using the secret from the previous step, create a webhook source to ingest data
from the webhook event generator. By default, the source will be created in the
current cluster.

```mzsql
CREATE SOURCE webhook_demo FROM WEBHOOK
  BODY FORMAT JSON
  CHECK (
    WITH (
      HEADERS,
      BODY AS request_body,
      SECRET demo_webhook AS validation_secret
    )
    -- The constant_time_eq validation function **does not support** fully
    -- qualified secret names. We recommend always aliasing the secret name
    -- for ease of use.
    constant_time_eq(headers->'x-api-key', validation_secret)
  );
```

After a successful run, the command returns a `NOTICE` message containing the
unique [webhook URL](/sql/create-source-v1/webhook/#webhook-url)
that allows you to `POST` events to the source. Copy and store it. You will need
it for the next step.

## Step 3. Generate webhook events

The webhook event generator uses [Faker.js](https://fakerjs.dev/) under the
covers, which means you can use any of the [supported modules](https://fakerjs.dev/api/)
to shape the events.

{{% plugins/webhooks-datagen %}}

In the SQL Shell, validate that the source is ingesting data:

```mzsql
SELECT jsonb_pretty(body) AS body FROM webhook_demo LIMIT 1;
```

As an example, if you use the `Sensor data` module of the webhook event
generator, the data will look like:
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

Webhook data is ingested as a JSON blob. We recommend creating a parsing view on
top of your webhook source that uses [jsonb operators](/sql/types/jsonb/#operators)
to map the individual fields to columns with the required data types. Using the
previous example:

```mzsql
CREATE VIEW webhook_demo_parsed AS SELECT
    (body->'location'->>'latitude')::numeric AS location_latitude,
    (body->'location'->>'longitude')::numeric AS location_longitude,
    (body->>'sensor_id')::numeric AS sensor_id,
    (body->>'temperature')::numeric AS temperature,
    try_parse_monotonic_iso8601_timestamp(body->>'timestamp') AS timestamp
FROM webhook_demo;
```

## Step 5. Subscribe to see the output

To see results change over time, let’s [`SUBSCRIBE`](/sql/subscribe/) to the
`webhook_demo_parsed ` view:

```mzsql
SUBSCRIBE(SELECT * FROM webhook_demo_parsed) WITH (SNAPSHOT = FALSE);
```

You'll see results change as new webhook events are ingested. When you’re done,
cancel out of the `SUBSCRIBE` using **Stop streaming**.

## Step 6. Clean up

Once you’re done exploring the generated webhook data, remember to clean up your
environment:

```mzsql
DROP SOURCE webhook_demo CASCADE;

DROP SECRET demo_webhook;
```

## Next steps

To get started with your own data, check out the [reference documentation](/sql/create-source-v1/webhook/)
for the webhook source.
