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

Using the secret from **Step 1.**, create a webhook source to ingest data from
the webhook event generator:

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

## Next steps

To get started with your own data, [check out the reference documentation for the webhook source](/sql/create-source/webhook/).
