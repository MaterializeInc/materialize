---
title: "Hightouch"
description: "How to use Hightouch to export data out of Materialize."
menu:
  main:
    parent: "reverse-etl"
---

This guide walks you through the steps required to use [Hightouch](https://hightouch.com/) with Materialize. This will cover both setting up Materialize as a source and destination.

# Materialize as a Source

## Before you begin

In order to build a sync with Hightouch as a source you will need:

* A table, view, materialized view or source within your Materialize account that you would like to export.
* A [Braze](https://www.braze.com/) account. Hightouch supports a number of possible [destinations](https://hightouch.com/integrations), we will use Braze as an example.

## Step 1. Set up a Materialize Source

To begin you will need to add your Materialize database as a source in Hightouch.

1. In Hightouch, navigate to **Sources** and then click **Add Source**.

1. From the list of connection types, choose **Materialize**.

1. In **Step 1** and **Step 2** set the connection parameters using the credentials provided in the [Materialize console](https://console.materialize.com/).
   Then click the **Continue**. After the tests pass, click **Continue**. Name your source and click **Finish**.

## Step 2. Add Model

1. In Hightouch, navigate to **Models** and then click **Add Model**.

1. Under **Select a data source** select the Materialize source configured in step 1.

1. Under **Select a modeling method** in "Table sector" select the Materialize object you would to export then click **Continue**.

1. Under **Finalize settings for this model** name your model and click **Finish**.

## Step 3. Set up a Destination

Next you will add a destination where data will be sent.

{{< tabs >}}
{{< tab "Braze">}}

1. In Hightouch, navigate to **Destinations** and then click **Add Destination**.

1. From the list of destinations types, choose **Braze** and click **Continue**.

1. You will need to supply your Braze Region (which will most likely be `iad-03.braze.com`) and a Braze API key.
   The [Hightouch guide for Braze](https://hightouch.com/docs/destinations/braze) will explain how create an API key with the
   correct permissions. Then click the **Continue**. Name your destination and click **Finish**.

{{< /tab >}}
{{< /tabs >}}

## Step 4. Create a Sync

After successfully adding the Materialize source, you can create a sync to send data from Materialize to your downstream destination.

{{< tabs >}}
{{< tab "Braze">}}

1. In Hightouch, navigate to **Syncs** and then click **Add Sync**.

1. Under **Select a model** select the Materialize model configured in step 2.

1. Under **Select a destination** select the Braze destination configured in step 3.

1. Under **Configure sync to Braze** in **What would you like Hightouch to send to Braze?** select "User Identity". You will
   map your columns in your object to the Braze user object.

1. Under **Finalize settings for this sync** in **Schedule type** select "Cron expression". If you are using a source
   or materialized view as your source object, you can define a schedule that will update more regularly such as every minute
   (`* * * * *`). Then click **Finish**.

{{< /tab >}}
{{< /tabs >}}

# Materialize as a Destination

## Step 1. Set up a Materialize Source

You will need to create a [Webhook source](https://materialize.com/docs/sql/create-source/webhook/).

1. In Materialize create the source:
```
CREATE SOURCE hightouch_source IN CLUSTER ht FROM WEBHOOK
  BODY FORMAT JSON
  INCLUDE HEADERS;
```

1. Remember the name, schema and database from your source.

## Step 2. Set up a Destination

Next you will add a destination for your webhook source.

1. In Hightouch, navigate to **Destinations** and then click **Add Destination**.

1. From the list of destinations types, choose **HTTP Request** and click **Continue**.

1. You will need to supply the Base URL from your Materialize source. The webhook URL will be in the form `https://<HOST>/api/webhook/<database>/<schema>/<src_name>`. If you included any authorization for your webhook source, that will be included in the HTTP headers. Then click the **Continue**. Name your destination and click **Finish**.

## Step 3. Add Model

1. In Hightouch, navigate to **Models** and then click **Add Model**.

1. Any model can be used when sending data to Materialize

## Step 4. Create a Sync

After successfully adding the Materialize destination, you can create a sync to send data from your Hightouch model.

{{< tabs >}}
{{< tab "Braze">}}

1. In Hightouch, navigate to **Syncs** and then click **Add Sync**.

1. Under **Select a model** select the Materialize model configured in step 3.

1. Under **Select a destination** select the webhook destination configured in step 2.

1. Under **Which events should trigger HTTP requests?** select "Rows added." For **Which HTTP request method should be used?** select "POST." For **Which type of payload do you want to send?** select the type that matches the `BODY FORMAT` for the Materialize subsource (in this case "JSON").

1. Under **Finalize settings for this sync** in **Schedule type** select "Cron expression". If you are using a source
   or materialized view as your source object, you can define a schedule that will update more regularly such as every minute
   (`* * * * *`). Then click **Finish**.
