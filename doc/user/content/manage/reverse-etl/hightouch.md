---
title: "Hightouch"
description: "How to use Hightouch to export data out of Materialize."
menu:
  main:
    parent: "reverse-etl"
---

This guide walks you through the steps required to create a [Hightouch](https://hightouch.com/) sync using Materialize.

## Before you begin

In order to build a sync with Hightouch you will need:

* A table, view, materialized view or source within your Materialize account that you would like to export.
* A [Braze](https://www.braze.com/) account. Hightouch supports a number of possible [destinations](https://hightouch.com/integrations), we will use Braze as an example.

## Step 1. Set up a Materialize Source

To begin you will need to add your Materialize database as a source in Hightouch.

1. In Hightouch, navigate to **Sources** and then click **Add Source**.

1. From the list of connection types, choose **Materialize**.

1. In **Step 1** and **Step 2** set the connection parameters using the credentials provided in the [Materialize console](https://console.materialize.com/).
   Then click the **Continue**. After the tests pass, click **Continue**. Name your source and click **Finish**.

## Step 2. Set up a Destination

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

## Step 3. Create a Sync

After successfully adding the Materialize source, you can create a sync to send data from Materialize to your downstream destination.

{{< tabs >}}
{{< tab "Braze">}}

1. In Hightouch, navigate to **Syncs** and then click **Add Sync**.

1. Under **Select a model** select the Materialize object you would like to export.

1. Under **Select a destination** select the Braze destination configured in step 2.

1. Under **Configure sync to Braze** in **What would you like Hightouch to send to Braze?** select "User Identity". You will
   map your columns in your object to the Braze user object.

1. Under **Finalize settings for this sync** in **Schedule type** select "Cron expression". If you are using a source
   or materialized view as your source object, you can define a schedule that will update more regularly such as every minute
   (`* * * * *`). Then click **Finish**.

{{< /tab >}}
{{< /tabs >}}
