---
title: "Census"
description: "How to use Census to export data out of Materialize."
menu:
  main:
    parent: "reverse-etl"
---

This guide walks you through the steps required to create a [Census](https://www.getcensus.com/) sync using Materialize.

## Before you begin

In order to build a sync with Census you will need:

* A table, view, materialized view or source within your Materialize account that you would like to export.
* A [Braze](https://www.braze.com/) account. Census supports a number of possible [destinations](https://www.getcensus.com/integrations), we will use Braze as an example.

## Step 1. Set up a Materialize Source

To begin you will need to add your Materialize database as a source in Census.

1. In Census, navigate to **Sources** and select **New Source**.

1. From the list of connection types, choose **Materialize**.

1. Set the connection parameters using the credentials provided in the [Materialize console](https://console.materialize.com/).
   Then press the **Connect** button.

## Step 2. Set up a Destination

Next you will add a destination where data will be sent.

{{< tabs >}}
{{< tab "Braze">}}

1. In Census, navigate to **Destinations** and select **New Destination**.

1. From the list of destinations types, choose **Braze**.

1. You will need to supply your Braze URL (which will most likely be `https://rest.iad-03.braze.com`) and a Braze API key.
   The [Census guide for Braze](https://docs.getcensus.com/destinations/braze) will explain how create an API key with the
   correct permissions. Then press the **Connect** button.

{{< /tab >}}

### Step 3. Create a Sync

After successfully adding the Materialize source, you can create a sync to send data from Materialize to your downstream destination.

{{< tabs >}}
{{< tab "Braze">}}

1. In Census navigate to **Syncs** and select **New Sync**.

1. Under **Select a Source** choose **Select a Warehouse Table**. Using the drop-down, choose the Materialize source that was
   configured in step 1 as the **Connection**. Using the **Schema** and **Table** drop-downs you can select the
   Materialize object you would like to export.

1. **Select a Destination** choose the Braze destination configured in step 2 and select "User" as the **Object**.

1. **Select Sync Behavior** can be set to "Update or Create". This will only add and modify new data in Braze but never delete users.

1. **Select a Sync Key** select an id column from the Materialize object.

1. **Set Up Braze Field Mappings** allows any of the columns in the Materialize object to the user entity in Braze.

1. Click **Next** to see an overview of your sync. You can now **Create** your sync.

{{< /tab >}}

### Step 4. Add a Schedule (Optional)

Your Census sync is created and ready to run. It can be invoked manually but a schedule will ensure all new data
is sent to the destination.

1. In Census navigate to **Syncs** and select the sync that was just created.

1. Within your sync, select **Configuration**. Under **Sync Trigger** select **Schedule**. You can pick from a number of
   difference schedules. If you are using a source or materialized view as your source object, you can select "Continuous"
   and Census will retrieve new data as soon as it exists within Materialize.
