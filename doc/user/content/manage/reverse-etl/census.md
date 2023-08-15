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
* The connection details for one of [destinations](https://www.getcensus.com/integrations) supported by Census.

## Step 1. Set up a Materialize Source

To begin you will need to add your Materialize database as a source in Census.

1. In Census navigate to **Sources** and select **New Source**.

1. From the list of connection types, choose **Materialize**.

1. Set the connection parameters using the credentials provided in the [Materialize console](https://console.materialize.com/).
   Then press the **Connect** button.

### Step 2. Create a Sync

After successfully adding the Materialize source, you can create a sync to send data from Materialize to your downstream destination.

1. In Census navigate to **Syncs** and select **New Sync**.

1. Under **Select a Source** choose **Select a Warehouse Table**. Using the drop-down, choose the Materialize source that was
   just configured as the **Connection**. Using the **Schema** and **Table** drop-downs you can select the
   Materialize object you would like to export.

1. In **Select a Destination** choose the destination where you would like to send your data.

1. **Select Sync Behavior** and the remaining options will be determined by your destination type.

1. Click **Next** to see an overview of your sync. You can now **Create** your sync.

### Step 3. Add a Schedule (Optional)

Your Census sync is created and ready to run. It can be invoked manually but we can attach a schedule to ensure all new data
is sent to the destination.

1. In Census navigate to **Syncs** and select the sync that was just created.

1. Within your sync, select **Configuration**. Under **Sync Trigger** select **Schedule**. You can pick from a number of
   difference schedules. If you are using a source or materialized view as your source object, you can select "Continuous"
   and Census will retrieve new data as soon as it exists within Materialize.