---
title: "Deepnote"
description: "How to create collaborative data notebooks with Deepnote"
aliases:
  - /third-party/deepnote/
  - /integrations/deepnote/
menu:
  main:
    parent: "bi-tools"
    name: "Deepnote"
    weight: 5
---

This guide walks you through the steps required to use the collaborative data notebook [Deepnote](https://deepnote.com/) with Materialize.

## Step 1. Create an integration

1. Sign in to **[Deepnote](https://deepnote.com/)**.
2. Go to the **Workspace integrations** page.
  {{< note >}}
  If you are inside a workspace, in the {{% icons/burger_menu %}} **Menu**, click **Integrations**
  {{</ note >}}
1. Click in the **+ Add Integration** button.
2. Search and click the **Materialize** option.
3. Enter the connection fields as follows:
    Field             | Value
    ----------------- | ----------------------
    Integration name  | Materialize.
    Host name         | Materialize host name.
    Port              | **6875**
    Username          | Materialize user.
    Password          | App-specific password.
    Database          | **materialize**
    Cluster           | Your preferred cluster.
4. Click the **Create Integration** button.
5. After a successful test, in the popup dialog, you can either select an existing project or create a new one to continue.

## Step 2. Execute and visualize a query

1. Create a new SQL block.

2. Inside the block, select the new **Materialize** integration and paste the following query:
    ```mzsql
    SELECT
        number,
        row_num
    FROM (
        SELECT
            power(series_number, 2) AS number,
            row_number()
                OVER
                (ORDER BY series_number ASC, series_number DESC)
            AS row_num
        FROM (
            SELECT generate_series(0, 1000) AS series_number
        ) AS subquery
    );
    ```

    This query generates a series of 1000 numbers squared and assigns row numbers to each.
3. Click the **Run Notebook** button.

4. Inside the block, click the {{% icons/chart %}} **Visualize** button and configure as follows:
   1. In the **Y Axis** options, select the **number** column and set the aggregation to **None**.
   2. In the **X Axis** options, select the **row_num** column and set the aggregation to **None**.

   <img width="1002" alt="Deepnote guide" src="https://github.com/joacoc/materialize/assets/11491779/fdd21c0c-db2f-4096-8d7a-dd38bdfb646d">


### Related pages

For more information about Deepnote and the integration, visit [their documentation.](https://deepnote.com/docs/materialize)
