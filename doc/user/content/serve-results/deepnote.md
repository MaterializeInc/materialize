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
   - Integration name: Choose any name you prefer.
   - Host name: Use the host available in the [console](https://console.materialize.com/).
   - Port: Use the port available in the [console](https://console.materialize.com/).
   - Authentication:
      - Username: Enter the email associated with your console login.
      - Password: Use your app password or generate a new one in the [console](https://console.materialize.com/access).
      - Database: Use the database available in the [console](https://console.materialize.com/).
      - Cluster: Use your preferred cluster.

4. Click the **Create Integration** button.
5. After a successful test, in the popup dialog, you can either select an existing project or create a new one to continue.

## Step 2. Execute and visualize a query

1. Create a new SQL block.

2. Inside the block, select the new **Materialize** integration.

3. Inside the block, paste the following query:
    ```sql
    SELECT number, row_num
    FROM (
      SELECT power(series_number, 2) as number, ROW_NUMBER() OVER
        (ORDER BY series_number, series_number DESC) as row_num
      FROM (
        SELECT generate_series(0, 1000) as series_number
        ) AS subquery
    );
    ```
4. Click the **Run Notebook** button.

5. Click the {{% icons/chart %}} **Visualize** button and create the following chart:
<img width="1002" alt="Deepnote guide" src="https://github.com/joacoc/materialize/assets/11491779/fdd21c0c-db2f-4096-8d7a-dd38bdfb646d">

### Related pages

For more information about Deepnote and the integration, visit [their documentation.](https://deepnote.com/docs/materialize)
