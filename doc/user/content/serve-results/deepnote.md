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

1. Sign in to **Deepnote**.
2. Go to the **Workspace integrations** page.
  {{< note >}}
  If you are inside a workspace, in the <svg width="12px" height="12px" stroke="currentColor" fill="currentColor" stroke-width="0.5" viewBox="0 0 24 24" focusable="false" aria-hidden="true" height="1em" width="1em" xmlns="http://www.w3.org/2000/svg"><path d="M3 18h18v-2H3v2zm0-5h18v-2H3v2zm0-7v2h18V6H3z"></path></svg> **Menu**, click **Integrations**
  {{</ note >}}
3. Click in the **+ Add Integration** button.
4. Search and click the **Materialize** option.
5. Enter the connection fields as follows:
   - Integration name: Choose any name you prefer.
   - Host name: Use the host available in the [console](https://console.materialize.com/).
   - Port: Use the port available in the [console](https://console.materialize.com/).
   - Authentication:
      - Username: Enter the email associated with your console login.
      - Password: Use your app password or generate a new one in the [console](https://console.materialize.com/access).
      - Database: Use the database available in the [console](https://console.materialize.com/).
      - Cluster: Use your preferred cluster.

6. Click the **Create Integration** button.
7. After a successful test, in the popup dialog, you can either select an existing project or create a new one to continue.

## Step 2. Execute and visualize a query

1. Create a new SQL block.

2. From the integrations list in the block, select the **Materialize** integration.

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

5. Click the <svg height="11px" width="11px" viewBox="0 0 11 11" focusable="false"><path fill="currentColor" d="M0.333252 0.75V9.5C0.333252 10.1376 0.862299 10.6667 1.49992 10.6667H10.2499V9.5H1.49992V0.75H0.333252ZM9.83748 2.08756L7.33325 4.5918L5.58325 2.8418L2.25415 6.1709L3.07902 6.99577L5.58325 4.49154L7.33325 6.24154L10.6624 2.91243L9.83748 2.08756Z"></path></svg> **Visualize** button and create the following chart:
<img width="1002" alt="Deepnote guide" src="https://github.com/joacoc/materialize/assets/11491779/fdd21c0c-db2f-4096-8d7a-dd38bdfb646d">

### Related pages

For more information about Deepnote and the integration, visit [their documentation.](https://deepnote.com/docs/materialize)
