---
title: "Hex"
description: "How to create collaborative data notebooks with Hex"
aliases:
  - /third-party/hex/
  - /integrations/hex/
menu:
  main:
    parent: "bi-tools"
    name: "Hex"
    weight: 5
---

This guide walks you through the steps required to use the collaborative data notebook [Hex](https://hex.tech/) with Materialize.

## Step 1. Create an integration

1. Sign in to **[Hex](https://hex.tech/)**.

2. Go to an existing project or create a new one.

3. Go to {{% icons/hex_data_sources %}}**Data Sources > +Add > Create data connection... > Materialize**.

4. Search and click the **Materialize** option.

5. Enter the connection fields as follows:
    Field               | Value
    ------------------- | ----------------------
    Name                | Materialize.
    Description         | A description you prefer.
    Host & Port         | Materialize host name, and **6875** for the port.
    Database            | **materialize**
    Authentication type | Choose the **Password** option.
    Username            | Materialize user.
    Password            | App-specific password.

6. Click the **Create connection** button.

## Step 2. Execute and visualize a query

1. Create a new SQL cell.

2. Inside the cell, select the new **Materialize** connection and paste the following query:
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
3. Click the {{% icons/hex_run %}} **Run** button.

4. Inside the cell, click the **Chart** button and configure as follows:
   1. In the **X Axis** options, select the **row_num** column.
   2. In the **Y Axis** options, select the **number** column.

   <img width="1091" alt="Hex" src="https://github.com/MaterializeInc/materialize/assets/11491779/2da93aad-9332-4d7c-a407-c068a856b9ed">


### Related pages

For more information about Hex and data connections, visit [their documentation.](https://learn.hex.tech/docs/connect-to-data/data-connections/overview)
