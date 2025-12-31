# Use BI/data collaboration tools

Querying results from Materialize using external BI/data collaboration tools



Materialize uses the PostgreSQL wire protocol, which allows it to integrate out-of-the-box with various BI/data collaboration tools that support PostgreSQL.

To help you get started, the following guides are available:




---

## Deepnote


This guide walks you through the steps required to use the collaborative data notebook [Deepnote](https://deepnote.com/) with Materialize.

## Create an integration

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

## Configure a custom cluster

{{% alter-cluster/configure-cluster %}}

## Execute and visualize a query

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




---

## Hex


This guide walks you through the steps required to use the collaborative data notebook [Hex](https://hex.tech/) with Materialize.

## Create an integration

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

## Configure a custom cluster

{{% alter-cluster/configure-cluster %}}

## Execute and visualize a query

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




---

## Looker


You can use [Looker](https://cloud.google.com/looker-bi) to create dashboards
based on the data maintained in Materialize.

## Database connection details

To set up a connection from Looker to Materialize, use the native
[PostgreSQL 9.5+ database dialect](https://cloud.google.com/looker/docs/db-config-postgresql)
with the following parameters:

Field                  | Value
---------------------- | ----------------
Dialect                | **PostgreSQL 9.5+**.
Host                   | Materialize host name.
Port                   | **6875**
Database               | **materialize**
Schema                 | **public**
Database username      | Materialize user.
Database password      | App-specific password.

![Connect using the credentials provided in the Materialize console](https://github-production-user-asset-6210df.s3.amazonaws.com/21223421/272911799-2525c5ae-4594-4d33-bdfa-c20af835c7c5.png)

## Configure a custom cluster

{{% alter-cluster/configure-cluster %}}

## Known limitations

When using Looker with Materialize, be aware of the following limitations:

1. **Connection Test Error**: You might encounter this error when testing the connection to Materialize from Looker:

   ```
   Test kill: Cannot cancel queries: Query could not be found in database.
   ```

   This error occurs because Looker attempts to run a test query cancellation, which checks for `pg_stat_activity` (not currently supported in Materialize).

   While this error can be safely ignored and doesn't impact most Looker functionality, there are workarounds for query cancellation if you need to stop a running query:

   a. Use `pg_cancel_backend` in Materialize:

      ```sql
      SELECT pg_cancel_backend(connection_id)
      FROM mz_sessions
      WHERE id = 'your_session_id';
      ```

   b. Via the Materialize Console:
      - Go to [Materialize Console](/console/)
      - Navigate to Query History
      - Filter by 'Running' queries
      - Click on the query you want to cancel
      - Select "Request Cancellation"

2. **Symmetric Aggregates**: Looker uses symmetric aggregates, which rely on types and operations not fully supported in Materialize:

   - Materialize doesn't support the `BIT` type used by Looker for symmetric aggregates.
   - Looker may use various aggregations (e.g., `SUM DISTINCT`, `AVG DISTINCT`) for symmetric aggregates.
   - You can disable symmetric aggregates in Looker if needed. For instructions, refer to the [Looker documentation on disabling symmetric aggregates](https://cloud.google.com/looker/docs/reference/param-explore-symmetric-aggregates#not_all_database_dialects_support_median_and_percentile_measure_types_with_symmetric_aggregates).
   - Looker is fully functional for non-symmetric aggregations when visualizing Materialize data.

3. **Handling Symmetric Aggregates**:

   a. Test query performance with and without symmetric aggregates to determine the optimal configuration.

   b. If you encounter performance issues, disable symmetric aggregates in your Looker setup using the link provided above.

   c. For use cases requiring symmetric aggregates, contact Materialize support for optimization guidance.




---

## Metabase


You can use [Metabase](https://www.metabase.com/) to create real-time dashboards
based on the data maintained in Materialize.

## Database connection details

To set up a connection from Metabase to Materialize, use the native
[PostgreSQL database driver](https://www.metabase.com/docs/latest/administration-guide/databases/postgresql.html)
with the following parameters:

Field             | Value
----------------- | ----------------
Database type     | **PostgreSQL**
Host              | Materialize host name.
Port              | **6875**
Database name     | **materialize**
Database username | Materialize user.
Database password | App-specific password.
SSL mode          | Require

For more details and troubleshooting, check the
[Metabase documentation](https://www.metabase.com/docs/latest/administration-guide/databases/postgresql.html).

## Configure a custom cluster

{{% alter-cluster/configure-cluster %}}

## Refresh rate

By default, the lowest [refresh rate](https://www.metabase.com/docs/latest/users-guide/07-dashboards.html#auto-refresh)
for Metabase dashboards is 1 minute. You can manually set this to a lower
interval by adding `#refresh=1` (as an example, for a `1` second interval) to
the end of the URL, and opening the modified URL in a new tab.

Because Metabase queries are simply reading data out of self-updating views in
Materialize, setting your dashboards to auto-refresh at lower rates should not
have a significant impact on database performance. To minimize this impact, we
recommend carefully choosing an [indexing strategy](/sql/create-index/)
for any objects serving results to Metabase.

[//]: # "TODO(morsapaes) Once we revamp quickstarts, add Related pages section
pointing to a quickstart that uses Metabase"




---

## Power BI


You can use [Power BI](https://powerbi.microsoft.com/) to create dashboards
based on the data maintained in Materialize.

## Database connection details

To set up a connection from Power BI to Materialize, use the native
[PostgreSQL database driver](https://learn.microsoft.com/en-us/power-query/connectors/postgresql#connect-to-a-postgresql-database-from-power-query-desktop)
with the following parameters:

Field                  | Value
---------------------- | ----------------
Database type          | **PostgreSQL database**
Server                 | Your Materialize host name followed by `:6875`<br> For example: `id.us-east-1.aws.materialize.cloud:6875`
Database               | **materialize**
Data Connectivity mode | **DirectQuery**
Database username      | Materialize user.
Database password      | App-specific password.

![Connect using the credentials provided in the Materialize console](https://github-production-user-asset-6210df.s3.amazonaws.com/21223421/266625944-de7dfdc6-7a94-4e87-ac0a-01e104512ffe.png)

## Configure a custom cluster

{{% alter-cluster/configure-cluster %}}

## Troubleshooting

Errors like the following indicate that there is a problem with the connection settings:

1. `A non-recoverable error happened during a database lookup.`

    If you see this error, check that the server name is correct and that you are using port `6875`. Note that the server name should not include the protocol (`http://` or `https://`), and should not include a trailing slash (`/`) or the database name.

2. `PostgreSQL: No password has been provided but the backend requires one (in cleartext)`

    If you see this error, check that you have entered the correct password. If you are using an app-specific password, make sure that you have not included any spaces or other characters that are not part of the password. If the issue persists, try the following:

    - Go to **File**
    - Options and settings
    - Data source settings
    - Delete any references for the Materialize host from the data source list
    - Try to connect again

For more details and troubleshooting, check the
[Power BI documentation](https://learn.microsoft.com/en-us/power-query/connectors/postgresql#troubleshooting).

## Known limitations

When you connect to Materialize from Power BI, you will get a list of your tables and views.

However, [Power BI does not display materialized views](https://ideas.fabric.microsoft.com/ideas/idea/?ideaid=92420736-afdc-45b9-8962-743a53acfa66) in that list.

To work around this Power BI limitation, you can use one of the following options:

1. Create a view that selects from the materialized view, and then use the view in Power BI.

    For example, if you have a materialized view called `my_view`, you can create a view called `my_view_bi` with the following SQL:

    ```mzsql
    CREATE VIEW my_view_bi AS SELECT * FROM my_view;
    ```

    Then, in Power BI, you can use the `my_view_bi` view.

2. If applicable, instead of using a materialized view, create a view with an [index](/sql/create-index) instead.

3. Use the [Power BI Native query folding](https://learn.microsoft.com/en-us/power-query/connectors/postgresql#native-query-folding) to write your own query rather than using the Power BI UI. For example:

    ```
    = Value.NativeQuery(Source, "select * from my_view;")
    ```




---

## Tableau


You can use [Tableau Cloud](https://www.tableau.com/products/cloud-bi) and
[Tableau Desktop](https://www.tableau.com/products/desktop) to create real-time
dashboards based on the data maintained in Materialize.

## Tableau Cloud

### Database connection details

To set up a connection from Tableau Cloud to Materialize, use the native
[PostgreSQL database connector](https://help.tableau.com/current/pro/desktop/en-us/examples_postgresql.htm)
with the following parameters:

Field             | Value
----------------- | ----------------
Server            | Materialize host name.
Port              | **6875**
Database          | **materialize**
Username          | Materialize user.
Password          | App-specific password.
Require SSL       | ✓

For more details and troubleshooting, check the
[Tableau documentation](https://help.tableau.com/current/pro/desktop/en-us/examples_postgresql.htm).

[//]: # "TODO(morsapaes) Clarify minimum refresh rate and details about live
connections"

## Tableau Desktop

### Setup

{{< tabs >}}
{{< tab "macOS">}}

To set up a connection from Tableau Desktop to Materialize, you must:

1. Download the [Java 8 JDBC driver for PostgreSQL](https://jdbc.postgresql.org/download/)
1. Copy the `.jar` file to the following directory (which may have to be created manually):

   `~/Library/Tableau/Drivers`

{{< /tab >}}

{{< tab "Linux">}}

To set up a connection from Tableau Desktop to Materialize, you must:

1. Download the [Java 8 JDBC driver for PostgreSQL](https://jdbc.postgresql.org/download/)
1. Copy the `.jar` file to the following directory (which may have to be created manually):

   `/opt/tableau/tableau_driver/jdbc`

{{< /tab >}}

{{< tab "Windows">}}

To set up a connection from Tableau Desktop to Materialize, you must:

1. Download the [Java 8 JDBC driver for PostgreSQL](https://jdbc.postgresql.org/download/)
1. Copy the `.jar` file to the following directory (which may have to be created manually):

   `C:\Program Files\Tableau\Drivers`

{{< /tab >}}
{{< /tabs >}}

### Database connection details

Once you've set up the required driver, start Tableau and run through the
following steps:

1. On the left side, find the **Connect to a Server** section
1. Select **More** and then **PostgreSQL**
1. Use the following details to configure the connection:

    Field          | Value
    -------------- | ----------------------
    Server         | Materialize host name.
    Port           | **6875**
    Database       | **materialize**
    Authentication | Username and Password
    Username       | Materialize user.
    Password       | App-specific password.
    Require SSL    | ✓

4. Click **Sign In** to connect to Materialize

For more details and troubleshooting, check the
[Tableau documentation](https://help.tableau.com/current/pro/desktop/en-us/examples_postgresql.htm).

[//]: # "TODO(morsapaes) Clarify minimum refresh rate and details about live
connections"

## Configure a custom cluster

{{% alter-cluster/configure-cluster %}}

### Troubleshooting

Errors like the following indicate that the JDBC driver was not successfully
installed.

```
ERROR: Expected FOR, found WITH;
Error while executing the query
```

```
ERROR: WITH HOLD is unsupported for cursors;
Error while executing the query
```

The errors occur because Tableau falls back to a legacy PostgreSQL ODBC driver
that does not support connecting to Materialize. Follow the [Setup](#setup)
instructions again and ensure you've downloaded the driver to the correct folder
for your platform.



