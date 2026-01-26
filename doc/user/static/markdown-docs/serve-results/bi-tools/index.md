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
  > **Note:** If you are inside a workspace, in the <svg width="12px" height="12px" stroke="currentColor" fill="currentColor" stroke-width="0.5" viewBox="0 0 24 24" focusable="false" aria-hidden="true" height="1em" width="1em" xmlns="http://www.w3.org/2000/svg"><path d="M3 18h18v-2H3v2zm0-5h18v-2H3v2zm0-7v2h18V6H3z"></path></svg>
>  **Menu**, click **Integrations**
>

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

<p>To direct queries to a specific cluster, <a href="/sql/alter-role" >set the cluster at the role
level</a> using the following SQL statement:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-sql" data-lang="sql"><span class="line"><span class="cl"><span class="k">ALTER</span><span class="w"> </span><span class="k">ROLE</span><span class="w"> </span><span class="o">&lt;</span><span class="n">your_user</span><span class="o">&gt;</span><span class="w"> </span><span class="k">SET</span><span class="w"> </span><span class="k">CLUSTER</span><span class="w"> </span><span class="o">=</span><span class="w"> </span><span class="o">&lt;</span><span class="n">custom_cluster</span><span class="o">&gt;</span><span class="p">;</span><span class="w">
</span></span></span></code></pre></div><p>Replace <code>&lt;your_user&gt;</code> with the name of your Materialize role and <code>&lt;custom_cluster&gt;</code> with the name of the cluster you want to use.</p>
<p>Once set, all new sessions for that user will automatically run in the specified
cluster, eliminating the need to manually specify it in each query or
connection.</p>


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

4. Inside the block, click the <svg height="11px" width="11px" viewBox="0 0 11 11" focusable="false"><path fill="currentColor" d="M0.333252 0.75V9.5C0.333252 10.1376 0.862299 10.6667 1.49992 10.6667H10.2499V9.5H1.49992V0.75H0.333252ZM9.83748 2.08756L7.33325 4.5918L5.58325 2.8418L2.25415 6.1709L3.07902 6.99577L5.58325 4.49154L7.33325 6.24154L10.6624 2.91243L9.83748 2.08756Z"></path></svg>
 **Visualize** button and configure as follows:
   1. In the **Y Axis** options, select the **number** column and set the aggregation to **None**.
   2. In the **X Axis** options, select the **row_num** column and set the aggregation to **None**.

   <img width="1002" alt="Deepnote guide" src="https://github.com/joacoc/materialize/assets/11491779/fdd21c0c-db2f-4096-8d7a-dd38bdfb646d">


### Related pages

For more information about Deepnote and the integration, visit [their documentation.](https://deepnote.com/docs/materialize)


---

## Excel


Because Materialize is PostgreSQL wire-compatible, you can use a standard
PostgreSQL ODBC driver to serve data from Materialize into Excel spreadsheets.

> **Note:** The following procedure has only been tested on Windows machines using
> Materialize Cloud.
>


## Prerequisites

- A Windows machine with Excel.
- Access to your Materialize instance.
- Your Materialize Cloud connection details. For Materialize Cloud, you can find
  the connection details from the Materialize Console under `App Passwords ->
  Connect -> External Tools`.

## Setup guide

### Step 1: Install the Postgres ODBC driver

Install the latest version of the Postgres ODBC driver on your Windows machine from the [PostgreSQL ODBC driver download page](https://odbc.postgresql.org/).

### Step 2: Configure ODBC data source

You can set up your ODBC data source via the Windows Control Panel or via a
`.reg` file.



**Windows Control Panel:**
1. From the Windows control panel, find the `Set up ODBC data sources (64-bit)`
   option (assuming you are using 64-bit version of Excel).

1. If you have successfully installed pgODBC, you should see an option in
   `Create A New Data Source` called `PostgreSQL Unicode(x64)`. Select
   `PostgreSQL Unicode(x64)`.

1. Specify the [connection details for Materialize](/console/connect/). You can
   find the details from the Materialize console under `App Passwords -> Connect
   -> External Tools`. For `Password`, use your App Password (which is shown
   only once during the [service account
   creation](/console/create-new/#create-new-app-password-cloud-only)).

   ![Image of the Materialize Data Source configuration
setup](/images/excel/excel-setup-config.png "Set up Materialize Data Source
configuration.")

1. If you are using a cluster besides `default` for your view, click on
   `Datasource` to get to the **Advanced Options**. Go to `Page 2` and add `set
   cluster = <clustername>;` in the **Connect Settings** field. Click `OK`.

   ![Image of the Advanced Options](/images/excel/excel-advanced-options.png "In
Advanced Options, specify the cluster in the **Connect Settings** field.")


**.reg File:**

If you are deploying to multiple machines and do not want to use the GUI to
create the ODBC settings, you could instead create a `.reg` file to deploy the
registry settings. For example, you can save the following sample content as a
`.reg` file in Windows, updating the:
- `Driver` (path to your psqlODBC installation),
- `Database`,
- `Servername`,
- `Username`,
- `UID`,
- `Password`, and
- `ConnSettings`.

> **Note:** - Passwords stored in `.reg` files are saved in plain text. Restrict access to
> the file.
>
> - This example creates a User DSN. To create a System DSN, use
> HKEY_LOCAL_MACHINE instead.
>
>


```reg {hl_lines="4 19-25"}
Windows Registry Editor Version 5.00

[HKEY_CURRENT_USER\Software\ODBC\ODBC.INI\mz]
"Driver"="C:\\Program Files\\psqlODBC\\1700\\bin\\psqlodbcw.dll"
"CommLog"="2"
"Debug"="2"
"Fetch"="100"
"UniqueIndex"="0"
"UseDeclareFetch"="0"
"UnknownSizes"="0"
"TextAsLongVarchar"="0"
"UnknownsAsLongVarchar"="0"
"BoolsAsChar"="1"
"Parse"="0"
"MaxVarcharSize"="255"
"MaxLongVarcharSize"="8190"
"ExtraSysTablePrefixes"=""
"Description"=""
"Database"="materialize"
"Servername"="your.materialize.server.name"
"Port"="6875"
"Username"="user@materialize.com"
"UID"="user@materialize.com"
"Password"="my_app_password"
"ConnSettings"="set cluster = default;"
"ReadOnly"="0"
"ShowOidColumn"="0"
"FakeOidIndex"="0"
"RowVersioning"="0"
"ShowSystemTables"="0"
"Protocol"=""
"pqopt"=""
"UpdatableCursors"="1"
"LFConversion"="1"
"TrueIsMinus1"="0"
"BI"="0"
"AB"="0"
"ByteaAsLongVarBinary"="1"
"UseServerSidePrepare"="0"
"LowerCaseIdentifier"="0"
"GssAuthUseGSS"="0"
"SSLmode"="require"
"KeepaliveTime"="-1"
"KeepaliveInterval"="-1"
"XaOpt"="1"
"D6"="-101"
"OptionalErrors"="0"
"BatchSize"="100"
"IgnoreTimeout"="0"
"FetchRefcursors"="0"
```



### Step 3: Connect to Materialize from Excel

1. Open Excel. From the **Data** toolbar, click on `Get Data -> From Other
   Sources -> From ODBC`.

1. Select the Data source name created for Materialize and click `OK`:

   ![Image of Materialize ODBC Connection
   selection](/images/excel/excel-select-mz-source.png "Select the Materialize source
   created earlier.")

1. Select the `Default or Custom` tab, then click `Connect`. The **Navigator**
   pane will open.

1. Use the **Navigator** to select the view whose data you want to load into
   Excel. Click `Load`.

   ![Image of the Excel Navigator](/images/excel/excel-navigate-to-view.png
   "In the Navigator, select the view")

   You should see the data in your Excel spreadsheet.

1. Optional. You can manually click on `Data -> Refresh All` to refresh the
   data.

### Step 4: Configure automatic refresh

To refresh as frequently as once per minute, you can update the refresh
configuration:

1. Open **Query Properties** panel.  Go to `Data -> Queries & Connections`.
   Right click on a specific query and select `Properties`.

   ![Image of opening Query
    Properties](/images/excel/excel-open-query-properties.png "From the Data
    menubar, click Queries & Connections. Right click on your query and select
    Properties").

1. Check `Refresh every _ minutes` and set the frequency and click `OK`.

   ![Image of Query Properties - updated refresh
    rate](/images/excel/excel-query-properties-refresh.png "In Query Properties
    panel, update the Refresh every minutes field.").

To refresh more frequently than once per minute requires a custom VBA script.
See [Custom refresh rate](#custom-refresh-rate) below.

#### Custom refresh rate

For most scenarios, refreshing every minute is sufficient. However, if you need
to refresh more frequently for your use case, you can use a custom VBA script:

> **Note:** When configuring your refresh interval, note that Excel will throw an error
> if a refresh does not complete before the next one begins. Depending on your
> machine, this is typically between 5 and 15 seconds.
>
>


1. Press `Alt-F11` to open the VBA editor. On the left side, navigate to the
   VBAProject for your open spreadsheet.

1. Right click on `Microsoft Excel Objects` and select `Insert -> Module`.

   ![Image of VBA editor: Navigate to Insert ->
   Module](/images/excel/excel-vba-module.png "VBA editor: Navigate to Insert
   -> Module").

1. In the module editor, copy and paste the following code. Edit
   the `RefreshPeriod` and `Application.OnTime` values for different refresh
   rates as required.

   > **Note:** When configuring your refresh interval, note that Excel will throw an error
>    if a refresh does not complete before the next one begins. Depending on your
>    machine, this is typically between 5 and 15 seconds.
>
>


    ```text {hl_lines="7 13"}
    Sub AutoRefresh()
    ' Set the data connection to refresh every 15 seconds
    Dim conn As WorkbookConnection
    For Each conn In ThisWorkbook.Connections
        With conn.OLEDBConnection
            .BackgroundQuery = True
            .RefreshPeriod = 0.25   ' The property takes minutes as input. 0.25 minutes is equivalent to 15 seconds.
            .Refresh
        End With
    Next conn

    ' Set the macro to run itself again in 15 seconds
    Application.OnTime Now + TimeValue("00:00:15"), "AutoRefresh"
    End Sub
    ```

1. Press `Alt-Q` to close the VBA editor and return to Excel.

1. From Excel, you can use `Alt-F8` to open up the Macro window and run the
   `AutoRefresh` macro.

   ![Image of Macro window -> Run
   AutoRefresh](/images/excel/excel-macro-autorefresh.png "Macro window: run
   AutoRefresh.").


   Excel should now start updating your Materialize data at the refresh rate set
   in the macro.

## Best practices

- **Index your views**: Since Materialize efficiently serves queries from
  indexed views/indexed materialized views, ensure your views are properly
  indexed to support frequent refreshes without impacting performance.


---

## Hex


This guide walks you through the steps required to use the collaborative data notebook [Hex](https://hex.tech/) with Materialize.

## Create an integration

1. Sign in to **[Hex](https://hex.tech/)**.

2. Go to an existing project or create a new one.

3. Go to <svg data-icon="data-sources" height="13" viewBox="0 0 15 15" width="13"><g><path d="M11 2.5L5.5 9.5H9.5L9 13.5L15 6.5H10.5L11 2.5Z" fill="currentColor" opacity="0.2"></path><path clip-rule="evenodd" d="M2.14914 0.609383L2.19234 0.595681C3.37179 0.221591 4.07044 0 7 0C8.04628 0 9.01165 0.0642082 9.8677 0.181908L9.19429 1.10785C8.52823 1.03836 7.79287 1 7 1C4.21744 1 3.57924 1.20334 2.58084 1.52143L2.45136 1.56262C1.90997 1.73427 1.52165 1.92749 1.2791 2.1163C1.03316 2.30775 1 2.44121 1 2.5C1 2.55791 1.03273 2.69091 1.2788 2.88216C1.52136 3.07068 1.90973 3.26364 2.45118 3.43507L2.58077 3.47624C3.57918 3.7939 4.21751 3.997 7 3.997L7.09322 3.99682L6.36859 4.99318C3.96155 4.96242 3.28225 4.74726 2.19282 4.4022L2.14932 4.38843C1.71286 4.25024 1.32321 4.08702 1 3.89705V7.496C1 7.55479 1.03316 7.68825 1.2791 7.8797C1.52165 8.06851 1.90997 8.26173 2.45136 8.43338L2.58084 8.47457L2.58085 8.47457C2.93657 8.5879 3.24656 8.68667 3.6241 8.76686L2.98878 9.64043C2.72618 9.56964 2.47286 9.48929 2.19234 9.40032L2.14914 9.38662C1.71276 9.24826 1.32318 9.08485 1 8.89466V12.492C1 12.5507 1.03312 12.6841 1.27903 12.8755C1.52155 13.0642 1.90987 13.2573 2.45127 13.4289L2.58047 13.4699C3.47562 13.7549 4.08109 13.9477 6.20154 13.9846L6.10171 14.9829C3.90892 14.9399 3.24056 14.728 2.19209 14.3957L2.14923 14.3821C1.54888 14.1919 1.03707 13.9543 0.66491 13.6647C0.296134 13.3777 0 12.9853 0 12.492V2.5C0 2.00679 0.296094 1.61425 0.664835 1.3272C1.03698 1.03751 1.54878 0.79973 2.14914 0.609383ZM4 10L12 0L11 6H16L8 16L9 10H4ZM9.81954 7H13.9194L9.61759 12.3772L10.1805 9H6.08062L10.3824 3.62277L9.81954 7Z" fill="currentColor" fill-rule="evenodd"></path></g></g></svg>
**Data Sources > +Add > Create data connection... > Materialize**.

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

<p>To direct queries to a specific cluster, <a href="/sql/alter-role" >set the cluster at the role
level</a> using the following SQL statement:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-sql" data-lang="sql"><span class="line"><span class="cl"><span class="k">ALTER</span><span class="w"> </span><span class="k">ROLE</span><span class="w"> </span><span class="o">&lt;</span><span class="n">your_user</span><span class="o">&gt;</span><span class="w"> </span><span class="k">SET</span><span class="w"> </span><span class="k">CLUSTER</span><span class="w"> </span><span class="o">=</span><span class="w"> </span><span class="o">&lt;</span><span class="n">custom_cluster</span><span class="o">&gt;</span><span class="p">;</span><span class="w">
</span></span></span></code></pre></div><p>Replace <code>&lt;your_user&gt;</code> with the name of your Materialize role and <code>&lt;custom_cluster&gt;</code> with the name of the cluster you want to use.</p>
<p>Once set, all new sessions for that user will automatically run in the specified
cluster, eliminating the need to manually specify it in each query or
connection.</p>


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
3. Click the <svg color="#868ea4" data-icon="run" height="16" viewBox="0 0 13 13" width="16"><desc>dynamic</desc><g stroke-width="1"><g><path d="M4.5 12C4.5 12.1844 4.60149 12.3538 4.76407 12.4408C4.92665 12.5278 5.12392 12.5183 5.27735 12.416L11.2773 8.41603C11.4164 8.32329 11.5 8.16718 11.5 8C11.5 7.83282 11.4164 7.67671 11.2773 7.58397L5.27735 3.58397C5.12392 3.48169 4.92665 3.47215 4.76407 3.55916C4.60149 3.64617 4.5 3.8156 4.5 4V12Z" fill="currentColor" fill-opacity="0.3" stroke="currentColor" stroke-linejoin="round"></path></g></g></svg>
 **Run** button.

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

<p>To direct queries to a specific cluster, <a href="/sql/alter-role" >set the cluster at the role
level</a> using the following SQL statement:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-sql" data-lang="sql"><span class="line"><span class="cl"><span class="k">ALTER</span><span class="w"> </span><span class="k">ROLE</span><span class="w"> </span><span class="o">&lt;</span><span class="n">your_user</span><span class="o">&gt;</span><span class="w"> </span><span class="k">SET</span><span class="w"> </span><span class="k">CLUSTER</span><span class="w"> </span><span class="o">=</span><span class="w"> </span><span class="o">&lt;</span><span class="n">custom_cluster</span><span class="o">&gt;</span><span class="p">;</span><span class="w">
</span></span></span></code></pre></div><p>Replace <code>&lt;your_user&gt;</code> with the name of your Materialize role and <code>&lt;custom_cluster&gt;</code> with the name of the cluster you want to use.</p>
<p>Once set, all new sessions for that user will automatically run in the specified
cluster, eliminating the need to manually specify it in each query or
connection.</p>


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

<p>To direct queries to a specific cluster, <a href="/sql/alter-role" >set the cluster at the role
level</a> using the following SQL statement:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-sql" data-lang="sql"><span class="line"><span class="cl"><span class="k">ALTER</span><span class="w"> </span><span class="k">ROLE</span><span class="w"> </span><span class="o">&lt;</span><span class="n">your_user</span><span class="o">&gt;</span><span class="w"> </span><span class="k">SET</span><span class="w"> </span><span class="k">CLUSTER</span><span class="w"> </span><span class="o">=</span><span class="w"> </span><span class="o">&lt;</span><span class="n">custom_cluster</span><span class="o">&gt;</span><span class="p">;</span><span class="w">
</span></span></span></code></pre></div><p>Replace <code>&lt;your_user&gt;</code> with the name of your Materialize role and <code>&lt;custom_cluster&gt;</code> with the name of the cluster you want to use.</p>
<p>Once set, all new sessions for that user will automatically run in the specified
cluster, eliminating the need to manually specify it in each query or
connection.</p>


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

<p>To direct queries to a specific cluster, <a href="/sql/alter-role" >set the cluster at the role
level</a> using the following SQL statement:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-sql" data-lang="sql"><span class="line"><span class="cl"><span class="k">ALTER</span><span class="w"> </span><span class="k">ROLE</span><span class="w"> </span><span class="o">&lt;</span><span class="n">your_user</span><span class="o">&gt;</span><span class="w"> </span><span class="k">SET</span><span class="w"> </span><span class="k">CLUSTER</span><span class="w"> </span><span class="o">=</span><span class="w"> </span><span class="o">&lt;</span><span class="n">custom_cluster</span><span class="o">&gt;</span><span class="p">;</span><span class="w">
</span></span></span></code></pre></div><p>Replace <code>&lt;your_user&gt;</code> with the name of your Materialize role and <code>&lt;custom_cluster&gt;</code> with the name of the cluster you want to use.</p>
<p>Once set, all new sessions for that user will automatically run in the specified
cluster, eliminating the need to manually specify it in each query or
connection.</p>


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


**macOS:**

To set up a connection from Tableau Desktop to Materialize, you must:

1. Download the [Java 8 JDBC driver for PostgreSQL](https://jdbc.postgresql.org/download/)
1. Copy the `.jar` file to the following directory (which may have to be created manually):

   `~/Library/Tableau/Drivers`



**Linux:**

To set up a connection from Tableau Desktop to Materialize, you must:

1. Download the [Java 8 JDBC driver for PostgreSQL](https://jdbc.postgresql.org/download/)
1. Copy the `.jar` file to the following directory (which may have to be created manually):

   `/opt/tableau/tableau_driver/jdbc`



**Windows:**

To set up a connection from Tableau Desktop to Materialize, you must:

1. Download the [Java 8 JDBC driver for PostgreSQL](https://jdbc.postgresql.org/download/)
1. Copy the `.jar` file to the following directory (which may have to be created manually):

   `C:\Program Files\Tableau\Drivers`




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

<p>To direct queries to a specific cluster, <a href="/sql/alter-role" >set the cluster at the role
level</a> using the following SQL statement:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-sql" data-lang="sql"><span class="line"><span class="cl"><span class="k">ALTER</span><span class="w"> </span><span class="k">ROLE</span><span class="w"> </span><span class="o">&lt;</span><span class="n">your_user</span><span class="o">&gt;</span><span class="w"> </span><span class="k">SET</span><span class="w"> </span><span class="k">CLUSTER</span><span class="w"> </span><span class="o">=</span><span class="w"> </span><span class="o">&lt;</span><span class="n">custom_cluster</span><span class="o">&gt;</span><span class="p">;</span><span class="w">
</span></span></span></code></pre></div><p>Replace <code>&lt;your_user&gt;</code> with the name of your Materialize role and <code>&lt;custom_cluster&gt;</code> with the name of the cluster you want to use.</p>
<p>Once set, all new sessions for that user will automatically run in the specified
cluster, eliminating the need to manually specify it in each query or
connection.</p>


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
