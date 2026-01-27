# Deepnote
How to create collaborative data notebooks with Deepnote
This guide walks you through the steps required to use the collaborative data notebook [Deepnote](https://deepnote.com/) with Materialize.

## Create an integration

1. Sign in to **[Deepnote](https://deepnote.com/)**.
2. Go to the **Workspace integrations** page.
  > **Note:** If you are inside a workspace, in the <svg width="12px" height="12px" stroke="currentColor" fill="currentColor" stroke-width="0.5" viewBox="0 0 24 24" focusable="false" aria-hidden="true" height="1em" width="1em" xmlns="http://www.w3.org/2000/svg"><path d="M3 18h18v-2H3v2zm0-5h18v-2H3v2zm0-7v2h18V6H3z"></path></svg>
>  **Menu**, click **Integrations**

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
