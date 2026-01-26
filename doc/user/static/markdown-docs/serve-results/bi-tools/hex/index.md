# Hex

How to create collaborative data notebooks with Hex



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
