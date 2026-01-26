# Looker

How to create dashboards with Looker



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
