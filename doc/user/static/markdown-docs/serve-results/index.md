# Serve results

Serving results from Materialize



In Materialize, indexed views and materialized views maintain up-to-date query
results. This allows Materialize to serve fresh query results with low latency.

To serve results, you can:

- [Query using `SELECT` and `SUBSCRIBE`
  statements](/serve-results/query-results/)

- [Use BI/data collaboration tools](/serve-results/bi-tools/)

- [Sink results to to external systems](/serve-results/sink/)

- [Use Foreign Data Wrapper (FDW)](/serve-results/fdw/)

<div class="multilinkbox">
<div class="linkbox ">
  <div class="title">
    SELECT/SUBSCRIBE statements
  </div>
  <ul>
<li><a href="/serve-results/query-results/" >Query using <code>SELECT</code> and <code>SUBSCRIBE</code></a></li>
<li><a href="/serve-results/fdw/" >Use Foreign Data Wrapper (FDW)</a></li>
</ul>

</div>

<div class="linkbox ">
  <div class="title">
    External BI tools
  </div>
  <ul>
<li><a href="/serve-results/bi-tools/deepnote/" >Deepnote</a></li>
<li><a href="/serve-results/bi-tools/excel/" >Excel</a></li>
<li><a href="/serve-results/bi-tools/hex/" >Hex</a></li>
<li><a href="/serve-results/bi-tools/metabase/" >Metabase</a></li>
<li><a href="/serve-results/bi-tools/power-bi/" >Power BI</a></li>
<li><a href="/serve-results/bi-tools/tableau/" >Tableau</a></li>
<li><a href="/serve-results/bi-tools/looker/" >Looker</a></li>
</ul>

</div>


<div class="linkbox ">
  <div class="title">
    Sink results
  </div>
  <ul>
<li><a href="/serve-results/sink/s3/" >Sinking results to Amazon S3</a></li>
<li><a href="/serve-results/sink/census/" >Sinking results to Census</a></li>
<li><a href="/serve-results/sink/kafka/" >Sinking results to Kafka</a></li>
<li><a href="/serve-results/sink/snowflake/" >Sinking results to Snowflake</a></li>
</ul>

</div>


</div>




---

## `SELECT` and `SUBSCRIBE`


You can query results from Materialize using `SELECT` and `SUBSCRIBE` SQL
statements. Because Materialize uses the PostgreSQL wire protocol, it works
out-of-the-box with a wide range of SQL clients and tools that support
PostgreSQL.

## SELECT

You can query data in Materialize using the [`SELECT` statement](/sql/select/).
For example:

```mzsql
SELECT region.id, sum(purchase.total)
FROM mysql_simple_purchase AS purchase
JOIN mysql_simple_user AS user ON purchase.user_id = user.id
JOIN mysql_simple_region AS region ON user.region_id = region.id
GROUP BY region.id;
```

Performing a `SELECT` on an indexed view or a materialized view is
Materialize's ideal operation. When Materialize receives such a `SELECT` query,
it quickly returns the maintained results from memory.

Materialize also quickly returns results for queries that only filter, project,
transform with scalar functions, and re-order data that is maintained by an
index.

Queries that can't simply read out from an index will create an ephemeral dataflow to compute
the results. These dataflows are bound to the active [cluster](/concepts/clusters/),
 which you can change using:

```mzsql
SET cluster = <cluster name>;
```

Materialize will remove the dataflow as soon as it has returned the query
results to you.

For more information, see [`SELECT`](/sql/select/) reference page.  See
also the following client library guides:

<ul style="column-count: 2"><li><a href="/integrations/client-libraries/golang/#query" >Go</a></li></li><li><a href="/integrations/client-libraries/java-jdbc/#query" >Java</a></li></li><li><a href="/integrations/client-libraries/node-js/#query" >Node.js</a></li></li><li><a href="/integrations/client-libraries/php/#query" >PHP</a></li></li><li><a href="/integrations/client-libraries/python/#query" >Python</a></li></li><li><a href="/integrations/client-libraries/ruby/#query" >Ruby</a></li></li><li><a href="/integrations/client-libraries/rust/#query" >Rust</a></li></li></ul>


## SUBSCRIBE

You can use [`SUBSCRIBE`](/sql/subscribe/) to stream query results.  For
example:

```mzsql
BEGIN;
DECLARE c CURSOR FOR SUBSCRIBE (SELECT * FROM mv_counter_sum);
FETCH 10 c WITH (timeout='1s');
FETCH 20 c WITH (timeout='1s');
COMMIT;
```

The [`SUBSCRIBE`](/sql/subscribe/) statement is a more general form of a `SELECT` statement. While a `SELECT` statement computes a relation at a moment in time, a `SUBSCRIBE` operation computes how a relation changes over time.

You can use `SUBSCRIBE` to:

- Power event processors that react to every change to a relation or an
  arbitrary `SELECT` statement.

- Replicate the complete history of a relation while `SUBSCRIBE` is active.

> **Tip:** Use materialized view (instead of an indexed view) with `SUBSCRIBE`.
>


For more information, see [`SUBSCRIBE`](/sql/subscribe/) reference page.  See
also the following client library guides:

<ul style="column-count: 2"><li><a href="/integrations/client-libraries/golang/#stream" >Go</a></li></li><li><a href="/integrations/client-libraries/java-jdbc/#stream" >Java</a></li></li><li><a href="/integrations/client-libraries/node-js/#stream" >Node.js</a></li></li><li><a href="/integrations/client-libraries/php/#stream" >PHP</a></li></li><li><a href="/integrations/client-libraries/python/#stream" >Python</a></li></li><li><a href="/integrations/client-libraries/ruby/#stream" >Ruby</a></li></li><li><a href="/integrations/client-libraries/rust/#stream" >Rust</a></li></li></ul>



---

## Sink results


A [sink](/concepts/sinks/) describes the external system you want Materialize to
write data to and details the encoding of that data. You can sink data from a
**materialized** view, a source, or a table.

## Sink methods

To create a sink, you can:


| Method | External system | Guide(s) or Example(s) |
| --- | --- | --- |
| Use <code>COPY TO</code> command | Amazon S3 or S3-compatible storage | <ul> <li><a href="/serve-results/sink/s3/" >Sink to Amazon S3</a></li> </ul>  |
| Use Census as an intermediate step | Census supported destinations | <ul> <li><a href="/serve-results/sink/census/" >Sink to Census</a></li> </ul>  |
| Use <code>COPY TO</code> S3 or S3-compatible storage as an intermediate step | Snowflake and other systems that can read from S3 | <ul> <li><a href="/serve-results/sink/snowflake/" >Sink to Snowflake</a></li> </ul>  |
| Use a native connector | Kafka/Redpanda | <ul> <li><a href="/serve-results/sink/kafka/" >Sink to Kafka/Redpanda</a></li> </ul>  |
| Use <code>SUBSCRIBE</code> | Various | <ul> <li><a href="https://github.com/MaterializeInc/mz-catalog-sync" >Sink to Postgres</a></li> <li><a href="https://github.com/MaterializeIncLabs/mz-redis-sync" >Sink to Redis</a></li> </ul>  |


### Operational guideline

- Avoid putting sinks on the same cluster that hosts sources to allow for
[blue/green deployment](/manage/dbt/blue-green-deployments).

### Troubleshooting

For help, see [Troubleshooting
sinks](/serve-results/sink/sink-troubleshooting/).


---

## Use BI/data collaboration tools


Materialize uses the PostgreSQL wire protocol, which allows it to integrate out-of-the-box with various BI/data collaboration tools that support PostgreSQL.

To help you get started, the following guides are available:


---

## Use foreign data wrapper (FDW)


Materialize can be used as a remote server in a PostgreSQL foreign data wrapper
(FDW). This allows you to query any object in Materialize as foreign tables from
a PostgreSQL-compatible database. These objects appear as part of the local
schema, making them accessible over an existing Postgres connection without
requiring changes to application logic or tooling.

## Prerequisite

<ol>
<li>
<p>In Materialize, create a dedicated service account <code>fdw_svc_account</code> as an
<strong>Organization Member</strong>. For details on setting up a service account, see
<a href="https://materialize.com/docs/manage/users-service-accounts/create-service-accounts/" >Create a service
account</a></p>
> **Tip:** Per the linked instructions, be sure you connect at least once with the new
>    service account to finish creating the new account. You will also need the
>    connection details (host, port, password) when setting up the foreign server
>    and user mappings in PostgreSQL.
>
>

</li>
<li>
<p>After you have connected at least once with the new service account to finish
the new account creation, modify the <code>fdw_svc_account</code> role:</p>
<ol>
<li>
<p>Set the default cluster to the name of your serving cluster:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">ROLE</span> <span class="n">fdw_svc_account</span> <span class="k">SET</span> <span class="k">CLUSTER</span> <span class="o">=</span> <span class="o">&lt;</span><span class="n">serving_cluster</span><span class="o">&gt;</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p><a href="/sql/grant-privilege/" >Grant <code>USAGE</code> privileges</a> on the serving cluster,
and the database and schema of your views and materialized views.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">USAGE</span> <span class="k">ON</span> <span class="k">CLUSTER</span> <span class="o">&lt;</span><span class="n">serving_cluster</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">fdw_svc_account</span><span class="p">;</span>
</span></span><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">USAGE</span> <span class="k">ON</span> <span class="k">DATABASE</span> <span class="o">&lt;</span><span class="n">db_name</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">fdw_svc_account</span><span class="p">;</span>
</span></span><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">USAGE</span> <span class="k">ON</span> <span class="k">SCHEMA</span> <span class="o">&lt;</span><span class="n">db_name</span><span class="mf">.</span><span class="n">schema_name</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">fdw_svc_account</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p><a href="/sql/grant-privilege/" >Grant <code>SELECT</code> privileges</a> to the various
view(s)/materialized view(s):</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">GRANT</span> <span class="k">SELECT</span> <span class="k">ON</span> <span class="o">&lt;</span><span class="n">db_name</span><span class="mf">.</span><span class="n">schema_name</span><span class="mf">.</span><span class="n">view_name</span><span class="o">&gt;</span><span class="p">,</span> <span class="o">&lt;</span><span class="mf">...</span><span class="o">&gt;</span> <span class="k">TO</span> <span class="n">fdw_svc_account</span><span class="p">;</span>
</span></span></code></pre></div></li>
</ol>
</li>
</ol>


## Setup FDW in PostgreSQL

<p><strong>In your PostgreSQL instance</strong>:</p>
<ol>
<li>
<p>If not installed, create a <code>postgres_fdw</code> extension in your database:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="n">EXTENSION</span> <span class="n">postgres_fdw</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p>Create a foreign server to your Materialize, substitute your <a href="/console/connect/" >Materialize
connection details</a>.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="n">SERVER</span> <span class="n">remote_mz_server</span>
</span></span><span class="line"><span class="cl">   <span class="k">FOREIGN</span> <span class="n">DATA</span> <span class="n">WRAPPER</span> <span class="n">postgres_fdw</span>
</span></span><span class="line"><span class="cl">   <span class="k">OPTIONS</span> <span class="p">(</span><span class="k">host</span> <span class="s1">&#39;&lt;host&gt;&#39;</span><span class="p">,</span> <span class="n">dbname</span> <span class="s1">&#39;&lt;db_name&gt;&#39;</span><span class="p">,</span> <span class="k">port</span> <span class="s1">&#39;6875&#39;</span><span class="p">);</span>
</span></span></code></pre></div></li>
<li>
<p>Create a user mapping between your PostgreSQL user and the Materialize
<code>fdw_svc_account</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">USER</span> <span class="n">MAPPING</span> <span class="k">FOR</span> <span class="o">&lt;</span><span class="n">postgres_user</span><span class="o">&gt;</span>
</span></span><span class="line"><span class="cl">   <span class="n">SERVER</span> <span class="n">remote_mz_server</span>
</span></span><span class="line"><span class="cl">   <span class="k">OPTIONS</span> <span class="p">(</span><span class="k">user</span> <span class="s1">&#39;fdw_svc_account&#39;</span><span class="p">,</span> <span class="k">password</span> <span class="s1">&#39;&lt;service_account_password&gt;&#39;</span><span class="p">);</span>
</span></span></code></pre></div></li>
<li>
<p>For each view/materialized view you want to access, create the foreign table
mapping (you can use the <a href="/console/data/" >data explorer</a> to get the column
detials)</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">FOREIGN</span> <span class="k">TABLE</span> <span class="o">&lt;</span><span class="n">local_view_name_in_postgres</span><span class="o">&gt;</span> <span class="p">(</span>
</span></span><span class="line"><span class="cl">         <span class="o">&lt;</span><span class="k">column</span><span class="o">&gt;</span> <span class="o">&lt;</span><span class="k">type</span><span class="o">&gt;</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">         <span class="mf">...</span>
</span></span><span class="line"><span class="cl">     <span class="p">)</span>
</span></span><span class="line"><span class="cl"><span class="n">SERVER</span> <span class="n">remote_mz_server</span>
</span></span><span class="line"><span class="cl"><span class="k">OPTIONS</span> <span class="p">(</span><span class="n">schema_name</span> <span class="s1">&#39;&lt;schema&gt;&#39;</span><span class="p">,</span> <span class="n">table_name</span> <span class="s1">&#39;&lt;view_name_in_Materialize&gt;&#39;</span><span class="p">);</span>
</span></span></code></pre></div></li>
<li>
<p>Once created, you can select from within PostgreSQL:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">SELECT</span> <span class="o">*</span> <span class="k">from</span> <span class="o">&lt;</span><span class="n">local_view_name_in_postgres</span><span class="o">&gt;</span><span class="p">;</span>
</span></span></code></pre></div></li>
</ol>
