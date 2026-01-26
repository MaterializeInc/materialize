# Foreign data wrapper (FDW)

Use FDW to access Materialize



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
