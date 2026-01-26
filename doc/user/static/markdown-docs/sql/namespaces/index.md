# Namespaces

SQL namespaces let you create a taxonomy of objects in Materialize.



<p>Namespaces are a way to organize Materialize objects logically. In organizations
with multiple objects, namespaces help avoid naming conflicts and make it easier
to manage objects.</p>
<h2 id="namespace-hierarchy">Namespace hierarchy</h2>
<p>Materialize follows SQL standard&rsquo;s namespace hierarchy for most objects (for the
exceptions, see <a href="#other-objects" >Other objects</a>).</p>
<table>
<thead>
<tr>
<th></th>
<th></th>
</tr>
</thead>
<tbody>
<tr>
<td>1st/Highest level:</td>
<td><strong>Database</strong></td>
</tr>
<tr>
<td>2nd level:</td>
<td><strong>Schema</strong></td>
</tr>
<tr>
<td>3rd level:</td>
<td><table><tbody><tr><td><ul><li><strong>Table</strong></li><li><strong>View</strong></li><li><strong>Materialized view</strong></li><li><strong>Connection</strong></li></ul></td><td><ul><li><strong>Source</strong></li><li><strong>Sink</strong></li><li><strong>Index</strong></li></ul></td><td><ul><li><strong>Type</strong></li><li><strong>Function</strong></li><li><strong>Secret</strong></li></ul></td></tr></tbody></table></td>
</tr>
<tr>
<td>4th/Lowest level:</td>
<td><strong>Column</strong></td>
</tr>
</tbody>
</table>
<p>Each layer in the hierarchy can contain elements from the level immediately
beneath it. That is,</p>
<ul>
<li>Databases can contain: schemas;</li>
<li>Schemas can contain: tables, views, materialized views, connections, sources,
sinks, indexes, types, functions, and secrets;</li>
<li>Tables, views, and materialized views can contain: columns.</li>
</ul>
<h3 id="qualifying-names">Qualifying names</h3>
<p>Namespaces enable disambiguation and access to objects across different
databases and schemas. Namespaces use the dot notation format
(<code>&lt;database&gt;.&lt;schema&gt;....</code>) and allow you to refer to objects by:</p>
<ul>
<li>
<p><strong>Fully qualified names</strong></p>
<p>Used to reference objects in a different database (Materialize allows
cross-database queries); e.g.,</p>
<pre tabindex="0"><code>&lt;Database&gt;.&lt;Schema&gt;
&lt;Database&gt;.&lt;Schema&gt;.&lt;Source&gt;
&lt;Database&gt;.&lt;Schema&gt;.&lt;View&gt;
&lt;Database&gt;.&lt;Schema&gt;.&lt;Table&gt;.&lt;Column&gt;
</code></pre>> **Tip:** You can use fully qualified names to reference objects within the same
>   database (or within the same database and schema). However, for brevity and
>   readability, you may prefer to use qualified names instead.
>
>

</li>
<li>
<p><strong>Qualified names</strong></p>
<ul>
<li>
<p>Used to reference objects within the same database but different schema, use
the schema and object name; e.g.,</p>
<pre tabindex="0"><code>&lt;Schema&gt;.&lt;Source&gt;
&lt;Schema&gt;.&lt;View&gt;
&lt;Schema&gt;.&lt;Table&gt;.&lt;Column&gt;
</code></pre></li>
<li>
<p>Used to reference objects within the same database and schema, use the
object name; e.g.,</p>
<pre tabindex="0"><code>&lt;Source&gt;
&lt;View&gt;
&lt;Table&gt;.&lt;Column&gt;
&lt;View&gt;.&lt;Column&gt;
</code></pre></li>
</ul>
</li>
</ul>
<h2 id="namespace-constraints">Namespace constraints</h2>
<p>All namespaces must adhere to <a href="/sql/identifiers" >identifier rules</a>.</p>
<h2 id="other-objects">Other objects</h2>
<p>The following Materialize objects  exist outside the standard SQL namespace
hierarchy:</p>
<ul>
<li>
<p><strong>Clusters</strong>: Referenced directly by its name.</p>
<p>For example, to create a materialized view in the cluster <code>cluster1</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">CREATE</span> <span class="k">MATERIALIZED</span> <span class="k">VIEW</span> <span class="n">mv</span> <span class="k">IN</span> <span class="k">CLUSTER</span> <span class="n">cluster1</span> <span class="k">AS</span> <span class="mf">...</span><span class="p">;</span>
</span></span></code></pre></div></li>
<li>
<p><strong>Cluster replicas</strong>: Referenced as <code>&lt;cluster-name&gt;.&lt;replica-name&gt;</code>.</p>
<p>For example, to delete replica <code>r1</code> in cluster <code>cluster1</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">DROP</span> <span class="k">CLUSTER</span> <span class="k">REPLICA</span> <span class="n">cluster1</span><span class="mf">.</span><span class="n">r1</span>
</span></span></code></pre></div></li>
<li>
<p><strong>Roles</strong>: Referenced by their name. For example, to alter the <code>manager</code> role, your SQL statement would be:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-mzsql" data-lang="mzsql"><span class="line"><span class="cl"><span class="k">ALTER</span> <span class="k">ROLE</span> <span class="n">manager</span> <span class="mf">...</span>
</span></span></code></pre></div></li>
</ul>
<h3 id="other-object-namespace-constraints">Other object namespace constraints</h3>
<ul>
<li>
<p>Two clusters or two roles cannot have the same name. However, a cluster and a
role can have the same name.</p>
</li>
<li>
<p>Replicas can have the same names as long as they belong to different clusters.
Materialize automatically assigns names to replicas (e.g., <code>r1</code>, <code>r2</code>).</p>
</li>
</ul>
<h2 id="database-details">Database details</h2>
<ul>
<li>By default, Materialize regions have a database named <code>materialize</code>.</li>
<li>By default, each database has a schema called <code>public</code>.</li>
<li>You can specify which database you connect to either when you connect (e.g.
<code>psql -d my_db ...</code>) or within SQL using <a href="/sql/set/" ><code>SET DATABASE</code></a> (e.g.
<code>SET DATABASE = my_db</code>).</li>
<li>Materialize allows cross-database queries.</li>
</ul>
