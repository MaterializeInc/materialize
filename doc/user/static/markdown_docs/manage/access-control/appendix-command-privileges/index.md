<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)  /  [Manage
Materialize](/docs/self-managed/v25.2/manage/)  /  [Access control
(Role-based)](/docs/self-managed/v25.2/manage/access-control/)

</div>

# Appendix: Privileges by commands

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Command</th>
<th>Privileges</th>
</tr>
</thead>
<tbody>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/alter-cluster"><code>ALTER CLUSTER</code></a></td>
<td><ul>
<li><p>Ownership of the cluster.</p></li>
<li><p>To rename a cluster, you must also have membership in the
<code>&lt;new_owner_role&gt;</code>.</p></li>
<li><p>To swap names with another cluster, you must also have ownership
of the other cluster.</p></li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/alter-connection"><code>ALTER CONNECTION</code></a></td>
<td><ul>
<li>Ownership of the connection.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/alter-default-privileges"><code>ALTER DEFAULT PRIVILEGES</code></a></td>
<td><ul>
<li>Role membership in <code>role_name</code>.</li>
<li><code>USAGE</code> privileges on the containing database if
<code>database_name</code> is specified.</li>
<li><code>USAGE</code> privileges on the containing schema if
<code>schema_name</code> is specified.</li>
<li><em>superuser</em> status if the <em>target_role</em> is
<code>PUBLIC</code> or <strong>ALL ROLES</strong> is specified.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/alter-index"><code>ALTER INDEX</code></a></td>
<td><ul>
<li>Ownership of the index.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/alter-materialized-view"><code>ALTER MATERIALIZED VIEW</code></a></td>
<td><ul>
<li>Ownership of the materialized view.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/alter-role"><code>ALTER ROLE</code></a></td>
<td><ul>
<li><code>CREATEROLE</code> privileges on the system.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/alter-owner"><code>ALTER ... OWNER</code></a></td>
<td><ul>
<li>Ownership of the object being altered.</li>
<li>Role membership in <code>new_owner</code>.</li>
<li><code>CREATE</code> privileges on the containing cluster if the
object is a cluster replica.</li>
<li><code>CREATE</code> privileges on the containing database if the
object is a schema.</li>
<li><code>CREATE</code> privileges on the containing schema if the
object is namespaced by a schema.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/alter-rename"><code>ALTER ... RENAME</code></a></td>
<td><ul>
<li>Ownership of the object being renamed.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/alter-swap"><code>ALTER ... SWAP</code></a></td>
<td><ul>
<li>Ownership of both objects being swapped.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/alter-secret"><code>ALTER SECRET</code></a></td>
<td><ul>
<li>Ownership of the secret being altered.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/alter-sink"><code>ALTER SINK</code></a></td>
<td><ul>
<li>Ownership of the sink being altered.</li>
<li><code>SELECT</code> privileges on the new relation being written out
to an external system.</li>
<li><code>CREATE</code> privileges on the cluster maintaining the
sink.</li>
<li><code>USAGE</code> privileges on all connections and secrets used in
the sink definition.</li>
<li><code>USAGE</code> privileges on the schemas that all connections
and secrets in the statement are contained in.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/alter-source"><code>ALTER SOURCE</code></a></td>
<td><ul>
<li>Ownership of the source being altered.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/alter-system-reset"><code>ALTER SYSTEM RESET</code></a></td>
<td><ul>
<li><em>Superuser</em> privileges</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/alter-system-set"><code>ALTER SYSTEM SET</code></a></td>
<td><ul>
<li><em>Superuser</em> privileges</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/comment-on"><code>COMMENT ON</code></a></td>
<td><ul>
<li>Ownership of the object being commented on (unless the object is a
role).</li>
<li>To comment on a role, you must have the <code>CREATEROLE</code>
privilege.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/copy-from"><code>COPY FROM</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schema containing the
table.</li>
<li><code>INSERT</code> privileges on the table.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/copy-to"><code>COPY TO</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schemas that all relations and
types in the query are contained in.</li>
<li><code>SELECT</code> privileges on all relations in the query.
<ul>
<li>NOTE: if any item is a view, then the view owner must also have the
necessary privileges to execute the view definition. Even if the view
owner is a <em>superuser</em>, they still must explicitly be granted the
necessary privileges.</li>
</ul></li>
<li><code>USAGE</code> privileges on all types used in the query.</li>
<li><code>USAGE</code> privileges on the active cluster.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/create-cluster"><code>CREATE CLUSTER</code></a></td>
<td><ul>
<li><code>CREATECLUSTER</code> privileges on the system.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/create-cluster-replica"><code>CREATE CLUSTER REPLICA</code></a></td>
<td><ul>
<li>Ownership of the cluster.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/create-connection"><code>CREATE CONNECTION</code></a></td>
<td><ul>
<li><code>CREATE</code> privileges on the containing schema.</li>
<li><code>USAGE</code> privileges on all connections and secrets used in
the connection definition.</li>
<li><code>USAGE</code> privileges on the schemas that all connections
and secrets in the statement are contained in.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/create-database"><code>CREATE DATABASE</code></a></td>
<td><ul>
<li><code>CREATEDB</code> privileges on the system.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/create-index"><code>CREATE INDEX</code></a></td>
<td><ul>
<li>Ownership of the object on which to create the index.</li>
<li><code>CREATE</code> privileges on the containing schema.</li>
<li><code>CREATE</code> privileges on the containing cluster.</li>
<li><code>USAGE</code> privileges on all types used in the index
definition.</li>
<li><code>USAGE</code> privileges on the schemas that all types in the
statement are contained in.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/create-materialized-view"><code>CREATE MATERIALIZED VIEW</code></a></td>
<td><ul>
<li><code>CREATE</code> privileges on the containing schema.</li>
<li><code>CREATE</code> privileges on the containing cluster.</li>
<li><code>USAGE</code> privileges on all types used in the materialized
view definition.</li>
<li><code>USAGE</code> privileges on the schemas for the types used in
the statement.</li>
<li>Ownership of the existing view if replacing an existing view with
the same name (i.e., <code>OR REPLACE</code> is specified in
<code>CREATE MATERIALIZED VIEW</code> command).</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/create-role"><code>CREATE ROLE</code></a></td>
<td><ul>
<li><code>CREATEROLE</code> privileges on the system.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/create-schema"><code>CREATE SCHEMA</code></a></td>
<td><ul>
<li><code>CREATE</code> privileges on the containing database.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/create-secret"><code>CREATE SECRET</code></a></td>
<td><ul>
<li><code>CREATE</code> privileges on the containing schema.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/create-sink"><code>CREATE SINK</code></a></td>
<td><ul>
<li><code>CREATE</code> privileges on the containing schema.</li>
<li><code>SELECT</code> privileges on the item being written out to an
external system.
<ul>
<li>NOTE: if the item is a materialized view, then the view owner must
also have the necessary privileges to execute the view definition.</li>
</ul></li>
<li><code>CREATE</code> privileges on the containing cluster if the sink
is created in an existing cluster.</li>
<li><code>CREATECLUSTER</code> privileges on the system if the sink is
not created in an existing cluster.</li>
<li><code>USAGE</code> privileges on all connections and secrets used in
the sink definition.</li>
<li><code>USAGE</code> privileges on the schemas that all connections
and secrets in the statement are contained in.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/create-source"><code>CREATE SOURCE</code></a></td>
<td><ul>
<li><code>CREATE</code> privileges on the containing schema.</li>
<li><code>CREATE</code> privileges on the containing cluster if the
source is created in an existing cluster.</li>
<li><code>CREATECLUSTER</code> privileges on the system if the source is
not created in an existing cluster.</li>
<li><code>USAGE</code> privileges on all connections and secrets used in
the source definition.</li>
<li><code>USAGE</code> privileges on the schemas that all connections
and secrets in the statement are contained in.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/create-table"><code>CREATE TABLE</code></a></td>
<td><ul>
<li><code>CREATE</code> privileges on the containing schema.</li>
<li><code>USAGE</code> privileges on all types used in the table
definition.</li>
<li><code>USAGE</code> privileges on the schemas that all types in the
statement are contained in.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/create-type"><code>CREATE TYPE</code></a></td>
<td><ul>
<li><code>CREATE</code> privileges on the containing schema.</li>
<li><code>USAGE</code> privileges on all types used in the type
definition.</li>
<li><code>USAGE</code> privileges on the schemas that all types in the
statement are contained in.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/create-view"><code>CREATE VIEW</code></a></td>
<td><ul>
<li><code>CREATE</code> privileges on the containing schema.</li>
<li><code>USAGE</code> privileges on all types used in the view
definition.</li>
<li><code>USAGE</code> privileges on the schemas for the types in the
statement.</li>
<li>Ownership of the existing view if replacing an existing view with
the same name (i.e., <code>OR REPLACE</code> is specified in
<code>CREATE VIEW</code> command).</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/delete"><code>DELETE</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schemas that all relations and
types in the query are contained in.</li>
<li><code>DELETE</code> privileges on <code>table_name</code>.</li>
<li><code>SELECT</code> privileges on all relations in the query.
<ul>
<li>NOTE: if any item is a view, then the view owner must also have the
necessary privileges to execute the view definition. Even if the view
owner is a <em>superuser</em>, they still must explicitly be granted the
necessary privileges.</li>
</ul></li>
<li><code>USAGE</code> privileges on all types used in the query.</li>
<li><code>USAGE</code> privileges on the active cluster.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/drop-cluster-replica"><code>DROP CLUSTER REPLICA</code></a></td>
<td><ul>
<li>Ownership of the dropped cluster replica.</li>
<li><code>USAGE</code> privileges on the containing cluster.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/drop-cluster"><code>DROP CLUSTER</code></a></td>
<td><ul>
<li>Ownership of the dropped cluster.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/drop-connection"><code>DROP CONNECTION</code></a></td>
<td><ul>
<li>Ownership of the dropped connection.</li>
<li><code>USAGE</code> privileges on the containing schema.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/drop-database"><code>DROP DATABASE</code></a></td>
<td><ul>
<li>Ownership of the dropped database.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/drop-index"><code>DROP INDEX</code></a></td>
<td><ul>
<li>Ownership of the dropped index.</li>
<li><code>USAGE</code> privileges on the containing schema.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/drop-materialized-view"><code>DROP MATERIALIZED VIEW</code></a></td>
<td><ul>
<li>Ownership of the dropped materialized view.</li>
<li><code>USAGE</code> privileges on the containing schema.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/drop-owned"><code>DROP OWNED</code></a></td>
<td><ul>
<li>Role membership in <code>role_name</code>.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/drop-role"><code>DROP ROLE</code></a></td>
<td><ul>
<li><code>CREATEROLE</code> privileges on the system.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/drop-schema"><code>DROP SCHEMA</code></a></td>
<td><ul>
<li>Ownership of the dropped schema.</li>
<li><code>USAGE</code> privileges on the containing database.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/drop-secret"><code>DROP SECRET</code></a></td>
<td><ul>
<li>Ownership of the dropped secret.</li>
<li><code>USAGE</code> privileges on the containing schema.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/drop-sink"><code>DROP SINK</code></a></td>
<td><ul>
<li>Ownership of the dropped sink.</li>
<li><code>USAGE</code> privileges on the containing schema.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/drop-source"><code>DROP SOURCE</code></a></td>
<td><ul>
<li>Ownership of the dropped source.</li>
<li><code>USAGE</code> privileges on the containing schema.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/drop-table"><code>DROP TABLE</code></a></td>
<td><ul>
<li>Ownership of the dropped table.</li>
<li><code>USAGE</code> privileges on the containing schema.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/drop-type"><code>DROP TYPE</code></a></td>
<td><ul>
<li>Ownership of the dropped type.</li>
<li><code>USAGE</code> privileges on the containing schema.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/drop-user"><code>DROP USER</code></a></td>
<td><ul>
<li><code>CREATEROLE</code> privileges on the system.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/drop-view"><code>DROP VIEW</code></a></td>
<td><ul>
<li>Ownership of the dropped view.</li>
<li><code>USAGE</code> privileges on the containing schema.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/explain-analyze"><code>EXPLAIN ANALYZE</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schemas that all relations in
the explainee are contained in.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/explain-filter-pushdown"><code>EXPLAIN FILTER PUSHDOWN</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schemas that all relations in
the explainee are contained in.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/explain-plan"><code>EXPLAIN PLAN</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schemas that all relations in
the explainee are contained in.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/explain-schema"><code>EXPLAIN SCHEMA</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schemas that all items in the
query are contained in.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/explain-timestamp"><code>EXPLAIN TIMESTAMP</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schemas that all relations in
the query are contained in.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/grant-privilege"><code>GRANT PRIVILEGE</code></a></td>
<td><ul>
<li>Ownership of affected objects.</li>
<li><code>USAGE</code> privileges on the containing database if the
affected object is a schema.</li>
<li><code>USAGE</code> privileges on the containing schema if the
affected object is namespaced by a schema.</li>
<li><em>superuser</em> status if the privilege is a system
privilege.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/grant-role"><code>GRANT ROLE</code></a></td>
<td><ul>
<li><code>CREATEROLE</code> privileges on the system.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/insert"><code>INSERT</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schemas that all relations and
types in the query are contained in.</li>
<li><code>INSERT</code> privileges on <code>table_name</code>.</li>
<li><code>SELECT</code> privileges on all relations in the query.
<ul>
<li>NOTE: if any item is a view, then the view owner must also have the
necessary privileges to execute the view definition. Even if the view
owner is a <em>superuser</em>, they still must explicitly be granted the
necessary privileges.</li>
</ul></li>
<li><code>USAGE</code> privileges on all types used in the query.</li>
<li><code>USAGE</code> privileges on the active cluster.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/reassign-owned"><code>REASSIGN OWNED</code></a></td>
<td><ul>
<li>Role membership in <code>old_role</code> and
<code>new_role</code>.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/revoke-privilege"><code>REVOKE PRIVILEGE</code></a></td>
<td><ul>
<li>Ownership of affected objects.</li>
<li><code>USAGE</code> privileges on the containing database if the
affected object is a schema.</li>
<li><code>USAGE</code> privileges on the containing schema if the
affected object is namespaced by a schema.</li>
<li><em>superuser</em> status if the privilege is a system
privilege.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/revoke-role"><code>REVOKE ROLE</code></a></td>
<td><ul>
<li><code>CREATEROLE</code> privileges on the systems.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/select"><code>SELECT</code></a></td>
<td><ul>
<li><p><code>SELECT</code> privileges on all <strong>directly</strong>
referenced relations in the query. If the directly referenced relation
is a view or materialized view:</p>
<ul>
<li><p><code>SELECT</code> privileges are required only on the directly
referenced view/materialized view. <code>SELECT</code> privileges are
<strong>not</strong> required for the underlying relations referenced in
the view/materialized view definition unless those relations themselves
are directly referenced in the query.</p></li>
<li><p>However, the owner of the view/materialized view (including those
with <strong>superuser</strong> privileges) must have all required
<code>SELECT</code> and <code>USAGE</code> privileges to run the view
definition regardless of who is selecting from the view/materialized
view.</p></li>
</ul></li>
<li><p><code>USAGE</code> privileges on the schemas that contain the
relations in the query.</p></li>
<li><p><code>USAGE</code> privileges on the active cluster.</p></li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/show-columns"><code>SHOW COLUMNS</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schema containing
<code>item_ref</code>.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/show-create-cluster"><code>SHOW CREATE CLUSTER</code></a></td>
<td>There are no privileges required to execute this statement.</td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/show-create-connection"><code>SHOW CREATE CONNECTION</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schema containing the
connection.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/show-create-index"><code>SHOW CREATE INDEX</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schema containing the
index.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/show-create-materialized-view"><code>SHOW CREATE MATERIALIZED VIEW</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schema containing the
materialized view.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/show-create-sink"><code>SHOW CREATE SINK</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schema containing the
sink.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/show-create-source"><code>SHOW CREATE SOURCE</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schema containing the
source.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/show-create-table"><code>SHOW CREATE TABLE</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schema containing the
table.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/show-create-view"><code>SHOW CREATE VIEW</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schema containing the
view.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/subscribe"><code>SUBSCRIBE</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schemas that all relations and
types in the query are contained in.</li>
<li><code>SELECT</code> privileges on all relations in the query.
<ul>
<li>NOTE: if any item is a view, then the view owner must also have the
necessary privileges to execute the view definition. Even if the view
owner is a <em>superuser</em>, they still must explicitly be granted the
necessary privileges.</li>
</ul></li>
<li><code>USAGE</code> privileges on all types used in the query.</li>
<li><code>USAGE</code> privileges on the active cluster.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/update"><code>UPDATE</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the schemas that all relations and
types in the query are contained in.</li>
<li><code>UPDATE</code> privileges on the table being updated.</li>
<li><code>SELECT</code> privileges on all relations in the query.
<ul>
<li>NOTE: if any item is a view, then the view owner must also have the
necessary privileges to execute the view definition. Even if the view
owner is a <em>superuser</em>, they still must explicitly be granted the
necessary privileges.</li>
</ul></li>
<li><code>USAGE</code> privileges on all types used in the query.</li>
<li><code>USAGE</code> privileges on the active cluster.</li>
</ul></td>
</tr>
<tr>
<td><a
href="/docs/self-managed/v25.2/sql/validate-connection"><code>VALIDATE CONNECTION</code></a></td>
<td><ul>
<li><code>USAGE</code> privileges on the containing schema.</li>
<li><code>USAGE</code> privileges on the connection.</li>
</ul></td>
</tr>
</tbody>
</table>

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/manage/access-control/appendix-command-privileges.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
