<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

# SQL commands

## Create/Alter/Drop Objects

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>CREATE</th>
<th>ALTER</th>
<th>DROP</th>
</tr>
</thead>
<tbody>
<tr>
<td><a
href="/docs/sql/create-cluster"><code>CREATE CLUSTER</code></a></td>
<td><a
href="/docs/sql/alter-cluster"><code>ALTER CLUSTER</code></a></td>
<td><a href="/docs/sql/drop-cluster"><code>DROP CLUSTER</code></a></td>
</tr>
<tr>
<td><a
href="/docs/sql/create-cluster-replica"><code>CREATE CLUSTER REPLICA</code></a></td>
<td></td>
<td><a
href="/docs/sql/drop-cluster-replica"><code>DROP CLUSTER REPLICA</code></a></td>
</tr>
<tr>
<td><a
href="/docs/sql/create-connection"><code>CREATE CONNECTION</code></a></td>
<td><a
href="/docs/sql/alter-connection"><code>ALTER CONNECTION</code></a></td>
<td><a
href="/docs/sql/drop-connection"><code>DROP CONNECTION</code></a></td>
</tr>
<tr>
<td><a
href="/docs/sql/create-database"><code>CREATE DATABASE</code></a></td>
<td></td>
<td><a
href="/docs/sql/drop-database"><code>DROP DATABASE</code></a></td>
</tr>
<tr>
<td><a href="/docs/sql/create-index"><code>CREATE INDEX</code></a></td>
<td><a href="/docs/sql/alter-index"><code>ALTER INDEX</code></a></td>
<td><a href="/docs/sql/drop-index"><code>DROP INDEX</code></a></td>
</tr>
<tr>
<td><a
href="/docs/sql/create-materialized-view"><code>CREATE MATERIALIZED VIEW</code></a></td>
<td><a
href="/docs/sql/alter-materialized-view"><code>ALTER MATERIALIZED VIEW</code></a></td>
<td><a
href="/docs/sql/drop-materialized-view"><code>DROP MATERIALIZED VIEW</code></a></td>
</tr>
<tr>
<td><a
href="/docs/sql/create-network-policy"><code>CREATE NETWORK POLICY</code></a></td>
<td><a
href="/docs/sql/alter-network-policy"><code>ALTER NETWORK POLICY</code></a></td>
<td><a
href="/docs/sql/drop-network-policy"><code>DROP NETWORK POLICY</code></a></td>
</tr>
<tr>
<td><a href="/docs/sql/create-role"><code>CREATE ROLE</code></a></td>
<td><a href="/docs/sql/alter-role"><code>ALTER ROLE</code></a></td>
<td><a href="/docs/sql/drop-role"><code>DROP ROLE</code></a><br />
<a href="/docs/sql/drop-user"><code>DROP USER</code></a></td>
</tr>
<tr>
<td><a
href="/docs/sql/create-schema"><code>CREATE SCHEMA</code></a></td>
<td></td>
<td><a href="/docs/sql/drop-schema"><code>DROP SCHEMA</code></a></td>
</tr>
<tr>
<td><a
href="/docs/sql/create-secret"><code>CREATE SECRET</code></a></td>
<td><a href="/docs/sql/alter-secret"><code>ALTER SECRET</code></a></td>
<td><a href="/docs/sql/drop-secret"><code>DROP SECRET</code></a></td>
</tr>
<tr>
<td><a href="/docs/sql/create-sink"><code>CREATE SINK</code></a></td>
<td><a href="/docs/sql/alter-sink"><code>ALTER SINK</code></a></td>
<td><a href="/docs/sql/drop-sink"><code>DROP SINK</code></a></td>
</tr>
<tr>
<td><a
href="/docs/sql/create-source"><code>CREATE SOURCE</code></a></td>
<td><a href="/docs/sql/alter-source"><code>ALTER SOURCE</code></a></td>
<td><a href="/docs/sql/drop-source"><code>DROP SOURCE</code></a></td>
</tr>
<tr>
<td><a href="/docs/sql/create-table"><code>CREATE TABLE</code></a></td>
<td><a href="/docs/sql/alter-table"><code>ALTER TABLE</code></a></td>
<td><a href="/docs/sql/drop-table"><code>DROP TABLE</code></a></td>
</tr>
<tr>
<td><a href="/docs/sql/create-type"><code>CREATE TYPE</code></a></td>
<td></td>
<td><a href="/docs/sql/drop-type"><code>DROP TYPE</code></a></td>
</tr>
<tr>
<td><a href="/docs/sql/create-view"><code>CREATE VIEW</code></a></td>
<td></td>
<td><a href="/docs/sql/drop-view"><code>DROP VIEW</code></a></td>
</tr>
</tbody>
</table>

## Create/Read/Update/Delete Data

The following commands perform CRUD operations on materialized views,
views, sources, and tables:

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<tbody>
<tr>
<td><strong>Select/Subscribe</strong></td>
<td><ul>
<li><a href="/docs/sql/select"><code>SELECT</code></a></li>
<li><a href="/docs/sql/subscribe"><code>SUBSCRIBE</code></a></li>
</ul>
<ul class="task-list">
</ul></td>
</tr>
<tr>
<td><strong>Cursor</strong></td>
<td><ul>
<li><a href="/docs/sql/close"><code>CLOSE</code></a></li>
<li><a href="/docs/sql/declare"><code>DECLARE</code></a></li>
<li><a href="/docs/sql/fetch"><code>FETCH</code></a></li>
</ul></td>
</tr>
<tr>
<td><strong>Sink</strong></td>
<td><ul>
<li><a href="/docs/sql/alter-sink"><code>ALTER SINK</code></a></li>
<li><a href="/docs/sql/create-sink"><code>CREATE SINK</code></a></li>
<li><a href="/docs/sql/drop-sink"><code>DROP SINK</code></a></li>
</ul></td>
</tr>
<tr>
<td><strong>Transactions</strong></td>
<td><ul>
<li><a href="/docs/sql/begin"><code>BEGIN</code></a></li>
<li><a href="/docs/sql/commit"><code>COMMIT</code></a></li>
<li><a href="/docs/sql/rollback"><code>ROLLBACK</code></a></li>
</ul></td>
</tr>
</tbody>
</table>

## RBAC

Commands to manage roles and privileges:

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<tbody>
<tr>
<td><strong>Roles</strong></td>
<td><ul>
<li><a href="/docs/sql/alter-role"><code>ALTER ROLE</code></a></li>
<li><a href="/docs/sql/create-role"><code>CREATE ROLE</code></a></li>
<li><a href="/docs/sql/drop-role"><code>DROP ROLE</code></a><br />
<a href="/docs/sql/drop-user"><code>DROP USER</code></a></li>
<li><a href="/docs/sql/grant-role"><code>GRANT ROLE</code></a></li>
<li><a href="/docs/sql/revoke-role"><code>REVOKE ROLE</code></a></li>
<li><a href="/docs/sql/show-roles"><code>SHOW ROLES</code></a></li>
</ul></td>
</tr>
<tr>
<td><strong>Privileges</strong></td>
<td><ul>
<li><a
href="/docs/sql/alter-default-privileges"><code>ALTER DEFAULT PRIVILEGES</code></a></li>
<li><a
href="/docs/sql/grant-privilege"><code>GRANT PRIVILEGE</code></a></li>
<li><a
href="/docs/sql/revoke-privilege"><code>REVOKE PRIVILEGE</code></a></li>
</ul></td>
</tr>
<tr>
<td><strong>Owners</strong></td>
<td><ul>
<li><a href="/docs/sql/alter-owner"><code>ALTER OWNER</code></a></li>
<li><a href="/docs/sql/drop-owned"><code>DROP OWNED</code></a></li>
<li><a
href="/docs/sql/reassign-owned"><code>REASSIGN OWNED</code></a></li>
</ul></td>
</tr>
</tbody>
</table>

## Query Introspection (`Explain`)

- [`EXPLAIN ANALYZE`](/docs/sql/explain-analyze)
- [`EXPLAIN FILTER PUSHDOWN`](/docs/sql/explain-filter-pushdown)
- [`EXPLAIN PLAN`](/docs/sql/explain-plan)
- [`EXPLAIN SCHEMA`](/docs/sql/explain-schema)
- [`EXPLAIN TIMESTAMP`](/docs/sql/explain-timestamp)

## Object Introspection (`SHOW`)

- [`SHOW`](/docs/sql/show)
- [`SHOW CLUSTER REPLICAS`](/docs/sql/show-cluster-replicas)
- [`SHOW CLUSTERS`](/docs/sql/show-clusters)
- [`SHOW COLUMNS`](/docs/sql/show-columns)
- [`SHOW CONNECTIONS`](/docs/sql/show-connections)
- [`SHOW CREATE CLUSTER`](/docs/sql/show-create-cluster)
- [`SHOW CREATE CONNECTION`](/docs/sql/show-create-connection)
- [`SHOW CREATE INDEX`](/docs/sql/show-create-index)
- [`SHOW CREATE MATERIALIZED VIEW`](/docs/sql/show-create-materialized-view)
- [`SHOW CREATE SINK`](/docs/sql/show-create-sink)
- [`SHOW CREATE SOURCE`](/docs/sql/show-create-source)
- [`SHOW CREATE TABLE`](/docs/sql/show-create-table)
- [`SHOW CREATE TYPE`](/docs/sql/show-create-type)
- [`SHOW CREATE VIEW`](/docs/sql/show-create-view)
- [`SHOW DATABASES`](/docs/sql/show-databases)
- [`SHOW DEFAULT PRIVILEGES`](/docs/sql/show-default-privileges)
- [`SHOW INDEXES`](/docs/sql/show-indexes)
- [`SHOW MATERIALIZED VIEWS`](/docs/sql/show-materialized-views)
- [`SHOW NETWORK POLICIES (Cloud)`](/docs/sql/show-network-policies)
- [`SHOW OBJECTS`](/docs/sql/show-objects)
- [`SHOW PRIVILEGES`](/docs/sql/show-privileges)
- [`SHOW ROLE MEMBERSHIP`](/docs/sql/show-role-membership)
- [`SHOW ROLES`](/docs/sql/show-roles)
- [`SHOW SCHEMAS`](/docs/sql/show-schemas)
- [`SHOW SECRETS`](/docs/sql/show-secrets)
- [`SHOW SINKS`](/docs/sql/show-sinks)
- [`SHOW SOURCES`](/docs/sql/show-sources)
- [`SHOW SUBSOURCES`](/docs/sql/show-subsources)
- [`SHOW TABLES`](/docs/sql/show-tables)
- [`SHOW TYPES`](/docs/sql/show-types)
- [`SHOW VIEWS`](/docs/sql/show-views)

## Session

Commands related with session state and configurations:

- [`DISCARD`](/docs/sql/discard)
- [`RESET`](/docs/sql/reset)
- [`SET`](/docs/sql/set)
- [`SHOW`](/docs/sql/show)

## Validations

- [`VALIDATE CONNECTION`](/docs/sql/validate-connection)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/_index.md"
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
