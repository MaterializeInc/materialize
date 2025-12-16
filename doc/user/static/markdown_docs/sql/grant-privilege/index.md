<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Reference](/docs/self-managed/v25.2/sql/)

</div>

# GRANT PRIVILEGE

`GRANT PRIVILEGE` grants privileges to [database
role(s)](/docs/self-managed/v25.2/manage/access-control/manage-roles/).

## Syntax

<div class="note">

**NOTE:** The syntax supports the `ALL [PRIVILEGES]` shorthand to refer
to all [*applicable*
privileges](/docs/self-managed/v25.2/sql/grant-privilege/#available-privileges)
for the object type.

</div>

<div class="code-tabs">

<div class="tab-content">

<div id="tab-cluster" class="tab-pane" title="Cluster">

For specific cluster(s):

<div class="highlight">

``` chroma
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON CLUSTER <name> [, ...]
TO <role_name> [, ... ];
```

</div>

For all clusters:

<div class="highlight">

``` chroma
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL CLUSTERS
TO <role_name> [, ... ];
```

</div>

</div>

<div id="tab-connection" class="tab-pane" title="Connection">

For specific connection(s):

<div class="highlight">

``` chroma
GRANT <USAGE | ALL [PRIVILEGES]>
ON CONNECTION <name> [, ...]
TO <role_name> [, ... ];
```

</div>

For all connections or all connections in specific schema(s) or in
database(s):

<div class="highlight">

``` chroma
GRANT <USAGE | ALL [PRIVILEGES]>
ON ALL CONNECTIONS
 [ IN <SCHEMA | DATABASE> <name> [, <name> ...] ]
TO <role_name> [, ... ];
```

</div>

</div>

<div id="tab-database" class="tab-pane" title="Database">

For specific database(s):

<div class="highlight">

``` chroma
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON DATABASE <name> [, ...]
TO <role_name> [, ... ];
```

</div>

For all database:

<div class="highlight">

``` chroma
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL DATABASES
TO <role_name> [, ... ];
```

</div>

</div>

<div id="tab-materialized-viewviewsource" class="tab-pane"
title="Materialized view/view/source">

<div class="note">

**NOTE:** To read from a views or a materialized views, you must have
`SELECT` privileges on the view/materialized views. That is, having
`SELECT` privileges on the underlying objects defining the
view/materialized view is insufficient.

</div>

For specific materialized view(s)/view(s)/source(s):

<div class="highlight">

``` chroma
GRANT <SELECT | ALL [PRIVILEGES]>
ON [TABLE] <name> [, <name> ...] -- For PostgreSQL compatibility, if specifying type, use TABLE
TO <role_name> [, ... ];
```

</div>

</div>

<div id="tab-schema" class="tab-pane" title="Schema">

For specific schema(s):

<div class="highlight">

``` chroma
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON SCHEMA <name> [, ...]
TO <role_name> [, ... ];
```

</div>

For all schemas or all schemas in a specific database(s):

<div class="highlight">

``` chroma
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL SCHEMAS [IN DATABASE <name> [, <name> ...]]
TO <role_name> [, ... ];
```

</div>

</div>

<div id="tab-secret" class="tab-pane" title="Secret">

For specific secret(s):

<div class="highlight">

``` chroma
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON SECRET <name> [, ...]
TO <role_name> [, ... ];
```

</div>

For all secrets or all secrets in a specific database(s):

<div class="highlight">

``` chroma
GRANT <USAGE | CREATE | ALL [PRIVILEGES]> [, ... ]
ON ALL SECRET [IN DATABASE <name> [, <name> ...]]
TO <role_name> [, ... ];
```

</div>

</div>

<div id="tab-system" class="tab-pane" title="System">

<div class="highlight">

``` chroma
GRANT <CREATEROLE | CREATEDB | CREATECLUSTER | CREATENETWORKPOLICY | ALL [PRIVILEGES]> [, ... ]
ON SYSTEM
TO <role_name> [, ... ];
```

</div>

</div>

<div id="tab-type" class="tab-pane" title="Type">

For specific view(s):

<div class="highlight">

``` chroma
GRANT <USAGE | ALL [PRIVILEGES]>
ON TYPE <name> [, <name> ...]
TO <role_name> [, ... ];
```

</div>

For all types or all types in a specific schema(s) or in a specific
database(s):

<div class="highlight">

``` chroma
GRANT <USAGE | ALL [PRIVILEGES]>
ON ALL TYPES
  [ IN <SCHEMA|DATABASE> <name> [, <name> ...] ]
TO <role_name> [, ... ];
```

</div>

</div>

<div id="tab-table" class="tab-pane" title="Table">

For specific table(s):

<div class="highlight">

``` chroma
GRANT <SELECT | INSERT | UPDATE | DELETE | ALL [PRIVILEGES]> [, ...]
ON [TABLE] <name> [, <name> ...]
TO <role_name> [, ... ];
```

</div>

For all tables or all tables in a specific schema(s) or in a specific
database(s):

<div class="note">

**NOTE:** Granting privileges via `ALL TABLES […]` also applies to
sources, views, and materialized views (for the applicable privileges).

</div>

<div class="highlight">

``` chroma
GRANT <SELECT | INSERT | UPDATE | DELETE | ALL [PRIVILEGES]> [, ...]
ON ALL TABLES
  [ IN <SCHEMA|DATABASE> <name> [, <name> ...] ]
TO <role_name> [, ... ];
```

</div>

</div>

</div>

</div>

## Details

### Available privileges

<div class="code-tabs">

<div class="tab-content">

<div id="tab-by-privilege" class="tab-pane" title="By Privilege">

<table>
<colgroup>
<col style="width: 25%" />
<col style="width: 25%" />
<col style="width: 25%" />
<col style="width: 25%" />
</colgroup>
<thead>
<tr>
<th>Privilege</th>
<th>Description</th>
<th>Abbreviation</th>
<th>Applies to</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>SELECT</strong></td>
<td>Permission to read rows from an object.</td>
<td><code>r</code></td>
<td><ul>
<li><code>MATERIALIZED VIEW</code></li>
<li><code>SOURCE</code></li>
<li><code>TABLE</code></li>
<li><code>VIEW</code></li>
</ul></td>
</tr>
<tr>
<td><strong>INSERT</strong></td>
<td>Permission to insert rows into an object.</td>
<td><code>a</code></td>
<td><ul>
<li><code>TABLE</code></li>
</ul></td>
</tr>
<tr>
<td><strong>UPDATE</strong></td>
<td><p>Permission to modify rows in an object.</p>
<p>Modifying rows may also require <strong>SELECT</strong> if a read is
needed to determine which rows to update.</p></td>
<td><code>w</code></td>
<td><ul>
<li><code>TABLE</code></li>
</ul></td>
</tr>
<tr>
<td><strong>DELETE</strong></td>
<td><p>Permission to delete rows from an object.</p>
<p>Deleting rows may also require <strong>SELECT</strong> if a read is
needed to determine which rows to delete.</p></td>
<td><code>d</code></td>
<td><ul>
<li><code>TABLE</code></li>
</ul></td>
</tr>
<tr>
<td><strong>CREATE</strong></td>
<td>Permission to create a new objects within the specified object.</td>
<td><code>C</code></td>
<td><ul>
<li><code>DATABASE</code></li>
<li><code>SCHEMA</code></li>
<li><code>CLUSTER</code></li>
</ul></td>
</tr>
<tr>
<td><strong>USAGE</strong></td>
<td><span id="privilege-usage"></span> Permission to use or reference an
object (e.g., schema/type lookup).</td>
<td><code>U</code></td>
<td><ul>
<li><code>CLUSTER</code></li>
<li><code>CONNECTION</code></li>
<li><code>DATABASE</code></li>
<li><code>SCHEMA</code></li>
<li><code>SECRET</code></li>
<li><code>TYPE</code></li>
</ul></td>
</tr>
<tr>
<td><strong>CREATEROLE</strong></td>
<td><p>Permission to create/modify/delete roles and manage role
memberships for any role in the system.</p>
<div class="warning">
<strong>WARNING!</strong> Roles with the <code>CREATEROLE</code>
privilege can obtain the privileges of any other role in the system by
granting themselves that role. Avoid granting <code>CREATEROLE</code>
unnecessarily.
</div></td>
<td><code>R</code></td>
<td><ul>
<li><code>SYSTEM</code></li>
</ul></td>
</tr>
<tr>
<td><strong>CREATEDB</strong></td>
<td>Permission to create new databases.</td>
<td><code>B</code></td>
<td><ul>
<li><code>SYSTEM</code></li>
</ul></td>
</tr>
<tr>
<td><strong>CREATECLUSTER</strong></td>
<td>Permission to create new clusters.</td>
<td><code>N</code></td>
<td><ul>
<li><code>SYSTEM</code></li>
</ul></td>
</tr>
<tr>
<td><strong>CREATENETWORKPOLICY</strong></td>
<td>Permission to create network policies to control access at the
network layer.</td>
<td><code>P</code></td>
<td><ul>
<li><code>SYSTEM</code></li>
</ul></td>
</tr>
</tbody>
</table>

</div>

<div id="tab-by-object" class="tab-pane" title="By Object">

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Object</th>
<th>Privileges</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>CLUSTER</code></td>
<td><ul>
<li><code>USAGE</code></li>
<li><code>CREATE</code></li>
</ul></td>
</tr>
<tr>
<td><code>CONNECTION</code></td>
<td><ul>
<li><code>USAGE</code></li>
</ul></td>
</tr>
<tr>
<td><code>DATABASE</code></td>
<td><ul>
<li><code>USAGE</code></li>
<li><code>CREATE</code></li>
</ul></td>
</tr>
<tr>
<td><code>MATERIALIZED VIEW</code></td>
<td><ul>
<li><code>SELECT</code></li>
</ul></td>
</tr>
<tr>
<td><code>SCHEMA</code></td>
<td><ul>
<li><code>USAGE</code></li>
<li><code>CREATE</code></li>
</ul></td>
</tr>
<tr>
<td><code>SECRET</code></td>
<td><ul>
<li><code>USAGE</code></li>
</ul></td>
</tr>
<tr>
<td><code>SOURCE</code></td>
<td><ul>
<li><code>SELECT</code></li>
</ul></td>
</tr>
<tr>
<td><code>SYSTEM</code></td>
<td><ul>
<li><code>CREATEROLE</code></li>
<li><code>CREATEDB</code></li>
<li><code>CREATECLUSTER</code></li>
</ul></td>
</tr>
<tr>
<td><code>TABLE</code></td>
<td><ul>
<li><code>INSERT</code></li>
<li><code>SELECT</code></li>
<li><code>UPDATE</code></li>
<li><code>DELETE</code></li>
</ul></td>
</tr>
<tr>
<td><code>TYPE</code></td>
<td><ul>
<li><code>USAGE</code></li>
</ul></td>
</tr>
<tr>
<td><code>VIEW</code></td>
<td><ul>
<li><code>SELECT</code></li>
</ul></td>
</tr>
</tbody>
</table>

</div>

</div>

</div>

## Privileges

The privileges required to execute this statement are:

- Ownership of affected objects.
- `USAGE` privileges on the containing database if the affected object
  is a schema.
- `USAGE` privileges on the containing schema if the affected object is
  namespaced by a schema.
- *superuser* status if the privilege is a system privilege.

## Examples

<div class="highlight">

``` chroma
GRANT SELECT ON mv_quarterly_sales TO data_analysts, reporting;
```

</div>

<div class="highlight">

``` chroma
GRANT USAGE, CREATE ON DATABASE materialize TO data_analysts;
```

</div>

<div class="highlight">

``` chroma
GRANT ALL ON CLUSTER dev_cluster TO data_analysts, developers;
```

</div>

<div class="highlight">

``` chroma
GRANT CREATEDB ON SYSTEM TO source_owners;
```

</div>

## Useful views

- [`mz_internal.mz_show_system_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_system_privileges)
- [`mz_internal.mz_show_my_system_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_my_system_privileges)
- [`mz_internal.mz_show_cluster_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_cluster_privileges)
- [`mz_internal.mz_show_my_cluster_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_my_cluster_privileges)
- [`mz_internal.mz_show_database_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_database_privileges)
- [`mz_internal.mz_show_my_database_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_my_database_privileges)
- [`mz_internal.mz_show_schema_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_schema_privileges)
- [`mz_internal.mz_show_my_schema_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_my_schema_privileges)
- [`mz_internal.mz_show_object_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_object_privileges)
- [`mz_internal.mz_show_my_object_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_my_object_privileges)
- [`mz_internal.mz_show_all_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_all_privileges)
- [`mz_internal.mz_show_all_my_privileges`](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_show_all_my_privileges)

## Related pages

- [`SHOW PRIVILEGES`](../show-privileges)
- [`CREATE ROLE`](../create-role)
- [`ALTER ROLE`](../alter-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](../alter-owner)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
- [`ALTER DEFAULT PRIVILEGES`](../alter-default-privileges)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/grant-privilege.md"
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
