<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)

</div>

# ALTER SYSTEM SET

`ALTER SYSTEM SET` globally modifies the value of a configuration
parameter.

To see the current value of a configuration parameter, use
[`SHOW`](../show).

## Syntax

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI0NTkiIGhlaWdodD0iMTkxIj4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDE3IDEgMTMgMSAyMSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDE3IDkgMTMgOSAyMSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSIzMSIgeT0iMyIgd2lkdGg9IjY2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjI5IiB5PSIxIiB3aWR0aD0iNjYiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjM5IiB5PSIyMSI+QUxURVI8L3RleHQ+CiAgIDxyZWN0IHg9IjExNyIgeT0iMyIgd2lkdGg9Ijc4IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjExNSIgeT0iMSIgd2lkdGg9Ijc4IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIxMjUiIHk9IjIxIj5TWVNURU08L3RleHQ+CiAgIDxyZWN0IHg9IjIxNSIgeT0iMyIgd2lkdGg9IjQ2IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjIxMyIgeT0iMSIgd2lkdGg9IjQ2IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIyMjMiIHk9IjIxIj5TRVQ8L3RleHQ+CiAgIDxyZWN0IHg9IjI4MSIgeT0iMyIgd2lkdGg9IjU2IiBoZWlnaHQ9IjMyIiAvPgogICA8cmVjdCB4PSIyNzkiIHk9IjEiIHdpZHRoPSI1NiIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjI4OSIgeT0iMjEiPm5hbWU8L3RleHQ+CiAgIDxyZWN0IHg9IjM3NyIgeT0iMyIgd2lkdGg9IjQwIiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjM3NSIgeT0iMSIgd2lkdGg9IjQwIiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzODUiIHk9IjIxIj5UTzwvdGV4dD4KICAgPHJlY3QgeD0iMzc3IiB5PSI0NyIgd2lkdGg9IjI4IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjM3NSIgeT0iNDUiIHdpZHRoPSIyOCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMzg1IiB5PSI2NSI+PTwvdGV4dD4KICAgPHJlY3QgeD0iMzI3IiB5PSIxMTMiIHdpZHRoPSI1NCIgaGVpZ2h0PSIzMiIgLz4KICAgPHJlY3QgeD0iMzI1IiB5PSIxMTEiIHdpZHRoPSI1NCIgaGVpZ2h0PSIzMiIgY2xhc3M9Im5vbnRlcm1pbmFsIiAvPgogICA8dGV4dCBjbGFzcz0ibm9udGVybWluYWwiIHg9IjMzNSIgeT0iMTMxIj52YWx1ZTwvdGV4dD4KICAgPHJlY3QgeD0iMzI3IiB5PSIxNTciIHdpZHRoPSI4NCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSIzMjUiIHk9IjE1NSIgd2lkdGg9Ijg0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSIzMzUiIHk9IjE3NSI+REVGQVVMVDwvdGV4dD4KICAgPHBhdGggY2xhc3M9ImxpbmUiIGQ9Im0xNyAxNyBoMiBtMCAwIGgxMCBtNjYgMCBoMTAgbTAgMCBoMTAgbTc4IDAgaDEwIG0wIDAgaDEwIG00NiAwIGgxMCBtMCAwIGgxMCBtNTYgMCBoMTAgbTIwIDAgaDEwIG00MCAwIGgxMCBtLTgwIDAgaDIwIG02MCAwIGgyMCBtLTEwMCAwIHExMCAwIDEwIDEwIG04MCAwIHEwIC0xMCAxMCAtMTAgbS05MCAxMCB2MjQgbTgwIDAgdi0yNCBtLTgwIDI0IHEwIDEwIDEwIDEwIG02MCAwIHExMCAwIDEwIC0xMCBtLTcwIDEwIGgxMCBtMjggMCBoMTAgbTAgMCBoMTIgbTIyIC00NCBsMiAwIG0yIDAgbDIgMCBtMiAwIGwyIDAgbS0xNzQgMTEwIGwyIDAgbTIgMCBsMiAwIG0yIDAgbDIgMCBtMjIgMCBoMTAgbTU0IDAgaDEwIG0wIDAgaDMwIG0tMTI0IDAgaDIwIG0xMDQgMCBoMjAgbS0xNDQgMCBxMTAgMCAxMCAxMCBtMTI0IDAgcTAgLTEwIDEwIC0xMCBtLTEzNCAxMCB2MjQgbTEyNCAwIHYtMjQgbS0xMjQgMjQgcTAgMTAgMTAgMTAgbTEwNCAwIHExMCAwIDEwIC0xMCBtLTExNCAxMCBoMTAgbTg0IDAgaDEwIG0yMyAtNDQgaC0zIiAvPgogICA8cG9seWdvbiBwb2ludHM9IjQ0OSAxMjcgNDU3IDEyMyA0NTcgMTMxIj48L3BvbHlnb24+CiAgIDxwb2x5Z29uIHBvaW50cz0iNDQ5IDEyNyA0NDEgMTIzIDQ0MSAxMzEiPjwvcG9seWdvbj4KPC9zdmc+)

</div>

| Field | Use |
|----|----|
| *name* | The name of the configuration parameter to modify. |
| *value* | The value to assign to the configuration parameter. |
| **DEFAULT** | Reset the configuration parameter’s default value. Equivalent to [`ALTER SYSTEM RESET`](../alter-system-reset). |

### Key configuration parameters

<table>
<colgroup>
<col style="width: 25%" />
<col style="width: 25%" />
<col style="width: 25%" />
<col style="width: 25%" />
</colgroup>
<thead>
<tr>
<th>Name</th>
<th>Default value</th>
<th>Description</th>
<th>Modifiable?</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>cluster</code></td>
<td><code>quickstart</code></td>
<td>The current cluster.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>cluster_replica</code></td>
<td></td>
<td>The target cluster replica for <code>SELECT</code> queries.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>database</code></td>
<td><code>materialize</code></td>
<td>The current database.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>search_path</code></td>
<td><code>public</code></td>
<td>The schema search order for names that are not
schema-qualified.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>transaction_isolation</code></td>
<td><code>strict serializable</code></td>
<td>The transaction isolation level. For more information, see <a
href="/docs/overview/isolation-level/">Consistency guarantees</a>.<br />
<br />
Accepts values: <code>serializable</code>,
<code>strict serializable</code>.</td>
<td>Yes</td>
</tr>
</tbody>
</table>

### Other configuration parameters

<table>
<colgroup>
<col style="width: 25%" />
<col style="width: 25%" />
<col style="width: 25%" />
<col style="width: 25%" />
</colgroup>
<thead>
<tr>
<th>Name</th>
<th>Default value</th>
<th>Description</th>
<th>Modifiable?</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>allowed_cluster_replica_sizes</code></td>
<td><em>Varies</em></td>
<td>The allowed sizes when creating a new cluster replica.</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>application_name</code></td>
<td></td>
<td>The application name to be reported in statistics and logs. This
parameter is typically set by an application upon connection to
Materialize (e.g. <code>psql</code>).</td>
<td>Yes</td>
</tr>
<tr>
<td><code>auto_route_catalog_queries</code></td>
<td><code>true</code></td>
<td>Boolean flag indicating whether to force queries that depend only on
system tables to run on the <code>mz_catalog_server</code> cluster for
improved performance.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>client_encoding</code></td>
<td><code>UTF8</code></td>
<td>The client’s character set encoding. The only supported value is
<code>UTF-8</code>.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>client_min_messages</code></td>
<td><code>notice</code></td>
<td>The message levels that are sent to the client.<br />
<br />
Accepts values: <code>debug5</code>, <code>debug4</code>,
<code>debug3</code>, <code>debug2</code>, <code>debug1</code>,
<code>log</code>, <code>notice</code>, <code>warning</code>,
<code>error</code>. Each level includes all the levels that follow
it.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>datestyle</code></td>
<td><code>ISO, MDY</code></td>
<td>The display format for date and time values. The only supported
value is <code>ISO, MDY</code>.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>emit_introspection_query_notice</code></td>
<td><code>true</code></td>
<td>Whether to print a notice when querying replica introspection
relations.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>emit_timestamp_notice</code></td>
<td><code>false</code></td>
<td>Boolean flag indicating whether to send a <code>notice</code>
specifying query timestamps.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>emit_trace_id_notice</code></td>
<td><code>false</code></td>
<td>Boolean flag indicating whether to send a <code>notice</code>
specifying the trace ID, when available.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>enable_rbac_checks</code></td>
<td><code>true</code></td>
<td>Boolean flag indicating whether to apply RBAC checks before
executing statements.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>enable_session_rbac_checks</code></td>
<td><code>false</code></td>
<td>Boolean flag indicating whether RBAC is enabled for the current
session.</td>
<td>No</td>
</tr>
<tr>
<td><code>extra_float_digits</code></td>
<td><code>3</code></td>
<td>Boolean flag indicating whether to adjust the number of digits
displayed for floating-point values.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>failpoints</code></td>
<td></td>
<td>Allows failpoints to be dynamically activated.</td>
<td>No</td>
</tr>
<tr>
<td><code>idle_in_transaction_session_timeout</code></td>
<td><code>120s</code></td>
<td>The maximum allowed duration that a session can sit idle in a
transaction before being terminated. If this value is specified without
units, it is taken as milliseconds (<code>ms</code>). A value of zero
disables the timeout.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>integer_datetimes</code></td>
<td><code>true</code></td>
<td>Boolean flag indicating whether the server uses 64-bit-integer dates
and times.</td>
<td>No</td>
</tr>
<tr>
<td><code>intervalstyle</code></td>
<td><code>postgres</code></td>
<td>The display format for interval values. The only supported value is
<code>postgres</code>.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>is_superuser</code></td>
<td></td>
<td>Reports whether the current session is a <em>superuser</em> with
admin privileges.</td>
<td>No</td>
</tr>
<tr>
<td><code>max_aws_privatelink_connections</code></td>
<td><code>0</code></td>
<td>The maximum number of AWS PrivateLink connections in the region,
across all schemas.</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>max_clusters</code></td>
<td><code>10</code></td>
<td>The maximum number of clusters in the region</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>max_connections</code></td>
<td><code>5000</code></td>
<td>The maximum number of concurrent connections in the region</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>max_credit_consumption_rate</code></td>
<td><code>1024</code></td>
<td>The maximum rate of credit consumption in a region. Credits are
consumed based on the size of cluster replicas in use.</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>max_databases</code></td>
<td><code>1000</code></td>
<td>The maximum number of databases in the region.</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>max_identifier_length</code></td>
<td><code>255</code></td>
<td>The maximum length in bytes of object identifiers.</td>
<td>No</td>
</tr>
<tr>
<td><code>max_kafka_connections</code></td>
<td><code>1000</code></td>
<td>The maximum number of Kafka connections in the region, across all
schemas.</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>max_mysql_connections</code></td>
<td><code>1000</code></td>
<td>The maximum number of MySQL connections in the region, across all
schemas.</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>max_objects_per_schema</code></td>
<td><code>1000</code></td>
<td>The maximum number of objects in a schema.</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>max_postgres_connections</code></td>
<td><code>1000</code></td>
<td>The maximum number of PostgreSQL connections in the region, across
all schemas.</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>max_query_result_size</code></td>
<td><code>1073741824</code></td>
<td>The maximum size in bytes for a single query’s result.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>max_replicas_per_cluster</code></td>
<td><code>5</code></td>
<td>The maximum number of replicas of a single cluster</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>max_result_size</code></td>
<td><code>1 GiB</code></td>
<td>The maximum size in bytes for a single query’s result.</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>max_roles</code></td>
<td><code>1000</code></td>
<td>The maximum number of roles in the region.</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>max_schemas_per_database</code></td>
<td><code>1000</code></td>
<td>The maximum number of schemas in a database.</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>max_secrets</code></td>
<td><code>100</code></td>
<td>The maximum number of secrets in the region, across all
schemas.</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>max_sinks</code></td>
<td><code>1000</code></td>
<td>The maximum number of sinks in the region, across all schemas.</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>max_sources</code></td>
<td><code>25</code></td>
<td>The maximum number of sources in the region, across all
schemas.</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>max_tables</code></td>
<td><code>200</code></td>
<td>The maximum number of tables in the region, across all schemas</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>mz_version</code></td>
<td>Version-dependent</td>
<td>Shows the Materialize server version.</td>
<td>No</td>
</tr>
<tr>
<td><code>network_policy</code></td>
<td><code>default</code></td>
<td>The default network policy for the region.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>real_time_recency</code></td>
<td><code>false</code></td>
<td>Boolean flag indicating whether <a
href="/docs/get-started/isolation-level/#real-time-recency">real-time
recency</a> is enabled for the current session.</td>
<td><a href="/docs/support">Contact support</a></td>
</tr>
<tr>
<td><code>real_time_recency_timeout</code></td>
<td><code>10s</code></td>
<td>Sets the maximum allowed duration of <code>SELECT</code> statements
that actively use <a
href="/docs/get-started/isolation-level/#real-time-recency">real-time
recency</a>. If this value is specified without units, it is taken as
milliseconds (<code>ms</code>).</td>
<td>Yes</td>
</tr>
<tr>
<td><code>server_version_num</code></td>
<td>Version-dependent</td>
<td>The PostgreSQL compatible server version as an integer.</td>
<td>No</td>
</tr>
<tr>
<td><code>server_version</code></td>
<td>Version-dependent</td>
<td>The PostgreSQL compatible server version.</td>
<td>No</td>
</tr>
<tr>
<td><code>sql_safe_updates</code></td>
<td><code>false</code></td>
<td>Boolean flag indicating whether to prohibit SQL statements that may
be overly destructive.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>standard_conforming_strings</code></td>
<td><code>true</code></td>
<td>Boolean flag indicating whether ordinary string literals
(<code>'...'</code>) should treat backslashes literally. The only
supported value is <code>true</code>.</td>
<td>Yes</td>
</tr>
<tr>
<td><code>statement_timeout</code></td>
<td><code>10s</code></td>
<td>The maximum allowed duration of the read portion of write
operations; i.e., the <code>SELECT</code> portion of
<code>INSERT INTO ... (SELECT ...)</code>; the <code>WHERE</code>
portion of <code>UPDATE ... WHERE ...</code> and
<code>DELETE FROM ... WHERE ...</code>. If this value is specified
without units, it is taken as milliseconds (<code>ms</code>).</td>
<td>Yes</td>
</tr>
<tr>
<td><code>timezone</code></td>
<td><code>UTC</code></td>
<td>The time zone for displaying and interpreting timestamps. The only
supported value is <code>UTC</code>.</td>
<td>Yes</td>
</tr>
</tbody>
</table>

## Privileges

The privileges required to execute this statement are:

- [*Superuser*
  privileges](/docs/security/cloud/users-service-accounts/#organization-roles)

## Related pages

- [`ALTER SYSTEM RESET`](../alter-system-reset)
- [`SHOW`](../show)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/alter-system-set.md"
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
