# Appendix





---

## Appendix: Built-in roles


## `Public` role

All roles in Materialize are automatically members of
<a href="/security/appendix/appendix-built-in-roles/#public-role" ><code>PUBLIC</code></a>. As
such, every role includes inherited privileges from <code>PUBLIC</code>.

<p>By default, the <code>PUBLIC</code> role has the following privileges:</p>

**Baseline privileges via PUBLIC role:**

| Privilege | Description | On database object(s) |
| --- | --- | --- |
| <code>USAGE</code> | Permission to use or reference an object. | <ul> <li>All <code>*.public</code> schemas (e.g., <code>materialize.public</code>);</li> <li><code>materialize</code> database; and</li> <li><code>quickstart</code> cluster.</li> </ul>  |


**Default privileges on future objects set up for PUBLIC:**

| Object(s) | Object owner | Default Privilege | Granted to | Description |
| --- | --- | --- | --- | --- |
| <a href="/sql/types/" ><code>TYPE</code></a> | <code>PUBLIC</code> | <code>USAGE</code> | <code>PUBLIC</code> | When a <a href="/sql/types/" >data type</a> is created (regardless of the owner), all roles are granted the <code>USAGE</code> privilege. However, to use a data type, the role must also have <code>USAGE</code> privilege on the schema containing the type. |

Default privileges apply only to objects created after these privileges are
defined. They do not affect objects that were created before the default
privileges were set.

You can modify the privileges of your organization's `PUBLIC` role as well as
the define default privileges for `PUBLIC`.

## System catalog roles

Certain internal objects may only be queried by superusers or by users
belonging to a particular builtin role, which superusers may
[grant](/sql/grant-role). These include the following:

| Name                  | Description                                                                                                                                                                                                                                                                                                                                                                                                   |
|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `mz_monitor`          | Grants access to objects that reveal actions taken by other users, in particular, SQL statements they have issued. Includes [`mz_recent_activity_log`](/sql/system-catalog/mz_internal#mz_recent_activity_log) and [`mz_notices`](/sql/system-catalog/mz_internal#mz_notices).                                                                                                                                    |
| `mz_monitor_redacted` | Grants access to objects that reveal less sensitive information about actions taken by other users, for example, SQL statements they have issued with constant values redacted. Includes `mz_recent_activity_log_redacted`, [`mz_notices_redacted`](/sql/system-catalog/mz_internal#mz_notices_redacted), and [`mz_statement_lifecycle_history`](/sql/system-catalog/mz_internal#mz_statement_lifecycle_history). |
|                       |


---

## Appendix: Privileges


> **Note:** <p>Various SQL operations require additional privileges on related objects, such
> as:</p>
> <ul>
> <li>
> <p>For objects that use compute resources (e.g., indexes, materialized views,
> replicas, sources, sinks), access is also required for the associated cluster.</p>
> </li>
> <li>
> <p>For objects in a schema, access is also required for the schema.</p>
> </li>
> </ul>
> <p>For details on SQL operations and needed privileges, see <a href="/security/appendix/appendix-command-privileges/" >Appendix: Privileges
> by command</a>.</p>
>
>


The following privileges are available in Materialize:


**By Privilege:**

| Privilege | Description | Abbreviation | Applies to |
| --- | --- | --- | --- |
| <strong>SELECT</strong> | Permission to read rows from an object. | <code>r</code> | <ul> <li><code>MATERIALIZED VIEW</code></li> <li><code>SOURCE</code></li> <li><code>TABLE</code></li> <li><code>VIEW</code></li> </ul>  |
| <strong>INSERT</strong> | Permission to insert rows into an object. | <code>a</code> | <ul> <li><code>TABLE</code></li> </ul>  |
| <strong>UPDATE</strong> | <p>Permission to modify rows in an object.</p> <p>Modifying rows may also require <strong>SELECT</strong> if a read is needed to determine which rows to update.</p>  | <code>w</code> | <ul> <li><code>TABLE</code></li> </ul>  |
| <strong>DELETE</strong> | <p>Permission to delete rows from an object.</p> <p>Deleting rows may also require <strong>SELECT</strong> if a read is needed to determine which rows to delete.</p>  | <code>d</code> | <ul> <li><code>TABLE</code></li> </ul>  |
| <strong>CREATE</strong> | Permission to create a new objects within the specified object. | <code>C</code> | <ul> <li><code>DATABASE</code></li> <li><code>SCHEMA</code></li> <li><code>CLUSTER</code></li> </ul>  |
| <strong>USAGE</strong> | <a name="privilege-usage"></a> Permission to use or reference an object (e.g., schema/type lookup). | <code>U</code> | <ul> <li><code>CLUSTER</code></li> <li><code>CONNECTION</code></li> <li><code>DATABASE</code></li> <li><code>SCHEMA</code></li> <li><code>SECRET</code></li> <li><code>TYPE</code></li> </ul>  |
| <strong>CREATEROLE</strong> | <p>Permission to create/modify/delete roles and manage role memberships for any role in the system.</p> > **Warning:** Roles with the <code>CREATEROLE</code> privilege can obtain the privileges of any other > role in the system by granting themselves that role. Avoid granting > <code>CREATEROLE</code> unnecessarily. > | <code>R</code> | <ul> <li><code>SYSTEM</code></li> </ul>  |
| <strong>CREATEDB</strong> | Permission to create new databases. | <code>B</code> | <ul> <li><code>SYSTEM</code></li> </ul>  |
| <strong>CREATECLUSTER</strong> | Permission to create new clusters. | <code>N</code> | <ul> <li><code>SYSTEM</code></li> </ul>  |
| <strong>CREATENETWORKPOLICY</strong> | Permission to create network policies to control access at the network layer. | <code>P</code> | <ul> <li><code>SYSTEM</code></li> </ul>  |


**By Object:**

| Object | Privileges |
| --- | --- |
| <code>CLUSTER</code> | <ul> <li><code>USAGE</code></li> <li><code>CREATE</code></li> </ul>  |
| <code>CONNECTION</code> | <ul> <li><code>USAGE</code></li> </ul>  |
| <code>DATABASE</code> | <ul> <li><code>USAGE</code></li> <li><code>CREATE</code></li> </ul>  |
| <code>MATERIALIZED VIEW</code> | <ul> <li><code>SELECT</code></li> </ul>  |
| <code>SCHEMA</code> | <ul> <li><code>USAGE</code></li> <li><code>CREATE</code></li> </ul>  |
| <code>SECRET</code> | <ul> <li><code>USAGE</code></li> </ul>  |
| <code>SOURCE</code> | <ul> <li><code>SELECT</code></li> </ul>  |
| <code>SYSTEM</code> | <ul> <li><code>CREATEROLE</code></li> <li><code>CREATEDB</code></li> <li><code>CREATECLUSTER</code></li> <li><code>CREATENETWORKPOLICY</code></li> </ul>  |
| <code>TABLE</code> | <ul> <li><code>INSERT</code></li> <li><code>SELECT</code></li> <li><code>UPDATE</code></li> <li><code>DELETE</code></li> </ul>  |
| <code>TYPE</code> | <ul> <li><code>USAGE</code></li> </ul>  |
| <code>VIEW</code> | <ul> <li><code>SELECT</code></li> </ul>  |





---

## Appendix: Privileges by commands



| Command | Privileges |
| --- | --- |
| <a href="/sql/alter-cluster" ><code>ALTER CLUSTER</code></a> | <ul> <li> <p>Ownership of the cluster.</p> </li> <li> <p>To rename a cluster, you must also have membership in the <code>&lt;new_owner_role&gt;</code>.</p> </li> <li> <p>To swap names with another cluster, you must also have ownership of the other cluster.</p> </li> </ul>   |
| <a href="/sql/alter-cluster-replica" ><code>ALTER CLUSTER REPLICA</code></a> | <ul> <li>Ownership of the cluster replica.</li> <li>In addition, to change owners: <ul> <li>Role membership in <code>new_owner</code>.</li> <li><code>CREATE</code> privileges on the containing cluster.</li> </ul> </li> </ul>   |
| <a href="/sql/alter-connection" ><code>ALTER CONNECTION</code></a> | <ul> <li>Ownership of the connection.</li> <li>In addition, to change owners: <ul> <li>Role membership in <code>new_owner</code>.</li> <li><code>CREATE</code> privileges on the containing schema if the connection is namespaced by a schema.</li> </ul> </li> </ul>   |
| <a href="/sql/alter-database" ><code>ALTER DATABASE</code></a> | <ul> <li>Ownership of the database.</li> <li>In addition, to change owners: <ul> <li>Role membership in <code>new_owner</code>.</li> </ul> </li> </ul>   |
| <a href="/sql/alter-default-privileges" ><code>ALTER DEFAULT PRIVILEGES</code></a> | <ul> <li>Role membership in <code>role_name</code>.</li> <li><code>USAGE</code> privileges on the containing database if <code>database_name</code> is specified.</li> <li><code>USAGE</code> privileges on the containing schema if <code>schema_name</code> is specified.</li> <li><em>superuser</em> status if the <em>target_role</em> is <code>PUBLIC</code> or <strong>ALL ROLES</strong> is specified.</li> </ul>   |
| <a href="/sql/alter-index" ><code>ALTER INDEX</code></a> | <ul> <li>Ownership of the index.</li> </ul>   |
| <a href="/sql/alter-materialized-view" ><code>ALTER MATERIALIZED VIEW</code></a> | <ul> <li>Ownership of the materialized view.</li> <li>In addition, to change owners: <ul> <li>Role membership in <code>new_owner</code>.</li> <li><code>CREATE</code> privileges on the containing schema if the materialized view is namespaced by a schema.</li> </ul> </li> </ul>           |
| <a href="/sql/alter-network-policy" ><code>ALTER NETWORK POLICY</code></a> | <ul> <li>Ownership of the network policy.</li> </ul>   |
| <a href="/sql/alter-role" ><code>ALTER ROLE</code></a> | <ul> <li><code>CREATEROLE</code> privileges on the system.</li> </ul>   |
| <a href="/sql/alter-schema" ><code>ALTER SCHEMA</code></a> | <ul> <li>Ownership of the schema.</li> <li>In addition, <ul> <li>To swap with another schema: <ul> <li>Ownership of the other schema</li> </ul> </li> <li>To change owners: <ul> <li>Role membership in <code>new_owner</code>.</li> <li><code>CREATE</code> privileges on the containing database.</li> </ul> </li> </ul> </li> </ul>   |
| <a href="/sql/alter-secret" ><code>ALTER SECRET</code></a> | <ul> <li>Ownership of the secret being altered.</li> <li>In addition, to change owners: <ul> <li>Role membership in <code>new_owner</code>.</li> <li><code>CREATE</code> privileges on the containing schema if the secret is namespaced by a schema.</li> </ul> </li> </ul>   |
| <a href="/sql/alter-sink" ><code>ALTER SINK</code></a> | <ul> <li>Ownership of the sink being altered.</li> <li>In addition, <ul> <li>To change the sink from relation: <ul> <li><code>SELECT</code> privileges on the new relation being written out to an external system.</li> <li><code>CREATE</code> privileges on the cluster maintaining the sink.</li> <li><code>USAGE</code> privileges on all connections and secrets used in the sink definition.</li> <li><code>USAGE</code> privileges on the schemas that all connections and secrets in the statement are contained in.</li> </ul> </li> <li>To change owners: <ul> <li>Role membership in <code>new_owner</code>.</li> <li><code>CREATE</code> privileges on the containing schema if the sink is namespaced by a schema.</li> </ul> </li> </ul> </li> </ul>   |
| <a href="/sql/alter-source" ><code>ALTER SOURCE</code></a> | <ul> <li>Ownership of the source being altered.</li> <li>In addition, to change owners: <ul> <li>Role membership in <code>new_owner</code>.</li> <li><code>CREATE</code> privileges on the containing schema if the source is namespaced by a schema.</li> </ul> </li> </ul>   |
| <a href="/sql/alter-system-reset" ><code>ALTER SYSTEM RESET</code></a> | <ul> <li><a href="/security/cloud/users-service-accounts/#organization-roles" ><em>Superuser</em> privileges</a></li> </ul>   |
| <a href="/sql/alter-system-set" ><code>ALTER SYSTEM SET</code></a> | <ul> <li><a href="/security/cloud/users-service-accounts/#organization-roles" ><em>Superuser</em> privileges</a></li> </ul>   |
| <a href="/sql/alter-table" ><code>ALTER TABLE</code></a> | <ul> <li>Ownership of the table being altered.</li> <li>In addition, to change owners: <ul> <li>Role membership in <code>new_owner</code>.</li> <li><code>CREATE</code> privileges on the containing schema if the table is namespaced by a schema.</li> </ul> </li> </ul>   |
| <a href="/sql/alter-type" ><code>ALTER TYPE</code></a> | <ul> <li>Ownership of the type being altered.</li> <li>In addition, to change owners: <ul> <li>Role membership in <code>new_owner</code>.</li> <li><code>CREATE</code> privileges on the containing schema if the type is namespaced by a schema.</li> </ul> </li> </ul>   |
| <a href="/sql/alter-view" ><code>ALTER VIEW</code></a> | <ul> <li>Ownership of the view being altered.</li> <li>In addition, to change owners: <ul> <li>Role membership in <code>new_owner</code>.</li> <li><code>CREATE</code> privileges on the containing schema if the view is namespaced by a schema.</li> </ul> </li> </ul>   |
| <a href="/sql/comment-on" ><code>COMMENT ON</code></a> | <ul> <li>Ownership of the object being commented on (unless the object is a role).</li> <li>To comment on a role, you must have the <code>CREATEROLE</code> privilege.</li> </ul>   |
| <a href="/sql/copy-from" ><code>COPY FROM</code></a> | <ul> <li><code>USAGE</code> privileges on the schema containing the table.</li> <li><code>INSERT</code> privileges on the table.</li> </ul>   |
| <a href="/sql/copy-to" ><code>COPY TO</code></a> | <ul> <li><code>USAGE</code> privileges on the schemas that all relations and types in the query are contained in.</li> <li><code>SELECT</code> privileges on all relations in the query. <ul> <li>NOTE: if any item is a view, then the view owner must also have the necessary privileges to execute the view definition. Even if the view owner is a <em>superuser</em>, they still must explicitly be granted the necessary privileges.</li> </ul> </li> <li><code>USAGE</code> privileges on all types used in the query.</li> <li><code>USAGE</code> privileges on the active cluster.</li> </ul>   |
| <a href="/sql/create-cluster" ><code>CREATE CLUSTER</code></a> | <ul> <li><code>CREATECLUSTER</code> privileges on the system.</li> </ul>   |
| <a href="/sql/create-cluster-replica" ><code>CREATE CLUSTER REPLICA</code></a> | <ul> <li>Ownership of the cluster.</li> </ul>   |
| <a href="/sql/create-connection" ><code>CREATE CONNECTION</code></a> | <ul> <li><code>CREATE</code> privileges on the containing schema.</li> <li><code>USAGE</code> privileges on all connections and secrets used in the connection definition.</li> <li><code>USAGE</code> privileges on the schemas that all connections and secrets in the statement are contained in.</li> </ul>   |
| <a href="/sql/create-database" ><code>CREATE DATABASE</code></a> | <ul> <li><code>CREATEDB</code> privileges on the system.</li> </ul>   |
| <a href="/sql/create-index" ><code>CREATE INDEX</code></a> | <ul> <li>Ownership of the object on which to create the index.</li> <li><code>CREATE</code> privileges on the containing schema.</li> <li><code>CREATE</code> privileges on the containing cluster.</li> <li><code>USAGE</code> privileges on all types used in the index definition.</li> <li><code>USAGE</code> privileges on the schemas that all types in the statement are contained in.</li> </ul>   |
| <a href="/sql/create-materialized-view" ><code>CREATE MATERIALIZED VIEW</code></a> | <ul> <li><code>CREATE</code> privileges on the containing schema.</li> <li><code>CREATE</code> privileges on the containing cluster.</li> <li><code>USAGE</code> privileges on all types used in the materialized view definition.</li> <li><code>USAGE</code> privileges on the schemas for the types used in the statement.</li> </ul>   |
| <a href="/sql/create-network-policy/" ><code>CREATE NETWORK POLICY</code></a> | <ul> <li><code>CREATENETWORKPOLICY</code> privileges on the system.</li> </ul>   |
| <a href="/sql/create-role" ><code>CREATE ROLE</code></a> | <ul> <li><code>CREATEROLE</code> privileges on the system.</li> </ul>   |
| <a href="/sql/create-schema" ><code>CREATE SCHEMA</code></a> | <ul> <li><code>CREATE</code> privileges on the containing database.</li> </ul>   |
| <a href="/sql/create-secret" ><code>CREATE SECRET</code></a> | <ul> <li><code>CREATE</code> privileges on the containing schema.</li> </ul>   |
| <a href="/sql/create-sink" ><code>CREATE SINK</code></a> | <ul> <li><code>CREATE</code> privileges on the containing schema.</li> <li><code>SELECT</code> privileges on the item being written out to an external system. <ul> <li>NOTE: if the item is a materialized view, then the view owner must also have the necessary privileges to execute the view definition.</li> </ul> </li> <li><code>CREATE</code> privileges on the containing cluster if the sink is created in an existing cluster.</li> <li><code>CREATECLUSTER</code> privileges on the system if the sink is not created in an existing cluster.</li> <li><code>USAGE</code> privileges on all connections and secrets used in the sink definition.</li> <li><code>USAGE</code> privileges on the schemas that all connections and secrets in the statement are contained in.</li> </ul>   |
| <a href="/sql/create-source" ><code>CREATE SOURCE</code></a> | <ul> <li><code>CREATE</code> privileges on the containing schema.</li> <li><code>CREATE</code> privileges on the containing cluster if the source is created in an existing cluster.</li> <li><code>CREATECLUSTER</code> privileges on the system if the source is not created in an existing cluster.</li> <li><code>USAGE</code> privileges on all connections and secrets used in the source definition.</li> <li><code>USAGE</code> privileges on the schemas that all connections and secrets in the statement are contained in.</li> </ul>   |
| <a href="/sql/create-table" ><code>CREATE TABLE</code></a> | <ul> <li><code>CREATE</code> privileges on the containing schema.</li> <li><code>USAGE</code> privileges on all types used in the table definition.</li> <li><code>USAGE</code> privileges on the schemas that all types in the statement are contained in.</li> </ul>   |
| <a href="/sql/create-type" ><code>CREATE TYPE</code></a> | <ul> <li><code>CREATE</code> privileges on the containing schema.</li> <li><code>USAGE</code> privileges on all types used in the type definition.</li> <li><code>USAGE</code> privileges on the schemas that all types in the statement are contained in.</li> </ul>   |
| <a href="/sql/create-view" ><code>CREATE VIEW</code></a> | <ul> <li><code>CREATE</code> privileges on the containing schema.</li> <li><code>USAGE</code> privileges on all types used in the view definition.</li> <li><code>USAGE</code> privileges on the schemas for the types in the statement.</li> <li>Ownership of the existing view if replacing an existing view with the same name (i.e., <code>OR REPLACE</code> is specified in <code>CREATE VIEW</code> command).</li> </ul>   |
| <a href="/sql/delete" ><code>DELETE</code></a> | <ul> <li><code>USAGE</code> privileges on the schemas that all relations and types in the query are contained in.</li> <li><code>DELETE</code> privileges on <code>table_name</code>.</li> <li><code>SELECT</code> privileges on all relations in the query. <ul> <li>NOTE: if any item is a view, then the view owner must also have the necessary privileges to execute the view definition. Even if the view owner is a <em>superuser</em>, they still must explicitly be granted the necessary privileges.</li> </ul> </li> <li><code>USAGE</code> privileges on all types used in the query.</li> <li><code>USAGE</code> privileges on the active cluster.</li> </ul>   |
| <a href="/sql/drop-cluster-replica" ><code>DROP CLUSTER REPLICA</code></a> | <ul> <li>Ownership of the dropped cluster replica.</li> <li><code>USAGE</code> privileges on the containing cluster.</li> </ul>   |
| <a href="/sql/drop-cluster" ><code>DROP CLUSTER</code></a> | <ul> <li>Ownership of the dropped cluster.</li> </ul>   |
| <a href="/sql/drop-connection" ><code>DROP CONNECTION</code></a> | <ul> <li>Ownership of the dropped connection.</li> <li><code>USAGE</code> privileges on the containing schema.</li> </ul>   |
| <a href="/sql/drop-database" ><code>DROP DATABASE</code></a> | <ul> <li>Ownership of the dropped database.</li> </ul>   |
| <a href="/sql/drop-index" ><code>DROP INDEX</code></a> | <ul> <li>Ownership of the dropped index.</li> <li><code>USAGE</code> privileges on the containing schema.</li> </ul>   |
| <a href="/sql/drop-materialized-view" ><code>DROP MATERIALIZED VIEW</code></a> | <ul> <li>Ownership of the dropped materialized view.</li> <li><code>USAGE</code> privileges on the containing schema.</li> </ul>   |
| <a href="/sql/drop-network-policy" ><code>DROP NETWORK POLICY</code></a> | <ul> <li><code>CREATENETWORKPOLICY</code> privileges on the system.</li> </ul>   |
| <a href="/sql/drop-owned" ><code>DROP OWNED</code></a> | <ul> <li>Role membership in <code>role_name</code>.</li> </ul>   |
| <a href="/sql/drop-role" ><code>DROP ROLE</code></a> | <ul> <li><code>CREATEROLE</code> privileges on the system.</li> </ul>   |
| <a href="/sql/drop-schema" ><code>DROP SCHEMA</code></a> | <ul> <li>Ownership of the dropped schema.</li> <li><code>USAGE</code> privileges on the containing database.</li> </ul>   |
| <a href="/sql/drop-secret" ><code>DROP SECRET</code></a> | <ul> <li>Ownership of the dropped secret.</li> <li><code>USAGE</code> privileges on the containing schema.</li> </ul>   |
| <a href="/sql/drop-sink" ><code>DROP SINK</code></a> | <ul> <li>Ownership of the dropped sink.</li> <li><code>USAGE</code> privileges on the containing schema.</li> </ul>   |
| <a href="/sql/drop-source" ><code>DROP SOURCE</code></a> | <ul> <li>Ownership of the dropped source.</li> <li><code>USAGE</code> privileges on the containing schema.</li> </ul>   |
| <a href="/sql/drop-table" ><code>DROP TABLE</code></a> | <ul> <li>Ownership of the dropped table.</li> <li><code>USAGE</code> privileges on the containing schema.</li> </ul>   |
| <a href="/sql/drop-type" ><code>DROP TYPE</code></a> | <ul> <li>Ownership of the dropped type.</li> <li><code>USAGE</code> privileges on the containing schema.</li> </ul>   |
| <a href="/sql/drop-user" ><code>DROP USER</code></a> | <ul> <li><code>CREATEROLE</code> privileges on the system.</li> </ul>   |
| <a href="/sql/drop-view" ><code>DROP VIEW</code></a> | <ul> <li>Ownership of the dropped view.</li> <li><code>USAGE</code> privileges on the containing schema.</li> </ul>   |
| <a href="/sql/explain-analyze" ><code>EXPLAIN ANALYZE</code></a> | <ul> <li><code>USAGE</code> privileges on the schemas that all relations in the explainee are contained in.</li> </ul>   |
| <a href="/sql/explain-filter-pushdown" ><code>EXPLAIN FILTER PUSHDOWN</code></a> | <ul> <li><code>USAGE</code> privileges on the schemas that all relations in the explainee are contained in.</li> </ul>   |
| <a href="/sql/explain-plan" ><code>EXPLAIN PLAN</code></a> | <ul> <li><code>USAGE</code> privileges on the schemas that all relations in the explainee are contained in.</li> </ul>   |
| <a href="/sql/explain-schema" ><code>EXPLAIN SCHEMA</code></a> | <ul> <li><code>USAGE</code> privileges on the schemas that all items in the query are contained in.</li> </ul>   |
| <a href="/sql/explain-timestamp" ><code>EXPLAIN TIMESTAMP</code></a> | <ul> <li><code>USAGE</code> privileges on the schemas that all relations in the query are contained in.</li> </ul>   |
| <a href="/sql/grant-privilege" ><code>GRANT PRIVILEGE</code></a> | <ul> <li>Ownership of affected objects.</li> <li><code>USAGE</code> privileges on the containing database if the affected object is a schema.</li> <li><code>USAGE</code> privileges on the containing schema if the affected object is namespaced by a schema.</li> <li><em>superuser</em> status if the privilege is a system privilege.</li> </ul>   |
| <a href="/sql/grant-role" ><code>GRANT ROLE</code></a> | <ul> <li><code>CREATEROLE</code> privileges on the system.</li> </ul>   |
| <a href="/sql/insert" ><code>INSERT</code></a> | <ul> <li><code>USAGE</code> privileges on the schemas that all relations and types in the query are contained in.</li> <li><code>INSERT</code> privileges on <code>table_name</code>.</li> <li><code>SELECT</code> privileges on all relations in the query. <ul> <li>NOTE: if any item is a view, then the view owner must also have the necessary privileges to execute the view definition. Even if the view owner is a <em>superuser</em>, they still must explicitly be granted the necessary privileges.</li> </ul> </li> <li><code>USAGE</code> privileges on all types used in the query.</li> <li><code>USAGE</code> privileges on the active cluster.</li> </ul>   |
| <a href="/sql/reassign-owned" ><code>REASSIGN OWNED</code></a> | <ul> <li>Role membership in <code>old_role</code> and <code>new_role</code>.</li> </ul>   |
| <a href="/sql/revoke-privilege" ><code>REVOKE PRIVILEGE</code></a> | <ul> <li>Ownership of affected objects.</li> <li><code>USAGE</code> privileges on the containing database if the affected object is a schema.</li> <li><code>USAGE</code> privileges on the containing schema if the affected object is namespaced by a schema.</li> <li><em>superuser</em> status if the privilege is a system privilege.</li> </ul>   |
| <a href="/sql/revoke-role" ><code>REVOKE ROLE</code></a> | <ul> <li><code>CREATEROLE</code> privileges on the systems.</li> </ul>   |
| <a href="/sql/select" ><code>SELECT</code></a> | <ul> <li> <p><code>SELECT</code> privileges on all <strong>directly</strong> referenced relations in the query. If the directly referenced relation is a view or materialized view: </p> </li> <li> <p><code>USAGE</code> privileges on the schemas that contain the relations in the query.</p> </li> <li> <p><code>USAGE</code> privileges on the active cluster.</p> </li> </ul>   |
| <a href="/sql/show-columns" ><code>SHOW COLUMNS</code></a> | <ul> <li><code>USAGE</code> privileges on the schema containing <code>item_ref</code>.</li> </ul>   |
| <a href="/sql/show-create-cluster" ><code>SHOW CREATE CLUSTER</code></a> | There are no privileges required to execute this statement.  |
| <a href="/sql/show-create-connection" ><code>SHOW CREATE CONNECTION</code></a> | <ul> <li><code>USAGE</code> privileges on the schema containing the connection.</li> </ul>   |
| <a href="/sql/show-create-index" ><code>SHOW CREATE INDEX</code></a> | <ul> <li><code>USAGE</code> privileges on the schema containing the index.</li> </ul>   |
| <a href="/sql/show-create-materialized-view" ><code>SHOW CREATE MATERIALIZED VIEW</code></a> | <ul> <li><code>USAGE</code> privileges on the schema containing the materialized view.</li> </ul>   |
| <a href="/sql/show-create-type" ><code>SHOW CREATE TYPE</code></a> | <ul> <li><code>USAGE</code> privileges on the schema containing the table.</li> </ul>   |
| <a href="/sql/show-create-sink" ><code>SHOW CREATE SINK</code></a> | <ul> <li><code>USAGE</code> privileges on the schema containing the sink.</li> </ul>   |
| <a href="/sql/show-create-source" ><code>SHOW CREATE SOURCE</code></a> | <ul> <li><code>USAGE</code> privileges on the schema containing the source.</li> </ul>   |
| <a href="/sql/show-create-table" ><code>SHOW CREATE TABLE</code></a> | <ul> <li><code>USAGE</code> privileges on the schema containing the table.</li> </ul>   |
| <a href="/sql/show-create-view" ><code>SHOW CREATE VIEW</code></a> | <ul> <li><code>USAGE</code> privileges on the schema containing the view.</li> </ul>   |
| <a href="/sql/subscribe" ><code>SUBSCRIBE</code></a> | <ul> <li><code>USAGE</code> privileges on the schemas that all relations and types in the query are contained in.</li> <li><code>SELECT</code> privileges on all relations in the query. <ul> <li>NOTE: if any item is a view, then the view owner must also have the necessary privileges to execute the view definition. Even if the view owner is a <em>superuser</em>, they still must explicitly be granted the necessary privileges.</li> </ul> </li> <li><code>USAGE</code> privileges on all types used in the query.</li> <li><code>USAGE</code> privileges on the active cluster.</li> </ul>   |
| <a href="/sql/update" ><code>UPDATE</code></a> | <ul> <li><code>USAGE</code> privileges on the schemas that all relations and types in the query are contained in.</li> <li><code>UPDATE</code> privileges on the table being updated.</li> <li><code>SELECT</code> privileges on all relations in the query. <ul> <li>NOTE: if any item is a view, then the view owner must also have the necessary privileges to execute the view definition. Even if the view owner is a <em>superuser</em>, they still must explicitly be granted the necessary privileges.</li> </ul> </li> <li><code>USAGE</code> privileges on all types used in the query.</li> <li><code>USAGE</code> privileges on the active cluster.</li> </ul>   |
| <a href="/sql/validate-connection" ><code>VALIDATE CONNECTION</code></a> | <ul> <li><code>USAGE</code> privileges on the containing schema.</li> <li><code>USAGE</code> privileges on the connection.</li> </ul>   |
