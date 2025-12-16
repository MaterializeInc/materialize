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

# Manage database roles

In Materialize, role-based access control (RBAC) governs access to
objects through privileges granted to database roles.

## Enabling RBAC

<div class="warning">

**WARNING!** If RBAC is not enabled, all users have **superuser**
privileges.

</div>

By default, role-based access control (RBAC) checks are not enabled
(i.e., enforced) when turning on [password
authentication](/docs/self-managed/v25.2/manage/authentication/#configuring-password-authentication).
To enable RBAC, set the system parameter `enable_rbac_checks` to `'on'`
or `True`. You can enable the parameter in one of the following ways:

- For [local installations using
  Kind/Minikube](/docs/self-managed/v25.2/installation/#installation-guides),
  set `spec.enableRbac: true` option when instantiating the Materialize
  object.

- For [Cloud deployments using Materialize’s
  Terraforms](/docs/self-managed/v25.2/installation/#installation-guides),
  set `enable_rbac_checks` in the environment CR via the
  `environmentdExtraArgs` flag option.

- After the Materialize instance is running, run the following command
  as `mz_system` user:

  <div class="highlight">

  ``` chroma
  ALTER SYSTEM SET enable_rbac_checks = 'on';
  ```

  </div>

If more than one method is used, the `ALTER SYSTEM` command will take
precedence over the Kubernetes configuration.

To view the current value for `enable_rbac_checks`, run the following
`SHOW` command:

<div class="highlight">

``` chroma
SHOW enable_rbac_checks;
```

</div>

<div class="important">

**! Important:** If RBAC is not enabled, all users have **superuser**
privileges.

</div>

## Required privileges for managing roles

<div class="note">

**NOTE:** Initially, only the `mz_system` user (which has
superuser/administrator privileges) is available to manage roles.

</div>

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Role management operations</th>
<th>Required privileges</th>
</tr>
</thead>
<tbody>
<tr>
<td>To create/revoke/grant roles</td>
<td><ul>
<li><code>CREATEROLE</code> privileges on the system.</li>
</ul>
<div class="warning">
<strong>WARNING!</strong> Roles with the <code>CREATEROLE</code>
privilege can obtain the privileges of any other role in the system by
granting themselves that role. Avoid granting <code>CREATEROLE</code>
unnecessarily.
</div></td>
</tr>
<tr>
<td>To view privileges for a role</td>
<td>None</td>
</tr>
<tr>
<td>To grant/revoke role privileges</td>
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
<td>To alter default privileges</td>
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
</tbody>
</table>

See also [Appendix: Privileges by
command](/docs/self-managed/v25.2/manage/access-control/appendix-command-privileges/)

## Create a role

In Materialize, you can create both:

- Individual user or service account roles; i.e., roles associated with
  a specific user or service account.
- Functional roles, not associated with any single user or service
  account, but typically used to define a set of shared privileges that
  can be granted to other user/service/functional roles.

Initially, only the `mz_system` user is available.

### Create individual user/service account roles

To create additional users or service accounts, login as the `mz_system`
user, using the `external_login_password_mz_system` password, and use
[`CREATE ROLE ... WITH LOGIN PASSWORD ...`](/docs/self-managed/v25.2/sql/create-role):

<div class="highlight">

``` chroma
CREATE ROLE <user> WITH LOGIN PASSWORD '<password>';
```

</div>

<div class="annotation">

<div class="annotation-title">

Privilege(s) required to run the command

</div>

<div>

- `CREATEROLE` privileges on the system.

</div>

</div>

For example, the following creates:

- A new user `blue.berry@example.com` (or more specifically, a new user
  role).
- A new service account `sales_report_app` (or more specifically, a new
  service account role).

<div class="code-tabs">

<div class="tab-content">

<div id="tab-a-new-user-role" class="tab-pane" title="A new user role">

The following creates a new user
[`blue.berry@example.com`](mailto:blue.berry@example.com), or more
specifically, creates a role for a user
[`blue.berry@example.com`](mailto:blue.berry@example.com) using
[`CREATE ROLE … WITH LOGIN PASSWORD`](/docs/self-managed/v25.2/sql/create-role).

<div class="highlight">

``` chroma
CREATE ROLE "blue.berry@example.com" WITH LOGIN PASSWORD '<password>';
```

</div>

<div class="note">

**NOTE:** The role/user name `blue.berry@example.com` is enclosed in
double quotes to override the [naming
restrictions](/docs/self-managed/v25.2/sql/identifiers/#naming-restrictions).

</div>

Once created, the user `blue.berry@example.com` can login using the
password.

</div>

<div id="tab-a-new-service-account-role" class="tab-pane"
title="A new service account role">

The following creates a new service account `sales_report_app`, or more
specifically, creates a role for a service account `sales_report_app`
using
[`CREATE ROLE … WITH LOGIN PASSWORD`](/docs/self-managed/v25.2/sql/create-role).

<div class="highlight">

``` chroma
CREATE ROLE "sales_report_app" WITH LOGIN PASSWORD '<password>';
```

</div>

Once created, the associated application can use the `sales_report_app`
service account to connect to Materialize.

</div>

</div>

</div>

In Materialize, a role is created with inheritance support. With
inheritance, when a role is granted to another role (i.e., the target
role), the target role inherits privileges (not role attributes and
parameters) through the other role. All roles in Materialize are
automatically members of
[`PUBLIC`](/docs/self-managed/v25.2/manage/access-control/appendix-built-in-roles/#public-role).
As such, every role includes inherited privileges from `PUBLIC`.

Once a role is created, you can:

- [Manage its current
  privileges](/docs/self-managed/v25.2/manage/access-control/manage-roles/#manage-current-privileges-for-a-role)
  (i.e., privileges on existing objects):
  - By granting privileges for a role or revoking privileges from a
    role.
  - By granting other roles to the role or revoking roles from the role.
    *Recommended for user account/service account roles.*
- [Manage its future
  privileges](/docs/self-managed/v25.2/manage/access-control/manage-roles/#manage-future-privileges-for-a-role)
  (i.e., privileges on objects created in the future):
  - By defining default privileges for objects. With default privileges
    in place, a role is automatically granted/revoked privileges as new
    objects are created by **others** (When an object is created, the
    creator is granted all [applicable
    privileges](/docs/self-managed/v25.2/manage/access-control/appendix-privileges/)
    for that object automatically).

<div class="annotation">

<div class="annotation-title">

Disambiguation

</div>

<div>

- Use `GRANT|REVOKE ...` to modify privileges on **existing** objects.

- Use `ALTER DEFAULT PRIVILEGES` to ensure that privileges are
  automatically granted or revoked when **new objects** of a certain
  type are created by others. Then, as needed, you can use
  `GRANT|REVOKE <privilege>` to adjust those privileges.

</div>

</div>

See also:

- For a list of required privileges for specific operations, see
  [Appendix: Privileges by
  command](/docs/self-managed/v25.2/manage/access-control/appendix-command-privileges/).

### Create functional roles

To create functional roles, login as the `mz_system` user, using the
`external_login_password_mz_system` password, and use
[`CREATE ROLE`](/docs/self-managed/v25.2/sql/create-role):

<div class="highlight">

``` chroma
CREATE ROLE <role>;
```

</div>

<div class="tip">

**💡 Tip:** Role names cannot start with `mz_` and `pg_` as they are
reserved for system roles.

</div>

For example, the following creates:

- A role for users who need to perform compute/transform operations in
  the compute/transform.
- A role for users who need to manage indexes on the serving cluster(s).
- A role for users who need to read results from the serving cluster.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-view-manager-role" class="tab-pane"
title="View manager role">

Create a role for users who need to perform compute/transform operations
in the compute/transform cluster(s). This role will handle creating
views, materialized views, and other transformation objects.

<div class="highlight">

``` chroma
CREATE ROLE view_manager;
```

</div>

</div>

<div id="tab-serving-index-manager-role" class="tab-pane"
title="Serving index manager role">

Create a role for users who need to manage indexes on the serving
cluster(s). This role will handle creating indexes to serve results.

<div class="highlight">

``` chroma
CREATE ROLE serving_index_manager;
```

</div>

</div>

<div id="tab-data-reader-role" class="tab-pane"
title="Data reader role">

Create a role for users who need to read results from the serving
cluster.

<div class="highlight">

``` chroma
CREATE ROLE data_reader;
```

</div>

</div>

</div>

</div>

In Materialize, a role is created with inheritance support. With
inheritance, when a role is granted to another role (i.e., the target
role), the target role inherits privileges (not role attributes and
parameters) through the other role. All roles in Materialize are
automatically members of
[`PUBLIC`](/docs/self-managed/v25.2/manage/access-control/appendix-built-in-roles/#public-role).
As such, every role includes inherited privileges from `PUBLIC`.

Once a role is created, you can:

- [Manage its current
  privileges](/docs/self-managed/v25.2/manage/access-control/manage-roles/#manage-current-privileges-for-a-role)
  (i.e., privileges on existing objects):
  - By granting privileges for a role or revoking privileges from a
    role.
  - By granting other roles to the role or revoking roles from the role.
    *Recommended for user account/service account roles.*
- [Manage its future
  privileges](/docs/self-managed/v25.2/manage/access-control/manage-roles/#manage-future-privileges-for-a-role)
  (i.e., privileges on objects created in the future):
  - By defining default privileges for objects. With default privileges
    in place, a role is automatically granted/revoked privileges as new
    objects are created by **others** (When an object is created, the
    creator is granted all [applicable
    privileges](/docs/self-managed/v25.2/manage/access-control/appendix-privileges/)
    for that object automatically).

<div class="annotation">

<div class="annotation-title">

Disambiguation

</div>

<div>

- Use `GRANT|REVOKE ...` to modify privileges on **existing** objects.

- Use `ALTER DEFAULT PRIVILEGES` to ensure that privileges are
  automatically granted or revoked when **new objects** of a certain
  type are created by others. Then, as needed, you can use
  `GRANT|REVOKE <privilege>` to adjust those privileges.

</div>

</div>

See also:

- For a list of required privileges for specific operations, see
  [Appendix: Privileges by
  command](/docs/self-managed/v25.2/manage/access-control/appendix-command-privileges/).

## Manage current privileges for a role

<div class="note">

**NOTE:**

- The examples below assume the existence of a `mydb` database and a
  `sales` schema within the `mydb` database.

- The examples below assume the roles only need privileges to objects in
  the `mydb.sales` schema.

</div>

### View privileges for a role

<div class="annotation">

<div class="annotation-title">

Privilege(s) required to run the command

</div>

<div>

No specific privilege is required to run the `SHOW PRIVILEGES`

</div>

</div>

To view privileges granted to a role, you can use the
[`SHOW PRIVILEGES`](/docs/self-managed/v25.2/sql/show-privileges)
command, substituting `<role>` with the role name (see
[`SHOW PRIVILEGES`](/docs/self-managed/v25.2/sql/show-default-privileges)
for the full syntax):

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR <role>;
```

</div>

<div class="note">

**NOTE:** All roles in Materialize are automatically members of
[`PUBLIC`](/docs/self-managed/v25.2/manage/access-control/appendix-built-in-roles/#public-role).
As such, every role includes inherited privileges from `PUBLIC`.

</div>

For example:

<div class="code-tabs">

<div class="tab-content">

<div id="tab-user" class="tab-pane" title="User">

To view privileges for a user, run
[`SHOW PRIVILEGES`](/docs/self-managed/v25.2/sql/show-privileges) on the
user’s role. For example, show the privileges for the
[`blue.berry@example.com`](mailto:blue.berry@example.com) role created
in the [Create a role
section](#create-individual-userservice-account-roles).

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR "blue.berry@example.com";
```

</div>

The results show that the role currently has only the privileges
inherited through the `PUBLIC` role.

```
| grantor   | grantee | database    | schema | name        | object_type | privilege_type |
| --------- | ------- | ----------- | ------ | ----------- | ----------- | -------------- |
| mz_system | PUBLIC  | materialize | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | mydb        | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | null        | null   | materialize | database    | USAGE          |
| mz_system | PUBLIC  | null        | null   | quickstart  | cluster     | USAGE          |
```

</div>

<div id="tab-service-account-role" class="tab-pane"
title="Service account role">

To view privileges for a service account, run
[`SHOW PRIVILEGES`](/docs/self-managed/v25.2/sql/show-privileges) on the
service account’s role. For example, show the privileges for the
`sales_report_app` role created in the [Create a role
section](#create-individual-userservice-account-roles).

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR sales_report_app;
```

</div>

The results show that the role currently has only the privileges
inherited through the `PUBLIC` role.

```
| grantor   | grantee | database    | schema | name        | object_type | privilege_type |
| --------- | ------- | ----------- | ------ | ----------- | ----------- | -------------- |
| mz_system | PUBLIC  | materialize | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | mydb        | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | null        | null   | materialize | database    | USAGE          |
| mz_system | PUBLIC  | null        | null   | quickstart  | cluster     | USAGE          |
```

</div>

<div id="tab-functional-roles" class="tab-pane"
title="Functional roles">

<div class="code-tabs">

<div class="tab-content">

<div id="tab-view-manager-role" class="tab-pane"
title="View manager role">

Show the privileges for the `view_manager` role created in the [Create a
role section](#create-a-role).

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR view_manager;
```

</div>

The results show that the role currently has only the privileges
inherited through the `PUBLIC` role.

```
| grantor   | grantee | database    | schema | name        | object_type | privilege_type |
| --------- | ------- | ----------- | ------ | ----------- | ----------- | -------------- |
| mz_system | PUBLIC  | materialize | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | mydb        | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | null        | null   | materialize | database    | USAGE          |
| mz_system | PUBLIC  | null        | null   | quickstart  | cluster     | USAGE          |
```

</div>

<div id="tab-serving-index-manager-role" class="tab-pane"
title="Serving index manager role">

Show the privileges for the `serving_index_manager` role created in the
[Create a role section](#create-a-role).

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR serving_index_manager;
```

</div>

The results show that the role currently has only the privileges
inherited through the `PUBLIC` role.

```
| grantor   | grantee | database    | schema | name        | object_type | privilege_type |
| --------- | ------- | ----------- | ------ | ----------- | ----------- | -------------- |
| mz_system | PUBLIC  | materialize | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | mydb        | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | null        | null   | materialize | database    | USAGE          |
| mz_system | PUBLIC  | null        | null   | quickstart  | cluster     | USAGE          |
```

</div>

<div id="tab-data-reader-role" class="tab-pane"
title="Data reader role">

Show the privileges for the `data_reader` role created in the [Create a
role section](#create-a-role).

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR data_reader;
```

</div>

The results show that the role currently has only the privileges
inherited through the `PUBLIC` role.

```
| grantor   | grantee | database    | schema | name        | object_type | privilege_type |
| --------- | ------- | ----------- | ------ | ----------- | ----------- | -------------- |
| mz_system | PUBLIC  | materialize | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | mydb        | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | null        | null   | materialize | database    | USAGE          |
| mz_system | PUBLIC  | null        | null   | quickstart  | cluster     | USAGE          |
```

</div>

</div>

</div>

</div>

</div>

</div>

<div class="tip">

**💡 Tip:** For the `SHOW PRIVILEGES` command, you can add a `WHERE`
clause to filter by the return fields; e.g.,
`SHOW PRIVILEGES FOR view_manager WHERE name='quickstart';`.

</div>

### Grant privileges to a role

To grant
[privileges](/docs/self-managed/v25.2/manage/access-control/appendix-command-privileges/)
to a role, use the
[`GRANT PRIVILEGE`](/docs/self-managed/v25.2/sql/grant-privilege/)
statement (see
[`GRANT PRIVILEGE`](/docs/self-managed/v25.2/sql/grant-privilege/) for
the full syntax)

<div class="annotation">

<div class="annotation-title">

Privilege(s) required to run the command

</div>

<div>

- Ownership of affected objects.
- `USAGE` privileges on the containing database if the affected object
  is a schema.
- `USAGE` privileges on the containing schema if the affected object is
  namespaced by a schema.
- *superuser* status if the privilege is a system privilege.

To override the **object ownership** requirements to grant privileges,
run as a user with superuser privileges; e.g. `mz_system` user.

</div>

</div>

<div class="highlight">

``` chroma
GRANT <PRIVILEGE> ON <OBJECT_TYPE> <object_name> TO <role>;
```

</div>

When possible, avoid granting privileges directly to individual user or
service account roles. Instead, create reusable, functional roles (e.g.,
`data_reader`, `view_manager`) with well-defined privileges, and grant
these roles to the individual user or service account roles. You can
also grant functional roles to other functional roles to compose more
complex functional roles.

For example, the following grants privileges to the functional roles.

<div class="note">

**NOTE:**

Various SQL operations require additional privileges on related objects,
such as:

- For objects that use compute resources (e.g., indexes, materialized
  views, replicas, sources, sinks), access is also required for the
  associated cluster.

- For objects in a schema, access is also required for the schema.

For details on SQL operations and needed privileges, see [Appendix:
Privileges by
command](/docs/self-managed/v25.2/manage/access-control/appendix-command-privileges/).

</div>

<div class="code-tabs">

<div class="tab-content">

<div id="tab-view-manager-role" class="tab-pane"
title="View manager role">

The following example grants the `view_manager` role various privileges
to run:

- [`SELECT`](/docs/self-managed/v25.2/sql/select/#privileges) from
  currently existing materialized views/views/tables/sources in the
  `mydb.sales` schema.
- [`CREATE MATERIALIZED VIEW`](/docs/self-managed/v25.2/sql/create-materialized-view/#privileges)
  in the `mydb.sales` schema on the `compute_cluster`.
- [`CREATE VIEW`](/docs/self-managed/v25.2/sql/create-view/#privileges)
  if using intermediate views as part of a stacked view definition
  (i.e., views whose definition depends on other views).

<div class="note">

**NOTE:**

If a query directly references a view or materialized view:

- `SELECT` privileges are required only on the directly referenced
  view/materialized view. `SELECT` privileges are **not** required for
  the underlying relations referenced in the view/materialized view
  definition unless those relations themselves are directly referenced
  in the query.

- However, the owner of the view/materialized view (including those with
  **superuser** privileges) must have all required `SELECT` and `USAGE`
  privileges to run the view definition regardless of who is selecting
  from the view/materialized view.

</div>

<div class="highlight">

``` chroma
-- To SELECT from currently **existing** relations in `mydb.sales` schema:
-- Need USAGE on schema and cluster
-- Need SELECT on existing materialized views/views/tables/sources
GRANT USAGE ON SCHEMA mydb.sales TO view_manager;
GRANT USAGE ON CLUSTER compute_cluster TO view_manager;
GRANT SELECT ON ALL TABLES IN SCHEMA mydb.sales TO view_manager;
-- ALL TABLES encompasses tables, views, materialized views, and sources,
-- and refers only to currently existing objects.

-- To CREATE materialized views/views:
-- Need CREATE on cluster for materialized views
-- Need CREATE on schema for the materialized views/views
GRANT CREATE ON CLUSTER compute_cluster TO view_manager;
GRANT CREATE ON SCHEMA mydb.sales TO view_manager;
```

</div>

Review the privileges granted to the `view_manager` role:

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR view_manager;
```

</div>

The results should reflect the new privileges granted to the
`view_manager` role in addition to those privileges inherited through
the `PUBLIC` role.

```
| grantor   | grantee      | database    | schema | name            | object_type | privilege_type |
| --------- | ------------ | ----------- | ------ | --------------- | ----------- | -------------- |
| mz_system | PUBLIC       | materialize | null   | public          | schema      | USAGE          |
| mz_system | PUBLIC       | mydb        | null   | public          | schema      | USAGE          |
| mz_system | PUBLIC       | null        | null   | materialize     | database    | USAGE          |
| mz_system | PUBLIC       | null        | null   | quickstart      | cluster     | USAGE          |
| mz_system | view_manager | mydb        | sales  | items           | table       | SELECT         |
| mz_system | view_manager | mydb        | sales  | orders          | table       | SELECT         |
| mz_system | view_manager | mydb        | sales  | sales_items     | table       | SELECT         |
| mz_system | view_manager | mydb        | null   | sales           | schema      | CREATE         |
| mz_system | view_manager | mydb        | null   | sales           | schema      | USAGE          |
| mz_system | view_manager | null        | null   | compute_cluster | cluster     | CREATE         |
| mz_system | view_manager | null        | null   | compute_cluster | cluster     | USAGE          |
```

<div class="important">

**! Important:**

The `GRANT SELECT ON ALL TABLES IN SCHEMA mydb.sales TO view_manager;`
statement results in `view_manager` having `SELECT` privileges on
specific objects, namely the three tables that existed in the
`mydb.sales` schema at the time of the grant. It **does not** grant
`SELECT` privileges on any tables, views, materialized views, or sources
created by others in the schema afterwards.

For new objects created by others, you can either:

- Manually grant privileges on new objects; or
- Use [default
  privileges](/docs/self-managed/v25.2/manage/access-control/manage-roles/#manage-future-privileges-for-a-role)
  to automatically grant privileges on new objects.

</div>

</div>

<div id="tab-serving-index-manager-role" class="tab-pane"
title="Serving index manager role">

The following example grants the `serving_index_manager` role various
privileges to:

- [`CREATE INDEX`](/docs/self-managed/v25.2/sql/create-index/#privileges)
  in the `mydb.sales` schema on the `serving_cluster`.

- Use the `serving_cluster` (i.e., `USAGE`). Although you can create an
  index without the `USAGE`, this allows the person creating the index
  to use the index to verify.

<div class="note">

**NOTE:** In addition to database privileges, to create an index, a role
must be the owner of the object on which the index is created.

</div>

<div class="highlight">

``` chroma
-- To create an index on an object **owned** by the role:
-- Need CREATE on the cluster.
-- Need CREATE on the schema.
GRANT CREATE ON CLUSTER serving_cluster TO serving_index_manager;
GRANT CREATE ON SCHEMA mydb.sales TO serving_index_manager;

-- Optional.
GRANT USAGE ON CLUSTER serving_cluster TO serving_index_manager;
```

</div>

Review the privileges granted to the `serving_index_manager` role:

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR serving_index_manager;
```

</div>

The results should reflect the new privileges granted to the
`index_manager` role in addition to those privileges inherited through
the `PUBLIC` role.

```
| grantor   | grantee               | database    | schema | name            | object_type | privilege_type |
| --------- | --------------------- | ----------- | ------ | --------------- | ----------- | -------------- |
| mz_system | PUBLIC                | materialize | null   | public          | schema      | USAGE          |
| mz_system | PUBLIC                | mydb        | null   | public          | schema      | USAGE          |
| mz_system | PUBLIC                | null        | null   | materialize     | database    | USAGE          |
| mz_system | PUBLIC                | null        | null   | quickstart      | cluster     | USAGE          |
| mz_system | serving_index_manager | mydb        | null   | sales           | schema      | CREATE         |
| mz_system | serving_index_manager | null        | null   | serving_cluster | cluster     | CREATE         |
| mz_system | serving_index_manager | null        | null   | serving_cluster | cluster     | USAGE          |
```

<div class="note">

**NOTE:**

In addition to database privileges, a role must be the owner of the
object on which the index is created. In our examples, `view_manager`
role has privileges to create the various materialized views that will
be indexed:

- See [Grant a role to another
  role](/docs/self-managed/v25.2/manage/access-control/manage-roles/#grant-a-role-to-another-role)
  for details and example of granting `serving_cluster` role to
  `view_manager`.

- See [Change ownership of
  objects](/docs/self-managed/v25.2/manage/access-control/manage-roles/#change-ownership-of-objects)
  for details and example of changing ownership of objects.

</div>

</div>

<div id="tab-data-reader-role" class="tab-pane"
title="Data reader role">

The following example grants the `data_reader` role privileges to run:

- [`SELECT`](/docs/self-managed/v25.2/sql/select/#privileges) from all
  existing tables/materialized views/views/sources in the `mydb.sales`
  schema on the `serving_cluster`.

<div class="note">

**NOTE:**

If a query directly references a view or materialized view:

- `SELECT` privileges are required only on the directly referenced
  view/materialized view. `SELECT` privileges are **not** required for
  the underlying relations referenced in the view/materialized view
  definition unless those relations themselves are directly referenced
  in the query.

- However, the owner of the view/materialized view (including those with
  **superuser** privileges) must have all required `SELECT` and `USAGE`
  privileges to run the view definition regardless of who is selecting
  from the view/materialized view.

</div>

<div class="highlight">

``` chroma
-- To select from **existing** views/materialized views/tables/sources:
-- Need USAGE on schema and cluster
-- Need SELECT on the materialized views/views/tables/sources
GRANT USAGE ON SCHEMA mydb.sales TO data_reader;
GRANT USAGE ON CLUSTER serving_cluster TO data_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA mydb.sales TO data_reader;
-- For PostgreSQL compatibility, ALL TABLES encompasses tables, views,
-- materialized views, and sources.
```

</div>

Review the privileges granted to the `data_reader` role:

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR data_reader;
```

</div>

The results should reflect the new privileges granted to the
`data_reader` role in addition to those privileges inherited through the
`PUBLIC` role.

```
| grantor   | grantee     | database    | schema | name            | object_type | privilege_type |
| --------- | ----------- | ----------- | ------ | --------------- | ----------- | -------------- |
| mz_system | PUBLIC      | materialize | null   | public          | schema      | USAGE          |
| mz_system | PUBLIC      | mydb        | null   | public          | schema      | USAGE          |
| mz_system | PUBLIC      | null        | null   | materialize     | database    | USAGE          |
| mz_system | PUBLIC      | null        | null   | quickstart      | cluster     | USAGE          |
| mz_system | data_reader | mydb        | sales  | items           | table       | SELECT         |
| mz_system | data_reader | mydb        | sales  | orders          | table       | SELECT         |
| mz_system | data_reader | mydb        | sales  | sales_items     | table       | SELECT         |
| mz_system | data_reader | mydb        | null   | sales           | schema      | USAGE          |
| mz_system | data_reader | null        | null   | serving_cluster | cluster     | USAGE          |
```

<div class="important">

**! Important:**

The `GRANT SELECT ON ALL TABLES IN SCHEMA mydb.sales TO data_reader;`
statement results in `data_reader` having `SELECT` privileges on
specific objects, namely the three tables that existed in the
`mydb.sales` schema at the time of the grant. It **does not** grant
`SELECT` privileges on any tables, views, materialized views, or sources
created in the schema afterwards by others.

For new objects created by others, you can either:

- Manually grant privileges on new objects; or
- Use [default
  privileges](/docs/self-managed/v25.2/manage/access-control/manage-roles/#manage-future-privileges-for-a-role)
  to automatically grant privileges on new objects.

</div>

</div>

</div>

</div>

### Grant a role to another role

Once a role is created, you can modify its privileges either:

- Directly by [granting privileges for a
  role](#grant-privileges-to-a-role) or [revoking privileges from a
  role](#revoke-privileges-from-a-role).
- Indirectly (through inheritance) by granting other roles to the role
  or [revoking roles from the role](#revoke-a-role-from-another-role).

<div class="tip">

**💡 Tip:** When possible, avoid granting privileges directly to
individual user or service account roles. Instead, create reusable,
functional roles (e.g., `data_reader`, `view_manager`) with well-defined
privileges, and grant these roles to the individual user or service
account roles. You can also grant functional roles to other functional
roles to compose more complex functional roles.

</div>

To grant a role to another role (where the role can be a user
role/service account role/functional role), use the
[`GRANT ROLE`](/docs/self-managed/v25.2/sql/grant-role/) statement (see
[`GRANT ROLE`](/docs/self-managed/v25.2/sql/grant-role/) for full
syntax):

<div class="annotation">

<div class="annotation-title">

Privilege(s) required to run the command

</div>

<div>

- `CREATEROLE` privileges on the system.

`mz_system` user has the required privileges on the system.

</div>

</div>

<div class="highlight">

``` chroma
GRANT <role> [, <role>...] to <target_role> [, <target_role> ...];
```

</div>

When a role is granted to another role, the target role becomes a member
of the other role and inherits the privileges through the other role.

In the following examples,

- The functional role `view_manager` is granted to the user role
  `blue.berry@example.com`.
- The functional role `serving_index_manager` is granted to the
  functional role `view_manager`.
- The functional role `data_reader` is granted to the service account
  role `sales_report_app`.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-grant-view_manager-role" class="tab-pane"
title="Grant view_manager role">

The following grants the `view_manager` role to the role associated with
the user [`blue.berry@example.com`](mailto:blue.berry@example.com).

<div class="highlight">

``` chroma
GRANT view_manager TO "blue.berry@example.com";
```

</div>

Review the privileges granted to the
[`blue.berry@example.com`](mailto:blue.berry@example.com) role:

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR "blue.berry@example.com";
```

</div>

The results should include the privileges inherited through the
`view_manager` role in addition to those privileges through the `PUBLIC`
role. If the role had been granted direct privileges, those would also
be included.

```
| grantor   | grantee      | database    | schema | name            | object_type | privilege_type |
| --------- | ------------ | ----------- | ------ | --------------- | ----------- | -------------- |
| mz_system | PUBLIC       | materialize | null   | public          | schema      | USAGE          |
| mz_system | PUBLIC       | mydb        | null   | public          | schema      | USAGE          |
| mz_system | PUBLIC       | null        | null   | materialize     | database    | USAGE          |
| mz_system | PUBLIC       | null        | null   | quickstart      | cluster     | USAGE          |
| mz_system | view_manager | mydb        | sales  | items           | table       | SELECT         |
| mz_system | view_manager | mydb        | sales  | orders          | table       | SELECT         |
| mz_system | view_manager | mydb        | sales  | sales_items     | table       | SELECT         |
| mz_system | view_manager | mydb        | null   | sales           | schema      | CREATE         |
| mz_system | view_manager | mydb        | null   | sales           | schema      | USAGE          |
| mz_system | view_manager | null        | null   | compute_cluster | cluster     | CREATE         |
| mz_system | view_manager | null        | null   | compute_cluster | cluster     | USAGE          |
```

After the `view_manager` role is granted to `"blue.berry@example.com"`,
`"blue.berry@example.com"` can create objects in the `mydb.sales` schema
on the `compute_cluster`.

<div class="highlight">

``` chroma
SET CLUSTER TO compute_cluster;
SET DATABASE TO mydb;
SET SCHEMA TO sales;

-- Create an intermediate view for a stacked materialized view
CREATE VIEW orders_view AS
SELECT o.*,i.price,o.quantity * i.price as subtotal
FROM orders as o
JOIN items as i
ON o.item = i.item;

-- Create a materialized view
CREATE MATERIALIZED VIEW orders_daily_totals AS
SELECT date_trunc('day',order_date) AS order_date,
      sum(subtotal) AS daily_total
FROM orders_view
GROUP BY date_trunc('day',order_date);

-- Select from the materialized view
SELECT * from orders_daily_totals;
```

</div>

In Materialize, a role automatically gets all [applicable
privileges](/docs/self-managed/v25.2/manage/access-control/appendix-privileges/)
for an object they create; for example, the creator of a schema gets
`CREATE` and `USAGE`; the creator of a table gets `SELECT`, `INSERT`,
`UPDATE`, and `DELETE`.

For example, if you show privileges for `"blue.berry@example.com"` after
creating the view and materialized view, you will see that the role has
`SELECT` privileges on the `orders_daily_totals` and `orders_view`.

```
| grantor                | grantee                | database    | schema | name                | object_type       | privilege_type |
| ---------------------- | ---------------------- | ----------- | ------ | ------------------- | ----------------- | -------------- |
| blue.berry@example.com | blue.berry@example.com | mydb        | sales  | orders_daily_totals | materialized-view | SELECT         |
| blue.berry@example.com | blue.berry@example.com | mydb        | sales  | orders_view         | view              | SELECT         |
| mz_system              | PUBLIC                 | materialize | null   | public              | schema            | USAGE          |
| mz_system              | PUBLIC                 | mydb        | null   | public              | schema            | USAGE          |
... -- Rest omitted for brevity
```

<div class="note">

**NOTE:**

If a query directly references a view or materialized view:

- `SELECT` privileges are required only on the directly referenced
  view/materialized view. `SELECT` privileges are **not** required for
  the underlying relations referenced in the view/materialized view
  definition unless those relations themselves are directly referenced
  in the query.

- However, the owner of the view/materialized view (including those with
  **superuser** privileges) must have all required `SELECT` and `USAGE`
  privileges to run the view definition regardless of who is selecting
  from the view/materialized view.

</div>

However, with the current privileges, `"blue.berry@example.com"` cannot
select from new views/materialized views created by **others** in the
schema and vice versa. For privileges on new objects, you can either:

- Manually grant privileges on new objects; or
- Use [default
  privileges](/docs/self-managed/v25.2/manage/access-control/manage-roles/#manage-future-privileges-for-a-role)
  to automatically grant privileges on new objects.

</div>

<div id="tab-grant-serving_index_manager-role" class="tab-pane"
title="Grant serving_index_manager role">

The following grants the `serving_index_manager` role to the functional
role `view_manager`, which already has privileges to create materialized
views in `mydb.sales` schema. This allows members of the `view_manager`
role to create indexes on their objects on the `serving_cluster`.

<div class="highlight">

``` chroma
GRANT serving_index_manager TO view_manager;
```

</div>

Review the privileges of `view_manager` as well as
`"blue.berry@example.com"` (a member of `view_manager`) after the grant.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-privileges-for-view_manager" class="tab-pane"
title="Privileges for view_manager">

Review the privileges granted to the `view_manager` role:

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR view_manager;
```

</div>

The results include the privileges inherited through the
`serving_index_manager` role in addition to those privileges inherited
through the `PUBLIC` role as well as those granted directly to the role,
if any.

```
| grantor   | grantee               | database    | schema | name            | object_type | privilege_type |
| --------- | --------------------- | ----------- | ------ | --------------- | ----------- | -------------- |
| mz_system | PUBLIC                | materialize | null   | public          | schema      | USAGE          |
| mz_system | PUBLIC                | mydb        | null   | public          | schema      | USAGE          |
| mz_system | PUBLIC                | null        | null   | materialize     | database    | USAGE          |
| mz_system | PUBLIC                | null        | null   | quickstart      | cluster     | USAGE          |
| mz_system | serving_index_manager | mydb        | null   | sales           | schema      | CREATE         |
| mz_system | serving_index_manager | null        | null   | serving_cluster | cluster     | CREATE         |
| mz_system | serving_index_manager | null        | null   | serving_cluster | cluster     | USAGE          |
| mz_system | view_manager          | mydb        | sales  | items           | table       | SELECT         |
| mz_system | view_manager          | mydb        | sales  | orders          | table       | SELECT         |
| mz_system | view_manager          | mydb        | sales  | sales_items     | table       | SELECT         |
| mz_system | view_manager          | mydb        | null   | sales           | schema      | CREATE         |
| mz_system | view_manager          | mydb        | null   | sales           | schema      | USAGE          |
| mz_system | view_manager          | null        | null   | compute_cluster | cluster     | CREATE         |
| mz_system | view_manager          | null        | null   | compute_cluster | cluster     | USAGE          |
```

</div>

<div id="tab-privileges-for-blueberryexamplecom" class="tab-pane"
title="Privileges for blue.berry@example.com">

Review the privileges for `"blue.berry@example.com"` (a member of
`view_manager`):

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR "blue.berry@example.com";
```

</div>

The results include the privileges inherited through the
`serving_index_manager` role in addition to those privileges inherited
through the `PUBLIC` role as well as those granted directly to the role,
if any. For example, after being granted the `view_manager` role,
`"blue.berry@example.com"` created the `orders_daily_totals` and
`orders_view`. As the creator, `"blue.berry@example.com"` automatically
gets all applicable privileges on the objects they create.

```
| grantor                | grantee                | database    | schema | name                | object_type       | privilege_type |
| ---------------------- | ---------------------- | ----------- | ------ | ------------------- | ----------------- | -------------- |
| blue.berry@example.com | blue.berry@example.com | mydb        | sales  | orders_daily_totals | materialized-view | SELECT         |
| blue.berry@example.com | blue.berry@example.com | mydb        | sales  | orders_view         | view              | SELECT         |
| mz_system              | PUBLIC                 | materialize | null   | public              | schema            | USAGE          |
| mz_system              | PUBLIC                 | mydb        | null   | public              | schema            | USAGE          |
| mz_system              | PUBLIC                 | null        | null   | materialize         | database          | USAGE          |
| mz_system              | PUBLIC                 | null        | null   | quickstart          | cluster           | USAGE          |
| mz_system              | serving_index_manager  | mydb        | null   | sales               | schema            | CREATE         |
| mz_system              | serving_index_manager  | null        | null   | serving_cluster     | cluster           | CREATE         |
| mz_system              | serving_index_manager  | null        | null   | serving_cluster     | cluster           | USAGE          |
| mz_system              | view_manager           | mydb        | sales  | items               | table             | SELECT         |
| mz_system              | view_manager           | mydb        | sales  | orders              | table             | SELECT         |
| mz_system              | view_manager           | mydb        | sales  | sales_items         | table             | SELECT         |
| mz_system              | view_manager           | mydb        | null   | sales               | schema            | CREATE         |
| mz_system              | view_manager           | mydb        | null   | sales               | schema            | USAGE          |
| mz_system              | view_manager           | null        | null   | compute_cluster     | cluster           | CREATE         |
| mz_system              | view_manager           | null        | null   | compute_cluster     | cluster           | USAGE          |
```

</div>

</div>

</div>

To create indexes on an object, in addition to specific `CREATE`
privileges (granted by the `serving_index_manager` role), the user needs
to be the owner of the object.

After the `serving_index_manager` role is granted to the `view_manager`
role, members of `view_manager` can create indexes on the
`serving_cluster` for objects that they own. For example,
`"blue.berry@example.com"` can create an index on the
`orders_daily_totals` materialized view.

<div class="highlight">

``` chroma
-- run as "blue.berry@example.com"
SET CLUSTER TO serving_cluster;
SET DATABASE TO mydb;
SET SCHEMA TO sales;

CREATE INDEX ON orders_daily_totals (order_date);

-- If the role has `USAGE` on the `serving_cluster`:
SELECT * from orders_daily_totals;
```

</div>

To allow others in the `view_manager` role to create indexes, see
[Change ownership of
objects](/docs/self-managed/v25.2/manage/access-control/manage-roles/#change-ownership-of-objects).

</div>

<div id="tab-grant-data_reader-role" class="tab-pane"
title="Grant data_reader role">

The following grants the `data_reader` role to the service account role
`sales_report_app`.

<div class="highlight">

``` chroma
GRANT data_reader TO sales_report_app;
```

</div>

Review the privileges for `sales_report_app` after the grant:

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR sales_report_app;
```

</div>

The results should include the privileges inherited through the
`data_reader` role in addition to those privileges inherited through the
`PUBLIC` role. If the role had been granted direct privileges, those
would also be included.

```
| grantor   | grantee     | database    | schema | name            | object_type | privilege_type |
| --------- | ----------- | ----------- | ------ | --------------- | ----------- | -------------- |
| mz_system | PUBLIC      | materialize | null   | public          | schema      | USAGE          |
| mz_system | PUBLIC      | mydb        | null   | public          | schema      | USAGE          |
| mz_system | PUBLIC      | null        | null   | materialize     | database    | USAGE          |
| mz_system | PUBLIC      | null        | null   | quickstart      | cluster     | USAGE          |
| mz_system | data_reader | mydb        | sales  | items           | table       | SELECT         |
| mz_system | data_reader | mydb        | sales  | orders          | table       | SELECT         |
| mz_system | data_reader | mydb        | sales  | sales_items     | table       | SELECT         |
| mz_system | data_reader | mydb        | null   | sales           | schema      | USAGE          |
| mz_system | data_reader | null        | null   | serving_cluster | cluster     | USAGE          |
```

As the privileges show, after the `data_reader` role is granted to the
`sales_report_app` service account role, `sales_report_app` can read
from the three tables in the `mydb.sales` schema on the
`serving_cluster`.

<div class="highlight">

``` chroma
SET CLUSTER TO serving_cluster;
SET DATABASE TO mydb;
SET SCHEMA TO sales;

SELECT * FROM sales_items;
```

</div>

However, `sales_report_app` cannot read from the new objects in
`mydb.sales`; e.g., `orders_daily_totals` materialized view and its
underlying view `orders_view` that were created after the `SELECT`
privileges were granted to the `data_reader` role.

To allow `sales_report_app` or `data_reader` to read from the new
objects in `mydb.sales`, you can either:

- Manually grant `SELECT` privileges on the new objects; or
- Use [default
  privileges](/docs/self-managed/v25.2/manage/access-control/manage-roles/#manage-future-privileges-for-a-role)
  to automatically grant `SELECT` privileges on new objects.

</div>

</div>

</div>

### Revoke privileges from a role

To remove privileges from a role, use the
[`REVOKE <privilege>`](/docs/self-managed/v25.2/sql/revoke-privilege/)
statement:

<div class="annotation">

<div class="annotation-title">

Privilege(s) required to run the command

</div>

<div>

- Ownership of affected objects.
- `USAGE` privileges on the containing database if the affected object
  is a schema.
- `USAGE` privileges on the containing schema if the affected object is
  namespaced by a schema.
- *superuser* status if the privilege is a system privilege.

</div>

</div>

<div class="highlight">

``` chroma
REVOKE <PRIVILEGE> ON <OBJECT_TYPE> <object_name> FROM <role>;
```

</div>

### Revoke a role from another role

To revoke a role from another role, use the
[`REVOKE <role>`](/docs/self-managed/v25.2/sql/revoke-role/) statement:

<div class="annotation">

<div class="annotation-title">

Privilege(s) required to run the command

</div>

<div>

- `CREATEROLE` privileges on the systems.

</div>

</div>

<div class="highlight">

``` chroma
REVOKE <role> FROM <target_role>;
```

</div>

For example:

<div class="highlight">

``` chroma
REVOKE data_reader FROM sales_report_app;
```

</div>

<div class="important">

**! Important:** When you revoke a role from another role (user
role/service account role/independent role), the target role is no
longer a member of the revoked role nor inherits the revoked role’s
privileges. **However**, privileges are cumulative: if the target role
inherits the same privilege(s) from another role, the target role still
has the privilege(s) through the other role.

</div>

## Manage future privileges for a role

In Materialize, a role automatically gets all [applicable
privileges](/docs/self-managed/v25.2/manage/access-control/appendix-privileges/)
for an object they create/own; for example, the creator of a schema gets
`CREATE` and `USAGE`; the creator of a table gets `SELECT`, `INSERT`,
`UPDATE`, and `DELETE`. However, for others to access the new object,
you can either manually grant privileges on new objects or use default
privileges to automatically grant privileges to others as new objects
are created.

Default privileges can be specified for a given object type and scoped
to:

- all future objects of that type;
- all future objects of that type within specific databases or schemas;
- all future objects of that type created by specific roles (or by all
  roles `PUBLIC`).

Default privileges apply only to objects created after these privileges
are defined. They do not affect objects that were created before the
default privileges were set.

<div class="annotation">

<div class="annotation-title">

Disambiguation

</div>

<div>

- Use `GRANT|REVOKE ...` to modify privileges on **existing** objects.

- Use `ALTER DEFAULT PRIVILEGES` to ensure that privileges are
  automatically granted or revoked when **new objects** of a certain
  type are created by others. Then, as needed, you can use
  `GRANT|REVOKE <privilege>` to adjust those privileges.

</div>

</div>

### View default privileges

To view default privileges, you can use the
[`SHOW DEFAULT PRIVILEGES`](/docs/self-managed/v25.2/sql/show-default-privileges)
command, substituting `<role>` with the role name (see
[`SHOW DEFAULT PRIVILEGES`](/docs/self-managed/v25.2/sql/show-default-privileges)
for the full syntax):

<div class="annotation">

<div class="annotation-title">

Privilege(s) required to run the command

</div>

<div>

No specific privilege is required to run the `SHOW DEFAULT PRIVILEGES`.

</div>

</div>

<div class="highlight">

``` chroma
SHOW DEFAULT PRIVILEGES FOR <role>;
```

</div>

For example:

<div class="code-tabs">

<div class="tab-content">

<div id="tab-user" class="tab-pane" title="User">

To view default privileges for a user, run
[`SHOW DEFAULT PRIVILEGES`](/docs/self-managed/v25.2/sql/show-default-privileges)
on the user’s role. For example, show the defaultprivileges for the
[`blue.berry@example.com`](mailto:blue.berry@example.com) role created
in the [Create a role
section](#create-individual-userservice-account-roles).

<div class="highlight">

``` chroma
SHOW DEFAULT PRIVILEGES FOR "blue.berry@example.com";
```

</div>

The example results show that the default privileges for
`"blue.berry@example.com"` are the default privileges it has as a member
of the `PUBLIC` role.

```
| object_owner | database | schema | object_type | grantee | privilege_type |
| ------------ | -------- | ------ | ----------- | ------- | -------------- |
| PUBLIC       | null     | null   | type        | PUBLIC  | USAGE          |
```

The example results show one default privilege. This default privilege
grants `USAGE` privilege to all users (`grantee` `PUBLIC`) for **new**
[types](/docs/self-managed/v25.2/sql/types/#custom-types) created by any
user (`object_owner` `PUBLIC`).

<div class="note">

**NOTE:**

- To use a [type](/docs/self-managed/v25.2/sql/types/#custom-types)
  created in a schema, the `USAGE` access is required on the containing
  schema as well.
- Default privileges apply only to objects created after these
  privileges are defined. They do not affect objects that were created
  before the default privileges were set.

</div>

</div>

<div id="tab-service-account-role" class="tab-pane"
title="Service account role">

To view default privileges for a service account, run
[`SHOW DEFAULT PRIVILEGES`](/docs/self-managed/v25.2/sql/show-default-privileges)
on the service account’s role. For example, show the default privileges
for the `sales_report_app` role created in the [Create a role
section](#create-individual-userservice-account-roles).

<div class="highlight">

``` chroma
SHOW DEFAULT PRIVILEGES FOR sales_report_app;
```

</div>

The example results show that the default privileges for
`sales_report_app` are the default privileges it has as a member of the
`PUBLIC` role.

```
| object_owner | database | schema | object_type | grantee | privilege_type |
| ------------ | -------- | ------ | ----------- | ------- | -------------- |
| PUBLIC       | null     | null   | type        | PUBLIC  | USAGE          |
```

The example results show one default privilege. This default privilege
grants `USAGE` privilege to all users (`grantee` `PUBLIC`) for **new**
[types](/docs/self-managed/v25.2/sql/types/#custom-types) created by any
user (`object_owner` `PUBLIC`).

<div class="note">

**NOTE:**

- To use a [type](/docs/self-managed/v25.2/sql/types/#custom-types)
  created in a schema, the `USAGE` access is required on the containing
  schema as well.
- Default privileges apply only to objects created after these
  privileges are defined. They do not affect objects that were created
  before the default privileges were set.

</div>

</div>

<div id="tab-functional-roles" class="tab-pane"
title="Functional roles">

<div class="code-tabs">

<div class="tab-content">

<div id="tab-view-manager-role" class="tab-pane"
title="View manager role">

Show the default privileges for the `view_manager` role created in the
[Create a role section](#create-a-role).

<div class="highlight">

``` chroma
SHOW DEFAULT PRIVILEGES FOR view_manager;
```

</div>

The example results show that the default privileges for `view_manager`
are the default privileges it has as a member of the `PUBLIC` role.

```
| object_owner | database | schema | object_type | grantee | privilege_type |
| ------------ | -------- | ------ | ----------- | ------- | -------------- |
| PUBLIC       | null     | null   | type        | PUBLIC  | USAGE          |
```

The example results show one default privilege. This default privilege
grants `USAGE` privilege to all users (`grantee` `PUBLIC`) for **new**
[types](/docs/self-managed/v25.2/sql/types/#custom-types) created by any
user (`object_owner` `PUBLIC`).

<div class="note">

**NOTE:**

- To use a [type](/docs/self-managed/v25.2/sql/types/#custom-types)
  created in a schema, the `USAGE` access is required on the containing
  schema as well.
- Default privileges apply only to objects created after these
  privileges are defined. They do not affect objects that were created
  before the default privileges were set.

</div>

</div>

<div id="tab-serving-index-manager-role" class="tab-pane"
title="Serving index manager role">

Show the default privileges for the `serving_index_manager` role created
in the [Create a role section](#create-a-role).

<div class="highlight">

``` chroma
SHOW DEFAULT PRIVILEGES FOR serving_index_manager;
```

</div>

The example results show that the default privileges for
`serving_index_manager` are the default privileges it has as a member of
the `PUBLIC` role.

```
| object_owner | database | schema | object_type | grantee | privilege_type |
| ------------ | -------- | ------ | ----------- | ------- | -------------- |
| PUBLIC       | null     | null   | type        | PUBLIC  | USAGE          |
```

The example results show one default privilege. This default privilege
grants `USAGE` privilege to all users (`grantee` `PUBLIC`) for **new**
[types](/docs/self-managed/v25.2/sql/types/#custom-types) created by any
user (`object_owner` `PUBLIC`).

<div class="note">

**NOTE:**

- To use a [type](/docs/self-managed/v25.2/sql/types/#custom-types)
  created in a schema, the `USAGE` access is required on the containing
  schema as well.
- Default privileges apply only to objects created after these
  privileges are defined. They do not affect objects that were created
  before the default privileges were set.

</div>

</div>

<div id="tab-data-reader-role" class="tab-pane"
title="Data reader role">

Show the default privileges for the `data_reader` role created in the
[Create a role section](#create-a-role).

<div class="highlight">

``` chroma
SHOW DEFAULT PRIVILEGES FOR data_reader;
```

</div>

The example results show that the default privileges for `data_reader`
are the default privileges it has as a member of the `PUBLIC` role.

```
| object_owner | database | schema | object_type | grantee | privilege_type |
| ------------ | -------- | ------ | ----------- | ------- | -------------- |
| PUBLIC       | null     | null   | type        | PUBLIC  | USAGE          |
```

The example results show one default privilege. This default privilege
grants `USAGE` privilege to all users (`grantee` `PUBLIC`) for **new**
[types](/docs/self-managed/v25.2/sql/types/#custom-types) created by any
user (`object_owner` `PUBLIC`).

<div class="note">

**NOTE:**

- To use a [type](/docs/self-managed/v25.2/sql/types/#custom-types)
  created in a schema, the `USAGE` access is required on the containing
  schema as well.
- Default privileges apply only to objects created after these
  privileges are defined. They do not affect objects that were created
  before the default privileges were set.

</div>

</div>

</div>

</div>

</div>

</div>

</div>

### Alter default privileges

To define default privilege for objects created by a role, use the
[`ALTER DEFAULT PRIVILEGES`](/docs/self-managed/v25.2/sql/alter-default-privileges)
command (see
[`ALTER DEFAULT PRIVILEGES`](/docs/self-managed/v25.2/sql/alter-default-privileges)
for the full syntax):

<div class="annotation">

<div class="annotation-title">

Privilege(s) required to run the command

</div>

<div>

- Role membership in `role_name`.
- `USAGE` privileges on the containing database if `database_name` is
  specified.
- `USAGE` privileges on the containing schema if `schema_name` is
  specified.
- *superuser* status if the *target_role* is `PUBLIC` or **ALL ROLES**
  is specified.

</div>

</div>

<div class="highlight">

``` chroma
ALTER DEFAULT PRIVILEGES FOR ROLE <object_creator>
   IN SCHEMA <schema>    -- Optional. If specified, need USAGE on database and schema.
   GRANT <privilege> ON <object_type> TO <target_role>;
```

</div>

<div class="note">

**NOTE:**

- With the exception of the `PUBLIC` role, the `<object_creator>` role
  is **not** transitive. That is, default privileges that specify a
  functional role like `view_manager` as the `<object_creator>` do
  **not** apply to objects created by its members.

  However, you can approximate default privileges for a functional role
  by restricting `CREATE` privileges for the objects to the desired
  functional roles (e.g., only `view_managers` have privileges to create
  tables in `mydb.sales` schema) and then specify `PUBLIC` as the
  `<object_creator>`.

- As with any other grants, the privileges granted to the
  `<target_role>` are inherited by the members of the `<target_role>`.

</div>

<div class="code-tabs">

<div class="tab-content">

<div id="tab-specify-blueberry-as-the-object-creator" class="tab-pane"
title="Specify blue.berry as the object creator">

The following updates the default privileges for new tables, views,
materialized views, and sources created in `mydb.sales` schema by the
[`blue.berry@example.com`](mailto:blue.berry@example.com) role;
specifically, grants `SELECT` privileges on these objects to
`view_manager` and `data_reader` roles.

<div class="highlight">

``` chroma
-- For new relations created by the `"blue.berry@example.com"` role
-- Grant `SELECT` privileges to the `view_manager` and `data_reader` roles
ALTER DEFAULT PRIVILEGES FOR ROLE "blue.berry@example.com"
IN SCHEMA mydb.sales  -- Optional. If specified, need USAGE on database and schema.
GRANT SELECT ON TABLES TO view_manager, data_reader;
-- `TABLES` refers to tables, views, materialized views, and sources.
```

</div>

Afterwards, if [`blue.berry@example.com`](mailto:blue.berry@example.com)
creates a new materialized view in the `mydb.sales` schema, the
`view_manager` and `data_reader` roles are automatically granted
`SELECT` privileges on the new object.

<div class="highlight">

``` chroma
-- Run as `blue.berry@example.com`
SET CLUSTER TO compute_cluster;
SET DATABASE TO mydb;
SET SCHEMA TO sales;

-- Create a materialized view
CREATE MATERIALIZED VIEW magic AS
SELECT o.*,i.price,o.quantity * i.price as subtotal
FROM orders as o
JOIN items as i
ON o.item = i.item;
```

</div>

To verify that the default privileges have been automatically granted,
you can run `SHOW PRIVILEGES`:

<div class="code-tabs">

<div class="tab-content">

<div id="tab-view_manager" class="tab-pane" title="view_manager">

Verify the privileges for `view_manager`:

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR view_manager where grantor = 'blue.berry@example.com';
```

</div>

The results include the `SELECT` privilege on newly created `magic`
materialized view:

```
        grantor         |        grantee        |  database   | schema |        name         |    object_type    | privilege_type
------------------------+-----------------------+-------------+--------+---------------------+-------------------+----------------
 blue.berry@example.com | view_manager          | mydb        | sales  | magic               | materialized-view | SELECT
```

</div>

<div id="tab-data_reader" class="tab-pane" title="data_reader">

Verify the privileges for `data_reader`:

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR data_reader where grantor = 'blue.berry@example.com';
```

</div>

The results include the `SELECT` privilege on newly created `magic`
materialized view:

```
        grantor         |   grantee   |  database   | schema |        name         |    object_type    | privilege_type
------------------------+-------------+-------------+--------+---------------------+-------------------+----------------
 blue.berry@example.com | data_reader | mydb        | sales  | magic               | materialized-view | SELECT
```

</div>

<div id="tab-sales_report_app-a-member-of-data_reader" class="tab-pane"
title="sales_report_app (a member of data_reader)">

Verify the privileges for `sales_report_app` (a member of the
`data_reader` role):

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR sales_report_app where grantor = 'blue.berry@example.com';
```

</div>

The results include the `SELECT` privilege on the `magic` materialized
view it inherits through the `data_reader` role:

```
        grantor         |   grantee   |  database   | schema |        name         |    object_type    | privilege_type
------------------------+-------------+-------------+--------+---------------------+-------------------+----------------
 blue.berry@example.com | data_reader | mydb        | sales  | magic               | materialized-view | SELECT
```

</div>

</div>

</div>

</div>

<div id="tab-specify-public-as-the-object-creator" class="tab-pane"
title="Specify PUBLIC as the object creator">

With the exception of the `PUBLIC` role, the `<object_creator>` role is
**not** transitive. That is, default privileges that specify a
functional role like `view_manager` as the `<object_creator>` do **not**
apply to objects created by its members.

To illustrate, the following:

- creates a new user `lemon@example.com`,
- adds the user to the `view_manager` role, and
- creates a new default privilege, specifying `view_manager` as the
  `<object_creator>`.

<div class="highlight">

``` chroma
CREATE ROLE "lemon@example.com" WITH LOGIN PASSWORD '<password>';
GRANT view_manager TO "lemon@example.com";

ALTER DEFAULT PRIVILEGES FOR ROLE view_manager
IN SCHEMA mydb.sales -- Optional. If specified, need USAGE on database and schema.
GRANT INSERT ON TABLES TO view_manager;
-- Although `TABLES` refers to tables, views, materialized views, and
-- sources, the INSERT privilege will only apply to tables.
```

</div>

If [`lemon@example.com`](mailto:lemon@example.com) creates a new table
`only_lemon`, the above default `INSERT` privilege will not apply as the
object creator must be `view_manager`, not a member of `view_manager`.

<div class="highlight">

``` chroma
-- Run as `lemon@example.com` (a member of `view_manager`)
SET CLUSTER TO compute_cluster;
SET DATABASE TO mydb;
SET SCHEMA TO sales;

CREATE TABLE only_lemon (id INT);

SHOW PRIVILEGES FOR view_manager where name = 'only_lemon';
```

</div>

The `SHOW PRIVILEGES FOR view_manager where name = ‘only_lemon’;`
returns 0 rows.

However, if `view_manager` is the **only role** that has `CREATE`
privileges on `mydb.sales` schema, you can specify `PUBLIC` as the
`<object_creator>`. Then, the default privilege will apply to all
objects created by `view_manager` and its members.

<div class="highlight">

``` chroma
ALTER DEFAULT PRIVILEGES FOR ROLE PUBLIC
IN SCHEMA mydb.sales
GRANT INSERT ON TABLES TO view_manager;
-- Although `TABLES` refers to tables, views, materialized views, and
-- sources, the `CREATE` privilege will only apply to tables.
```

</div>

If [`lemon@example.com`](mailto:lemon@example.com) now creates a new
table `shared_lemon`, the above default `INSERT` privilege will be
granted to `view_manager`.

<div class="highlight">

``` chroma
-- Run as `lemon@example.com`
SET CLUSTER TO compute_cluster;
SET DATABASE TO mydb;
SET SCHEMA TO sales;

CREATE TABLE shared_lemon (id INT);
```

</div>

To verify that the default privileges have been automatically granted to
others, you can run `SHOW PRIVILEGES`:

<div class="code-tabs">

<div class="tab-content">

<div id="tab-view_manager" class="tab-pane" title="view_manager">

Verify the privileges for `view_manager`:

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR view_manager where name = 'shared_lemon';
```

</div>

The returned privileges should include the `INSERT` privilege on the
`shared_lemon` table.

```
      grantor       |   grantee    | database | schema |     name     | object_type | privilege_type
--------------------+--------------+----------+--------+--------------+-------------+----------------
  lemon@example.com | view_manager | mydb     | sales  | shared_lemon | table       | INSERT
```

</div>

<div id="tab-blueberryexamplecom" class="tab-pane"
title="blue.berry@example.com">

Verify the privileges for
[`blue.berry@example.com`](mailto:blue.berry@example.com):

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR "blue.berry@example.com" where name = 'shared_lemon';
```

</div>

The returned privileges should include the `INSERT` privilege on the
`shared_lemon` table.

```
      grantor       |   grantee    | database | schema |     name     | object_type | privilege_type
--------------------+--------------+----------+--------+--------------+-------------+----------------
  lemon@example.com | view_manager | mydb     | sales  | shared_lemon | table       | INSERT
```

</div>

</div>

</div>

</div>

</div>

</div>

## Show roles in system

To view the roles in the system, use the
[`SHOW ROLES`](/docs/self-managed/v25.2/sql/show-roles/) command:

<div class="highlight">

``` chroma
SHOW ROLES [ LIKE <pattern>  | WHERE <condition(s)> ];
```

</div>

For example, to show all roles:

<div class="highlight">

``` chroma
SHOW ROLES;
```

</div>

The results should list all roles:

```
         name          | comment
-----------------------+---------
blue.berry@example.com |
data_reader            |
lemon@example.com      |
sales_report_app       |
serving_index_manager  |
view_manager           |
```

## Drop a role

To remove a role from the system, use the
[`DROP ROLE`](/docs/self-managed/v25.2/sql/drop-role/) command:

<div class="annotation">

<div class="annotation-title">

Privilege(s) required to run the command

</div>

<div>

- `CREATEROLE` privileges on the system.

</div>

</div>

<div class="highlight">

``` chroma
DROP ROLE <role>;
```

</div>

<div class="note">

**NOTE:** You cannot drop a role if it contains any members. Before
dropping a role, revoke the role from all its members. See [Revoke a
role](#revoke-a-role-from-another-role).

</div>

## Alter role

When granting privileges, the privileges may be scoped to a particular
cluster, database, and schema.

You can use
[`ALTER ROLE ... SET`](/docs/self-managed/v25.2/sql/alter-role/) to set
various configuration parameters, including cluster, database, and
schema.

<div class="highlight">

``` chroma
ALTER ROLE <role> SET <config> =|TO <value>;
```

</div>

The following example configures the
[`blue.berry@example.com`](mailto:blue.berry@example.com) role to use
the `compute_cluster` cluster, `mydb` database, and `sales` schema by
default.

<div class="highlight">

``` chroma
ALTER ROLE "blue.berry@example.com" SET CLUSTER = compute_cluster;
ALTER ROLE "blue.berry@example.com" SET DATABASE = mydb;
ALTER ROLE "blue.berry@example.com" SET search_path = sales; -- i.e., schema
```

</div>

- These changes will take effect in the next session for the role; the
  changes have **NO** effect on the current session.

- These configurations are just the defaults. For example, the
  connection string can specify a different database for the session or
  the user can issue a `SET ...` command to override these values for
  the current session.

In Materialize, when you grant a role to another role (user role/service
account role/independent role), the target role inherits only the
privileges of the granted role. **Role configurations are not
inherited.** For example, the following example updates the
`data_reader` role to use `serving_cluster` by default.

<div class="highlight">

``` chroma
ALTER ROLE data_reader SET CLUSTER = serving_cluster;
```

</div>

This change affects only the `data_reader` role and does not affect
roles that have been granted `data_reader`, such as `sales_report_app`.
That is, after this change:

- The default cluster for `data_reader` is `serving_cluster` for new
  sessions.

- The default cluster for `sales_report_app` is not affected.

<div class="tip">

**💡 Tip:** Since role configurations are not inherited, setting role
configurations for functional roles (i.e., not specific user/service
account roles) has limited utility. Instead, configure the specific
user/service account roles instead of the functional roles.

</div>

## Change ownership of objects

Certain [commands on an
object](/docs/self-managed/v25.2/manage/access-control/appendix-command-privileges/)
(such as creating an index on a materialized view or changing owner of
an object) require ownership of the object itself (or *superuser*
privileges).

In Materialize, when a role creates an object, the role becomes the
owner of the object and is automatically granted all [applicable
privileges](/docs/self-managed/v25.2/manage/access-control/appendix-privileges/)
for the object. To transfer ownership (and privileges) to another role
(another user role/service account role/functional role), you can use
the [ALTER … OWNER TO](/docs/self-managed/v25.2/sql/alter-owner/)
command:

<div class="annotation">

<div class="annotation-title">

Privilege(s) required to run the command

</div>

<div>

- Ownership of the object being altered.
- Role membership in `new_owner`.
- `CREATE` privileges on the containing cluster if the object is a
  cluster replica.
- `CREATE` privileges on the containing database if the object is a
  schema.
- `CREATE` privileges on the containing schema if the object is
  namespaced by a schema.

</div>

</div>

<div class="highlight">

``` chroma
ALTER <object_type> <object_name> OWNER TO <role>;
```

</div>

Before changing the ownership, review the privileges of the current
owner (`lemon@example.com`) and the future owner (`view_manage`):

Review [`lemon@example.com`](mailto:lemon@example.com)`"`’s privileges
on the `shared_lemon` table.

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR "lemon@example.com" where name = 'shared_lemon';
```

</div>

As the owner, `lemon@example.com` has all applicable privileges
(`INSERT`/`SELECT`/`UPDATE`/`DELETE`) for the table as well as the
`INSERT` through its membership in `view_manager` (from [Alter default
privileges
example](/docs/self-managed/v25.2/manage/access-control/manage-roles/#alter-default-privileges)).

```
      grantor      |      grantee      | database | schema |     name     | object_type | privilege_type
-------------------+-------------------+----------+--------+--------------+-------------+----------------
 lemon@example.com | lemon@example.com | mydb     | sales  | shared_lemon | table       | DELETE
 lemon@example.com | lemon@example.com | mydb     | sales  | shared_lemon | table       | INSERT
 lemon@example.com | lemon@example.com | mydb     | sales  | shared_lemon | table       | SELECT
 lemon@example.com | lemon@example.com | mydb     | sales  | shared_lemon | table       | UPDATE
 lemon@example.com | view_manager      | mydb     | sales  | shared_lemon | table       | INSERT
```

Review `view_manager`’s privileges on the `shared_lemon` table.

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR view_manager where name = 'shared_lemon';
```

</div>

The results show that the `view_manager` role has `INSERT` privileges on
the `shared_lemon` table (from [Alter default privileges
example](/docs/self-managed/v25.2/manage/access-control/manage-roles/#alter-default-privileges)).

```
      grantor      |   grantee    | database | schema |     name     | object_type | privilege_type
-------------------+--------------+----------+--------+--------------+-------------+----------------
 lemon@example.com | view_manager | mydb     | sales  | shared_lemon | table       | INSERT
```

Change the owner of the `shared_lemon` table to `view_manager`.

<div class="highlight">

``` chroma
ALTER TABLE mydb.sales.shared_lemon OWNER TO view_manager;
```

</div>

After running the command, review `view_manager`’s privileges on the
`shared_lemon` table.

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR view_manager where name = 'shared_lemon';
```

</div>

The results show that the `view_manager` role has all applicable
privileges for a table (`INSERT`, `SELECT`, `UPDATE`, `DELETE`):

```
  grantor    |   grantee    | database | schema |     name     | object_type | privilege_type
-------------+--------------+----------+--------+--------------+-------------+----------------
view_manager | view_manager | mydb     | sales  | shared_lemon | table       | DELETE
view_manager | view_manager | mydb     | sales  | shared_lemon | table       | INSERT
view_manager | view_manager | mydb     | sales  | shared_lemon | table       | SELECT
view_manager | view_manager | mydb     | sales  | shared_lemon | table       | UPDATE
```

Review [`lemon@example.com`](mailto:lemon@example.com)’s privileges on
the `shared_lemon` table.

<div class="highlight">

``` chroma
SHOW PRIVILEGES FOR "lemon@example.com" where name = 'shared_lemon';
```

</div>

The results show that `lemon@example.com` now only has access through
`view_manager`.

```
   grantor    |   grantee    | database | schema |     name     | object_type | privilege_type
--------------+--------------+----------+--------+--------------+-------------+----------------
 view_manager | view_manager | mydb     | sales  | shared_lemon | table       | DELETE
 view_manager | view_manager | mydb     | sales  | shared_lemon | table       | INSERT
 view_manager | view_manager | mydb     | sales  | shared_lemon | table       | SELECT
 view_manager | view_manager | mydb     | sales  | shared_lemon | table       | UPDATE
```

## See also

- [Access control best
  practices](/docs/self-managed/v25.2/manage/access-control/#best-practices)
- [Manage privileges with
  Terraform](/docs/self-managed/v25.2/manage/access-control/rbac-terraform-tutorial/)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/manage/access-control/manage-roles.md"
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
