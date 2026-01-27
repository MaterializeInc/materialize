# Manage database roles
Create and manage database roles and privileges in Materialize
In Materialize, role-based access control (RBAC) governs access to objects
through privileges granted to database roles.

## Enabling RBAC

> **Warning:** If RBAC is not enabled, all users have <red>**superuser**</red> privileges.


By default, role-based access control (RBAC) checks are not enabled (i.e.,
enforced) when using [authentication](/security/self-managed/authentication/#configuring-authentication-type). To
enable RBAC, set the system parameter `enable_rbac_checks` to `'on'` or `True`.
You can enable the parameter in one of the following ways:

- For [local installations using
  Kind/Minikube](/self-managed-deployments/installation/#installation-guides), set `spec.enableRbac:
  true` option when instantiating the Materialize object.

- For [Cloud deployments using Materialize's
  Terraforms](/self-managed-deployments/installation/#installation-guides), set
  `enable_rbac_checks` in the environment CR via the `environmentdExtraArgs`
  flag option.

- After the Materialize instance is running, run the following command as
  `mz_system` user:

  ```mzsql
  ALTER SYSTEM SET enable_rbac_checks = 'on';
  ```

If more than one method is used, the `ALTER SYSTEM` command will take precedence
over the Kubernetes configuration.

To view the current value for `enable_rbac_checks`, run the following `SHOW`
command:

```mzsql
SHOW enable_rbac_checks;
```

> **Important:** If RBAC is not enabled, all users have <red>**superuser**</red> privileges.

## Required privileges for managing roles

> **Note:** Initially, only the `mz_system` user (which has superuser/administrator
> privileges) is available to manage roles.



| Role management operations | Required privileges |
| --- | --- |
| To create/revoke/grant roles | <ul> <li><code>CREATEROLE</code> privileges on the system. > **Warning:** Roles with the `CREATEROLE` privilege can obtain the privileges of any other > role in the system by granting themselves that role. Avoid granting > `CREATEROLE` unnecessarily. </li> </ul>  |
| To view privileges for a role | None |
| To grant/revoke role privileges | <ul> <li>Ownership of affected objects.</li> <li><code>USAGE</code> privileges on the containing database if the affected object is a schema.</li> <li><code>USAGE</code> privileges on the containing schema if the affected object is namespaced by a schema.</li> <li><em>superuser</em> status if the privilege is a system privilege.</li> </ul>  |
| To alter default privileges | <ul> <li>Role membership in <code>role_name</code>.</li> <li><code>USAGE</code> privileges on the containing database if <code>database_name</code> is specified.</li> <li><code>USAGE</code> privileges on the containing schema if <code>schema_name</code> is specified.</li> <li><em>superuser</em> status if the <em>target_role</em> is <code>PUBLIC</code> or <strong>ALL ROLES</strong> is specified.</li> </ul>  |


See also [Appendix: Privileges by
command](/security/appendix/appendix-command-privileges/)

## Create a role

In Materialize, you can create both:
- Individual user or service account roles; i.e., roles associated with a
  specific user or service account.
- Functional roles, not associated with any single user or service
  account, but typically used to define a set of shared
  privileges that can be granted to other user/service/functional roles.

Initially, only the `mz_system` user is available.

### Create individual user/service account roles

To create additional users or service accounts, login as the `mz_system` user,
using the `external_login_password_mz_system` password, and use [`CREATE ROLE
... WITH LOGIN PASSWORD ...`](/sql/create-role):

```mzsql
CREATE ROLE <user> WITH LOGIN PASSWORD '<password>';
```

> **Privilege(s) required to run the command:** - `CREATEROLE` privileges on the system.


For example, the following creates:

- A new user `blue.berry@example.com` (or more specifically, a new user role).
- A new service account `sales_report_app` (or more specifically, a new service
  account role).


**A new user role:**

The following creates a new user `blue.berry@example.com`, or more specifically, creates a role for a user `blue.berry@example.com` using [`CREATE ROLE ... WITH LOGIN PASSWORD`](/sql/create-role).
```mzsql
CREATE ROLE "blue.berry@example.com" WITH LOGIN PASSWORD '<password>';

```{{< note >}}
The role/user name `blue.berry@example.com` is enclosed in double quotes to
override the [naming restrictions](/sql/identifiers/#naming-restrictions).
{{</ note >}}

Once created, the user `blue.berry@example.com` can login using the
password.


**A new service account role:**

The following creates a new service account `sales_report_app`, or more
specifically, creates a role for a service account `sales_report_app` using
[`CREATE ROLE ... WITH LOGIN PASSWORD`](/sql/create-role).
```mzsql
CREATE ROLE "sales_report_app" WITH LOGIN PASSWORD '<password>';

```
Once created, the associated application can use the `sales_report_app`
service account to connect to Materialize.




In Materialize, a role is created with inheritance support. With inheritance,
when a role is granted to another role (i.e., the target role), the target role
inherits privileges (not role attributes and parameters) through the other role.
All roles in Materialize are automatically members of
[`PUBLIC`](/security/appendix/appendix-built-in-roles/#public-role). As
such, every role includes inherited privileges from `PUBLIC`.

Once a role is created, you can:

- [Manage its current
  privileges](/security/self-managed/access-control/manage-roles/#manage-current-privileges-for-a-role)
  (i.e., privileges on existing objects):
  - By granting privileges for a role or revoking privileges from a role.
  - By granting other roles to the role or revoking roles from the role.
    *Recommended for user account/service account roles.*
- [Manage its future
  privileges](/security/self-managed/access-control/manage-roles/#manage-future-privileges-for-a-role)
  (i.e., privileges on objects created in the future):
  - By defining default privileges for objects. With default privileges in
   place, a role is automatically granted/revoked privileges as new objects are
   created by **others** (When an object is created, the creator is granted all
   [applicable privileges](/security/appendix/appendix-privileges/) for that
   object automatically).

> **Disambiguation:** - Use `GRANT|REVOKE ...` to modify privileges on **existing** objects. - Use `ALTER DEFAULT PRIVILEGES` to ensure that privileges are automatically granted or revoked when **new objects** of a certain type are created by others. Then, as needed, you can use `GRANT|REVOKE <privilege>` to adjust those privileges.


See also:

- For a list of required privileges for specific operations, see [Appendix:
Privileges by command](/security/appendix/appendix-command-privileges/).

### Create functional roles

To create functional roles, login as the `mz_system` user,
using the `external_login_password_mz_system` password, and use [`CREATE ROLE`](/sql/create-role):

```mzsql
CREATE ROLE <role>;
```

> **Tip:** Role names cannot start with `mz_` and `pg_` as they are reserved for system
> roles.


For example, the following creates:
- A role for users who need to perform compute/transform operations in the
  compute/transform.
- A role for users who need to manage indexes on the serving cluster(s).
- A role for users who need to read results from the serving cluster.


**View manager role:**

Create a role for users who need to perform compute/transform operations in
the compute/transform cluster(s). This role will handle creating views,
materialized views, and other transformation objects.
```mzsql
CREATE ROLE view_manager;

```

**Serving index manager role:**

Create a role for users who need to manage indexes on the serving
cluster(s). This role will handle creating indexes to serve results.
```mzsql
CREATE ROLE serving_index_manager;

```

**Data reader role:**

Create a role for users who need to read results from the serving cluster.
```mzsql
CREATE ROLE data_reader;

```



In Materialize, a role is created with inheritance support. With inheritance,
when a role is granted to another role (i.e., the target role), the target role
inherits privileges (not role attributes and parameters) through the other role.
All roles in Materialize are automatically members of
[`PUBLIC`](/security/appendix/appendix-built-in-roles/#public-role). As
such, every role includes inherited privileges from `PUBLIC`.

Once a role is created, you can:

- [Manage its current
  privileges](/security/self-managed/access-control/manage-roles/#manage-current-privileges-for-a-role)
  (i.e., privileges on existing objects):
  - By granting privileges for a role or revoking privileges from a role.
  - By granting other roles to the role or revoking roles from the role.
    *Recommended for user account/service account roles.*
- [Manage its future
  privileges](/security/self-managed/access-control/manage-roles/#manage-future-privileges-for-a-role)
  (i.e., privileges on objects created in the future):
  - By defining default privileges for objects. With default privileges in
   place, a role is automatically granted/revoked privileges as new objects are
   created by **others** (When an object is created, the creator is granted all
   [applicable privileges](/security/appendix/appendix-privileges/) for that
   object automatically).

> **Disambiguation:** - Use `GRANT|REVOKE ...` to modify privileges on **existing** objects. - Use `ALTER DEFAULT PRIVILEGES` to ensure that privileges are automatically granted or revoked when **new objects** of a certain type are created by others. Then, as needed, you can use `GRANT|REVOKE <privilege>` to adjust those privileges.


See also:

- For a list of required privileges for specific operations, see [Appendix:
Privileges by command](/security/appendix/appendix-command-privileges/).

## Manage current privileges for a role

> **Note:** - The examples below assume the existence of a `mydb` database and a `sales`
> schema within the `mydb` database.
> - The examples below assume the roles only need privileges to objects in the
>   `mydb.sales` schema.


### View privileges for a role

> **Privilege(s) required to run the command:** No specific privilege is required to run the `SHOW PRIVILEGES`


To view privileges granted to a role, you can use the [`SHOW
PRIVILEGES`](/sql/show-privileges) command, substituting `<role>` with the role
name (see [`SHOW PRIVILEGES`](/sql/show-default-privileges) for the full
syntax):

```mzsql
SHOW PRIVILEGES FOR <role>;
```

> **Note:** All roles in Materialize are automatically members of
> [`PUBLIC`](/security/appendix/appendix-built-in-roles/#public-role). As
> such, every role includes inherited privileges from `PUBLIC`.


For example:


**User:**


To view privileges for a user, run [`SHOW PRIVILEGES`](/sql/show-privileges)
on the user's role. For example, show the privileges for the `blue.berry@example.com` role created in the
[Create a role section](#create-individual-userservice-account-roles).
```mzsql
SHOW PRIVILEGES FOR "blue.berry@example.com";

```


The results show that the role currently has only the privileges inherited
through the `PUBLIC` role.

```none
| grantor   | grantee | database    | schema | name        | object_type | privilege_type |
| --------- | ------- | ----------- | ------ | ----------- | ----------- | -------------- |
| mz_system | PUBLIC  | materialize | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | mydb        | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | null        | null   | materialize | database    | USAGE          |
| mz_system | PUBLIC  | null        | null   | quickstart  | cluster     | USAGE          |
```



**Service account role:**


To view privileges for a service account, run [`SHOW
PRIVILEGES`](/sql/show-privileges) on the service account's role. For example, show the privileges for the `sales_report_app` role created in the
[Create a role section](#create-individual-userservice-account-roles).
```mzsql
SHOW PRIVILEGES FOR sales_report_app;

```

The results show that the role currently has only the privileges inherited
through the `PUBLIC` role.

```none
| grantor   | grantee | database    | schema | name        | object_type | privilege_type |
| --------- | ------- | ----------- | ------ | ----------- | ----------- | -------------- |
| mz_system | PUBLIC  | materialize | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | mydb        | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | null        | null   | materialize | database    | USAGE          |
| mz_system | PUBLIC  | null        | null   | quickstart  | cluster     | USAGE          |
```


**Functional roles:**

**View manager role:**

Show the privileges for the `view_manager` role created in the
[Create a role section](#create-a-role).
```mzsql
SHOW PRIVILEGES FOR view_manager;

```

The results show that the role currently has only the privileges inherited
through the `PUBLIC` role.

```none
| grantor   | grantee | database    | schema | name        | object_type | privilege_type |
| --------- | ------- | ----------- | ------ | ----------- | ----------- | -------------- |
| mz_system | PUBLIC  | materialize | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | mydb        | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | null        | null   | materialize | database    | USAGE          |
| mz_system | PUBLIC  | null        | null   | quickstart  | cluster     | USAGE          |
```


**Serving index manager role:**

Show the privileges for the `serving_index_manager` role created in the
[Create a role section](#create-a-role).
```mzsql
SHOW PRIVILEGES FOR serving_index_manager;

```

The results show that the role currently has only the privileges inherited
through the `PUBLIC` role.

```none
| grantor   | grantee | database    | schema | name        | object_type | privilege_type |
| --------- | ------- | ----------- | ------ | ----------- | ----------- | -------------- |
| mz_system | PUBLIC  | materialize | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | mydb        | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | null        | null   | materialize | database    | USAGE          |
| mz_system | PUBLIC  | null        | null   | quickstart  | cluster     | USAGE          |
```


**Data reader role:**

Show the privileges for the `data_reader` role created in the
[Create a role section](#create-a-role).
```mzsql
SHOW PRIVILEGES FOR data_reader;

```

The results show that the role currently has only the privileges inherited
through the `PUBLIC` role.

```none
| grantor   | grantee | database    | schema | name        | object_type | privilege_type |
| --------- | ------- | ----------- | ------ | ----------- | ----------- | -------------- |
| mz_system | PUBLIC  | materialize | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | mydb        | null   | public      | schema      | USAGE          |
| mz_system | PUBLIC  | null        | null   | materialize | database    | USAGE          |
| mz_system | PUBLIC  | null        | null   | quickstart  | cluster     | USAGE          |
```







> **Tip:** For the `SHOW PRIVILEGES` command, you can add a `WHERE` clause to filter by the
> return fields; e.g., `SHOW PRIVILEGES FOR view_manager WHERE
> name='quickstart';`.


### Grant privileges to a role

To grant [privileges](/security/appendix/appendix-command-privileges/) to
a role, use the [`GRANT PRIVILEGE`](/sql/grant-privilege/) statement (see
[`GRANT PRIVILEGE`](/sql/grant-privilege/) for the full syntax)

> **Privilege(s) required to run the command:** - Ownership of affected objects. - `USAGE` privileges on the containing database if the affected object is a schema. - `USAGE` privileges on the containing schema if the affected object is namespaced by a schema. - _superuser_ status if the privilege is a system privilege. To override the **object ownership** requirements to grant privileges, run as a user with superuser privileges; e.g. `mz_system` user.


```mzsql
GRANT <PRIVILEGE> ON <OBJECT_TYPE> <object_name> TO <role>;
```

When possible, avoid granting privileges directly to individual user or service
account roles. Instead, create reusable, functional roles (e.g., `data_reader`,
`view_manager`) with well-defined privileges, and grant these roles to the
individual user or service account roles. You can also grant functional roles to
other functional roles to compose more complex functional roles.

For example, the following grants privileges to the functional roles.

> **Note:** Various SQL operations require additional privileges on related objects, such
> as:
> - For objects that use compute resources (e.g., indexes, materialized views,
>   replicas, sources, sinks), access is also required for the associated cluster.
> - For objects in a schema, access is also required for the schema.
> For details on SQL operations and needed privileges, see [Appendix: Privileges
> by command](/security/appendix/appendix-command-privileges/).



**View manager role:**

The following example grants the `view_manager` role various privileges to
run:

- [`SELECT`](/sql/select/#privileges) from currently existing
  materialized views/views/tables/sources in the `mydb.sales` schema.
- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/#privileges)
  in the `mydb.sales` schema on the `compute_cluster`.
- [`CREATE VIEW`](/sql/create-view/#privileges) if using intermediate views
  as part of a stacked view definition (i.e., views whose definition depends
  on other views).

{{< note >}}
If a query directly references a view or materialized view:
{{% include-headless "/headless/rbac-sm/select-views-privileges" %}}
{{</ note >}}
```mzsql
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


Review the privileges granted to the `view_manager` role:
```mzsql
SHOW PRIVILEGES FOR view_manager;

```
The results should reflect the new privileges granted to the `view_manager`
role in addition to those privileges inherited through the `PUBLIC` role.

```none
| grantor   | grantee      | database    | schema | name                | object_type       | privilege_type |
| --------- | ------------ | ----------- | ------ | ------------------- | ----------------- | -------------- |
| mz_system | PUBLIC       | materialize | null   | public              | schema            | USAGE          |
| mz_system | PUBLIC       | mydb        | null   | public              | schema            | USAGE          |
| mz_system | PUBLIC       | null        | null   | materialize         | database          | USAGE          |
| mz_system | PUBLIC       | null        | null   | quickstart          | cluster           | USAGE          |
| mz_system | view_manager | mydb        | sales  | items               | table             | SELECT         |
| mz_system | view_manager | mydb        | sales  | orders              | table             | SELECT         |
| mz_system | view_manager | mydb        | sales  | orders_daily_totals | materialized-view | SELECT         |
| mz_system | view_manager | mydb        | sales  | orders_view         | view              | SELECT         |
| mz_system | view_manager | mydb        | sales  | sales_items         | table             | SELECT         |
| mz_system | view_manager | mydb        | null   | sales               | schema            | CREATE         |
| mz_system | view_manager | mydb        | null   | sales               | schema            | USAGE          |
| mz_system | view_manager | null        | null   | compute_cluster     | cluster           | CREATE         |
| mz_system | view_manager | null        | null   | compute_cluster     | cluster           | USAGE          |
```

{{< important >}}

The `GRANT SELECT ON ALL TABLES IN SCHEMA mydb.sales TO view_manager;`
statement results in `view_manager` having `SELECT` privileges on specific
objects, namely the three tables that existed in the `mydb.sales` schema at
the time of the grant. It **does not** grant `SELECT` privileges on any
tables, views, materialized views, or sources created by others in the
schema afterwards.

For new objects created by others, you can either:
- Manually grant privileges on new objects; or
- Use [default
privileges](/security/self-managed/access-control/manage-roles/#manage-future-privileges-for-a-role)
to automatically grant privileges on new objects.

{{</ important >}}


**Serving index manager role:**

The following example grants the `serving_index_manager` role various
privileges to:

- [`CREATE INDEX`](/sql/create-index/#privileges) in the `mydb.sales`
  schema on the `serving_cluster`.

- Use the `serving_cluster` (i.e., `USAGE`). Although you can create an
index without the `USAGE`, this allows the person creating the index to use
the index to verify.

{{< note >}}

In addition to database privileges, to create an index, a role must be the
owner of the object on which the index is created.

{{</ note >}}
```mzsql
-- To create an index on an object **owned** by the role:
-- Need CREATE on the cluster.
-- Need CREATE on the schema.
GRANT CREATE ON CLUSTER serving_cluster TO serving_index_manager;
GRANT CREATE ON SCHEMA mydb.sales TO serving_index_manager;

-- Optional.
GRANT USAGE ON CLUSTER serving_cluster TO serving_index_manager;

```

Review the privileges granted to the `serving_index_manager` role:
```mzsql
SHOW PRIVILEGES FOR serving_index_manager;

```
The results should reflect the new privileges granted to the `index_manager`
role in addition to those privileges inherited through the `PUBLIC` role.

```none
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

{{< note >}}

In addition to database privileges, a role must be the owner of the object
on which the index is created. In our examples, `view_manager` role has
privileges to create the various materialized views that will be indexed:

- See [Grant a role to another
role](/security/self-managed/access-control/manage-roles/#grant-a-role-to-another-role) for
details and example of granting `serving_cluster` role to `view_manager`.

- See [Change ownership of
objects](/security/self-managed/access-control/manage-roles/#change-ownership-of-objects)
for details and example of changing ownership of objects.

{{</ note >}}


**Data reader role:**
The following example grants the `data_reader` role privileges to run:

- [`SELECT`](/sql/select/#privileges) from all existing tables/materialized
  views/views/sources in the `mydb.sales` schema on the `serving_cluster`.

{{< note >}}
If a query directly references a view or materialized view:
{{% include-headless "/headless/rbac-sm/select-views-privileges" %}}
{{</ note >}}
```mzsql
-- To select from **existing** views/materialized views/tables/sources:
-- Need USAGE on schema and cluster
-- Need SELECT on the materialized views/views/tables/sources
GRANT USAGE ON SCHEMA mydb.sales TO data_reader;
GRANT USAGE ON CLUSTER serving_cluster TO data_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA mydb.sales TO data_reader;
-- For PostgreSQL compatibility, ALL TABLES encompasses tables, views,
-- materialized views, and sources.

```


Review the privileges granted to the `data_reader` role:
```mzsql
SHOW PRIVILEGES FOR data_reader;

```
The results should reflect the new privileges granted to the `data_reader`
role in addition to those privileges inherited through the `PUBLIC` role.

```none
| grantor   | grantee     | database    | schema | name                | object_type       | privilege_type |
| --------- | ----------- | ----------- | ------ | ------------------- | ----------------- | -------------- |
| mz_system | PUBLIC      | materialize | null   | public              | schema            | USAGE          |
| mz_system | PUBLIC      | mydb        | null   | public              | schema            | USAGE          |
| mz_system | PUBLIC      | null        | null   | materialize         | database          | USAGE          |
| mz_system | PUBLIC      | null        | null   | quickstart          | cluster           | USAGE          |
| mz_system | data_reader | mydb        | sales  | items               | table             | SELECT         |
| mz_system | data_reader | mydb        | sales  | orders              | table             | SELECT         |
| mz_system | data_reader | mydb        | sales  | orders_daily_totals | materialized-view | SELECT         |
| mz_system | data_reader | mydb        | sales  | orders_view         | view              | SELECT         |
| mz_system | data_reader | mydb        | sales  | sales_items         | table             | SELECT         |
| mz_system | data_reader | mydb        | null   | sales               | schema            | USAGE          |
| mz_system | data_reader | null        | null   | serving_cluster     | cluster           | USAGE          |
```

{{< important >}}

The `GRANT SELECT ON ALL TABLES IN SCHEMA mydb.sales TO data_reader;`
statement results in `data_reader` having `SELECT` privileges on specific
objects, namely the three tables that existed in the `mydb.sales` schema at
the time of the grant. It **does not** grant `SELECT` privileges on any
tables, views, materialized views, or sources created in the schema
afterwards by others.

For new objects created by others, you can either:
- Manually grant privileges on new objects; or
- Use [default
privileges](/security/self-managed/access-control/manage-roles/#manage-future-privileges-for-a-role)
to automatically grant privileges on new objects.

{{</ important >}}




### Grant a role to another role

Once a role is created, you can modify its privileges either:

- Directly by [granting privileges for a role](#grant-privileges-to-a-role) or
  [revoking privileges from a role](#revoke-privileges-from-a-role).
- Indirectly (through inheritance) by granting other roles to the role or
  [revoking roles from the role](#revoke-a-role-from-another-role).

> **Tip:** When possible, avoid granting privileges directly to individual user or service
> account roles. Instead, create reusable, functional roles (e.g., `data_reader`,
> `view_manager`) with well-defined privileges, and grant these roles to the
> individual user or service account roles. You can also grant functional roles to
> other functional roles to compose more complex functional roles.


To grant a role to another role (where the role can be a user role/service
account role/functional role), use the [`GRANT ROLE`](/sql/grant-role/)
statement (see [`GRANT ROLE`](/sql/grant-role/) for full syntax):

> **Privilege(s) required to run the command:** - `CREATEROLE` privileges on the system. `mz_system` user has the required privileges on the system.


```mzsql
GRANT <role> [, <role>...] to <target_role> [, <target_role> ...];
```

When a role is granted to another role, the target role becomes a member of the
other role and inherits the privileges through the other role.

In the following examples,

- The functional role `view_manager` is granted to the user role
  `blue.berry@example.com`.
- The functional role `serving_index_manager` is granted to the functional role
  `view_manager`.
- The functional role `data_reader` is granted to the service account role
  `sales_report_app`.


**Grant view_manager role:**


The following grants the `view_manager` role to the role associated with the
user `blue.berry@example.com`.
```mzsql
GRANT view_manager TO "blue.berry@example.com";

```


Review the privileges granted to the `blue.berry@example.com` role:
```mzsql
SHOW PRIVILEGES FOR "blue.berry@example.com";

```
The results should include the privileges inherited through the
`view_manager` role in addition to those privileges through the `PUBLIC`
role. If the role had been granted direct privileges, those would also be
included.

```none
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
`"blue.berry@example.com"` can create objects in the `mydb.sales` schema on
the `compute_cluster`.
```mzsql
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
-- Requires SELECT on the materialized view and its underlying relations.
SELECT * FROM orders_daily_totals;

```
In Materialize, a role automatically gets all [applicable
privileges](/security/appendix/appendix-privileges/) for an
object they create; for example, the creator of a schema gets `CREATE` and
`USAGE`; the creator of a table gets `SELECT`, `INSERT`, `UPDATE`, and
`DELETE`.

For example, if you show privileges for `"blue.berry@example.com"` after
creating the view and materialized view, you will see that the role has
`SELECT` privileges on the `orders_daily_totals`  and `orders_view`.

```none
| grantor                | grantee                | database    | schema | name                | object_type       | privilege_type |
| ---------------------- | ---------------------- | ----------- | ------ | ------------------- | ----------------- | -------------- |
| blue.berry@example.com | blue.berry@example.com | mydb        | sales  | orders_daily_totals | materialized-view | SELECT         |
| blue.berry@example.com | blue.berry@example.com | mydb        | sales  | orders_view         | view              | SELECT         |
| mz_system              | PUBLIC                 | materialize | null   | public              | schema            | USAGE          |
| mz_system              | PUBLIC                 | mydb        | null   | public              | schema            | USAGE          |
... -- Rest omitted for brevity
```

{{< note >}}
If a query directly references a view or materialized view:
{{% include-headless "/headless/rbac-sm/select-views-privileges" %}}
{{</ note >}}

However, with the current privileges, `"blue.berry@example.com"` cannot
select from new views/materialized views created by **others** in
the schema and vice versa. For privileges on new objects, you can either:
- Manually grant privileges on new objects; or
- Use [default
privileges](/security/self-managed/access-control/manage-roles/#manage-future-privileges-for-a-role)
to automatically grant privileges on new objects.




**Grant serving_index_manager role:**
The following grants the `serving_index_manager` role to the functional role
`view_manager`, which already has privileges to create materialized views in
`mydb.sales` schema. This allows members of the `view_manager` role to
create indexes on their objects on the `serving_cluster`.
```mzsql
GRANT serving_index_manager TO view_manager;

```

Review the privileges of `view_manager` as well as `"blue.berry@example.com"`
(a member of  `view_manager`) after the grant.


**Privileges for view_manager:**
Review the privileges granted to the `view_manager` role:
```mzsql
SHOW PRIVILEGES FOR view_manager;

```
The results include the privileges inherited through the
`serving_index_manager` role in addition to those privileges inherited
through the `PUBLIC` role as well as those granted directly to the role, if
any.

```none
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



**Privileges for blue.berry@example.com:**

Review the privileges for `"blue.berry@example.com"` (a member of `view_manager`):
```mzsql
SHOW PRIVILEGES FOR "blue.berry@example.com";

```
The results include the privileges inherited through the
`serving_index_manager` role in addition to those privileges inherited
through the `PUBLIC` role as well as those granted directly to the role, if
any. For example, after being granted the `view_manager` role,
`"blue.berry@example.com"` created the `orders_daily_totals` and
`orders_view`. As the creator, `"blue.berry@example.com"` automatically gets
all applicable privileges on the objects they create.

```none
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








To create indexes on an object, in addition to specific `CREATE` privileges
(granted by the `serving_index_manager` role), the user needs to be the
owner of the object.

After the `serving_index_manager` role is granted to the `view_manager`
role, members of `view_manager` can create indexes on the `serving_cluster`
for objects that they own. For example, `"blue.berry@example.com"` can
create an index on the `orders_daily_totals` materialized view.
```mzsql
-- run as "blue.berry@example.com"
SET CLUSTER TO serving_cluster;
SET DATABASE TO mydb;
SET SCHEMA TO sales;

CREATE INDEX ON orders_daily_totals (order_date);

-- If the role has `USAGE` on the `serving_cluster`:
SELECT * from orders_daily_totals;

```
To allow others in the `view_manager` role to create indexes, see [Change
ownership of objects](/security/self-managed/access-control/manage-roles/#change-ownership-of-objects).




**Grant data_reader role:**

The following grants the `data_reader` role to the service account role
`sales_report_app`.
```mzsql
GRANT data_reader TO sales_report_app;

```

Review the privileges for `sales_report_app` after the grant:
```mzsql
SHOW PRIVILEGES FOR sales_report_app;

```The results should include the privileges inherited through the
`data_reader` role in addition to those privileges inherited through the
`PUBLIC` role. If the role had been granted direct privileges, those would
also be included.

```none
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
`sales_report_app` service account role, `sales_report_app` can read from
the three tables in the `mydb.sales` schema on the `serving_cluster`.
```mzsql
SET CLUSTER TO serving_cluster;
SET DATABASE TO mydb;
SET SCHEMA TO sales;

SELECT * FROM sales_items;

```
However, `sales_report_app` cannot read from the new objects in
`mydb.sales`; e.g., `orders_daily_totals` materialized view and its
underlying view `orders_view` that were created after the `SELECT`
privileges were granted to the `data_reader` role.

To allow `sales_report_app` or `data_reader` to read from the new objects in
`mydb.sales`, you can either:
- Manually grant `SELECT` privileges on the new objects; or
- Use [default
privileges](/security/self-managed/access-control/manage-roles/#manage-future-privileges-for-a-role)
to automatically grant `SELECT` privileges on new objects.





### Revoke privileges from a role

To remove privileges from a role, use the [`REVOKE <privilege>`](/sql/revoke-privilege/) statement:

> **Privilege(s) required to run the command:** - Ownership of affected objects. - `USAGE` privileges on the containing database if the affected object is a schema. - `USAGE` privileges on the containing schema if the affected object is namespaced by a schema. - _superuser_ status if the privilege is a system privilege.


```mzsql
REVOKE <PRIVILEGE> ON <OBJECT_TYPE> <object_name> FROM <role>;
```

### Revoke a role from another role

To revoke a role from another role, use the [`REVOKE <role>`](/sql/revoke-role/) statement:

> **Privilege(s) required to run the command:** - `CREATEROLE` privileges on the systems.


```mzsql
REVOKE <role> FROM <target_role>;
```

For example:

```mzsql
REVOKE data_reader FROM sales_report_app;
```

> **Important:** When you revoke a role from another role (user role/service account
> role/independent role), the target role is no longer a member of the revoked
> role nor inherits the revoked role's privileges. **However**, privileges are
> cumulative: if the target role inherits the same privilege(s) from another role,
> the target role still has the privilege(s) through the other role.


## Manage future privileges for a role

In Materialize, a role automatically gets all [applicable
privileges](/security/appendix/appendix-privileges/) for an object they
create/own; for example, the creator of a schema gets `CREATE` and `USAGE`; the
creator of a table gets `SELECT`, `INSERT`, `UPDATE`, and `DELETE`. However, for
others to access the new object, you can either manually grant privileges on new
objects or use default privileges to automatically grant privileges to others as
new objects are created.

Default privileges can be specified for a given object type and scoped to:

- all future objects of that type;
- all future objects of that type within specific databases or schemas;
- all future objects of that type created by specific roles (or by all roles
  `PUBLIC`).

Default privileges apply only to objects created after these privileges are
defined. They do not affect objects that were created before the default
privileges were set.

> **Disambiguation:** - Use `GRANT|REVOKE ...` to modify privileges on **existing** objects. - Use `ALTER DEFAULT PRIVILEGES` to ensure that privileges are automatically granted or revoked when **new objects** of a certain type are created by others. Then, as needed, you can use `GRANT|REVOKE <privilege>` to adjust those privileges.


### View default privileges

To view default privileges, you can use the [`SHOW DEFAULT
PRIVILEGES`](/sql/show-default-privileges) command, substituting `<role>` with
the role name (see [`SHOW DEFAULT PRIVILEGES`](/sql/show-default-privileges) for
the full syntax):

> **Privilege(s) required to run the command:** No specific privilege is required to run the `SHOW DEFAULT PRIVILEGES`.


```mzsql
SHOW DEFAULT PRIVILEGES FOR <role>;
```

For example:


**User:**


To view default privileges for a user, run [`SHOW DEFAULT
PRIVILEGES`](/sql/show-default-privileges) on the user's role. For example,
show the defaultprivileges for the `blue.berry@example.com` role created in
the [Create a role section](#create-individual-userservice-account-roles).
```mzsql
SHOW DEFAULT PRIVILEGES FOR "blue.berry@example.com";

```
The example results show that the default privileges for
`"blue.berry@example.com"` are the default privileges it has as a member of
the `PUBLIC` role.

{{% include-headless
"/headless/rbac-sm/show-default-privileges-new-roles" %}}



**Service account role:**


To view default privileges for a service account, run [`SHOW DEFAULT
PRIVILEGES`](/sql/show-default-privileges) on the service account's role. For example, show the default privileges for the `sales_report_app` role created in the
[Create a role section](#create-individual-userservice-account-roles).
```mzsql
SHOW DEFAULT PRIVILEGES FOR sales_report_app;

```
The example results show that the default privileges for `sales_report_app`
are the default privileges it has as a member of the `PUBLIC` role.

{{% include-headless
"/headless/rbac-sm/show-default-privileges-new-roles" %}}



**Functional roles:**

**View manager role:**

Show the default privileges for the `view_manager` role created in the
[Create a role section](#create-a-role).
```mzsql
SHOW DEFAULT PRIVILEGES FOR view_manager;

```
The example results show that the default privileges for `view_manager` are
the default privileges it has as a member of the `PUBLIC` role.

{{% include-headless
"/headless/rbac-sm/show-default-privileges-new-roles" %}}


**Serving index manager role:**

Show the default privileges for the `serving_index_manager` role created in
the [Create a role section](#create-a-role).
```mzsql
SHOW DEFAULT PRIVILEGES FOR serving_index_manager;

```
The example results show that the default privileges for
`serving_index_manager` are the default privileges it has as a member of
the `PUBLIC` role.

{{% include-headless
"/headless/rbac-sm/show-default-privileges-new-roles" %}}


**Data reader role:**

Show the default privileges for the `data_reader` role created in the
[Create a role section](#create-a-role).
```mzsql
SHOW DEFAULT PRIVILEGES FOR data_reader;

```
The example results show that the default privileges for `data_reader` are
the default privileges it has as a member of the `PUBLIC` role.

{{% include-headless
"/headless/rbac-sm/show-default-privileges-new-roles" %}}







### Alter default privileges

To define default privilege for objects created by a role, use the [`ALTER
DEFAULT PRIVILEGES`](/sql/alter-default-privileges) command (see  [`ALTER
DEFAULT PRIVILEGES`](/sql/alter-default-privileges) for the full syntax):

> **Privilege(s) required to run the command:** - Role membership in `role_name`. - `USAGE` privileges on the containing database if `database_name` is specified. - `USAGE` privileges on the containing schema if `schema_name` is specified. - _superuser_ status if the _target_role_ is `PUBLIC` or **ALL ROLES** is specified.


```mzsql
ALTER DEFAULT PRIVILEGES FOR ROLE <object_creator>
   IN SCHEMA <schema>    -- Optional. If specified, need USAGE on database and schema.
   GRANT <privilege> ON <object_type> TO <target_role>;
```

> **Note:** - With the exception of the `PUBLIC` role, the `<object_creator>` role is
>   **not** transitive. That is, default privileges that specify a functional role
>   like `view_manager` as the `<object_creator>` do **not** apply to objects
>   created by its members.
>   However, you can approximate default privileges for a functional role by
>   restricting `CREATE` privileges for the objects to the desired functional
>   roles (e.g., only `view_managers` have privileges to create tables in
>   `mydb.sales` schema) and then specify `PUBLIC` as the `<object_creator>`.
> - As with any other grants, the privileges granted to the `<target_role>` are
>   inherited by the members of the `<target_role>`.



**Specify blue.berry as the object creator:**

The following updates the default privileges for new tables, views,
materialized views, and sources created in `mydb.sales` schema by the
`blue.berry@example.com` role; specifically, grants `SELECT` privileges on
these objects to `view_manager` and `data_reader` roles.
```mzsql
-- For new relations created by the `"blue.berry@example.com"` role
-- Grant `SELECT` privileges to the `view_manager` and `data_reader` roles
ALTER DEFAULT PRIVILEGES FOR ROLE "blue.berry@example.com"
IN SCHEMA mydb.sales  -- Optional. If specified, need USAGE on database and schema.
GRANT SELECT ON TABLES TO view_manager, data_reader;
-- `TABLES` refers to tables, views, materialized views, and sources.

```


Afterwards, if `blue.berry@example.com` creates a new materialized view in
the `mydb.sales` schema, the `view_manager` and `data_reader` roles are
automatically granted `SELECT` privileges on the new object.
```mzsql
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

To verify that the default privileges have been automatically granted, you can
run `SHOW PRIVILEGES`:



**view_manager:**

Verify the privileges for `view_manager`:
```mzsql
SHOW PRIVILEGES FOR view_manager where grantor = 'blue.berry@example.com';

```
The results include the `SELECT` privilege on newly created `magic`
materialized view:

```none
        grantor         |        grantee        |  database   | schema |        name         |    object_type    | privilege_type
------------------------+-----------------------+-------------+--------+---------------------+-------------------+----------------
 blue.berry@example.com | view_manager          | mydb        | sales  | magic               | materialized-view | SELECT
```


**data_reader:**


Verify the privileges for `data_reader`:
```mzsql
SHOW PRIVILEGES FOR data_reader where grantor = 'blue.berry@example.com';

```
The results include the `SELECT` privilege on newly created `magic`
materialized view:

```none
        grantor         |   grantee   |  database   | schema |        name         |    object_type    | privilege_type
------------------------+-------------+-------------+--------+---------------------+-------------------+----------------
 blue.berry@example.com | data_reader | mydb        | sales  | magic               | materialized-view | SELECT
```



**sales_report_app (a member of data_reader):**
Verify the privileges for `sales_report_app` (a member of the
`data_reader` role):
```mzsql
SHOW PRIVILEGES FOR sales_report_app where grantor = 'blue.berry@example.com';

```
The results include the `SELECT` privilege on the `magic` materialized view
it inherits through the `data_reader` role:

```none
        grantor         |   grantee   |  database   | schema |        name         |    object_type    | privilege_type
------------------------+-------------+-------------+--------+---------------------+-------------------+----------------
 blue.berry@example.com | data_reader | mydb        | sales  | magic               | materialized-view | SELECT
```






**Specify PUBLIC as the object creator:**


With the exception of the `PUBLIC` role, the `<object_creator>` role is
**not** transitive. That is, default privileges that specify a functional
role like `view_manager` as the `<object_creator>` do **not** apply to
objects created by its members.

To illustrate, the following:
- creates a new user `lemon@example.com`,
- adds the user to the `view_manager` role, and
- creates a new default privilege, specifying `view_manager` as the
`<object_creator>`.
```mzsql
CREATE ROLE "lemon@example.com" WITH LOGIN PASSWORD '<password>';
GRANT view_manager TO "lemon@example.com";

ALTER DEFAULT PRIVILEGES FOR ROLE view_manager
IN SCHEMA mydb.sales -- Optional. If specified, need USAGE on database and schema.
GRANT INSERT ON TABLES TO view_manager;
-- Although `TABLES` refers to tables, views, materialized views, and
-- sources, the INSERT privilege will only apply to tables.

```


If `lemon@example.com` creates a new table `only_lemon`, the above default
`INSERT` privilege will not apply as the object creator must be
`view_manager`, not a member of `view_manager`.
```mzsql
-- Run as `lemon@example.com` (a member of `view_manager`)
SET CLUSTER TO compute_cluster;
SET DATABASE TO mydb;
SET SCHEMA TO sales;

CREATE TABLE only_lemon (id INT);

SHOW PRIVILEGES FOR view_manager where name = 'only_lemon';

```
The `SHOW PRIVILEGES FOR view_manager  where name = 'only_lemon';` returns 0
rows.



However, if `view_manager` is the **only role** that has `CREATE` privileges
on `mydb.sales` schema, you can specify `PUBLIC` as the `<object_creator>`.
Then, the default privilege will apply to all objects created by
`view_manager` and its members.
```mzsql
ALTER DEFAULT PRIVILEGES FOR ROLE PUBLIC
IN SCHEMA mydb.sales
GRANT INSERT ON TABLES TO view_manager;
-- Although `TABLES` refers to tables, views, materialized views, and
-- sources, the `CREATE` privilege will only apply to tables.

```




If `lemon@example.com` now creates a new table `shared_lemon`, the above
default `INSERT` privilege will be granted to `view_manager`.
```mzsql
-- Run as `lemon@example.com`
SET CLUSTER TO compute_cluster;
SET DATABASE TO mydb;
SET SCHEMA TO sales;

CREATE TABLE shared_lemon (id INT);

```

To verify that the default privileges have been automatically granted to others,
you can run `SHOW PRIVILEGES`:


**view_manager:**


Verify the privileges for `view_manager`:
```mzsql
SHOW PRIVILEGES FOR view_manager where name = 'shared_lemon';

```The returned privileges should include the `INSERT` privilege on the
`shared_lemon` table.

```none
      grantor       |   grantee    | database | schema |     name     | object_type | privilege_type
--------------------+--------------+----------+--------+--------------+-------------+----------------
  lemon@example.com | view_manager | mydb     | sales  | shared_lemon | table       | INSERT
```


**blue.berry@example.com:**

Verify the privileges for `blue.berry@example.com`:
```mzsql
SHOW PRIVILEGES FOR "blue.berry@example.com" where name = 'shared_lemon';

```The returned privileges should include the `INSERT` privilege on the
`shared_lemon` table.

```none
      grantor       |   grantee    | database | schema |     name     | object_type | privilege_type
--------------------+--------------+----------+--------+--------------+-------------+----------------
  lemon@example.com | view_manager | mydb     | sales  | shared_lemon | table       | INSERT
```






## Show roles in system

To view the roles in the system, use the [`SHOW ROLES`](/sql/show-roles/) command:

```mzsql
SHOW ROLES [ LIKE <pattern>  | WHERE <condition(s)> ];
```


For example, to show all roles:
```mzsql
SHOW ROLES;

```
The results should list all roles:

```none
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

To remove a role from the system, use the [`DROP ROLE`](/sql/drop-role/)
command:

> **Privilege(s) required to run the command:** - `CREATEROLE` privileges on the system.


```mzsql
DROP ROLE <role>;
```

> **Note:** You cannot drop a role if it contains any members. Before dropping a role,
> revoke the role from all its members. See [Revoke a role](#revoke-a-role-from-another-role).



## Alter role

When granting privileges, the privileges may be scoped to a particular cluster,
database, and schema.

You can use [`ALTER ROLE ... SET`](/sql/alter-role/) to set various
configuration parameters, including cluster, database, and schema.

```mzsql
ALTER ROLE <role> SET <config> =|TO <value>;
```

The following example configures the `blue.berry@example.com` role to use
the `compute_cluster` cluster, `mydb` database, and `sales` schema by
default.
```mzsql
ALTER ROLE "blue.berry@example.com" SET CLUSTER = compute_cluster;
ALTER ROLE "blue.berry@example.com" SET DATABASE = mydb;
ALTER ROLE "blue.berry@example.com" SET search_path = sales; -- i.e., schema

```
- These changes will take effect in the next session for the role; the
  changes have **NO** effect on the current session.

- These configurations are just the defaults. For example, the connection
  string can specify a different database for the session or the user can
  issue a `SET ...` command to override these values for the current
  session.



In Materialize, when you grant a role to another role (user role/service
account role/independent role), the target role inherits only the privileges
of the granted role. **Role configurations are not inherited.** For example,
the following example updates the `data_reader` role to use
`serving_cluster` by default.
```mzsql
ALTER ROLE data_reader SET CLUSTER = serving_cluster;

```
This change affects only the `data_reader` role and does not affect roles
that have been granted `data_reader`, such as `sales_report_app`. That is,
after this change:

- The default cluster for `data_reader` is `serving_cluster` for new
  sessions.

- The default cluster for `sales_report_app` is not affected.

{{< tip >}}
{{% include-headless "/headless/rbac-sm/alter-role-tip" %}}
{{</ tip >}}


## Change ownership of objects

Certain [commands on an
object](/security/appendix/appendix-command-privileges/) (such as creating
an index on a materialized view or changing owner of an object) require
ownership of the object itself (or *superuser* privileges).

In Materialize, when a role creates an object, the role becomes the owner of the
object and is automatically  granted all [applicable
privileges](/security/appendix/appendix-privileges/) for the object. To
transfer ownership (and privileges) to another role (another user role/service
account role/functional role), you can use the [ALTER ... OWNER
TO](/sql/#rbac) command:

> **Privilege(s) required to run the command:** - Ownership of the object being altered. - Role membership in `new_owner`. - `CREATE` privileges on the containing cluster if the object is a cluster replica. - `CREATE` privileges on the containing database if the object is a schema. - `CREATE` privileges on the containing schema if the object is namespaced by a schema.


```mzsql
ALTER <object_type> <object_name> OWNER TO <role>;
```

Before changing the ownership, review the privileges of the current owner
(`lemon@example.com`) and the future owner (`view_manage`):

Review `lemon@example.com"`'s privileges on the `shared_lemon` table.
```mzsql
SHOW PRIVILEGES FOR "lemon@example.com" where name = 'shared_lemon';

```
As the owner, `lemon@example.com`  has all applicable privileges
(`INSERT`/`SELECT`/`UPDATE`/`DELETE`) for the table as well as the `INSERT`
through its membership in `view_manager` (from [Alter default privileges
example](/security/self-managed/access-control/manage-roles/#alter-default-privileges)).

```none
      grantor      |      grantee      | database | schema |     name     | object_type | privilege_type
-------------------+-------------------+----------+--------+--------------+-------------+----------------
 lemon@example.com | lemon@example.com | mydb     | sales  | shared_lemon | table       | DELETE
 lemon@example.com | lemon@example.com | mydb     | sales  | shared_lemon | table       | INSERT
 lemon@example.com | lemon@example.com | mydb     | sales  | shared_lemon | table       | SELECT
 lemon@example.com | lemon@example.com | mydb     | sales  | shared_lemon | table       | UPDATE
 lemon@example.com | view_manager      | mydb     | sales  | shared_lemon | table       | INSERT
```



Review `view_manager`'s privileges on the `shared_lemon` table.
```mzsql
SHOW PRIVILEGES FOR view_manager where name = 'shared_lemon';

```
The results show that the `view_manager` role has `INSERT` privileges on the
`shared_lemon` table (from [Alter default privileges
example](/security/self-managed/access-control/manage-roles/#alter-default-privileges)).

```none
      grantor      |   grantee    | database | schema |     name     | object_type | privilege_type
-------------------+--------------+----------+--------+--------------+-------------+----------------
 lemon@example.com | view_manager | mydb     | sales  | shared_lemon | table       | INSERT
```



Change the owner of the `shared_lemon` table to `view_manager`.
```mzsql
ALTER TABLE mydb.sales.shared_lemon OWNER TO view_manager;

```


After running the command, review `view_manager`'s privileges on the
`shared_lemon` table.
```mzsql
SHOW PRIVILEGES FOR view_manager where name = 'shared_lemon';

```
The results show that the `view_manager` role has all applicable privileges
for a table (`INSERT`, `SELECT`, `UPDATE`, `DELETE`):

```none
  grantor    |   grantee    | database | schema |     name     | object_type | privilege_type
-------------+--------------+----------+--------+--------------+-------------+----------------
view_manager | view_manager | mydb     | sales  | shared_lemon | table       | DELETE
view_manager | view_manager | mydb     | sales  | shared_lemon | table       | INSERT
view_manager | view_manager | mydb     | sales  | shared_lemon | table       | SELECT
view_manager | view_manager | mydb     | sales  | shared_lemon | table       | UPDATE
```



Review `lemon@example.com`'s privileges on the `shared_lemon` table.
```mzsql
SHOW PRIVILEGES FOR "lemon@example.com" where name = 'shared_lemon';

```
The results show that `lemon@example.com` now only has access through
`view_manager`.

```none
   grantor    |   grantee    | database | schema |     name     | object_type | privilege_type
--------------+--------------+----------+--------+--------------+-------------+----------------
 view_manager | view_manager | mydb     | sales  | shared_lemon | table       | DELETE
 view_manager | view_manager | mydb     | sales  | shared_lemon | table       | INSERT
 view_manager | view_manager | mydb     | sales  | shared_lemon | table       | SELECT
 view_manager | view_manager | mydb     | sales  | shared_lemon | table       | UPDATE
```


## See also

- [Access control best practices](/security/self-managed/access-control/#best-practices)
- [Manage privileges with Terraform](/manage/terraform/manage-rbac/)
