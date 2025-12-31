---
audience: developer
canonical_url: https://materialize.com/docs/security/self-managed/access-control/
complexity: beginner
description: How to configure and manage role-based database access control (RBAC)
  in Materialize.
doc_type: reference
keywords:
- CREATE ROLE
- 'Warning:'
- superuser
- SHOW ENABLE_RBAC_CHECKS
- ALTER SYSTEM
- CREATE ADDITIONAL
- Access control (Role-based)
- 'Note:'
- 'Important:'
product_area: Security
status: stable
title: Access control (Role-based)
---

# Access control (Role-based)

## Purpose
How to configure and manage role-based database access control (RBAC) in Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


How to configure and manage role-based database access control (RBAC) in Materialize.


> **Note:** 
Initially, only the `mz_system` user (which has superuser/administrator
privileges) is available to manage roles.


<a name="role-based-access-control-rbac" ></a>

## Role-based access control

In Materialize, role-based access control (RBAC) governs access to objects
through privileges granted to [database
roles](/security/self-managed/access-control/manage-roles/).

## Enabling RBAC

> **Warning:** 
If RBAC is not enabled, all users have <red>**superuser**</red> privileges.


By default, role-based access control (RBAC) checks are not enabled (i.e.,
enforced) when using [authentication](/security/self-managed/authentication/#configuring-authentication-type). To
enable RBAC, set the system parameter `enable_rbac_checks` to `'on'` or `True`.
You can enable the parameter in one of the following ways:

- For [local installations using
  Kind/Minikube](/installation/#installation-guides), set `spec.enableRbac:
  true` option when instantiating the Materialize object.

- For [Cloud deployments using Materialize's
  Terraforms](/installation/#installation-guides), set `enable_rbac_checks` in
  the environment CR via the `environmentdExtraArgs` flag option.

- After the Materialize instance is running, run the following command as
  `mz_system` user:

  ```mzsql
  ALTER SYSTEM SET enable_rbac_checks = 'on';
  ```text

If more than one method is used, the `ALTER SYSTEM` command will take precedence
over the Kubernetes configuration.

To view the current value for `enable_rbac_checks`, run the following `SHOW`
command:

```mzsql
SHOW enable_rbac_checks;
```text

> **Important:** 
If RBAC is not enabled, all users have <red>**superuser**</red> privileges.


## Roles and privileges

<!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/db-rol... -->

- To create additional users or service accounts, login as the `mz_system` user,
using the `external_login_password_mz_system` password, and use [`CREATE ROLE
... WITH LOGIN PASSWORD ...`](/sql/create-role):

```mzsql
CREATE ROLE <user> WITH LOGIN PASSWORD '<password>';
```text


- To create functional roles, login as the `mz_system` user,
using the `external_login_password_mz_system` password, and use [`CREATE ROLE`](/sql/create-role):

```mzsql
CREATE ROLE <role>;
```bash


### Managing privileges

<!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/db-rol... -->

> **Disambiguation:** <!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/grant-... -->

### Initial privileges

<!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/db-rol... -->

<!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/public... -->

In addition, all roles have:
- `USAGE` on all built-in types and [all system catalog
schemas](/sql/system-catalog/).
- `SELECT` on [system catalog objects](/sql/system-catalog/).
- All [applicable privileges](/security/appendix/appendix-privileges/) for
  an object they create; for example, the creator of a schema gets `CREATE` and
  `USAGE`; the creator of a table gets `SELECT`, `INSERT`, `UPDATE`, and
  `DELETE`.


You can modify the privileges of your organization's `PUBLIC` role as well as
the modify default privileges for `PUBLIC`.

## Privilege inheritance and modular access control

In Materialize, when you grant a role to another role (user role/service account
role/independent role), the target role inherits privileges through the granted
role.

In general, to grant a user or service account privileges, create roles with the
desired privileges and grant these roles to the user or service account role.
Although you can grant privileges directly to the user or service account role,
using separate, reusable roles is recommended for better access management.

With privilege inheritance, you can compose more complex roles by
combining existing roles, enabling modular access control. However:

- Inheritance only applies to role privileges; role attributes and parameters
  are not inherited.
- <!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/revoke... -->

## Best practices

<!-- Unresolved shortcode: {{% yaml-sections data="rbac/recommendations-sm" h... -->


---

## Manage database roles


In Materialize, role-based access control (RBAC) governs access to objects
through privileges granted to database roles.

## Enabling RBAC

> **Warning:** 
If RBAC is not enabled, all users have <red>**superuser**</red> privileges.


By default, role-based access control (RBAC) checks are not enabled (i.e.,
enforced) when using [authentication](/security/self-managed/authentication/#configuring-authentication-type). To
enable RBAC, set the system parameter `enable_rbac_checks` to `'on'` or `True`.
You can enable the parameter in one of the following ways:

- For [local installations using
  Kind/Minikube](/installation/#installation-guides), set `spec.enableRbac:
  true` option when instantiating the Materialize object.

- For [Cloud deployments using Materialize's
  Terraforms](/installation/#installation-guides), set `enable_rbac_checks` in
  the environment CR via the `environmentdExtraArgs` flag option.

- After the Materialize instance is running, run the following command as
  `mz_system` user:

  ```mzsql
  ALTER SYSTEM SET enable_rbac_checks = 'on';
  ```text

If more than one method is used, the `ALTER SYSTEM` command will take precedence
over the Kubernetes configuration.

To view the current value for `enable_rbac_checks`, run the following `SHOW`
command:

```mzsql
SHOW enable_rbac_checks;
```text

> **Important:** 
If RBAC is not enabled, all users have <red>**superuser**</red> privileges.


## Required privileges for managing roles

> **Note:** 
Initially, only the `mz_system` user (which has superuser/administrator
privileges) is available to manage roles.


| Role management operations          | Required privileges      |
| ----------------------------------- | ------------------------ |
| To create/revoke/grant roles        | - `CREATEROLE` privileges on the system.
 > **Warning:** 
Roles with the `CREATEROLE` privilege can obtain the privileges of any other
role in the system by granting themselves that role. Avoid granting
`CREATEROLE` unnecessarily.

 |
| To view privileges for a role       | None                      |
| To grant/revoke role privileges     | - Ownership of affected objects.
- `USAGE` privileges on the containing database if the affected object is a schema.
- `USAGE` privileges on the containing schema if the affected object is namespaced by a schema.
- _superuser_ status if the privilege is a system privilege.
|
| To alter default privileges         | - Role membership in `role_name`.
- `USAGE` privileges on the containing database if `database_name` is specified.
- `USAGE` privileges on the containing schema if `schema_name` is specified.
- _superuser_ status if the _target_role_ is `PUBLIC` or **ALL ROLES** is
  specified.
 |

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
```text


> **Privilege(s) required to run the command:** - `CREATEROLE` privileges on the system.

For example, the following creates:

- A new user `blue.berry@example.com` (or more specifically, a new user role).
- A new service account `sales_report_app` (or more specifically, a new service
  account role).


#### A new user role

<!-- Example: examples/rbac-sm/create_roles / create-role-user -->

#### A new service account role

<!-- Example: examples/rbac-sm/create_roles / create-role-service-account -->


In Materialize, a role is created with inheritance support. With inheritance,
when a role is granted to another role (i.e., the target role), the target role
inherits privileges (not role attributes and parameters) through the other role.
All roles in Materialize are automatically members of
[`PUBLIC`](/security/appendix/appendix-built-in-roles/#public-role). As
such, every role includes inherited privileges from `PUBLIC`.


<!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/db-rol... -->

> **Disambiguation:** <!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/grant-... -->

See also:

- For a list of required privileges for specific operations, see [Appendix:
Privileges by command](/security/appendix/appendix-command-privileges/).

### Create functional roles

To create functional roles, login as the `mz_system` user,
using the `external_login_password_mz_system` password, and use [`CREATE ROLE`](/sql/create-role):

```mzsql
CREATE ROLE <role>;
```text


> **Tip:** 
Role names cannot start with `mz_` and `pg_` as they are reserved for system
roles.


For example, the following creates:
- A role for users who need to perform compute/transform operations in the
  compute/transform.
- A role for users who need to manage indexes on the serving cluster(s).
- A role for users who need to read results from the serving cluster.


#### View manager role

<!-- Example: examples/rbac-sm/create_roles / create-role-view-manager -->

#### Serving index manager role

<!-- Example: examples/rbac-sm/create_roles / create-role-serving-index-manager -->

#### Data reader role

<!-- Example: examples/rbac-sm/create_roles / create-role-data-reader -->


In Materialize, a role is created with inheritance support. With inheritance,
when a role is granted to another role (i.e., the target role), the target role
inherits privileges (not role attributes and parameters) through the other role.
All roles in Materialize are automatically members of
[`PUBLIC`](/security/appendix/appendix-built-in-roles/#public-role). As
such, every role includes inherited privileges from `PUBLIC`.


<!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/db-rol... -->

> **Disambiguation:** <!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/grant-... -->

See also:

- For a list of required privileges for specific operations, see [Appendix:
Privileges by command](/security/appendix/appendix-command-privileges/).

## Manage current privileges for a role

> **Note:** 

- The examples below assume the existence of a `mydb` database and a `sales`
schema within the `mydb` database.

- The examples below assume the roles only need privileges to objects in the
  `mydb.sales` schema.


### View privileges for a role

> **Privilege(s) required to run the command:** No specific privilege is required to run the `SHOW PRIVILEGES`

To view privileges granted to a role, you can use the [`SHOW
PRIVILEGES`](/sql/show-privileges) command, substituting `<role>` with the role
name (see [`SHOW PRIVILEGES`](/sql/show-default-privileges) for the full
syntax):

```mzsql
SHOW PRIVILEGES FOR <role>;
```text

> **Note:** 
All roles in Materialize are automatically members of
[`PUBLIC`](/security/appendix/appendix-built-in-roles/#public-role). As
such, every role includes inherited privileges from `PUBLIC`.


For example:


#### User


<!-- Example: examples/rbac-sm/show_privileges / for-user -->

<!-- Example: examples/rbac-sm/show_privileges / example-results -->


#### Service account role


<!-- Example: examples/rbac-sm/show_privileges / for-service-account -->
<!-- Example: examples/rbac-sm/show_privileges / example-results -->

#### Functional roles


#### View manager role

<!-- Example: examples/rbac-sm/show_privileges / for-view-manager -->
<!-- Example: examples/rbac-sm/show_privileges / example-results -->

#### Serving index manager role

<!-- Example: examples/rbac-sm/show_privileges / for-serving-index-manager -->
<!-- Example: examples/rbac-sm/show_privileges / example-results -->

#### Data reader role

<!-- Example: examples/rbac-sm/show_privileges / for-data-reader -->
<!-- Example: examples/rbac-sm/show_privileges / example-results -->


> **Tip:** 

For the `SHOW PRIVILEGES` command, you can add a `WHERE` clause to filter by the
return fields; e.g., `SHOW PRIVILEGES FOR view_manager WHERE
name='quickstart';`.


### Grant privileges to a role

To grant [privileges](/security/appendix/appendix-command-privileges/) to
a role, use the [`GRANT PRIVILEGE`](/sql/grant-privilege/) statement (see
[`GRANT PRIVILEGE`](/sql/grant-privilege/) for the full syntax)

> **Privilege(s) required to run the command:** - Ownership of affected objects.
- `USAGE` privileges on the containing database if the affected object is a schema.
- `USAGE` privileges on the containing schema if the affected object is namespaced by a schema.
- _superuser_ status if the privilege is a system privilege.


To override the **object ownership** requirements to grant privileges, run as a
user with superuser privileges; e.g. `mz_system` user.

```mzsql
GRANT <PRIVILEGE> ON <OBJECT_TYPE> <object_name> TO <role>;
```text

When possible, avoid granting privileges directly to individual user or service
account roles. Instead, create reusable, functional roles (e.g., `data_reader`,
`view_manager`) with well-defined privileges, and grant these roles to the
individual user or service account roles. You can also grant functional roles to
other functional roles to compose more complex functional roles.


For example, the following grants privileges to the functional roles.

> **Note:** 
Various SQL operations require additional privileges on related objects, such
as:

- For objects that use compute resources (e.g., indexes, materialized views,
  replicas, sources, sinks), access is also required for the associated cluster.

- For objects in a schema, access is also required for the schema.

For details on SQL operations and needed privileges, see [Appendix: Privileges
by command](/security/appendix/appendix-command-privileges/).


#### View manager role

<!-- Example: examples/rbac-sm/grant_privileges / to-view-manager -->

<!-- Example: examples/rbac-sm/grant_privileges / to-view-manager-results -->

#### Serving index manager role

<!-- Example: examples/rbac-sm/grant_privileges / to-serving-index-manager -->

<!-- Example: examples/rbac-sm/grant_privileges / to-serving-index-manager-results -->

#### Data reader role

<!-- Example: examples/rbac-sm/grant_privileges / to-data-reader -->

<!-- Example: examples/rbac-sm/grant_privileges / to-data-reader-results -->


### Grant a role to another role

Once a role is created, you can modify its privileges either:

- Directly by [granting privileges for a role](#grant-privileges-to-a-role) or
  [revoking privileges from a role](#revoke-privileges-from-a-role).
- Indirectly (through inheritance) by granting other roles to the role or
  [revoking roles from the role](#revoke-a-role-from-another-role).

> **Tip:** 

When possible, avoid granting privileges directly to individual user or service
account roles. Instead, create reusable, functional roles (e.g., `data_reader`,
`view_manager`) with well-defined privileges, and grant these roles to the
individual user or service account roles. You can also grant functional roles to
other functional roles to compose more complex functional roles.


To grant a role to another role (where the role can be a user role/service
account role/functional role), use the [`GRANT ROLE`](/sql/grant-role/)
statement (see [`GRANT ROLE`](/sql/grant-role/) for full syntax):

> **Privilege(s) required to run the command:** - `CREATEROLE` privileges on the system.


`mz_system` user has the required privileges on the system.

```mzsql
GRANT <role> [, <role>...] to <target_role> [, <target_role> ...];
```text

When a role is granted to another role, the target role becomes a member of the
other role and inherits the privileges through the other role.

In the following examples,

- The functional role `view_manager` is granted to the user role
  `blue.berry@example.com`.
- The functional role `serving_index_manager` is granted to the functional role
  `view_manager`.
- The functional role `data_reader` is granted to the service account role
  `sales_report_app`.


#### Grant view_manager role


<!-- Example: examples/rbac-sm/grant_roles / view_manager -->

<!-- Example: examples/rbac-sm/grant_roles / view_manager-results-show-privileges -->

<!-- Example: examples/rbac-sm/grant_roles / view_manager-results-create-objects -->


#### Grant serving_index_manager role

<!-- Example: examples/rbac-sm/grant_roles / serving_index_manager -->

Review the privileges of `view_manager` as well as `"blue.berry@example.com"`
(a member of  `view_manager`) after the grant.


#### Privileges for view_manager

<!-- Example: examples/rbac-sm/grant_roles / serving_index_manager-results-show-privileges-view_manager -->


#### Privileges for blue.berry@example.com


<!-- Example: examples/rbac-sm/grant_roles / serving_index_manager-results-show-privileges-view_manager-member -->


<!-- Example: examples/rbac-sm/grant_roles / serving_index_manager-results-show-privileges-results -->

<!-- Example: examples/rbac-sm/grant_roles / serving_index_manager-results-create-index -->


#### Grant data_reader role

<!-- Example: examples/rbac-sm/grant_roles / data_reader -->

<!-- Example: examples/rbac-sm/grant_roles / data_reader-results-show-privileges -->

<!-- Example: examples/rbac-sm/grant_roles / data_reader-results-select -->


### Revoke privileges from a role

To remove privileges from a role, use the [`REVOKE <privilege>`](/sql/revoke-privilege/) statement:

> **Privilege(s) required to run the command:** - Ownership of affected objects.
- `USAGE` privileges on the containing database if the affected object is a schema.
- `USAGE` privileges on the containing schema if the affected object is namespaced by a schema.
- _superuser_ status if the privilege is a system privilege.

```mzsql
REVOKE <PRIVILEGE> ON <OBJECT_TYPE> <object_name> FROM <role>;
```bash

### Revoke a role from another role

To revoke a role from another role, use the [`REVOKE <role>`](/sql/revoke-role/) statement:

> **Privilege(s) required to run the command:** - `CREATEROLE` privileges on the systems.

```mzsql
REVOKE <role> FROM <target_role>;
```text

For example:

```mzsql
REVOKE data_reader FROM sales_report_app;
```text

> **Important:** 
When you revoke a role from another role (user role/service account
role/independent role), the target role is no longer a member of the revoked
role nor inherits the revoked role's privileges. **However**, privileges are
cumulative: if the target role inherits the same privilege(s) from another role,
the target role still has the privilege(s) through the other role.


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


> **Disambiguation:** <!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/grant-... -->

### View default privileges

To view default privileges, you can use the [`SHOW DEFAULT
PRIVILEGES`](/sql/show-default-privileges) command, substituting `<role>` with
the role name (see [`SHOW DEFAULT PRIVILEGES`](/sql/show-default-privileges) for
the full syntax):

> **Privilege(s) required to run the command:** No specific privilege is required to run the `SHOW DEFAULT PRIVILEGES`.

```mzsql
SHOW DEFAULT PRIVILEGES FOR <role>;
```text

For example:


#### User


<!-- Example: examples/rbac-sm/show_default_privileges / for-user -->


#### Service account role


<!-- Example: examples/rbac-sm/show_default_privileges / for-service-account -->


#### Functional roles


#### View manager role

<!-- Example: examples/rbac-sm/show_default_privileges / for-view-manager -->

#### Serving index manager role

<!-- Example: examples/rbac-sm/show_default_privileges / for-serving-index-manager -->

#### Data reader role

<!-- Example: examples/rbac-sm/show_default_privileges / for-data-reader -->


### Alter default privileges

To define default privilege for objects created by a role, use the [`ALTER
DEFAULT PRIVILEGES`](/sql/alter-default-privileges) command (see  [`ALTER
DEFAULT PRIVILEGES`](/sql/alter-default-privileges) for the full syntax):

> **Privilege(s) required to run the command:** - Role membership in `role_name`.
- `USAGE` privileges on the containing database if `database_name` is specified.
- `USAGE` privileges on the containing schema if `schema_name` is specified.
- _superuser_ status if the _target_role_ is `PUBLIC` or **ALL ROLES** is
  specified.

```mzsql
ALTER DEFAULT PRIVILEGES FOR ROLE <object_creator>
   IN SCHEMA <schema>    -- Optional. If specified, need USAGE on database and schema.
   GRANT <privilege> ON <object_type> TO <target_role>;
```text

> **Note:** 
- With the exception of the `PUBLIC` role, the `<object_creator>` role is
  **not** transitive. That is, default privileges that specify a functional role
  like `view_manager` as the `<object_creator>` do **not** apply to objects
  created by its members.

  However, you can approximate default privileges for a functional role by
  restricting `CREATE` privileges for the objects to the desired functional
  roles (e.g., only `view_managers` have privileges to create tables in
  `mydb.sales` schema) and then specify `PUBLIC` as the `<object_creator>`.

- As with any other grants, the privileges granted to the `<target_role>` are
  inherited by the members of the `<target_role>`.


#### Specify blue.berry as the object creator

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_d... -->

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_d... -->

To verify that the default privileges have been automatically granted, you can
run `SHOW PRIVILEGES`:


#### view_manager

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_d... -->

#### data_reader


<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_d... -->


#### sales_report_app (a member of data_reader)

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_d... -->


#### Specify PUBLIC as the object creator


<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_d... -->

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_d... -->

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_d... -->


<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_d... -->

To verify that the default privileges have been automatically granted to others,
you can run `SHOW PRIVILEGES`:


#### view_manager


<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_d... -->

#### blue.berry@example.com

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_d... -->


## Show roles in system

To view the roles in the system, use the [`SHOW ROLES`](/sql/show-roles/) command:

```mzsql
SHOW ROLES [ LIKE <pattern>  | WHERE <condition(s)> ];
```text

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/show_ro... -->

## Drop a role

To remove a role from the system, use the [`DROP ROLE`](/sql/drop-role/)
command:

> **Privilege(s) required to run the command:** - `CREATEROLE` privileges on the system.

```mzsql
DROP ROLE <role>;
```text

> **Note:** 
You cannot drop a role if it contains any members. Before dropping a role,
revoke the role from all its members. See [Revoke a role](#revoke-a-role-from-another-role).


## Alter role

When granting privileges, the privileges may be scoped to a particular cluster,
database, and schema.

You can use [`ALTER ROLE ... SET`](/sql/alter-role/) to set various
configuration parameters, including cluster, database, and schema.

```mzsql
ALTER ROLE <role> SET <config> =|TO <value>;
```text
<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_r... -->

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_r... -->

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

> **Privilege(s) required to run the command:** - Ownership of the object being altered.
- Role membership in `new_owner`.
- `CREATE` privileges on the containing cluster if the object is a cluster replica.
- `CREATE` privileges on the containing database if the object is a schema.
- `CREATE` privileges on the containing schema if the object is namespaced by a
  schema.

```mzsql
ALTER <object_type> <object_name> OWNER TO <role>;
```

Before changing the ownership, review the privileges of the current owner
(`lemon@example.com`) and the future owner (`view_manage`):

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_o... -->

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_o... -->

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_o... -->

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_o... -->

<!-- Unresolved shortcode: {{% include-example file="examples/rbac-sm/alter_o... -->

## See also

- [Access control best practices](/security/self-managed/access-control/#best-practices)
- [Manage privileges with Terraform](/manage/terraform/manage-rbac/)