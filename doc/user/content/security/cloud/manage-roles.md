---
title: "Manage database roles"
description: "Create and manage database roles and privileges in Cloud Materialize."
menu:
  main:
    parent: security-cloud
    weight: 15
aliases:
  - /sql/builtin-roles/
  - /manage/access-control/manage-privileges/
  - /manage/access-control/rbac-tutorial/
---

In Materialize, role-based access control (RBAC) governs access to **database
objects** through privileges granted to database roles.

{{< annotation type="Disambiguation" >}}

{{< include-md file="shared-content/rbac/rbac-intro-disambiguation.md" >}}

The focus of this page is on managing database roles. For information on
organization roles, see [Users and service
accounts](/manage/users-service-accounts/).
{{< /annotation >}}

## Required privileges for managing roles

{{< note >}}

With their **superuser** privileges, [**Organization
admins**](/manage/users-service-accounts/#organization-roles) can manage roles
(including overriding ownership requirements when granting privileges on various
objects).

{{</ note >}}

| Role management operations          | Required privileges      |
| ----------------------------------- | ------------------------ |
| To create/revoke/grant roles        | {{< include-md
file="shared-content/sql-command-privileges/create-role.md" >}} {{< warning >}}
{{< include-md file="shared-content/rbac/createrole-consideration.md" >}}
{{</ warning >}} |
| To view privileges for a role       | None                      |
| To grant/revoke role privileges     | {{< include-md
file="shared-content/sql-command-privileges/grant-privilege.md" >}}|
| To alter default privileges         | {{< include-md
file="shared-content/sql-command-privileges/alter-default-privileges.md" >}} |

See also [Appendix: Privileges by
command](/manage/access-control/appendix-command-privileges/)

## Create a role

{{< include-md file="shared-content/rbac/db-roles.md" >}}

To create a new role manually, use the [`CREATE ROLE`](/sql/create-role/)
statement.

{{< annotation type="Privilege(s) required to run the command" >}}

{{< include-md file="shared-content/sql-command-privileges/create-role.md" >}}

{{</ annotation>}}

```mzsql
CREATE ROLE <role_name> [WITH INHERIT];
-- WITH INHERIT behavior is implied and does not need to be specified.
```

{{< tip >}}
{{< include-md file="shared-content/rbac/role-name-restrictions.md" >}}
{{</ tip >}}

For example, the following creates:
- A role for users who need to perform compute/transform operations in the
  compute/transform.
- A role for users who need to manage indexes on the serving cluster(s).
- A role for users who need to read results from the serving cluster.

{{< tabs >}}
{{< tab "View manager role" >}}
{{< include-example file="examples/rbac/create_roles"
example="create-role-view-manager" >}}
{{</ tab >}}
{{< tab "Serving index manager role" >}}
{{< include-example file="examples/rbac/create_roles"
example="create-role-serving-index-manager" >}}
{{</ tab >}}
{{< tab "Data reader role" >}}
{{< include-example file="examples/rbac/create_roles" example="create-role-data-reader">}}
{{</ tab >}}
{{</ tabs >}}

In Materialize, a role is created with inheritance support. With inheritance,
when a role is granted to another role (i.e., the target role), the target role
inherits privileges (not role attributes and parameters) through the other role.
{{< include-md file="shared-content/rbac/db-roles-public-membership.md" >}}

{{% include-md file="shared-content/rbac/db-roles-managing-privileges.md" %}}

{{< annotation type="Disambiguation" >}}
{{% include-md file="shared-content/rbac/grant-vs-alter-default-privilege.md"
%}}
{{</ annotation >}}

See also:

- For a list of required privileges for specific operations, see [Appendix:
Privileges by command](/manage/access-control/appendix-command-privileges/).

## Manage current privileges for a role

### Example prerequisites

The examples below assume:

- The existence of a `source_cluster`, a `compute_cluster`, and a
  `serving_cluster`. For example:

  {{% include-example file="examples/rbac/manage_roles_prereq"
  example="create-clusters" %}}

- The existence of a `mydb` database and a `sales` schema within the `mydb`
  database. For example:

  {{% include-example file="examples/rbac/manage_roles_prereq"
  example="create-db-schema" %}}

- The existence of `items`, `orders`, and `sales_items` tables within the
  `mydb.sales` schema. For example:

  {{% include-example file="examples/rbac/manage_roles_prereq"
  example="create-tables" %}}

### View privileges for a role

{{< annotation type="Privilege(s) required to run the command" >}}

No specific privilege is required to run the `SHOW PRIVILEGES`

{{</ annotation>}}

To view privileges granted to a role, you can use the [`SHOW
PRIVILEGES`](/sql/show-privileges) command, substituting `<role>` with the role
name (see [`SHOW PRIVILEGES`](/sql/show-default-privileges) for the full
syntax):

```mzsql
SHOW PRIVILEGES FOR <role>;
```

{{< note >}}
{{< include-md file="shared-content/rbac/db-roles-public-membership.md" >}}
{{</ note >}}

For example:

{{< tabs >}}
{{< tab "User">}}

{{< include-example file="examples/rbac/show_privileges"
example="for-user">}}

{{< include-example file="examples/rbac/show_privileges"
example="example-results">}}

{{</ tab >}}
{{< tab "Service account role">}}

{{< include-example file="examples/rbac/show_privileges"
example="for-service-account">}}
{{< include-example file="examples/rbac/show_privileges"
example="example-results">}}
{{</ tab >}}
{{< tab "Manually created functional roles">}}
{{< tabs >}}
{{< tab "View manager role" >}}
{{< include-example file="examples/rbac/show_privileges"
example="for-view-manager" >}}
{{< include-example file="examples/rbac/show_privileges"
example="example-results">}}
{{</ tab >}}
{{< tab "Serving index manager role" >}}
{{< include-example file="examples/rbac/show_privileges"
example="for-serving-index-manager" >}}
{{< include-example file="examples/rbac/show_privileges"
example="example-results">}}
{{</ tab >}}
{{< tab "Data reader role" >}}
{{< include-example file="examples/rbac/show_privileges"
example="for-data-reader">}}
{{< include-example file="examples/rbac/show_privileges"
example="example-results">}}
{{</ tab >}}
{{</ tabs >}}

{{</ tab >}}
{{</ tabs >}}

{{< tip >}}

For the `SHOW PRIVILEGES` command, you can add a `WHERE` clause to filter by the
return fields; e.g., `SHOW PRIVILEGES FOR view_manager WHERE
name='quickstart';`.

{{</ tip >}}

### Grant privileges to a role

To grant [privileges](/manage/access-control/appendix-command-privileges/) to
a role, use the [`GRANT PRIVILEGE`](/sql/grant-privilege/) statement (see
[`GRANT PRIVILEGE`](/sql/grant-privilege/) for the full syntax)

{{< annotation type="Privilege(s) required to run the command" >}}

{{< include-md file="shared-content/sql-command-privileges/grant-privilege.md"
>}}

To override the **object ownership** requirements to grant privileges, run as an
Organization admin.

{{</ annotation>}}

```mzsql
GRANT <PRIVILEGE> ON <OBJECT_TYPE> <object_name> TO <role>;
```

{{< include-md file="shared-content/rbac/use-resusable-roles.md" >}}

For example, the following grants privileges to the manually created functional
roles.

{{< note >}}
{{< include-md file="shared-content/rbac/privileges-related-objects.md" >}}
{{</ note >}}

{{< tabs >}}
{{< tab "View manager role" >}}
{{< include-example file="examples/rbac/grant_privileges"
example="to-view-manager" >}}

{{< include-example file="examples/rbac/grant_privileges"
example="to-view-manager-results" >}}
{{</ tab >}}
{{< tab "Serving index manager role" >}}
{{< include-example file="examples/rbac/grant_privileges"
example="to-serving-index-manager" >}}

{{< include-example file="examples/rbac/grant_privileges"
example="to-serving-index-manager-results" >}}
{{</ tab >}}
{{< tab "Data reader role" >}}
{{< include-example file="examples/rbac/grant_privileges"
example="to-data-reader">}}

{{< include-example file="examples/rbac/grant_privileges" example="to-data-reader-results">}}
{{</ tab >}}
{{</ tabs >}}

### Grant a role to another role

Once a role is created, you can modify its privileges either:

- Directly by [granting privileges for a role](#grant-privileges-to-a-role) or
  [revoking privileges from a role](#revoke-privileges-from-a-role).
- Indirectly (through inheritance) by granting other roles to the role or
  [revoking roles from the role](#revoke-a-role-from-another-role).

{{< tip >}}

{{< include-md file="shared-content/rbac/use-resusable-roles.md" >}}

{{</ tip >}}

To grant a role to another role (where the role can be a user role/service
account role/functional role), use the [`GRANT ROLE`](/sql/grant-role/)
statement (see [`GRANT ROLE`](/sql/grant-role/) for full syntax):

{{< annotation type="Privilege(s) required to run the command" >}}

{{< include-md file="shared-content/sql-command-privileges/grant-role.md"
>}}

Organization admin has the required privileges on the system.
{{</ annotation>}}

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

{{< tabs >}}
{{< tab "Grant view_manager role" >}}

{{< include-example file="examples/rbac/grant_roles"
example="view_manager" >}}

{{< include-example file="examples/rbac/grant_roles"
example="view_manager-results-show-privileges" >}}

{{< include-example file="examples/rbac/grant_roles"
example="view_manager-results-create-objects" >}}

{{</ tab >}}

{{< tab "Grant serving_index_manager role" >}}
{{< include-example file="examples/rbac/grant_roles"
example="serving_index_manager" >}}

Review the privileges of `view_manager` as well as `"blue.berry@example.com"`
(a member of  `view_manager`) after the grant.

{{< tabs >}}
{{< tab "Privileges for view_manager">}}
{{< include-example file="examples/rbac/grant_roles"
example="serving_index_manager-results-show-privileges-view_manager"
>}}

{{</ tab >}}
{{< tab "Privileges for blue.berry@example.com">}}

{{< include-example file="examples/rbac/grant_roles"
example="serving_index_manager-results-show-privileges-view_manager-member"
>}}

{{</ tab >}}
{{</ tabs >}}

{{< include-example file="examples/rbac/grant_roles"
example="serving_index_manager-results-show-privileges-results"
>}}

{{< include-example file="examples/rbac/grant_roles"
example="serving_index_manager-results-create-index" >}}

{{</ tab >}}

{{< tab "Grant data_reader role" >}}
{{< include-example file="examples/rbac/grant_roles"
example="data_reader" >}}

{{< include-example file="examples/rbac/grant_roles"
example="data_reader-results-show-privileges" >}}

{{< include-example file="examples/rbac/grant_roles"
example="data_reader-results-select" >}}

{{</ tab >}}
{{</ tabs >}}

### Revoke privileges from a role

To remove privileges from a role, use the [`REVOKE <privilege>`](/sql/revoke-privilege/) statement:

{{< annotation type="Privilege(s) required to run the command" >}}

{{< include-md file="shared-content/sql-command-privileges/revoke-privilege.md"
>}}

{{</ annotation>}}

```mzsql
REVOKE <PRIVILEGE> ON <OBJECT_TYPE> <object_name> FROM <role>;
```

### Revoke a role from another role

To revoke a role from another role, use the [`REVOKE <role>`](/sql/revoke-role/) statement:

{{< annotation type="Privilege(s) required to run the command" >}}

{{< include-md file="shared-content/sql-command-privileges/revoke-role.md"
>}}

{{</ annotation>}}

```mzsql
REVOKE <role> FROM <target_role>;
```

For example:

```mzsql
REVOKE data_reader FROM sales_report_app;
```

{{< important >}}
{{< include-md file="shared-content/rbac/revoke-roles-consideration.md" >}}
{{</ important >}}

## Manage future privileges for a role

In Materialize, a role automatically gets all [applicable
privileges](/manage/access-control/appendix-privileges/) for an object they
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

{{< include-md file="shared-content/rbac/default-privilege-clarification.md" >}}

{{< annotation type="Disambiguation" >}}
{{% include-md file="shared-content/rbac/grant-vs-alter-default-privilege.md"
%}}

{{</ annotation >}}

### View default privileges

To view default privileges, you can use the [`SHOW DEFAULT
PRIVILEGES`](/sql/show-default-privileges) command, substituting `<role>` with
the role name (see [`SHOW DEFAULT PRIVILEGES`](/sql/show-default-privileges) for
the full syntax):

{{< annotation type="Privilege(s) required to run the command" >}}

No specific privilege is required to run the `SHOW DEFAULT PRIVILEGES`.

{{</ annotation>}}

```mzsql
SHOW DEFAULT PRIVILEGES FOR <role>;
```

For example:

{{< tabs >}}
{{< tab "User">}}

{{< include-example file="examples/rbac/show_default_privileges"
example="for-user">}}

{{</ tab >}}
{{< tab "Service account role">}}

{{< include-example file="examples/rbac/show_default_privileges"
example="for-service-account">}}

{{</ tab >}}
{{< tab "Manually created functional roles">}}
{{< tabs >}}
{{< tab "View manager role" >}}
{{< include-example file="examples/rbac/show_default_privileges"
example="for-view-manager" >}}
{{</ tab >}}
{{< tab "Serving index manager role" >}}
{{< include-example file="examples/rbac/show_default_privileges"
example="for-serving-index-manager" >}}
{{</ tab >}}
{{< tab "Data reader role" >}}
{{< include-example file="examples/rbac/show_default_privileges" example="for-data-reader">}}
{{</ tab >}}
{{</ tabs >}}

{{</ tab >}}
{{</ tabs >}}

### Alter default privileges

To define default privilege for objects created by a role, use the [`ALTER
DEFAULT PRIVILEGES`](/sql/alter-default-privileges) command (see  [`ALTER
DEFAULT PRIVILEGES`](/sql/alter-default-privileges) for the full syntax):

{{< annotation type="Privilege(s) required to run the command" >}}

{{< include-md
file="shared-content/sql-command-privileges/alter-default-privileges.md"
>}}

{{</ annotation>}}

```mzsql
ALTER DEFAULT PRIVILEGES FOR ROLE <object_creator>
   IN SCHEMA <schema>    -- Optional. If specified, need USAGE on database and schema.
   GRANT <privilege> ON <object_type> TO <target_role>;
```

{{< note >}}
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

{{</ note >}}

{{< tabs >}}
{{< tab "Specify blue.berry as the object creator" >}}
{{% include-example file="examples/rbac/alter_default_privileges"
example="for-tables-created-by-blueberry" %}}

{{% include-example file="examples/rbac/alter_default_privileges"
example="for-tables-created-by-blueberry-verification-setup" %}}

To verify that the default privileges have been automatically granted, you can
run `SHOW PRIVILEGES`:

{{< tabs >}}

{{< tab "view_manager">}}
{{% include-example file="examples/rbac/alter_default_privileges"
example="for-tables-created-by-blueberry-verification-view_manager" %}}
{{</ tab >}}
{{< tab "data_reader" >}}

{{% include-example file="examples/rbac/alter_default_privileges"
example="for-tables-created-by-blueberry-verification-data_reader" %}}

{{</ tab >}}
{{< tab "sales_report_app (a member of data_reader)" >}}
{{% include-example file="examples/rbac/alter_default_privileges"
example="for-tables-created-by-blueberry-verification-sales_report_app" %}}
{{</ tab >}}
{{</ tabs >}}


{{</ tab >}}
{{< tab "Specify PUBLIC as the object creator" >}}

{{% include-example file="examples/rbac/alter_default_privileges"
example="for-tables-created-by-view-manager" %}}

{{% include-example file="examples/rbac/alter_default_privileges"
example="for-tables-created-by-view-manager-member" %}}

{{% include-example file="examples/rbac/alter_default_privileges"
example="for-tables-created-by-public" %}}


{{% include-example file="examples/rbac/alter_default_privileges"
example="for-tables-created-by-public-member" %}}

To verify that the default privileges have been automatically granted to others,
you can run `SHOW PRIVILEGES`:

{{< tabs >}}
{{< tab "view_manager">}}

{{% include-example file="examples/rbac/alter_default_privileges"
example="for-tables-created-by-public-member-verification-view_manager" %}}
{{</ tab >}}
{{< tab "blue.berry@example.com" >}}
{{% include-example file="examples/rbac/alter_default_privileges"
example="for-tables-created-by-public-member-verification-blueberry" %}}

{{</ tab >}}
{{</ tabs >}}
{{</ tab >}}
{{</ tabs >}}
## Show roles in system

To view the roles in the system, use the [`SHOW ROLES`](/sql/show-roles/) command:

```mzsql
SHOW ROLES [ LIKE <pattern>  | WHERE <condition(s)> ];
```

{{% include-example file="examples/rbac/show_roles" example="all-roles" %}}

## Drop a role

To remove a role from the system, use the [`DROP ROLE`](/sql/drop-role/)
command:

{{< annotation type="Privilege(s) required to run the command" >}}

{{< include-md file="shared-content/sql-command-privileges/drop-role.md"
>}}

{{</ annotation>}}

```mzsql
DROP ROLE <role>;
```

{{< note >}}
You cannot drop a role if it contains any members. Before dropping a role,
revoke the role from all its members. See [Revoke a role](#revoke-a-role-from-another-role).
{{</ note >}}


## Alter role

When granting privileges, the privileges may be scoped to a particular cluster,
database, and schema.

You can use [`ALTER ROLE ... SET`](/sql/alter-role/) to set various
configuration parameters, including cluster, database, and schema.

```mzsql
ALTER ROLE <role> SET <config> =|TO <value>;
```
{{% include-example file="examples/rbac/alter_roles"
example="alter-roles-configurations" %}}

{{% include-example file="examples/rbac/alter_roles"
example="alter-roles-configs-not-inherited" %}}

## Change ownership of objects

Certain [commands on an
object](/manage/access-control/appendix-command-privileges/) (such as creating
an index on a materialized view or changing owner of an object) require
ownership of the object itself (or *superuser* privileges of an Organization
admin).

In Materialize, when a role creates an object, the role becomes the owner of the
object and is automatically  granted all [applicable
privileges](/manage/access-control/appendix-privileges/) for the object. To
transfer ownership (and privileges) to another role (another user role/service
account role/functional role), you can use the [ALTER ... OWNER
TO](/sql/alter-owner/) command:

{{< annotation type="Privilege(s) required to run the command" >}}

{{< include-md file="shared-content/sql-command-privileges/alter-owner.md"
>}}

{{</ annotation>}}

```mzsql
ALTER <object_type> <object_name> OWNER TO <role>;
```

Before changing the ownership, review the privileges of the current owner
(`lemon@example.com`) and the future owner (`view_manage`):

{{% include-example file="examples/rbac/alter_owner"
example="view-privileges-for-current-owner" %}}

{{% include-example file="examples/rbac/alter_owner"
example="view-privileges-for-future-owner" %}}

{{% include-example file="examples/rbac/alter_owner"
example="table-shared_lemon" %}}

{{% include-example file="examples/rbac/alter_owner"
example="view-privileges-for-new-owner" %}}

{{% include-example file="examples/rbac/alter_owner"
example="view-privileges-for-previous-owner" %}}

## See also

- [Access control best practices](/manage/access-control/#best-practices)
- [Manage privileges with
  Terraform](/manage/access-control/rbac-terraform-tutorial/)
