---
title: "CREATE ROLE"
description: "`CREATE ROLE` creates a new role."
menu:
  main:
    parent: commands
---

`CREATE ROLE` creates a new role, which is a user account in Materialize.[^1]

When you connect to Materialize, you must specify the name of a valid role in
the system.

[^1]: Materialize does not support the `CREATE USER` command.

## Syntax

{{< tabs >}}

{{< tab "Cloud" >}}

### Cloud
{{% include-example file="examples/rbac-cloud/create_roles" example="create-role-syntax" %}}

{{% include-example file="examples/rbac-cloud/create_roles" example="create-role-options" %}}

Note:
{{% include-example file="examples/rbac-cloud/create_roles" example="create-role-details" %}}
{{< /tab >}}
{{< tab "Self-Managed" >}}
### Self-Managed
{{% include-example file="examples/rbac-sm/create_roles" example="create-role-syntax" %}}

{{% include-example file="examples/rbac-sm/create_roles"
example="create-role-options" %}}

Note:
{{% include-example file="examples/rbac-sm/create_roles" example="create-role-details" %}}
{{< /tab >}}
{{< /tabs >}}

## Restrictions

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `CREATE ROLE ... INHERIT INHERIT`.

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-role.md" >}}

## Examples

### Create a functional role

In Materialize Cloud and Self-Managed, you can create a functional role:

```mzsql
CREATE ROLE db_reader;
```

### Create a role with login and password (Self-Managed)

```mzsql
CREATE ROLE db_reader WITH LOGIN PASSWORD 'password';
```

You can verify that the role was created by querying the `mz_roles` system catalog:

```mzsql
SELECT name FROM mz_roles;
```

```nofmt
 db_reader
 mz_system
 mz_support
```

### Create a superuser role (Self-Managed)

Unlike regular roles, superusers have unrestricted access to all objects in the system and can perform any action on them.

```mzsql
CREATE ROLE super_user WITH SUPERUSER LOGIN PASSWORD 'password';
```

You can verify that the superuser role was created by querying the `mz_roles` system catalog:

```mzsql
SELECT name FROM mz_roles;
```

```nofmt
 db_reader
 mz_system
 mz_support
 super_user
```

You can also verify that the role has superuser privileges by checking the `pg_authid` system catalog:

```mzsql
SELECT rolsuper FROM pg_authid WHERE rolname = 'super_user';
```

```nofmt
 true
```



## Related pages

- [`ALTER ROLE`](../alter-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](../alter-owner)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
