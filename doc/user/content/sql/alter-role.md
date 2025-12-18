---
title: "ALTER ROLE"
description: "`ALTER ROLE` alters the attributes of an existing role."
menu:
  main:
    parent: commands
---

`ALTER ROLE` alters the attributes of an existing role.[^1]

[^1]: Materialize does not support the `SET ROLE` command.

## Syntax


{{< tabs >}}

{{< tab "Cloud" >}}

### Cloud

{{% include-example file="examples/rbac-cloud/alter_roles" example="alter-role-syntax" %}}

{{% include-example file="examples/rbac-cloud/alter_roles"
example="alter-role-options" %}}

**Note:**
{{% include-example file="examples/rbac-cloud/alter_roles"
example="alter-role-details" %}}
{{< /tab >}}
{{< tab "Self-Managed" >}}
### Self-Managed

{{% include-example file="examples/rbac-sm/alter_roles" example="alter-role-syntax" %}}

{{% include-example file="examples/rbac-sm/alter_roles"
example="alter-role-options" %}}

**Note:**
{{% include-example file="examples/rbac-sm/alter_roles"
example="alter-role-details" %}}
{{< /tab >}}
{{< /tabs >}}

## Restrictions

You may not specify redundant or conflicting sets of options. For example,
Materialize will reject the statement `ALTER ROLE ... INHERIT INHERIT`.

## Examples

#### Altering the attributes of a role

```mzsql
ALTER ROLE rj INHERIT;
```
```mzsql
SELECT name, inherit FROM mz_roles WHERE name = 'rj';
```
```nofmt
rj  true
```

#### Setting configuration parameters for a role

```mzsql
SHOW cluster;
quickstart

ALTER ROLE rj SET cluster TO rj_compute;

-- Role parameters only take effect for new sessions.
SHOW cluster;
quickstart

-- Start a new SQL session with the role 'rj'.
SHOW cluster;
rj_compute

-- In a new SQL session with a role that is not 'rj'.
SHOW cluster;
quickstart
```


#### Making a role a superuser  (Self-Managed)

Unlike regular roles, superusers have unrestricted access to all objects in the system and can perform any action on them.

```mzsql
ALTER ROLE rj SUPERUSER;
```

To verify that the role has superuser privileges, you can query the `pg_authid` system catalog:

```mzsql
SELECT name, rolsuper FROM pg_authid WHERE rolname = 'rj';
```

```nofmt
rj  t
```

#### Removing the superuser attribute from a role (Self-Managed)

NOSUPERUSER will remove the superuser attribute from a role, preventing it from having unrestricted access to all objects in the system.

```mzsql
ALTER ROLE rj NOSUPERUSER;
```

```mzsql
SELECT name, rolsuper FROM pg_authid WHERE rolname = 'rj';
```

```nofmt
rj  f
```

#### Removing a role's password (Self-Managed)

{{< warning >}}
Setting a NULL password removes the password.
{{< /warning >}}

```mzsql
ALTER ROLE rj PASSWORD NULL;
```

#### Changing a role's password (Self-Managed)

```mzsql
ALTER ROLE rj PASSWORD 'new_password';
```
## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-role.md" >}}

## Related pages

- [`CREATE ROLE`](../create-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](/sql/#rbac)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
