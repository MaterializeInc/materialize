---
title: "GRANT ROLE"
description: "`GRANT` grants membership of one role to another role."
menu:
  main:
    parent: commands
---

`GRANT` grants membership of one role to another role. Roles can be members of
other roles, as well as inherit all the privileges of those roles.

## Syntax

{{% include-syntax file="examples/grant_role" example="syntax" %}}

## Examples

```mzsql
GRANT data_scientist TO joe;
```

```mzsql
GRANT data_scientist TO joe, mike;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/grant-role.md" >}}

## Useful views

- [`mz_internal.mz_show_role_members`](/sql/system-catalog/mz_internal/#mz_show_role_members)
- [`mz_internal.mz_show_my_role_members`](/sql/system-catalog/mz_internal/#mz_show_my_role_members)

## Related pages

- [`SHOW ROLE MEMBERSHIP`](../show-role-membership)
- [`CREATE ROLE`](../create-role)
- [`ALTER ROLE`](../alter-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](/sql/#rbac)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
