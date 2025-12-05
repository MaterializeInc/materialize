---
title: "REVOKE ROLE"
description: "`REVOKE` revokes membership of one role from another role."
menu:
  main:
    parent: commands
---

`REVOKE` revokes membership of a role from the target role.

## Syntax

```mzsql
REVOKE <role_to_remove> [, ...] FROM <target_role> [, ...];
```

Syntax element       | Description
---------------------|------------------
`<role_to_remove>`   | The name of the role to remove from the `<target_role>`.
`<target_role>`      | The name of the role from which the to remove the `<role_to_remove>`.


## Examples

```mzsql
REVOKE data_scientist FROM joe;
```

```mzsql
REVOKE data_scientist FROM joe, mike;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/revoke-role.md" >}}

## Useful views

- [`mz_internal.mz_show_role_members`](/sql/system-catalog/mz_internal/#mz_show_role_members)
- [`mz_internal.mz_show_my_role_members`](/sql/system-catalog/mz_internal/#mz_show_my_role_members)

## Related pages

- [`SHOW ROLE MEMBERSHIP`](../show-role-membership)
- [`CREATE ROLE`](../create-role)
- [`ALTER ROLE`](../alter-role)
- [`DROP ROLE`](../drop-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
