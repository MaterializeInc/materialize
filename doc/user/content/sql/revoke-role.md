---
title: "REVOKE ROLE"
description: "`REVOKE` revokes membership of one role from another role."
menu:
  main:
    parent: commands
---

`REVOKE` revokes membership of one role from another role. Roles can be members
of other roles, as well as inherit all the privileges of those
roles. This membership can also be revoked.

## Syntax

{{< diagram "revoke-role.svg" >}}

Field         | Use
--------------|--------------------------------------------------
_role_name_   | The role name to remove _member_name_ from.
_member_name_ | The role name to remove from _role_name_.

## Details

You may not set up circular membership loops.

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
- [`ALTER OWNER`](../alter-owner)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
