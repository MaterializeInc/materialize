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

{{< diagram "grant-role.svg" >}}

Field         | Use
--------------|--------------------------------------------------
_role_name_   | The role name to add _member_name_ as a member.
_member_name_ | The role name to add to _role_name_ as a member.

## Examples

```mzsql
GRANT data_scientist TO joe;
```

```mzsql
GRANT data_scientist TO joe, mike;
```

## Privileges

The privileges required to execute this statement are:

- `CREATEROLE` privileges on the systems.

## Useful views

- [`mz_internal.mz_show_role_members`](/sql/system-catalog/mz_internal/#mz_show_role_members)
- [`mz_internal.mz_show_my_role_members`](/sql/system-catalog/mz_internal/#mz_show_my_role_members)

## Related pages

- [SHOW ROLE MEMBERSHIP](../show-role-membership)
- [CREATE ROLE](../create-role)
- [ALTER ROLE](../alter-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
- [REVOKE ROLE](../revoke-role)
- [ALTER OWNER](../alter-owner)
- [GRANT PRIVILEGE](../grant-privilege)
- [REVOKE PRIVILEGE](../revoke-privilege)
