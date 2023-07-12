---
title: "GRANT ROLE"
description: "`GRANT` grants membership of one role to another role."
menu:
  main:
    parent: commands
---

`GRANT` grants membership of one role to another role. Roles can be members of
other roles, as well as inherit all the privileges of those roles.

{{< private-preview />}}

## Syntax

{{< diagram "grant-role.svg" >}}

Field         | Use
--------------|--------------------------------------------------
_role_name_   | The role name to add _member_name_ as a member.
_member_name_ | The role name to add to _role_name_ as a member.

## Examples

```sql
GRANT data_scientist TO joe;
```

```sql
GRANT data_scientist TO joe, mike;
```

## Privileges

{{< private-preview />}}

The privileges required to execute this statement are:

- `CREATEROLE` privileges on the systems.

## Related pages

- [CREATE ROLE](../create-role)
- [ALTER ROLE](../alter-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
- [REVOKE ROLE](../revoke-role)
- [ALTER OWNER](../alter-owner)
- [GRANT PRIVILEGE](../grant-privilege)
- [REVOKE PRIVILEGE](../revoke-privilege)
