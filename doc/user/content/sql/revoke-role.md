---
title: "REVOKE ROLE"
description: "`REVOKE` revokes membership of one role from another role."
menu:
  main:
    parent: commands
---

`REVOKE` revokes membership of one role from another role. Roles can be members
of other roles, as well as inherit all the attributes and privileges of those
roles. This membership can also be revoked.

{{< warning >}}
Currently, roles have limited functionality in Materialize. This is part of the
work to enable **Role-based access control** (RBAC) in a future release {{% gh 11579 %}}.
{{< /warning >}}


## Syntax

{{< diagram "revoke-role.svg" >}}

Field         | Use
--------------|--------------------------------------------------
_role_name_   | The role name to remove _member_name_ from.
_member_name_ | The role name to remove from _role_name_.

## Details

You may not set up circular membership loops.

## Examples

```sql
REVOKE data_scientist FROM joe;
```

```sql
REVOKE data_scientist FROM joe, mike;
```

## Related pages

- [CREATE ROLE](../create-role)
- [ALTER ROLE](../alter-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
- [GRANT ROLE](../grant-role)
- [ALTER OWNER](../alter-owner)
- [GRANT PRIVILEGE](../grant-privilege)
- [REVOKE PRIVILEGE](../revoke-privilege)
