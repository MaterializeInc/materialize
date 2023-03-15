---
title: "REVOKE ROLE"
description: "`REVOKE` revokes membership of one role from another role."
menu:
  main:
    parent: commands
---

`REVOKE` revokes membership of one role from another role.

## Conceptual framework

Roles can be a member of another role and inherit all the attributes and
privileges of the other role. They can also have this membership revoked.

{{< warning >}}
Roles in Materialize are currently limited in functionality. In the future they
will be used for role-based access control. See GitHub issue {{% gh 11579 %}}
for details.
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

## Related pages

- [CREATE ROLE](../create-role)
- [ALTER ROLE](../alter-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
- [GRANT ROLE](../grant-role)
