---
title: "GRANT ROLE"
description: "`GRANT` grants membership of one role to another role."
menu:
  main:
    parent: commands
---

`GRANT` grants membership of one role to another role.

## Conceptual framework

Roles can be a member of another role and inherit all the attributes and
privileges of the other role.

{{< warning >}}
Roles in Materialize are currently limited in functionality. In the future they
will be used for role-based access control. See GitHub issue {{% gh 11579 %}}
for details.
{{< /warning >}}


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

## Related pages

- [CREATE ROLE](../create-role)
- [ALTER ROLE](../alter-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
- [REVOKE ROLE](../revoke-role)
