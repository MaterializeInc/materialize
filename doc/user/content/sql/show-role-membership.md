---
title: "SHOW ROLE MEMBERSHIP"
description: "SHOW ROLE MEMBERSHIP lists the members of each role granted via role-based access control (RBAC)."
menu:
  main:
    parent: 'commands'

---

`SHOW ROLE MEMBERSHIP` lists the members of each role granted via
[role-based access control](/manage/access-control/#role-based-access-control-rbac) (RBAC).

## Syntax

{{< diagram "show-role-membership.svg" >}}

Field                                               | Use
----------------------------------------------------|--------------------------------------------------
_role_name_                                         | Only shows role memberships granted directly or indirectly to _role_name_.

[//]: # "TODO(morsapaes) Improve examples."

## Examples

```sql
SHOW ROLE MEMBERSHIP;
```

```nofmt
 role | member |  grantor
------+--------+-----------
 r2   | r1     | mz_system
 r3   | r2     | mz_system
 r4   | r3     | mz_system
 r6   | r5     | mz_system
```

```sql
SHOW ROLE MEMBERSHIP FOR r2;
```

```nofmt
 role | member |  grantor
------+--------+-----------
 r2   | r1     | mz_system
 r3   | r2     | mz_system
 r4   | r3     | mz_system
```

## Related pages

- [GRANT ROLE](../grant-role)
- [REVOKE ROLE](../revoke-role)
- [Access control](/manage/access-control/#role-based-access-control-rbac)
