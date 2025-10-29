---
title: "SHOW ROLE MEMBERSHIP"
description: "SHOW ROLE MEMBERSHIP lists the members of each role granted via role-based access control (RBAC)."
menu:
  main:
    parent: 'commands'

---

`SHOW ROLE MEMBERSHIP` lists the members of each role granted (directly or
indirectly) via [role-based access
control](/security/) (RBAC).

## Syntax

```mzsql
SHOW ROLE MEMBERSHIP [ FOR <role_name> ]
```

Option                     | Description
---------------------------|------------
**FOR** <role_name>        | If specified, only show membership for the specified role.

[//]: # "TODO(morsapaes) Improve examples."

## Examples

```mzsql
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

```mzsql
SHOW ROLE MEMBERSHIP FOR r2;
```

```nofmt
 role | member |  grantor
------+--------+-----------
 r3   | r2     | mz_system
 r4   | r3     | mz_system
```

## Related pages

- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
