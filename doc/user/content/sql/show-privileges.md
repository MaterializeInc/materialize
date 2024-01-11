---
title: "SHOW PRIVILEGES"
description: "SHOW PRIVILEGES lists the privileges granted on all objects via role-based access control (RBAC)."
menu:
  main:
    parent: 'commands'

---

`SHOW PRIVILEGES` lists the privileges granted on all objects via
[role-based access control](/manage/access-control/#role-based-access-control-rbac) (RBAC).

## Syntax

{{< diagram "show-privileges.svg" >}}

Field                                               | Use
----------------------------------------------------|--------------------------------------------------
_object_name_                                       | Only shows privileges for a specific object type.
_role_name_                                         | Only shows privileges granted directly or indirectly to _role_name_.

[//]: # "TODO(morsapaes) Improve examples."

## Examples

```sql
SHOW PRIVILEGES;
```

```nofmt
  grantor  |   grantee   |  database   | schema |    name     | object_type | privilege_type
-----------+-------------+-------------+--------+-------------+-------------+----------------
 mz_system | PUBLIC      | materialize |        | public      | schema      | USAGE
 mz_system | PUBLIC      |             |        | quickstart  | cluster     | USAGE
 mz_system | PUBLIC      |             |        | materialize | database    | USAGE
 mz_system | materialize | materialize |        | public      | schema      | CREATE
 mz_system | materialize | materialize |        | public      | schema      | USAGE
 mz_system | materialize |             |        | quickstart  | cluster     | CREATE
 mz_system | materialize |             |        | quickstart  | cluster     | USAGE
 mz_system | materialize |             |        | materialize | database    | CREATE
 mz_system | materialize |             |        | materialize | database    | USAGE
 mz_system | materialize |             |        |             | system      | CREATECLUSTER
 mz_system | materialize |             |        |             | system      | CREATEDB
 mz_system | materialize |             |        |             | system      | CREATEROLE
```

```sql
SHOW PRIVILEGES ON SCHEMAS;
```

```nofmt
  grantor  |   grantee   |  database   | schema |  name  | object_type | privilege_type
-----------+-------------+-------------+--------+--------+-------------+----------------
 mz_system | PUBLIC      | materialize |        | public | schema      | USAGE
 mz_system | materialize | materialize |        | public | schema      | CREATE
 mz_system | materialize | materialize |        | public | schema      | USAGE
```

```sql
SHOW PRIVILEGES FOR materialize;
```

```nofmt
  grantor  |   grantee   |  database   | schema |    name     | object_type | privilege_type
-----------+-------------+-------------+--------+-------------+-------------+----------------
 mz_system | materialize | materialize |        | public      | schema      | CREATE
 mz_system | materialize | materialize |        | public      | schema      | USAGE
 mz_system | materialize |             |        | quickstart  | cluster     | CREATE
 mz_system | materialize |             |        | quickstart  | cluster     | USAGE
 mz_system | materialize |             |        | materialize | database    | CREATE
 mz_system | materialize |             |        | materialize | database    | USAGE
 mz_system | materialize |             |        |             | system      | CREATECLUSTER
 mz_system | materialize |             |        |             | system      | CREATEDB
 mz_system | materialize |             |        |             | system      | CREATEROLE
```

## Related pages

- [GRANT PRIVILEGE](../grant-privilege)
- [REVOKE PRIVILEGE](../revoke-privilege)
- [Access control](/manage/access-control/#role-based-access-control-rbac)
