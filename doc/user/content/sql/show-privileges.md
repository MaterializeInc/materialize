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

```mzsql
SHOW PRIVILEGES [ ON <object_type> ] [ FOR <role_name> ]
```

Option                       | Description
-----------------------------|--------------------------------------------------
**ON** <object_type>         | If specified, only show privileges for the specified object type. Accepted object types: <div style="display: flex;"> <ul style="margin-right: 20px;"> <li><strong>CLUSTERS</strong></li> <li><strong>CONNECTION</strong></li> <li><strong>DATABASES</strong></li> <li><strong>SCHEMAS</strong></li> </ul> <ul> <li><strong>SECRETS</strong></li> <li><strong>SYSTEM</strong></li> <li><strong>TABLES</strong></li> <li><strong>TYPES</strong></li> </ul> </div>
**FOR** <role_name>          | If specified, only show privileges for the specified role.

[//]: # "TODO(morsapaes) Improve examples."

## Examples

```mzsql
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

```mzsql
SHOW PRIVILEGES ON SCHEMAS;
```

```nofmt
  grantor  |   grantee   |  database   | schema |  name  | object_type | privilege_type
-----------+-------------+-------------+--------+--------+-------------+----------------
 mz_system | PUBLIC      | materialize |        | public | schema      | USAGE
 mz_system | materialize | materialize |        | public | schema      | CREATE
 mz_system | materialize | materialize |        | public | schema      | USAGE
```

```mzsql
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
