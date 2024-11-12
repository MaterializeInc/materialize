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
**ON** <object_type>         | Specifies an object type to show privileges for a particular object type. Valid values are: <div style="display: flex;"> <ul style="margin-right: 20px;"> <li><strong>TABLES</strong></li> <li><strong>TYPES</strong></li> <li><strong>SECRETS</strong></li> <li><strong>CONNECTION</strong></li> </ul> <ul> <li><strong>DATABASES</strong></li> <li><strong>SCHEMAS</strong></li> <li><strong>CLUSTERS</strong></li> <li><strong>SYSTEM</strong></li> </ul> </div>Omit to show privileges for all object types.
**FOR** <role_name>          | Specifies a role for which to show the membership. Omit to show role membership for all roles.

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
