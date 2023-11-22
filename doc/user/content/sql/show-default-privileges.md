---
title: "SHOW DEFAULT PRIVILEGES"
description: "SHOW DEFAULT PRIVILEGES lists the default privileges granted on objects in Materialize."
menu:
  main:
    parent: 'commands'

---

`SHOW DEFAULT PRIVILEGES` lists the default privileges granted on objects in Materialize.

## Syntax

{{< diagram "show-default-privileges.svg" >}}

Field                                               | Use
----------------------------------------------------|--------------------------------------------------
_object_name_                                       | Only shows default privileges for a specific object type.
_role_name_                                         | Only shows default privileges granted directly or indirectly to _role_name_.

[//]: # "TODO(morsapaes) Improve examples."

## Examples

```sql
SHOW DEFAULT PRIVILEGES;
```

```nofmt
 object_owner | database | schema | object_type | grantee | privilege_type
--------------+----------+--------+-------------+---------+----------------
 PUBLIC       |          |        | cluster     | interns | USAGE
 PUBLIC       |          |        | schema      | mike    | CREATE
 PUBLIC       |          |        | type        | PUBLIC  | USAGE
 mike         |          |        | table       | joe     | SELECT
```

```sql
SHOW DEFAULT PRIVILEGES ON SCHEMAS;
```

```nofmt
 object_owner | database | schema | object_type | grantee | privilege_type
--------------+----------+--------+-------------+---------+----------------
 PUBLIC       |          |        | schema      | mike    | CREATE
```

```sql
SHOW DEFAULT PRIVILEGES FOR joe;
```

```nofmt
 object_owner | database | schema | object_type | grantee | privilege_type
--------------+----------+--------+-------------+---------+----------------
 PUBLIC       |          |        | cluster     | interns | USAGE
 PUBLIC       |          |        | type        | PUBLIC  | USAGE
 mike         |          |        | table       | joe     | SELECT
```

## Related pages

- [ALTER DEFAULT PRIVILEGES](../alter-default-privileges)
- [access control](/manage/access-control/)
