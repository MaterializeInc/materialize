---
title: "SHOW DEFAULT PRIVILEGES"
description: "SHOW DEFAULT PRIVILEGES lists the default privileges granted on objects in Materialize."
menu:
  main:
    parent: 'commands'

---

`SHOW DEFAULT PRIVILEGES` lists the default privileges granted on objects in Materialize.

## Syntax

```sql
SHOW DEFAULT PRIVILEGES [ON <object_type>] [FOR <role_name>];
```


Syntax element               | Description
-----------------------------|--------------------------------------
**ON** <object_type>         | If specified, only show default privileges for the specified object type. Accepted object types: <div style="display: flex;"> <ul style="margin-right: 20px;"> <li><strong>CLUSTERS</strong></li> <li><strong>CONNECTION</strong></li> <li><strong>DATABASES</strong></li> <li><strong>SCHEMAS</strong></li> </ul> <ul> <li><strong>SECRETS</strong></li> <li><strong>TABLES</strong></li> <li><strong>TYPES</strong></li> </ul> </div>
**FOR** <role_name>          | If specified, only show default privileges granted directly or indirectly to the specified role. For available role names, see [`SHOW ROLES`](/sql/show-roles).

[//]: # "TODO(morsapaes) Improve examples."

## Examples

```mzsql
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

```mzsql
SHOW DEFAULT PRIVILEGES ON SCHEMAS;
```

```nofmt
 object_owner | database | schema | object_type | grantee | privilege_type
--------------+----------+--------+-------------+---------+----------------
 PUBLIC       |          |        | schema      | mike    | CREATE
```

```mzsql
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

- [`ALTER DEFAULT PRIVILEGES`](../alter-default-privileges)
