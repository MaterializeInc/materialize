---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-default-privileges/
complexity: intermediate
description: SHOW DEFAULT PRIVILEGES lists the default privileges granted on objects
  in Materialize.
doc_type: reference
keywords:
- FOR
- SHOW DEFAULT PRIVILEGES
- 'ON'
- SHOW DEFAULT
product_area: Indexes
status: stable
title: SHOW DEFAULT PRIVILEGES
---

# SHOW DEFAULT PRIVILEGES

## Purpose
SHOW DEFAULT PRIVILEGES lists the default privileges granted on objects in Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


SHOW DEFAULT PRIVILEGES lists the default privileges granted on objects in Materialize.



`SHOW DEFAULT PRIVILEGES` lists the default privileges granted on objects in Materialize.

## Syntax

This section covers syntax.

```sql
SHOW DEFAULT PRIVILEGES [ON <object_type>] [FOR <role_name>];
```text


Syntax element               | Description
-----------------------------|--------------------------------------
**ON** <object_type>         | If specified, only show default privileges for the specified object type. Accepted object types: <div style="display: flex;"> <ul style="margin-right: 20px;"> <li><strong>CLUSTERS</strong></li> <li><strong>CONNECTION</strong></li> <li><strong>DATABASES</strong></li> <li><strong>SCHEMAS</strong></li> </ul> <ul> <li><strong>SECRETS</strong></li> <li><strong>TABLES</strong></li> <li><strong>TYPES</strong></li> </ul> </div>
**FOR** <role_name>          | If specified, only show default privileges granted directly or indirectly to the specified role. For available role names, see [`SHOW ROLES`](/sql/show-roles).

[//]: # "TODO(morsapaes) Improve examples."

## Examples

This section covers examples.

```mzsql
SHOW DEFAULT PRIVILEGES;
```text

```nofmt
 object_owner | database | schema | object_type | grantee | privilege_type
--------------+----------+--------+-------------+---------+----------------
 PUBLIC       |          |        | cluster     | interns | USAGE
 PUBLIC       |          |        | schema      | mike    | CREATE
 PUBLIC       |          |        | type        | PUBLIC  | USAGE
 mike         |          |        | table       | joe     | SELECT
```text

```mzsql
SHOW DEFAULT PRIVILEGES ON SCHEMAS;
```text

```nofmt
 object_owner | database | schema | object_type | grantee | privilege_type
--------------+----------+--------+-------------+---------+----------------
 PUBLIC       |          |        | schema      | mike    | CREATE
```text

```mzsql
SHOW DEFAULT PRIVILEGES FOR joe;
```text

```nofmt
 object_owner | database | schema | object_type | grantee | privilege_type
--------------+----------+--------+-------------+---------+----------------
 PUBLIC       |          |        | cluster     | interns | USAGE
 PUBLIC       |          |        | type        | PUBLIC  | USAGE
 mike         |          |        | table       | joe     | SELECT
```

## Related pages

- [`ALTER DEFAULT PRIVILEGES`](../alter-default-privileges)

