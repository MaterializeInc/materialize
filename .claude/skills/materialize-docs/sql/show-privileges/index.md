---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-privileges/
complexity: beginner
description: SHOW PRIVILEGES lists the privileges granted on all objects via role-based
  access control (RBAC).
doc_type: reference
keywords:
- FOR
- 'ON'
- SHOW PRIVILEGES
product_area: Indexes
status: stable
title: SHOW PRIVILEGES
---

# SHOW PRIVILEGES

## Purpose
SHOW PRIVILEGES lists the privileges granted on all objects via role-based access control (RBAC).

If you need to understand the syntax and options for this command, you're in the right place.


SHOW PRIVILEGES lists the privileges granted on all objects via role-based access control (RBAC).



`SHOW PRIVILEGES` lists the privileges granted on all objects via
[role-based access control](/security/) (RBAC).

## Syntax

This section covers syntax.

```mzsql
SHOW PRIVILEGES [ ON <object_type> ] [ FOR <role_name> ];
```text

Syntax element               | Description
-----------------------------|-----------------------------------------------
**ON** <object_type>         | If specified, only show privileges for the specified object type. Accepted object types: <div style="display: flex;"> <ul style="margin-right: 20px;"> <li><strong>CLUSTERS</strong></li> <li><strong>CONNECTION</strong></li> <li><strong>DATABASES</strong></li> <li><strong>SCHEMAS</strong></li> </ul> <ul> <li><strong>SECRETS</strong></li> <li><strong>SYSTEM</strong></li> <li><strong>TABLES</strong></li> <li><strong>TYPES</strong></li> </ul> </div>
**FOR** <role_name>          | If specified, only show privileges for the specified role.

[//]: # "TODO(morsapaes) Improve examples."

## Examples

This section covers examples.

```mzsql
SHOW PRIVILEGES;
```text

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
```text

```mzsql
SHOW PRIVILEGES ON SCHEMAS;
```text

```nofmt
  grantor  |   grantee   |  database   | schema |  name  | object_type | privilege_type
-----------+-------------+-------------+--------+--------+-------------+----------------
 mz_system | PUBLIC      | materialize |        | public | schema      | USAGE
 mz_system | materialize | materialize |        | public | schema      | CREATE
 mz_system | materialize | materialize |        | public | schema      | USAGE
```text

```mzsql
SHOW PRIVILEGES FOR materialize;
```text

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

- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)

