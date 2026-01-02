---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-role-membership/
complexity: intermediate
description: SHOW ROLE MEMBERSHIP lists the members of each role granted via role-based
  access control (RBAC).
doc_type: reference
keywords:
- SHOW MEMBERSHIP
- FOR
- SHOW ROLE MEMBERSHIP
- SHOW ROLE
product_area: Indexes
status: stable
title: SHOW ROLE MEMBERSHIP
---

# SHOW ROLE MEMBERSHIP

## Purpose
SHOW ROLE MEMBERSHIP lists the members of each role granted via role-based access control (RBAC).

If you need to understand the syntax and options for this command, you're in the right place.


SHOW ROLE MEMBERSHIP lists the members of each role granted via role-based access control (RBAC).



`SHOW ROLE MEMBERSHIP` lists the members of each role granted (directly or
indirectly) via [role-based access
control](/security/) (RBAC).

## Syntax

This section covers syntax.

```mzsql
SHOW ROLE MEMBERSHIP [ FOR <role_name> ];
```text

Syntax element             | Description
---------------------------|------------
**FOR** <role_name>        | If specified, only show membership for the specified role.

[//]: # "TODO(morsapaes) Improve examples."

## Examples

This section covers examples.

```mzsql
SHOW ROLE MEMBERSHIP;
```text

```nofmt
 role | member |  grantor
------+--------+-----------
 r2   | r1     | mz_system
 r3   | r2     | mz_system
 r4   | r3     | mz_system
 r6   | r5     | mz_system
```text

```mzsql
SHOW ROLE MEMBERSHIP FOR r2;
```text

```nofmt
 role | member |  grantor
------+--------+-----------
 r3   | r2     | mz_system
 r4   | r3     | mz_system
```

## Related pages

- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)

