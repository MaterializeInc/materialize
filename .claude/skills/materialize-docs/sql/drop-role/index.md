---
audience: developer
canonical_url: https://materialize.com/docs/sql/drop-role/
complexity: intermediate
description: '`DROP ROLE` removes a role from Materialize.'
doc_type: reference
keywords:
- DROP THE
- DROP ROLE
- IF EXISTS
product_area: Indexes
status: stable
title: DROP ROLE
---

# DROP ROLE

## Purpose
`DROP ROLE` removes a role from Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`DROP ROLE` removes a role from Materialize.



`DROP ROLE` removes a role from Materialize.

## Syntax

This section covers syntax.

```mzsql
DROP ROLE [IF EXISTS] <role_name>;
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified role does not exist.
`<role_name>` | The role you want to drop. For available roles, see [`mz_roles`](/sql/system-catalog/mz_catalog#mz_roles).

## Details

You cannot drop the current role.

## Privileges

The privileges required to execute this statement are:

- `CREATEROLE` privileges on the system.


## Related pages

- [`ALTER ROLE`](../alter-role)
- [`CREATE ROLE`](../create-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](/sql/#rbac)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)

