---
audience: developer
canonical_url: https://materialize.com/docs/sql/drop-user/
complexity: intermediate
description: '`DROP USER` removes a role from Materialize.'
doc_type: reference
keywords:
- DROP USER
- DROP ROLE
- IF EXISTS
product_area: Indexes
status: stable
title: DROP USER
---

# DROP USER

## Purpose
`DROP USER` removes a role from Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`DROP USER` removes a role from Materialize.



`DROP USER` removes a role from Materialize. `DROP USER` is an alias for [`DROP ROLE`](../drop-role).


## Syntax

This section covers syntax.

```mzsql
DROP USER [IF EXISTS] <role_name>;
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified role does not exist.
`<role_name>` | The role you want to drop. For available roles, see [`mz_roles`](/sql/system-catalog/mz_catalog#mz_roles).

## Privileges

The privileges required to execute this statement are:

- `CREATEROLE` privileges on the system.


## Related pages

- [`ALTER ROLE`](../alter-role)
- [`CREATE ROLE`](../create-role)
- [`DROP ROLE`](../drop-role)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)

