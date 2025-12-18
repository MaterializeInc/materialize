---
title: "DROP ROLE"
description: "`DROP ROLE` removes a role from Materialize."
menu:
  main:
    parent: commands
---

`DROP ROLE` removes a role from Materialize.

## Syntax

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

{{< include-md file="shared-content/sql-command-privileges/drop-role.md" >}}

## Related pages

- [`ALTER ROLE`](../alter-role)
- [`CREATE ROLE`](../create-role)
- [`DROP USER`](../drop-user)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`ALTER OWNER`](/sql/#rbac)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
