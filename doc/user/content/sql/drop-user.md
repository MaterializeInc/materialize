---
title: "DROP USER"
description: "`DROP USER` removes a role from Materialize."
menu:
  main:
    parent: commands
---

`DROP USER` removes a role from Materialize. `DROP USER` is an alias for [`DROP ROLE`](../drop-role).


## Syntax

{{< diagram "drop-user.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the specified role does not exist.
_role_name_ | The role you want to drop. For available roles, see [`mz_roles`](/sql/system-catalog/mz_catalog#mz_roles).

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-user.md" >}}

## Related pages

- [`ALTER ROLE`](../alter-role)
- [`CREATE ROLE`](../create-role)
- [`DROP ROLE`](../drop-role)
- [`GRANT ROLE`](../grant-role)
- [`REVOKE ROLE`](../revoke-role)
- [`GRANT PRIVILEGE`](../grant-privilege)
- [`REVOKE PRIVILEGE`](../revoke-privilege)
