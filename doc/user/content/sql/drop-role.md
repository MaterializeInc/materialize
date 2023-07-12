---
title: "DROP ROLE"
description: "`DROP ROLE` removes a role from Materialize."
menu:
  main:
    parent: commands
---

`DROP ROLE` removes a role from Materialize.

{{< private-preview />}}

## Syntax

{{< diagram "drop-role.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the specified role does not exist.
_role_name_ | The role you want to drop. For available roles, see [`mz_roles`](/sql/system-catalog/mz_catalog#mz_roles).

## Details

You cannot drop the current role.

## Privileges

{{< private-preview />}}

The privileges required to execute this statement are:

- `CREATEROLE` privileges on the system.

## Related pages

- [ALTER ROLE](../alter-role)
- [CREATE ROLE](../create-role)
- [DROP USER](../drop-user)
- [GRANT ROLE](../grant-role)
- [REVOKE ROLE](../revoke-role)
- [ALTER OWNER](../alter-owner)
- [GRANT PRIVILEGE](../grant-privilege)
- [REVOKE PRIVILEGE](../revoke-privilege)
