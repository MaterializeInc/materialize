---
title: "DROP USER"
description: "`DROP USER` removes a role from Materialize."
menu:
  main:
    parent: commands
---

`DROP USER` removes a role from Materialize.

## Syntax

{{< diagram "drop-user.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the specified role does not exist.
_role_name_ | The role you want to drop. For available roles, see [`mz_roles`](/sql/system-catalog/mz_catalog#mz_roles).

## Details

`DROP USER` is an alias for [`DROP ROLE`](../drop-role).

## Privileges

{{< private-preview />}}

The privileges required to execute this statement are:

- `CREATEROLE` privileges on the system.

## Related pages

- [ALTER ROLE](../alter-role)
- [CREATE ROLE](../create-role)
- [DROP ROLE](../drop-role)
- [GRANT ROLE](../grant-role)
- [REVOKE ROLE](../revoke-role)
- [GRANT PRIVILEGE](../grant-privilege)
- [REVOKE PRIVILEGE](../revoke-privilege)
