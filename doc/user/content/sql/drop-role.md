---
title: "DROP ROLE"
description: "`DROP ROLE` removes a role from your Materialize instance."
menu:
  main:
    parent: commands
---

`DROP ROLE` removes a role from your Materialize instance.

## Syntax

{{< diagram "drop-role.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the specified role does not exist.
_role_name_ | The role you want to drop. For available roles, see [`mz_roles`](/sql/system-catalog/#mz_roles).

## Details

You cannot drop the current role.

## Related pages

- [CREATE ROLE](../create-role)
- [CREATE USER](../create-user)
- [DROP USER](../drop-user)
