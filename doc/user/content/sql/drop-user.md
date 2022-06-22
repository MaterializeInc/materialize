---
title: "DROP USER"
description: "`DROP USER` removes a role from your Materialize instance."
menu:
  main:
    parent: commands
---

`DROP USER` removes a role from your Materialize instance.

## Syntax

{{< diagram "drop-user.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the specified role does not exist.
_role_name_ | The role you want to drop. For available roles, see [`mz_roles`](/sql/system-catalog/#mz_roles).

## Details

`DROP USER` is an alias for [`DROP ROLE`](../drop-role).

## Related pages

- [CREATE ROLE](../create-role)
- [CREATE USER](../create-user)
- [DROP ROLE](../drop-role)
