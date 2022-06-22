---
title: "CREATE USER"
description: "`CREATE USER` creates a new role."
menu:
  main:
    parent: commands
---

`CREATE USER` creates a new [role](/sql/create-role).

## Syntax

{{< diagram "create-user.svg" >}}

Field | Use
------|-----
**LOGIN** | Grants the user the ability to log in.
**NOLOGIN** | Denies the user the ability to log in.
**SUPERUSER** | Grants the user superuser permission, i.e., unrestricted access to the system.
**NOSUPERUSER** | Denies the user superuser permission.
_role_name_ | A name for the role.

## Details

`CREATE USER` is an alias for [`CREATE ROLE`](../create-role), except that the
`LOGIN` option is implied if it is not explicitly specified.

## Related pages

- [CREATE ROLE](../create-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
