---
title: "CREATE USER"
description: "`CREATE USER` creates a new role."
menu:
  main:
    parent: sql
---

{{< version-added v0.7.0 />}}

`CREATE USER` creates a new [role](/sql/create-role).

## Syntax

```sql
CREATE USER user_name [ { LOGIN | NOLOGIN | SUPERUSER | NOSUPERUSER } [, ... ] ]
```

<br/>
<details>
<summary>Diagram</summary>
<br>

{{< diagram "create-user.svg" >}}

</details>
<br/>

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


## Example

Create a login user named '_super_developer_':
```sql
CREATE USER super_developer LOGIN SUPERUSER;
```

## Related pages

- [CREATE ROLE](../create-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
