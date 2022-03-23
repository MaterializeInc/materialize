---
title: "DROP USER"
description: "`DROP USER` removes a role from your Materialize instance."
menu:
  main:
    parent: sql
---

{{< version-added v0.7.0 />}}

`DROP USER` removes a role from your Materialize instance.

## Syntax

```sql
DROP USER [ IF EXISTS ] role_name
```

<br/>
<details>
<summary>Diagram</summary>
<br>

{{< diagram "drop-user.svg" >}}

</details>
<br/>

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the specified role does not exist.
_role_name_ | The role you want to drop. For available roles, see [`mz_roles`](../system-catalog#mz_roles).

## Details

`DROP USER` is an alias for [`DROP ROLE`](../drop-role).

## Example

Drop user named _developer_:
```sql
DROP USER developer;
```

## Related pages

- [CREATE ROLE](../create-role)
- [CREATE USER](../create-user)
- [DROP ROLE](../drop-role)
