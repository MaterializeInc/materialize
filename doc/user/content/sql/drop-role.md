---
title: "DROP ROLE"
description: "`DROP ROLE` removes a role from your Materialize instance."
menu:
  main:
    parent: sql
---

{{< version-added v0.7.0 />}}

`DROP ROLE` removes a role from your Materialize instance.

## Syntax

```sql
DROP ROLE [ IF EXISTS ] role_name
```

<br/>
<details>
<summary>Diagram</summary>
<br>

{{< diagram "drop-role.svg" >}}

</details>
<br/>

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the specified role does not exist.
_role_name_ | The role you want to drop. For available roles, see [`mz_roles`](../system-catalog#mz_roles).

## Details

You cannot drop the current role.

## Related pages

- [CREATE ROLE](../create-role)
- [CREATE USER](../create-user)
- [DROP USER](../drop-user)
