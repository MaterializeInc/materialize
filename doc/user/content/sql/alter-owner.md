---
title: "ALTER ... OWNER"
description: "`ALTER ... OWNER` updates the owner of an item."
menu:
  main:
    parent: 'commands'
---

`ALTER ... OWNER` updates the owner of an item.

{{< warning >}}
Roles in Materialize are currently limited in functionality. In the future they
will be used for role-based access control. See GitHub issue {{% gh 11579 %}}
for details.
{{< /warning >}}

## Syntax

{{< diagram "alter-owner.svg" >}}

Field | Use
------|-----
_name_ | The identifier of the item you want to alter.
_new&lowbar;owner_ | The role name you want to set as the new owner.

## Details

You must be a member of the new owner role to alter the ownership of an object.


## Examples

```sql
ALTER TABLE t OWNER TO joe;
```

```sql
ALTER CLUSTER REPLICA production.r1 OWNER TO admin;
```

## See also

- [CREATE ROLE](../create-role)
- [ALTER ROLE](../alter-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
- [GRANT ROLE](../grant-role)
- [REVOKE ROLE](../revoke-role)
