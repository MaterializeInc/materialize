---
title: "ALTER ... OWNER"
description: "`ALTER ... OWNER` updates the owner of an item."
menu:
  main:
    parent: 'commands'
---

`ALTER ... OWNER` updates the owner of an item.

{{< alpha />}}

## Syntax

{{< diagram "alter-owner.svg" >}}

Field | Use
------|-----
_name_ | The identifier of the item you want to alter.
_new&lowbar;owner_ | The role name you want to set as the new owner.

## Details

You must be a member of the new owner role to alter the ownership of an object.
You cannot alter the owner of an index. If you try, it will return successfully with a warning, but
will not actually change the owner of the index. This is for backwards compatibility reasons. The
index owner is always kept in-sync with the owner of the underlying relation.

## Examples

```sql
ALTER TABLE t OWNER TO joe;
```

```sql
ALTER CLUSTER REPLICA production.r1 OWNER TO admin;
```

## See also

- [REASSIGN OWNED](../reassign-owned)
- [CREATE ROLE](../create-role)
- [ALTER ROLE](../alter-role)
- [DROP ROLE](../drop-role)
- [DROP USER](../drop-user)
- [GRANT ROLE](../grant-role)
- [REVOKE ROLE](../revoke-role)
- [GRANT PRIVILEGE](../grant-privilege)
- [REVOKE PRIVILEGE](../revoke-privilege)
