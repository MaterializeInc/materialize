---
title: "COMMENT ON"
description: "`COMMENT ON ...` adds or updates the comment of an object."
menu:
  main:
    parent: 'commands'
---

{{< public-preview />}}

`COMMENT ON ...` adds or updates the comment of an object.

## Syntax

{{< diagram "comment-on.svg" >}}

## Details

`COMMENT ON` stores a comment about an object in the database. Each object can only have one
comment associated with it, so successive calls of `COMMENT ON` to a single object will overwrite
the previous comment.

To read the comment on an object you need to query the [mz_internal.mz_comments](/sql/system-catalog/mz_internal/#mz_comments)
catalog table.

## Privileges

To comment on an object, the current role must be the owner of that object. Roles themselves do not
have owners, so to comment on a role object you must have the `CREATEROLE` privilege. For more
information on ownership and privileges, see [Role-based access control](/manage/access-control/rbac).

## Examples

```sql
--- Add comments.
COMMENT ON TABLE foo IS 'this table is important';
COMMENT ON COLUMN foo.x IS 'holds all of the important data';

--- Update a comment.
COMMENT ON TABLE foo IS 'holds non-important data';

--- Remove a comment.
COMMENT ON TABLE foo IS NULL;

--- Read comments.
SELECT * FROM mz_internal.mz_comments;
```
