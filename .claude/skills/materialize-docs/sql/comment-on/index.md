---
audience: developer
canonical_url: https://materialize.com/docs/sql/comment-on/
complexity: advanced
description: '`COMMENT ON ...` adds or updates the comment of an object.'
doc_type: reference
keywords:
- COMMENT ON
- UPDATE A
product_area: Indexes
status: stable
title: COMMENT ON
---

# COMMENT ON

## Purpose
`COMMENT ON ...` adds or updates the comment of an object.

If you need to understand the syntax and options for this command, you're in the right place.


`COMMENT ON ...` adds or updates the comment of an object.



`COMMENT ON ...` adds or updates the comment of an object.

## Syntax

[See diagram: comment-on.svg]

## Details

`COMMENT ON` stores a comment about an object in the database. Each object can only have one
comment associated with it, so successive calls of `COMMENT ON` to a single object will overwrite
the previous comment.

To read the comment on an object you need to query the [mz_internal.mz_comments](/sql/system-catalog/mz_internal/#mz_comments)
catalog table.

## Privileges

The privileges required to execute this statement are:

- Ownership of the object being commented on (unless the object is a role).
- To comment on a role, you must have the `CREATEROLE` privilege.


For more information on ownership and privileges, see [Role-based access
control](/security/).

## Examples

This section covers examples.

```mzsql
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

