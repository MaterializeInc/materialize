---
audience: developer
canonical_url: https://materialize.com/docs/sql/declare/
complexity: intermediate
description: '`DECLARE` creates a cursor.'
doc_type: reference
keywords:
- DECLARE
product_area: Indexes
status: stable
title: DECLARE
---

# DECLARE

## Purpose
`DECLARE` creates a cursor.

If you need to understand the syntax and options for this command, you're in the right place.


`DECLARE` creates a cursor.



`DECLARE` creates a cursor, which can be used with
[`FETCH`](/sql/fetch), to retrieve a limited number of rows at a time
from a larger query. Large queries or queries that don't ever complete
([`SUBSCRIBE`](/sql/subscribe)) can be difficult to use with many PostgreSQL drivers
that wait until all rows are returned before returning control to an
application. Using `DECLARE` and [`FETCH`](/sql/fetch) allows you to
fetch only some of the rows at a time.

## Syntax

This section covers syntax.

```mzsql
DECLARE <cursor_name> CURSOR FOR <query>;
```

Syntax element | Description
---------------|------------
`<cursor_name>` | The name of the cursor to be created.
`<query>` | The query ([`SELECT`](/sql/select) or [`SUBSCRIBE`](/sql/subscribe)) that will provide the rows to be returned by the cursor.

