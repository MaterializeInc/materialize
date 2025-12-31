---
audience: developer
canonical_url: https://materialize.com/docs/sql/close/
complexity: intermediate
description: '`CLOSE` closes a cursor.'
doc_type: reference
keywords:
- CLOSE
product_area: Indexes
status: stable
title: CLOSE
---

# CLOSE

## Purpose
`CLOSE` closes a cursor.

If you need to understand the syntax and options for this command, you're in the right place.


`CLOSE` closes a cursor.



Use `CLOSE` to close a cursor previously opened with [`DECLARE`](/sql/declare).

## Syntax

This section covers syntax.

```mzsql
CLOSE <cursor_name>;
```

Syntax element | Description
---------------|------------
`<cursor_name>` | The name of an open cursor to close.

