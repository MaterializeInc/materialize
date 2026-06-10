---
title: "CLOSE"
description: "`CLOSE` closes a cursor."
menu:
  main:
    parent: "commands"
---

Use `CLOSE` to close a cursor previously opened with [`DECLARE`](/sql/declare).

## Syntax

```mzsql
CLOSE <cursor_name>;
```

Syntax element | Description
---------------|------------
`<cursor_name>` | The name of an open cursor to close.
