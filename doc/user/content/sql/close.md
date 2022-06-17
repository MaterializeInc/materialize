---
title: "CLOSE"
description: "`CLOSE` closes a cursor."
menu:
  main:
    parent: "commands"
---

`CLOSE` closes a cursor previously opened with [`DECLARE`](/sql/declare).

## Syntax

{{< diagram "close.svg" >}}

Field | Use
------|-----
_cursor&lowbar;name_ | The name of an open cursor to close.
