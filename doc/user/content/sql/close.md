---
title: "CLOSE"
description: "`CLOSE` closes a cursor."
menu:
  main:
    parent: "commands"
---

{{< version-added v0.5.3 />}}

`CLOSE` closes a cursor previously opened with [`DECLARE`](/sql/declare).

## Syntax

{{< diagram "close.svg" >}}

Field | Use
------|-----
_cursor&lowbar;name_ | The name of an open cursor to close.
