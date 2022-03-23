---
title: "CLOSE"
description: "`CLOSE` closes a cursor."
menu:
  main:
    parent: "sql"
---

{{< version-added v0.5.3 />}}

`CLOSE` closes a cursor previously opened with [`DECLARE`](/sql/declare).

## Syntax

```sql
CLOSE cursor_name
```

<br/>
<details>
<summary>Diagram</summary>
<br>

{{< diagram "close.svg" >}}

</details>
<br/>

Field | Use
------|-----
_cursor&lowbar;name_ | The name of an open cursor to close.

## Example

Close a cursor named pointer:

```sql
CLOSE pointer;
```