---
title: "DISCARD"
description: "Discard resets state associated with the current session."
menu:
  main:
    parent: 'sql'
---

`DISCARD` resets state associated with the current session.

## Syntax

```sql
DISCARD { TEMP | TEMPORARY | ALL }
```

<br/>
<details>
<summary>Diagram</summary>
<br>

{{< diagram "discard.svg" >}}

</details>
<br/>

Field | Use
------|-----
**TEMP** | Drops any temporary objects created by the current session.
**TEMPORARY** | Alias for `TEMP`.
**ALL** | Drops any temporary objects, deallocates any extant prepared statements, and closes any extant cursors that were created by the current session.

## Example

Discard everything:
```sql
DISCARD ALL;
```
