---
title: "DEALLOCATE"
description: "`DEALLOCATE` clears a prepared statement."
menu:
  main:
    parent: "commands"
---

`DEALLOCATE` clears [prepared statements](../prepare) that have been created during the current session. Even without an explicit `DEALLOCATE` command, all prepared statements will be cleared at the end of a session.

## Syntax

{{< diagram "deallocate.svg" >}}

Field | Use
------|-----
**PREPARE** | Disregarded.
**name**  | The name of the prepared statement to clear.
**ALL**  |  Clear all prepared statements from this session.

## Example

```sql
DEALLOCATE a;
```

## Related pages

- [`PREPARE`]
- [`EXECUTE`]

[`PREPARE`]:../prepare
[`EXECUTE`]:../execute
