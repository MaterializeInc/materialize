---
title: "DEALLOCATE"
description: "`DEALLOCATE` clears a prepared statement."
menu:
  main:
    parent: "sql"
---

{{< version-added v0.9.7 />}}

`DEALLOCATE` clears [prepared statements](../prepare) that have been created during the current session. Even without an explicit `DEALLOCATE` command, all prepared statements will be cleared at the end of a session.

## Syntax

```sql
DEALLOCATE [ PREPARE ]  [ name | ALL ]
```

<br/>
<details>
<summary>Diagram</summary>
<br>

{{< diagram "deallocate.svg" >}}

</details>
<br/>

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
