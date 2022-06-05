---
title: "EXECUTE"
description: "`EXECUTE`"
menu:
  main:
    parent: commands
---

`EXECUTE` plans and executes [prepared statements](../prepare). Since prepared statements only last the duration of a session, the statement must have been prepared during the current session.

If the `PREPARE` statement specified some parameters, you must pass values compatible with those parameters to `EXECUTE`.


## Syntax

{{< diagram "execute.svg" >}}

Field | Use
------|-----
**name**  | The name of the prepared statement to execute.
**parameter**  |  The actual value of a parameter to the prepared statement.

## Example

```sql
EXECUTE a ('a', 'b', 1 + 2)
```

## Related pages

- [`PREPARE`]
- [`DEALLOCATE`]

[`PREPARE`]:../prepare
[`DEALLOCATE`]:../deallocate
