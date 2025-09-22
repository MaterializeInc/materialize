---
title: "EXECUTE"
description: "`EXECUTE`"
menu:
  main:
    parent: commands
---

`EXECUTE` plans and executes [prepared statements](../prepare). Since prepared statements only last the duration of a session, the statement must have been prepared during the current session.

If the `PREPARE` statement specified some parameters, you must pass values compatible with those parameters to `EXECUTE`. Values are considered compatible here when they can be [_assignment cast_](../../sql/functions/cast/#valid-casts). (This is the same category of casting that happens for `INSERT`.)


## Syntax

{{< diagram "execute.svg" >}}

Field | Use
------|-----
**name**  | The name of the prepared statement to execute.
**parameter**  |  The actual value of a parameter to the prepared statement.

## Example

The following example [prepares a statement](/sql/prepare/) `a` and runs it
using the `EXECUTE` statement:

```mzsql
PREPARE a AS SELECT 1 + $1;
EXECUTE a (2);
```

All prepared statements will be cleared at the end of a session. You can also
explicitly deallocate the statement using [`DEALLOCATE`].


## Related pages

- [`PREPARE`]
- [`DEALLOCATE`]

[`PREPARE`]:../prepare
[`DEALLOCATE`]:../deallocate
