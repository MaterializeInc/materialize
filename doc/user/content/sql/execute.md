---
title: "EXECUTE"
description: "`EXECUTE`"
menu:
  main:
    parent: "sql"
---

{{< version-added v0.9.7 />}}

`EXECUTE` plans and executes [prepared statements](../prepare). Since prepared statements only last the duration of a session, the statement must have been prepared during the current session.

If the `PREPARE` statement specified some parameters, you must pass values compatible with those parameters to `EXECUTE`.


## Syntax

```sql
EXECUTE name ( parameter_value [, ...] )
```

<br/>
<details>
<summary>Diagram</summary>
<br>

{{< diagram "execute.svg" >}}

</details>
<br/>

Field | Use
------|-----
**name**  | The name of the prepared statement to execute.
**parameter_value**  |  The actual value of a parameter to the prepared statement.

## Example

```sql
EXECUTE a ('a', 'b', 1 + 2)
```

## Related pages

- [`PREPARE`]
- [`DEALLOCATE`]

[`PREPARE`]:../prepare
[`DEALLOCATE`]:../deallocate
