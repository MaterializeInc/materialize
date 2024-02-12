
---
title: "SHOW"
description: "Display the value of a session or system variable."
menu:
  main:
    parent: 'commands'

---

`SHOW` displays the value of a session or system variable.

## Syntax

{{< diagram "show-variable.svg" >}}

Field                  | Use
-----------------------|-----
_variable&lowbar;name_ | The name of the session or system variable to display.
**ALL**                | Display the values of all session and system variables.

{{% session-variables %}}

## Special Syntax

`SHOW SCHEMA` will show the first resolvable schema on the search path, or `NULL` if no such schema exists.

{{% system-variables %}}

## Examples

### Show active cluster

```sql
SHOW cluster;
```
```
 cluster
---------
 quickstart
```

### Show transaction isolation level

```sql
SHOW transaction_isolation;
```
```
 transaction_isolation
-----------------------
 strict serializable
```

## Related pages

- [`RESET`](../reset)
- [`SET`](../set)
