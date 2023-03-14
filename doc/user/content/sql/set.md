---
title: "SET"
description: "Modify the value of a session variable."
menu:
  main:
    parent: 'commands'

---

`SET` modifies the value of a session variable. By default, values are set for the duration of the current session.

To see the current value of a session variable, use [`SHOW`](../show).

## Syntax

{{< diagram "set-session-variable.svg" >}}

Field                   | Use
------------------------|-----
_variable&lowbar;name_  | The name of the session variable to modify.
_variable&lowbar;value_ | The value to assign to the session variable.
**SESSION**             | **_(Default)_** Set the value for the duration of the current session.
**LOCAL**               | Set the value for the duration of a single transaction.
**DEFAULT**             | Reset the session variable's default value. Equivalent to [`RESET`](../reset).

{{% session-variables %}}

## Examples

### Set active cluster

```sql
SHOW cluster;

 cluster
---------
 default

SET cluster = 'quickstart';

SHOW cluster;

  cluster
------------
 quickstart
```

### Set transaction isolation level

```sql
SET transaction_isolation = 'serializable';
```

## Related pages

- [`RESET`](../reset)
- [`SHOW`](../show)
