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

## Special Syntax

There are a few session variables that act as aliases for other session variables.

- `SCHEMA`: `SCHEMA` is an alias for `search_path`. Only one schema can be specified using this syntax. The `TO` and `=` syntax are optional.
- `NAMES`: `NAMES` is an alias for `client_encoding`. The `TO` and `=` syntax must be omitted.
- `TIME ZONE`: `TIME ZONE` is an alias for `timezone`. The `TO` and `=` syntax must be omitted.

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
