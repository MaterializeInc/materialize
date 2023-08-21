---
title: "RESET"
description: "Reset a session variable to its default value."
menu:
  main:
    parent: 'commands'
---

`RESET` restores the value of a session variable to its default value. This command is an alternative spelling for [`SET...TO DEFAULT`](../set).

To see the current value of a session variable, use [`SHOW`](../show).

## Syntax

{{< diagram "reset-session-variable.svg" >}}

Field | Use
------|-----
_variable&lowbar;name_ | The session variable name.

{{% session-variables %}}

## Examples

### Reset search path

```sql
SHOW search_path;

 search_path
-------------
 qck

RESET search_path;

SHOW search_path;

 search_path
-------------
 public
```

## Related pages

- [`SHOW`](../show)
- [`SET`](../set)
