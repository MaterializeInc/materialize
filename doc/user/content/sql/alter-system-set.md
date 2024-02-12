---
title: "ALTER SYSTEM SET"
description: "`ALTER SYSTEM SET` modifies system variables, or session variables globally (i.e. for all users)."
menu:
  main:
    parent: 'commands'

---

`ALTER SYSTEM SET` modifies the value of a system variable, or the value of a
session variable globally (i.e. for all users).

To see the current value of a system variable, use [`SHOW`](../show).

## Syntax

{{< diagram "set-system-variable.svg" >}}

Field                   | Use
------------------------|-----
_variable&lowbar;name_  | The name of the system variable to modify.
_variable&lowbar;value_ | The value to assign to the system variable.
**DEFAULT**             | Reset the system variable's default value. Equivalent to [`ALTER SYSTEM RESET`](../alter-system-reset).

{{% system-variables %}}

## Examples

### Enable RBAC

```sql
SHOW enable_rbac_checks;

 enable_rbac_checks
---------
 off

ALTER SYSTEM SET enable_rbac_checks = true;

SHOW enable_rbac_checks;

 enable_rbac_checks
------------
 on
```

## Related pages

- [`ALTER SYSTEM RESET`](../alter-system-reset)
- [`SHOW`](../show)
