---
title: "ALTER SYSTEM RESET"
description: "Reset a system variable to its default value."
menu:
  main:
    parent: 'commands'
---

`ALTER SYSTEM RESET` restores the value of a system variable to its default value. This command is an alternative spelling for [`ALTER SYSTEM SET...TO DEFAULT`](../alter-system-set).

To see the current value of a system variable, use [`SHOW`](../show).

## Syntax

{{< diagram "reset-system-variable.svg" >}}

Field | Use
------|-----
_variable&lowbar;name_ | The system variable name.

{{% system-variables %}}

## Examples

### Reset enable RBAC

```sql
SHOW enable_rbac_checks;

 enable_rbac_checks
-------------
 on

RESET enable_rbac_checks;

SHOW enable_rbac_checks;

 enable_rbac_checks
-------------
 off
```

## Related pages

- [`SHOW`](../show)
- [`ALTER SYSTEM SET`](../alter-system-set)
