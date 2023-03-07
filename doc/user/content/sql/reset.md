---
title: "RESET"
description: "`RESET` a session variable value to its initial value."
menu:
  main:
    parent: 'commands'
---

Use the `RESET` command to revert a session variable to its initial value.

## Syntax
{{< diagram "reset-session-variable.svg" >}}

Field | Use
------|-----
_variable&lowbar;name_ | The session variable name.

#### Example

Reset the current cluster session variable:
```sql
RESET CLUSTER;
```