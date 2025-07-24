---
title: "ALTER SYSTEM RESET"
description: "Globally reset a configuration parameter to its default value."
menu:
  main:
    parent: 'commands'
---

`ALTER SYSTEM RESET` globally restores the value of a configuration parameter to
its default value. This command is an alternative spelling for [`ALTER SYSTEM
SET...TO DEFAULT`](../alter-system-set).

To see the current value of a configuration parameter, use [`SHOW`](../show).

## Syntax

{{< diagram "alter-system-reset-stmt.svg" >}}

Field  | Use
-------|-----
_name_ | The configuration parameter's name.

{{% configuration-parameters %}}

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/alter-system-reset.md" >}}

## Related pages

- [`SHOW`](../show)
- [`ALTER SYSTEM SET`](../alter-system-set)
