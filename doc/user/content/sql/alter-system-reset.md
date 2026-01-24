---
title: "ALTER SYSTEM RESET"
description: "Globally reset a configuration parameter to its default value."
menu:
  main:
    parent: 'commands'
---

Use `ALTER SYSTEM RESET` to globally restore the value of a configuration
parameter to its default value. This command is an alternative spelling for
[`ALTER SYSTEM SET...TO DEFAULT`](../alter-system-set).

To see the current value of a configuration parameter, use [`SHOW`](../show).

## Syntax

```mzsql
ALTER SYSTEM RESET <config>;
```

Syntax element | Description
---------------|------------
`<config>`     | The configuration parameter's name.

{{% include-headless "/headless/configuration-parameters" %}}

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/alter-system-reset" %}}

## Related pages

- [`SHOW`](../show)
- [`ALTER SYSTEM SET`](../alter-system-set)
