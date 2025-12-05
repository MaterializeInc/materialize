---
title: "RESET"
description: "Reset a configuration parameter to its default value."
menu:
  main:
    parent: 'commands'
---

`RESET` restores the value of a configuration parameter to its default value.
This command is an alternative spelling for [`SET...TO DEFAULT`](../set).

To see the current value of a configuration parameter, use [`SHOW`](../show).

## Syntax

```mzsql
RESET <parameter_name>;
```


Syntax element | Description
---------------|------------
`<parameter_name>` | The configuration parameter's name.

{{% configuration-parameters %}}

## Examples

### Reset search path

```mzsql
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
