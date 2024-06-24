
---
title: "SHOW"
description: "Display the value of a configuration parameter."
menu:
  main:
    parent: 'commands'

---

`SHOW` displays the value of a configuration parameter.

## Syntax

{{< diagram "show-stmt.svg" >}}

Field                  | Use
-----------------------|-----
_name_                 | The name of the configuration parameter to display.
**ALL**                | Display the values of all configuration parameters.

### Aliased configuration parameters

There are a few configuration parameters that act as aliases for other
configuration parameters.

- `schema`: an alias for showing the first resolvable schema in `search_path`
- `time zone`: an alias for `timezone`

{{% configuration-parameters %}}

## Examples

### Show active cluster

```mzsql
SHOW cluster;
```
```
 cluster
---------
 quickstart
```

### Show transaction isolation level

```mzsql
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
