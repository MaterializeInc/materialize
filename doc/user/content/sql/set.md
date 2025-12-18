---
title: "SET"
description: "Modify the value of a configuration parameter in the current session."
menu:
  main:
    parent: 'commands'

---

`SET` modifies the value of a configuration parameter for the current session.
By default, values are set for the duration of the current session.

To see the current value of a configuration parameter, use [`SHOW`](../show).

## Syntax

```mzsql
SET [SESSION|LOCAL] <config> TO|= <value|DEFAULT>;
```

Syntax element                | Description
------------------------------|------------
**SESSION**               | Optional. Set the value for the duration of the current session. **_(Default)_**
**LOCAL**                 | Optional. If specified, set the value for the duration of a single transaction.
`<config>`                | The name of the configuration parameter to modify.
`<value>`                 | The value to assign to the parameter.
**DEFAULT**               | Use the parameter's default value. Equivalent to [`RESET`](../reset).

{{% configuration-parameters %}}

### Aliased configuration parameters

There are a few configuration parameters that act as aliases for other
configuration parameters.

- `schema`: `schema` is an alias for `search_path`. Only one schema can be specified using this syntax. The `TO` and `=` syntax are optional.
- `names`: `names` is an alias for `client_encoding`. The `TO` and `=` syntax must be omitted.
- `time zone`: `time zone` is an alias for `timezone`. The `TO` and `=` syntax must be omitted.

## Examples

### Set active cluster

```mzsql
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

```mzsql
SET transaction_isolation = 'serializable';
```

### Set search path

```mzsql
SET search_path = public, qck;
```

```mzsql
SET schema = qck;
```

## Related pages

- [`RESET`](../reset)
- [`SHOW`](../show)
