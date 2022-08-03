---
title: "DROP CONNECTION"
description: "`DROP CONNECTION` removes a connection from Materialize."
menu:
  main:
    parent: 'commands'

---

`DROP CONNECTION` removes a connection from Materialize. If there are sources depending on the connection, you must explicitly drop them first, or use the `CASCADE` option.

## Syntax

{{< diagram "drop-connection.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the specified connection does not exist.
_connection&lowbar;name_ | The connection you want to drop. For available connections, see [`SHOW CONNECTIONS`](../show-connections).
**CASCADE** | Remove the connection and its dependent objects.
**RESTRICT** | Do not drop the connection if it has dependencies. _(Default)_

## Examples

### Dropping a connection with no dependencies

To drop an existing connection, run:

```sql
DROP CONNECTION kafka_connection;
```

To avoid issuing an error if the specified connection does not exist, use the `IF EXISTS` option:

```sql
DROP CONNECTION IF EXISTS kafka_connection;
```

### Dropping a connection with dependencies

If the connection has dependencies, Materialize will throw an error similar to:

```sql
DROP CONNECTION kafka_connection;
```

```nofmt
ERROR:  cannot drop materialize.public.kafka_connection: still depended upon by catalog item
'materialize.public.kafka_source'
```

, and you'll have to explicitly ask to also remove any dependent objects using the `CASCADE` option:

```sql
DROP CONNECTION kafka_connection CASCADE;
```

## Related pages

- [`SHOW CONNECTIONS`](../show-connections)
