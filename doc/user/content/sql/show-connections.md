---
title: "SHOW CONNECTIONS"
description: "`SHOW CONNECTIONS` lists the connections configured in Materialize."
menu:
  main:
    parent: commands
aliases:
    - /sql/show-connection
---

`SHOW CONNECTIONS` lists the connections configured in Materialize.

## Syntax

{{< diagram "show-connections.svg" >}}

Field                | Use
---------------------|-----
_schema&lowbar;name_ | The schema to show connections from. If omitted, connections from all schemas are shown. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**FULL**             | Returns all connections, including information about the connection type.

## Examples

```sql
SHOW CONNECTIONS;
```

```nofmt
       name
------------------
 kafka_connection
 postgres_connection
```

```sql
SHOW FULL CONNECTIONS;
```

```nofmt
       name          | type
---------------------+------
 kafka_connection    | user
 postgres_connection | user
```

```sql
SHOW CONNECTIONS LIKE 'kafka%';
```

```nofmt
       name
------------------
 kafka_connection
```


## Related pages

- [`CREATE CONNECTION`](../create-connection)
- [`DROP CONNECTION`](../drop-connection)
