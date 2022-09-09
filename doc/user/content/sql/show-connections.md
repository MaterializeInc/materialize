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

## Examples

```sql
SHOW CONNECTIONS;
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
       name      | type
-----------------+------
 kafka_connection| user
```


## Related pages

- [`CREATE CONNECTION`](../create-connection)
- [`DROP CONNECTION`](../drop-connection)
