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

```sql
SHOW CONNECTIONS
[FROM <schema_name>]
[LIKE <pattern> | WHERE <condition(s)>]
```

Option                        | Description
------------------------------|------------
**FROM** \<schema_name\>      | If specified, only show connections from the specified schema. For available schema names, see [`SHOW SCHEMAS`](/sql/show-schemas).
**LIKE** \<pattern\>          | If specified, only show connections that match the pattern.
**WHERE** <condition(s)>      | If specified, only show connections that match the condition(s).

## Examples

```mzsql
SHOW CONNECTIONS;
```

```nofmt
       name          | type
---------------------+---------
 kafka_connection    | kafka
 postgres_connection | postgres
```

```mzsql
SHOW CONNECTIONS LIKE 'kafka%';
```

```nofmt
       name       | type
------------------+------
 kafka_connection | kafka
```


## Related pages

- [`CREATE CONNECTION`](../create-connection)
- [`DROP CONNECTION`](../drop-connection)
