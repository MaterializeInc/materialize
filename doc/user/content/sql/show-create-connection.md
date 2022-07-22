---
title: "SHOW CREATE CONNECTION"
description: "`SHOW CREATE CONNECTION` returns the statement used to create the connection."
menu:
  main:
    parent: commands
---

`SHOW CREATE CONNECTION` returns the DDL statement used to create the connection.

## Syntax

{{< diagram "show-create-connection.svg" >}}

Field | Use
------|-----
_connection&lowbar;name_ | The connection you want to get the `CREATE` statement for. For available connections, see [`SHOW CONNECTIONS`](../show-connections).

## Examples

```sql
SHOW CREATE CONNECTION kafka_connection;
```

```nofmt
    Connection   |        Create Connection
-----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 kafka_connection   | CREATE CONNECTION "materialize"."public"."kafka_connection" FOR KAFKA BROKER 'unique-jellyfish-0000-kafka.upstash.io:9092', SASL MECHANISMS = 'PLAIN', SASL USERNAME = SECRET sasl_username, SASL PASSWORD = SECRET sasl_password
```

## Related pages

- [`SHOW CONNECTIONS`](../show-sources)
- [`CREATE CONNECTION`](../create-connection)
