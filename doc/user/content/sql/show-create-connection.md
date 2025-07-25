---
title: "SHOW CREATE CONNECTION"
description: "`SHOW CREATE CONNECTION` returns the statement used to create the connection."
menu:
  main:
    parent: commands
---

`SHOW CREATE CONNECTION` returns the DDL statement used to create the connection.

## Syntax

```sql
SHOW [REDACTED] CREATE CONNECTION <connection_name>
```

{{< yaml-table data="show_create_redacted_option" >}}

For available connection names, see [`SHOW CONNECTIONS`](/sql/show-connections).

## Examples

```mzsql
SHOW CREATE CONNECTION kafka_connection;
```

```nofmt
    name          |    create_sql
------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 kafka_connection | CREATE CONNECTION "materialize"."public"."kafka_connection" TO KAFKA (BROKER 'unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9092', SASL MECHANISMS = 'PLAIN', SASL USERNAME = SECRET sasl_username, SASL PASSWORD = SECRET sasl_password)
```

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/show-create-connection.md" >}}

## Related pages

- [`SHOW CONNECTIONS`](../show-sources)
- [`CREATE CONNECTION`](../create-connection)
