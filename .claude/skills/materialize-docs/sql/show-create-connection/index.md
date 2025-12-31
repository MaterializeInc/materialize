---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-create-connection/
complexity: intermediate
description: '`SHOW CREATE CONNECTION` returns the statement used to create the connection.'
doc_type: reference
keywords:
- CREATE THE
- SHOW CREATE CONNECTION
- SHOW CREATE
product_area: Indexes
status: stable
title: SHOW CREATE CONNECTION
---

# SHOW CREATE CONNECTION

## Purpose
`SHOW CREATE CONNECTION` returns the statement used to create the connection.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW CREATE CONNECTION` returns the statement used to create the connection.



`SHOW CREATE CONNECTION` returns the DDL statement used to create the connection.

## Syntax

This section covers syntax.

```sql
SHOW [REDACTED] CREATE CONNECTION <connection_name>;
```text

<!-- Dynamic table: show_create_redacted_option - see original docs -->

For available connection names, see [`SHOW CONNECTIONS`](/sql/show-connections).

## Examples

This section covers examples.

```mzsql
SHOW CREATE CONNECTION kafka_connection;
```text

```nofmt
    name          |    create_sql
------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 kafka_connection | CREATE CONNECTION "materialize"."public"."kafka_connection" TO KAFKA (BROKER 'unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9092', SASL MECHANISMS = 'PLAIN', SASL USERNAME = SECRET sasl_username, SASL PASSWORD = SECRET sasl_password)
```

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schema containing the connection.


## Related pages

- [`SHOW CONNECTIONS`](../show-sources)
- [`CREATE CONNECTION`](../create-connection)

