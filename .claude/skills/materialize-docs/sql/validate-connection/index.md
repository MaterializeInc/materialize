---
audience: developer
canonical_url: https://materialize.com/docs/sql/validate-connection/
complexity: intermediate
description: '`VALIDATE CONNECTION` validates the connection and authentication parameters
  provided in a `CREATE CONNECTION` statement against the target external system'
doc_type: reference
keywords:
- CREATE CONNECTION
- CREATE SOURCE
- CREATE SINK
- VALIDATE CONNECTION
product_area: Indexes
status: stable
title: VALIDATE CONNECTION
---

# VALIDATE CONNECTION

## Purpose
`VALIDATE CONNECTION` validates the connection and authentication parameters provided in a `CREATE CONNECTION` statement against the target external system

If you need to understand the syntax and options for this command, you're in the right place.


`VALIDATE CONNECTION` validates the connection and authentication parameters provided in a `CREATE CONNECTION` statement against the target external system



`VALIDATE CONNECTION` validates the connection and authentication parameters
provided in a `CREATE CONNECTION` statement against the target external
system.

## Syntax

This section covers syntax.

```mzsql
VALIDATE CONNECTION <connection_name>;
```

## Description

Upon executing the `VALIDATE CONNECTION` command, Materialize initiates a
connection to the target external system and attempts to authenticate in the
same way as if the connection were used in a `CREATE SOURCE` or `CREATE SINK`
statement. A successful connection and authentication attempt returns
successfully with no rows. If an issue is encountered, the command will return
a validation error.

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the containing schema.
- `USAGE` privileges on the connection.


## Related pages

- [`CREATE CONNECTION`](/sql/create-connection/)
- [`SHOW CONNECTIONS`](/sql/show-connections)

