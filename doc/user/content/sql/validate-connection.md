---
title: "VALIDATE CONNECTION"
description: "`VALIDATE CONNECTION` validates the connection and authentication parameters provided in a `CREATE CONNECTION` statement against the target external system"
pagerank: 40
menu:
  main:
    parent: 'commands'

---

`VALIDATE CONNECTION` validates the connection and authentication parameters
provided in a `CREATE CONNECTION` statement against the target external
system.

## Syntax

{{< diagram "validate-connection.svg" >}}

Field              | Use
-------------------|-----------
_connection_name_  | The identifier of the connection you want to validate.

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
