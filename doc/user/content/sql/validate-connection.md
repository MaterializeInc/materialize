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

{{< include-md
file="shared-content/sql-command-privileges/validate-connection.md" >}}

## Related pages

- [`CREATE CONNECTION`](/sql/create-connection/)
- [`SHOW CONNECTIONS`](/sql/show-connections)
