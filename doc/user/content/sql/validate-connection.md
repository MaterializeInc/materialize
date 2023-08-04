---
title: "VALIDATE CONNECTION"
description: "`VALIDATE CONNECTION` validates the connection and authentication parameters to an external system"
pagerank: 40
menu:
  main:
    parent: 'commands'

---

`VALIDATE CONNECTION` validates the connection and authentication parameters to an external system.

## Syntax

{{< diagram "validate-connection.svg" >}}

Field   | Use
--------|-----
_connection_name_  | The identifier of the connection you want to validate.

## Description

The `VALIDATE CONNECTION` command can be used to validate the connection
parameters of a `CONNECTION` object on demand.

Upon executing the `VALIDATE CONNECTION` command, Materialize will initiate a
connection to the external system and will attempt to authenticate in the same
way as if it was about to ingest data. A successful connection and
authentication attempt will make the query return successfully with no rows. If
any problem is encountered then the query will return an error with the
associated error message.

## Privileges

The privileges required to execute this statement are:

- Usage privileges on the schema the connection belongs in.
- Usage privileges on the connection.

## Related pages

- [`CREATE CONNECTION`](/sql/create-connection/)
- [`SHOW CONNECTIONS`](/sql/show-connections)
