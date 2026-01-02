---
audience: developer
canonical_url: https://materialize.com/docs/sql/drop-connection/
complexity: intermediate
description: '`DROP CONNECTION` removes a connection from Materialize.'
doc_type: reference
keywords:
- DROP THEM
- IF EXISTS
- DROP CONNECTION
- RESTRICT
- CASCADE
product_area: Indexes
status: stable
title: DROP CONNECTION
---

# DROP CONNECTION

## Purpose
`DROP CONNECTION` removes a connection from Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`DROP CONNECTION` removes a connection from Materialize.



`DROP CONNECTION` removes a connection from Materialize. If there are sources
depending on the connection, you must explicitly drop them first, or use the
`CASCADE` option.

## Syntax

This section covers syntax.

```mzsql
DROP CONNECTION [IF EXISTS] <connection_name> [CASCADE|RESTRICT];
```text

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified connection does not exist.
`connection_name>` | The connection you want to drop. For available connections, see [`SHOW CONNECTIONS`](../show-connections).
**CASCADE** | Optional. If specified, remove the connection and its dependent objects.
**RESTRICT** | Optional. Do not drop the connection if it has dependencies. _(Default)_

## Examples

This section covers examples.

### Dropping a connection with no dependencies

To drop an existing connection, run:

```mzsql
DROP CONNECTION kafka_connection;
```text

To avoid issuing an error if the specified connection does not exist, use the `IF EXISTS` option:

```mzsql
DROP CONNECTION IF EXISTS kafka_connection;
```bash

### Dropping a connection with dependencies

If the connection has dependencies, Materialize will throw an error similar to:

```mzsql
DROP CONNECTION kafka_connection;
```text

```nofmt
ERROR:  cannot drop materialize.public.kafka_connection: still depended upon by catalog item
'materialize.public.kafka_source'
```text

, and you'll have to explicitly ask to also remove any dependent objects using the `CASCADE` option:

```mzsql
DROP CONNECTION kafka_connection CASCADE;
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the dropped connection.
- `USAGE` privileges on the containing schema.



## Related pages

- [`SHOW CONNECTIONS`](../show-connections)
- [`DROP OWNED`](../drop-owned)

