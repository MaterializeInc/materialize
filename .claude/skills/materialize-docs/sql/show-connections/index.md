---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-connections/
complexity: intermediate
description: '`SHOW CONNECTIONS` lists the connections configured in Materialize.'
doc_type: reference
keywords:
- SHOW CONNECTIONS
- WHERE
- FROM
- LIKE
product_area: Indexes
status: stable
title: SHOW CONNECTIONS
---

# SHOW CONNECTIONS

## Purpose
`SHOW CONNECTIONS` lists the connections configured in Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW CONNECTIONS` lists the connections configured in Materialize.



`SHOW CONNECTIONS` lists the connections configured in Materialize.

## Syntax

This section covers syntax.

```sql
SHOW CONNECTIONS
[FROM <schema_name>]
[LIKE <pattern> | WHERE <condition(s)>]
;
```text

Syntax element                | Description
------------------------------|------------
**FROM** \<schema_name\>      | If specified, only show connections from the specified schema. For available schema names, see [`SHOW SCHEMAS`](/sql/show-schemas).
**LIKE** \<pattern\>          | If specified, only show connections that match the pattern.
**WHERE** <condition(s)>      | If specified, only show connections that match the condition(s).

## Examples

This section covers examples.

```mzsql
SHOW CONNECTIONS;
```text

```nofmt
       name          | type
---------------------+---------
 kafka_connection    | kafka
 postgres_connection | postgres
```text

```mzsql
SHOW CONNECTIONS LIKE 'kafka%';
```text

```nofmt
       name       | type
------------------+------
 kafka_connection | kafka
```


## Related pages

- [`CREATE CONNECTION`](../create-connection)
- [`DROP CONNECTION`](../drop-connection)

