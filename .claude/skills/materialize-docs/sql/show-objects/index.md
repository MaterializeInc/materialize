---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-objects/
complexity: intermediate
description: '`SHOW OBJECTS` returns a list of all objects in Materialize for a given
  schema.'
doc_type: reference
keywords:
- FROM
- SHOW OBJECTS
product_area: Indexes
status: stable
title: SHOW OBJECTS
---

# SHOW OBJECTS

## Purpose
`SHOW OBJECTS` returns a list of all objects in Materialize for a given schema.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW OBJECTS` returns a list of all objects in Materialize for a given schema.



`SHOW OBJECTS` returns a list of all objects in Materialize for a given schema.
Objects include tables, sources, sinks, views, materialized views, indexes,
secrets and connections.

## Syntax

This section covers syntax.

```mzsql
SHOW OBJECTS [ FROM <schema_name> ];
```text

Syntax element               | Description
-----------------------------|------------
**FROM** <schema_name>       | If specified, only show objects from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Details

This section covers details.

### Output format

`SHOW OBJECTS` will output a table with two columns, `name`and `type`.

## Examples

This section covers examples.

```mzsql
SHOW SCHEMAS;
```text
```nofmt
  name
--------
 public
```text
```mzsql
SHOW OBJECTS FROM public;
```text
```nofmt
  name          | type
----------------+-------
my_table        | table
my_source       | source
my_view         | view
my_other_source | source
```text
```mzsql
SHOW OBJECTS;
```text
```nofmt
  name    | type
----------+-------
my_table  | table
my_source | source
my_view   | view
```

## Related pages

- [`SHOW TABLES`](../show-tables)
- [`SHOW SOURCES`](../show-sources)
- [`SHOW SINKS`](../show-sinks)
- [`SHOW VIEWS`](../show-views)
- [`SHOW INDEXES`](../show-indexes)
- [`SHOW SECRETS`](../show-secrets)
- [`SHOW CONNECTIONS`](../show-connections)

