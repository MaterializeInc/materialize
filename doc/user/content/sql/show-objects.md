---
title: "SHOW OBJECTS"
description: "`SHOW OBJECTS` returns a list of all objects in Materialize for a given schema."
menu:
  main:
    parent: commands
aliases:
    - /sql/show-object
---

`SHOW OBJECTS` returns a list of all objects in Materialize for a given schema.
Objects include tables, sources, sinks, views, materialized views, indexes,
secrets and connections.

## Syntax

```mzsql
SHOW OBJECTS [ FROM <schema_name> ]
```

Option                       | Description
-----------------------------|------------
**FROM** <schema_name>       | If specified, only show objects from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Details

### Output format

`SHOW OBJECTS` will output a table with two columns, `name`and `type`.

## Examples

```mzsql
SHOW SCHEMAS;
```
```nofmt
  name
--------
 public
```
```mzsql
SHOW OBJECTS FROM public;
```
```nofmt
  name          | type
----------------+-------
my_table        | table
my_source       | source
my_view         | view
my_other_source | source
```
```mzsql
SHOW OBJECTS;
```
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
