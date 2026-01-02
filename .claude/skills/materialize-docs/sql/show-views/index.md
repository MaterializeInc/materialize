---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-views/
complexity: intermediate
description: '`SHOW VIEWS` returns a list of views in Materialize.'
doc_type: reference
keywords:
- SHOW VIEWS
- name
- FROM
product_area: Views
status: stable
title: SHOW VIEWS
---

# SHOW VIEWS

## Purpose
`SHOW VIEWS` returns a list of views in Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW VIEWS` returns a list of views in Materialize.



`SHOW VIEWS` returns a list of views in Materialize.

## Syntax

This section covers syntax.

```mzsql
SHOW VIEWS [FROM <schema_name>];
```text

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>        | If specified, only show views from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).

## Details

This section covers details.

### Output format for `SHOW VIEWS`

`SHOW VIEWS`'s output is a table, with this structure:

```nofmt
 name
-------
 ...
```text

Field | Meaning
------|--------
**name** | The name of the view.

## Examples

This section covers examples.

```mzsql
SHOW VIEWS;
```text
```nofmt
  name
---------
 my_view
```

## Related pages

- [`SHOW CREATE VIEW`](../show-create-view)
- [`CREATE VIEW`](../create-view)

