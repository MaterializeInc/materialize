---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-materialized-views/
complexity: beginner
description: '`SHOW MATERIALIZED VIEWS` returns a list of materialized views being
  maintained in Materialize.'
doc_type: reference
keywords:
- SHOW MATERIALIZED
- FROM
- IN
- SHOW MATERIALIZED VIEWS
product_area: Views
status: stable
title: SHOW MATERIALIZED VIEWS
---

# SHOW MATERIALIZED VIEWS

## Purpose
`SHOW MATERIALIZED VIEWS` returns a list of materialized views being maintained in Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW MATERIALIZED VIEWS` returns a list of materialized views being maintained in Materialize.



`SHOW MATERIALIZED VIEWS` returns a list of materialized views being maintained
in Materialize.

## Syntax

This section covers syntax.

```mzsql
SHOW MATERIALIZED VIEWS [ FROM <schema_name> ] [ IN <cluster_name> ];
```text

Syntax element                | Description
------------------------------|------------
**FROM** <schema_name>      | If specified, only show materialized views from the specified schema. Defaults to first resolvable schema in the search path. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**IN** <cluster_name>       | If specified, only show materialized views from the specified cluster.

## Examples

This section covers examples.

```mzsql
SHOW MATERIALIZED VIEWS;
```text

```nofmt
     name     | cluster
--------------+----------
 winning_bids | quickstart
```text

```mzsql
SHOW MATERIALIZED VIEWS LIKE '%bid%';
```text

```nofmt
     name     | cluster
--------------+----------
 winning_bids | quickstart
```

## Related pages

- [`SHOW CREATE MATERIALIZED VIEW`](../show-create-materialized-view)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)

