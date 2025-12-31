---
audience: developer
canonical_url: https://materialize.com/docs/sql/drop-view/
complexity: intermediate
description: '`DROP VIEW` removes a view from Materialize.'
doc_type: reference
keywords:
- DROP VIEW
- DROP ALL
- IF EXISTS
- DROP SOURCE...CASCADE
- CREATE THEM
- RESTRICT
- CASCADE
product_area: Views
status: stable
title: DROP VIEW
---

# DROP VIEW

## Purpose
`DROP VIEW` removes a view from Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


`DROP VIEW` removes a view from Materialize.



`DROP VIEW` removes a view from Materialize.

## Conceptual framework

Materialize maintains views after you create them. If you no longer need the
view, you can remove it.

Because views rely on receiving data from sources, you must drop all views that
rely on a source before you can [drop the source](../drop-source) itself. You can achieve this easily using **DROP SOURCE...CASCADE**.

## Syntax

This section covers syntax.

```mzsql
DROP VIEW [IF EXISTS] <view_name> [RESTRICT|CASCADE];
```text

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the named view does not exist.
`<view_name>` | The view you want to drop. You can find available view names through [`SHOW VIEWS`](../show-views).
**RESTRICT** | Optional. Do not drop this view if any other views depend on it. _(Default)_
**CASCADE** | Optional. Drop all views that depend on this view.

## Examples

This section covers examples.

```mzsql
SHOW VIEWS;
```text
```nofmt
  name
---------
 my_view
```text
```mzsql
DROP VIEW my_view;
```text
```nofmt
DROP VIEW
```

## Privileges

The privileges required to execute this statement are:

- Ownership of the dropped view.
- `USAGE` privileges on the containing schema.


## Related pages

- [`CREATE VIEW`](../create-view)
- [`SHOW VIEWS`](../show-views)
- [`SHOW CREATE VIEW`](../show-create-view)
- [`DROP OWNED`](../drop-owned)

