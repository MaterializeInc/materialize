---
audience: developer
canonical_url: https://materialize.com/docs/sql/alter-view/
complexity: intermediate
description: '`ALTER VIEW` changes properties of a view.'
doc_type: reference
keywords:
- ALTER VIEW
product_area: Views
status: stable
title: ALTER VIEW
---

# ALTER VIEW

## Purpose
`ALTER VIEW` changes properties of a view.

If you need to understand the syntax and options for this command, you're in the right place.


`ALTER VIEW` changes properties of a view.



Use `ALTER VIEW` to:
- Rename a view.
- Change owner of a view.

## Syntax

This section covers syntax.

#### Rename

### Rename

To rename a view:

<!-- Syntax example: examples/alter_view / syntax-rename -->

#### Change owner

### Change owner

To change the owner of a view:

<!-- Syntax example: examples/alter_view / syntax-change-owner -->

## Privileges

The privileges required to execute this statement are:

- Ownership of the view being altered.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the view is namespaced by
  a schema.


