---
audience: developer
canonical_url: https://materialize.com/docs/sql/alter-type/
complexity: intermediate
description: '`ALTER TYPE` changes properties of a type.'
doc_type: reference
keywords:
- ALTER TYPE
product_area: Indexes
status: stable
title: ALTER TYPE
---

# ALTER TYPE

## Purpose
`ALTER TYPE` changes properties of a type.

If you need to understand the syntax and options for this command, you're in the right place.


`ALTER TYPE` changes properties of a type.



Use `ALTER TYPE` to:
- Rename a type.
- Change owner of a type.

## Syntax

This section covers syntax.

#### Rename

### Rename

To rename a type:

<!-- Syntax example: examples/alter_type / syntax-rename -->

#### Change owner

### Change owner

To change the owner of a type:

<!-- Syntax example: examples/alter_type / syntax-change-owner -->

## Privileges

The privileges required to execute this statement are:

- Ownership of the type being altered.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the type is namespaced by a
    schema.


