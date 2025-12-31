---
audience: developer
canonical_url: https://materialize.com/docs/sql/alter-table/
complexity: intermediate
description: '`ALTER TABLE` changes properties of a table.'
doc_type: reference
keywords:
- ALTER TABLE
product_area: Indexes
status: stable
title: ALTER TABLE
---

# ALTER TABLE

## Purpose
`ALTER TABLE` changes properties of a table.

If you need to understand the syntax and options for this command, you're in the right place.


`ALTER TABLE` changes properties of a table.



Use `ALTER TABLE` to:

- Rename a table.
- Change owner of a table.
- Change retain history configuration for the table.

## Syntax

This section covers syntax.

#### Rename

### Rename

To rename a table:

<!-- Syntax example: examples/alter_table / syntax-rename -->

#### Change owner

### Change owner

To change the owner of a table:

<!-- Syntax example: examples/alter_table / syntax-change-owner -->

#### (Re)Set retain history config

### (Re)Set retain history config

To set the retention history for a user-populated table:

<!-- Syntax example: examples/alter_table / syntax-set-retain-history -->

To reset the retention history to the default for a user-populated table:

<!-- Syntax example: examples/alter_table / syntax-reset-retain-history -->

## Privileges

The privileges required to execute this statement are:

- Ownership of the table being altered.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the table is namespaced by
  a schema.


