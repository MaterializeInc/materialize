---
audience: developer
canonical_url: https://materialize.com/docs/sql/alter-materialized-view/
complexity: intermediate
description: '`ALTER MATERIALIZED VIEW` changes the parameters of a materialized view.'
doc_type: reference
keywords:
- ALTER MATERIALIZED
- ALTER MATERIALIZED VIEW
- SHOW MATERIALIZED
- SHOW CREATE
product_area: Views
status: stable
title: ALTER MATERIALIZED VIEW
---

# ALTER MATERIALIZED VIEW

## Purpose
`ALTER MATERIALIZED VIEW` changes the parameters of a materialized view.

If you need to understand the syntax and options for this command, you're in the right place.


`ALTER MATERIALIZED VIEW` changes the parameters of a materialized view.



Use `ALTER  MATERIALIZED VIEW` to:

- Rename a materialized view.
- Change owner of a materialized view.
- Change retain history configuration for the materialized view.

## Syntax

This section covers syntax.

#### Rename

### Rename

To rename a materialized view:

<!-- Syntax example: examples/alter_materialized_view / syntax-rename -->

#### Change owner

### Change owner

To change the owner of a materialized view:

<!-- Syntax example: examples/alter_materialized_view / syntax-change-owner -->

#### (Re)Set retain history config

### (Re)Set retain history config

To set the retention history for a materialized view:

<!-- Syntax example: examples/alter_materialized_view / syntax-set-retain-history -->

To reset the retention history to the default for a materialized view:

<!-- Syntax example: examples/alter_materialized_view / syntax-reset-retain-history -->

## Details

This section covers details.

## Privileges

The privileges required to execute this statement are:

- Ownership of the materialized view.
- In addition, to change owners:
  - Role membership in `new_owner`.
  - `CREATE` privileges on the containing schema if the materialized view is
  namespaced by a schema.


## Related pages

- [`SHOW MATERIALIZED VIEWS`](/sql/show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](/sql/show-create-materialized-view)

