---
audience: developer
canonical_url: https://materialize.com/docs/sql/create-view/
complexity: intermediate
description: '`CREATE VIEW` defines view, which provides an alias for the embedded
  `SELECT` statement.'
doc_type: reference
keywords:
- in memory
- CREATE VIEW
- IF NOT EXISTS
- TEMPORARY
- TEMP
- OR REPLACE
product_area: Views
status: stable
title: CREATE VIEW
---

# CREATE VIEW

## Purpose
`CREATE VIEW` defines view, which provides an alias for the embedded `SELECT` statement.

If you need to understand the syntax and options for this command, you're in the right place.


`CREATE VIEW` defines view, which provides an alias for the embedded `SELECT` statement.


`CREATE VIEW` defines a view, which simply provides an alias
for the embedded `SELECT` statement.

The results of a view can be incrementally maintained **in memory** within a
[cluster](/concepts/clusters/) by creating an [index](../create-index).
This allows you to serve queries without the overhead of
materializing the view.

### Usage patterns

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See views/indexes documentation for details --> --> -->
<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See views/indexes documentation for details --> --> -->

## Syntax

[See diagram: create-view.svg]

Field | Use
------|-----
**TEMP** / **TEMPORARY** | Mark the view as [temporary](#temporary-views).
**OR REPLACE** | If a view exists with the same name, replace it with the view defined in this statement. You cannot replace views that other views depend on, nor can you replace a non-view object with a view.
**IF NOT EXISTS** | If specified, _do not_ generate an error if a view of the same name already exists. <br/><br/>If _not_ specified, throw an error if a view of the same name already exists. _(Default)_
_view&lowbar;name_ | A name for the view.
**(** _col_ident_... **)** | Rename the `SELECT` statement's columns to the list of identifiers, both of which must be the same length. Note that this is required for statements that return multiple columns with the same identifier.
_select&lowbar;stmt_ | The [`SELECT` statement](../select) to embed in the view.

## Details

[//]: # "TODO(morsapaes) Add short usage patterns section + point to relevant
architecture patterns once these exist."

### Temporary views

The `TEMP`/`TEMPORARY` keyword creates a temporary view. Temporary views are
automatically dropped at the end of the SQL session and are not visible to other
connections. They are always created in the special `mz_temp` schema.

Temporary views may depend upon other temporary database objects, but non-temporary
views may not depend on temporary objects.

## Examples

This section covers examples.

### Creating a view

```mzsql
CREATE VIEW purchase_sum_by_region
AS
    SELECT sum(purchase.amount) AS region_sum,
           region.id AS region_id
    FROM region
    INNER JOIN user
        ON region.id = user.region_id
    INNER JOIN purchase
        ON purchase.user_id = user.id
    GROUP BY region.id;
```

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all types used in the view definition.
- `USAGE` privileges on the schemas for the types in the statement.
- Ownership of the existing view if replacing an existing
  view with the same name (i.e., `OR REPLACE` is specified in `CREATE VIEW` command).


## Additional information

- Views can be monotonic; that is, views can be recognized as append-only.

## Related pages

- [`SHOW VIEWS`](../show-views)
- [`SHOW CREATE VIEW`](../show-create-view)
- [`DROP VIEW`](../drop-view)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
- [`CREATE INDEX`](../create-index)