# CREATE VIEW
`CREATE VIEW` defines view, which provides an alias for the embedded `SELECT` statement.
Use `CREATE VIEW` to define a view, which simply provides an alias for the
embedded `SELECT` statement. The results of a view can be incrementally
maintained **in memory** within a [cluster](/concepts/clusters/) by creating an
[index](../create-index). This allows you to serve queries without the overhead
of materializing the view.

## Syntax


**CREATE VIEW:**
### Create view
To create a view:



```mzsql
CREATE [TEMP|TEMPORARY] VIEW [IF NOT EXISTS] <view_name>[(<col_ident>, ...)] AS
<select_stmt>;

```

| Syntax element | Description |
| --- | --- |
| `TEMP` / `TEMPORARY` | Optional. Mark the view as [temporary](/sql/create-view/#temporary-views). Temporary views are: - Created in the `mz_temp` schema. - Not visible to other connections. - Automatically dropped at the end of the SQL session  |
| `IF NOT EXISTS` | Optional. If specified, do not generate an error if a view with the same name already exists. If not specified, an error is generated if a view with the same name already exists.  |
| `<view_name>` | A name for the view.  |
| `(<col_ident>, ...)` | Optional if the `SELECT` statement return columns with unique names; else, is required if the `SELECT` statement returns multiple columns with the same identifier. If specified, renames the `SELECT` statement's columns to the list of identifiers. Both must be the same length.  |
| `<select_stmt>` | The [`SELECT` statement](/sql/select) that defines the view.  |


**CREATE OR REPLACE VIEW:**
### Create or replace view
To create, or if a view exists with the same name, replace it with the view
defined in this statement:

> **Note:** You cannot replace views that other views depend on,
> nor can you replace a non-view object with a view.




```mzsql
CREATE OR REPLACE VIEW <view_name> [(<col_ident>, ...)] AS <select_stmt>;

```

| Syntax element | Description |
| --- | --- |
| `<view_name>` | A name for the view.  |
| `(<col_ident>, ...)` | Optional if the `SELECT` statement return columns with unique names; else, is required if the `SELECT` statement returns multiple columns with the same identifier. If specified, renames the `SELECT` statement's columns to the list of identifiers. Both must be the same length.  |
| `<select_stmt>` | The [`SELECT` statement](/sql/select) that defines the view.  |




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
