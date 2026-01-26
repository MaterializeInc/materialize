---
title: "CREATE VIEW"
description: "`CREATE VIEW` defines view, which provides an alias for the embedded `SELECT` statement."
menu:
  # This should also have a "non-content entry" under Reference, which is
  # configured in doc/user/config.toml
  main:
    parent: 'commands'
---

Use `CREATE VIEW` to define a view, which simply provides an alias for the
embedded `SELECT` statement. The results of a view can be incrementally
maintained **in memory** within a [cluster](/concepts/clusters/) by creating an
[index](../create-index). This allows you to serve queries without the overhead
of materializing the view.

## Syntax

{{< tabs >}}
{{< tab "CREATE VIEW" >}}
### Create view
To create a view:

{{% include-syntax file="examples/create_view" example="syntax" %}}
{{< /tab >}}
{{< tab "CREATE OR REPLACE VIEW" >}}
### Create or replace view
To create, or if a view exists with the same name, replace it with the view
defined in this statement:

{{< note >}}
You cannot replace views that other views depend on,
nor can you replace a non-view object with a view.
{{< /note >}}

{{% include-syntax file="examples/create_view" example="syntax-replace" %}}
{{< /tab >}}
{{< /tabs >}}

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

{{% include-headless "/headless/sql-command-privileges/create-view" %}}

## Additional information

- Views can be monotonic; that is, views can be recognized as append-only.

## Related pages

- [`SHOW VIEWS`](../show-views)
- [`SHOW CREATE VIEW`](../show-create-view)
- [`DROP VIEW`](../drop-view)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
- [`CREATE INDEX`](../create-index)
