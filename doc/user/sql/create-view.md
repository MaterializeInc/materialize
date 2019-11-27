---
title: "CREATE VIEW"
description: "`CREATE VIEW` creates a materialized view, which Materialize will incrementally maintain as updates occur to the underlying data."
menu:
  main:
    parent: 'sql'
---

`CREATE VIEW` creates a materialized view, which lets you retrieve incrementally
updated results of a `SELECT` query very quickly. Despite the simplicity of
creating a view, it's Materialize's most powerful feature.

## Conceptual framework

`CREATE VIEW` computes and maintains the results of a `SELECT` query in memory,
which is similar to a materialized view in the language of RDBMSes. (For more
information about materialized views, see [What is
Materialize?](../../overview/what-is-materialize))

This means that as data streams in from Kafka, Materialize incrementally updates
the results of the view's `SELECT` statement. Because these results are
available in memory, you will have incredibly fast access to the query's result
set.

## Syntax

{{< diagram "create-view.html" >}}

Field | Use
------|-----
_view&lowbar;name_ | A name for the view.
_select&lowbar;stmt_ | The [`SELECT` statement](../select) whose output you want to materialize and maintain.

## Details

### Overview

When creating a view, Materialize's internal Differential dataflow engine
creates a persistent dataflow for the corresponding `SELECT` statement. All of
the data that is available from the view's source's arrangements (which is
similar to an index in the language of RDBMSes) is then used to create the first
result set of the view.

As data continues to stream in from Kafka, Differential passes the data to the
appropriate dataflows, which are then responsible for making any necessary
updates to maintain the views you've defined.

When reading from a view (e.g. `SELECT * FROM some_view`), Materialize simply
returns the current result set for the persisted dataflow.

### Memory

Views are maintained in memory. Because of this, one must be sure that all
intermediate stages of the query, as well as its result set can fit in the
memory of a single machine, while also understanding the rate at which the
query's result set will grow.

For more detail about how different clauses impact memory usage, check out our
[`SELECT`](../select) documentation.

## Examples

```sql
CREATE VIEW purchase_sum_by_region
AS
    SELECT  sum(purchase.amount) AS region_sum,
            region.id AS region_id
    FROM mysql_simple_region AS region
    INNER JOIN mysql_simple_user AS user
        ON region.id = user.region_id
    INNER JOIN mysql_simple_purchase AS purchase
        ON purchase.user_id = user.id
    GROUP BY region.id;
```

In this example, as new users or purchases come in, the results of the view are
incrementally updated. For example, if a new purchase comes in for a specific
user, the underlying dataflow will determine which region that user belongs to,
and then increment the `region_sum` field with those results.

## Related pages

- [`SELECT`](../select)
- [`CREATE SOURCES`](../create-sources)
