---
title: Views (Non-Materialized and Materialized )
description: Conceptual page on views.
menu:
  main:
    parent: concepts
    weight: 15
    identifier: 'concepts-views'
---

## Overview

In SQL, views represent a query that you save with some given name. These are
used primarily as shorthand for some lengthy, complicated `SELECT` statement.
Materialize uses the idiom of views similarly, but the implication of views is
fundamentally different.

Materialize offers the following types of views:

Type | Use
-----|-----
**Materialized views** | Incrementally updated views whose results are persisted in durable storage
**Non-materialized views** | Queries saved under a name for reference, like traditional SQL views

All views in Materialize are built by reading data from sources and other views.

## Non-materialized views

Non-materialized views simply store a verbatim query and provide a shorthand
for performing the query.

Unlike materialized views, non-materialized views _do not_ store the results of
their embedded queries. The results of a view can be incrementally
maintained in memory within a [cluster](/concepts/clusters/) by
creating an index. This allows you to serve queries without
the overhead of materializing the view.

See [`CREATE VIEW`](/sql/create-view) for details about
non-materialized views.

## Materialized views

Materialized views embed a query like a traditional SQL view, but&mdash;unlike a
SQL view&mdash;compute and incrementally update the results of this embedded
query. The results of a materialized view are persisted in durable storage,
which allows you to effectively decouple the computational resources used for
view maintenance from the resources used for query serving.

Materialize accomplishes incremental updates by creating a set of persistent
transformations&mdash;known as a "dataflow"&mdash;that represent the query's
output. As new data comes in from sources, it's fed into materialized views that
query the source. Materialize then incrementally updates the materialized view's
output by understanding what has changed in the data, based on the source's
envelope. Any changes to a view's output are then propagated to materialized
views that query it, and the process repeats.

When reading from a materialized view, Materialize simply returns the dataflow's
current result set from durable storage. To improve the speed of queries on
materialized views, we recommend creating [indexes] based on
common query patterns.

See [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view) for details
about materialized views.



## Related pages

- [`CREATE VIEW`](/sql/create-view)
- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view)
- [`CREATE INDEX`](/sql/create-index)
