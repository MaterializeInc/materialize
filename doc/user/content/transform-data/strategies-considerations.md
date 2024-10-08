---
title: "Strategies and considerations"
description: ""
menu:
  main:
    parent: "transform-data"
    weight: 95
    name: "Strategies and considerations"
---

Materialize provides various [indexing strategies to optimize query
performance](/transform-data/optimization/). However, because indexes in
Materialize maintain the complete query results in memory (not just the index
keys and the pointers to data rows), ... [will be updated].

The following lists some strategies and considerations that may help you better
utilize the benefits provided by Materialize while managing
[costs](/administration/billing/).

## Strategies to reduce dataset/memory usage

### Projections

Indexes store the complete query results in memory, not just the index keys and
the pointers to data rows. As such, avoid extraneous fields in your query
projections. That is, if you are interested in a smaller
subset of the fields in your dataset, select only those fields instead of using
`SELECT *`.

### Temporal filters

If you are only interested in the data from a recent period of time (e.g., the
last 7 days, the last 24 hours, etc.), you can use [temporal
filters](/transform-data/patterns/temporal-filters) to create a [sliding
window](/transform-data/patterns/temporal-filters/#sliding-window) over the
dataset.

Considerations:

- Determine if you need to add a grace period in order to account for late
  arriving data (e.g., records that arrive out of order due to network issues).

- If using stacked views with temporal filters, avoid unnecessarily duplicating
  the same temporal filter up the stack. Since Materialize maintains all future
  retractions of the data that is currently within the temporal window (in
  anticipation of when data will fall outside the window), removing unnecessary
  temporal filters help reduce memory use.

- [*Private preview*](/releases/previews/). Set an expiration for maintaining
  future retractions. When using temporal filters, Materialize (in anticipation
  of when data will fall outside the window) maintains all future retractions of
  the data that is currently within the temporal window. Depending on your
  dataset size and window size, you may want to configure Materialize to drop
  expired retractions.

### Static datasets

Specifying [`REFRESH AT
CREATION`](/sql/create-materialized-view/#refresh-strategies) for your
materialized view effectively flags that view as static.

If your data model consists of large datasets that join with smaller, static
(i.e., does not change) datasets (such as a dimensional data model where your
dimensions do not change), consider using materialized views with a [`REFRESH AT
CREATION`](/sql/create-materialized-view/#refresh-strategies) specification for
your static datasets. Joins with a static dataset may be able to reduce the
amount of memory required for the other join inputs.

### Hot/warm/cold data patterns

If the freshness requirements of your workload vary such that some staleness is
acceptable, consider using hot/warm/cold data patterns with differing refresh
strategies.

| Data Type       | Description                                                                                                                                               |
|-----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| Hot data      | For data that requires immediate freshness, keep it in memory and use the default refresh strategy, which incrementally updates the data upon writes.     |
| Warm/Cold data | For data that can support some degree of staleness, you can store this data in a materialized view that uses a less-frequent refresh strategy [(*Private preview*)](/sql/create-materialized-view/#refresh-strategies). |

### Scheduled clusters

For clusters that only contain [materialized views with scheduled refresh](/sql/create-materialized-view/#refresh-strategies), you can [schedule the cluster](/sql/create-cluster/#scheduling) to
automatically turn on and off based on the refresh schedule.

### Idiomatic Materialize SQL

For various window functions, Materialize provides idiomatic Materialize SQL.
Consider rewriting your queries using idiomatic SQL.

In addition, you can use various [query hints](/sql/select/#query-hints) with
the idiomatic SQL to further improve the memory usage:

| Query                           | Recommended Query Hint                  |
|--------------------------------------|-----------------------------------------|
| For First/Last value in group queries | `AGGREGATE INPUT GROUP SIZE`            |
| For Top-K queries                    | `INPUT GROUP SIZE`                      |
| For Top-1 queries using `DISTINCT ON` | `DISTINCT ON INPUT GROUP SIZE`          |

### Index creation order

An index can take advantage of the work performed by other indexes but only if
these other indexes are created before it. That is, an index cannot take
advantage of indexes that are created after it. However, during [blue-green
deployments](/manage/dbt/development-workflows/#bluegreen-deployments) for your
upgrades, the index creations ordering respects the dependency ordering.

For example, if you create stacked views (i.e., views that depend on other
views), we generally recommend that you start by creating an index only on the
view that will serve results (unless otherwise suggested by the expected data
access patterns). If later on, you also create an index on an intermediate view,
the serving view index cannot benefit from the newly created index until the
serving index is recreated after the new index, such as with a [blue-green
[blue-green
deployments](/manage/dbt/development-workflows/#bluegreen-deployments).

## Other considerations

Your workload may require considerations separate from the memory
considerations.

### Source envelope type

The following considerations apply when using an upsert envelope:

- Larger source cluster is required when using upsert envelope since in addition
  to writing data to S3, a copy of each key consumed and the latest value is
  maintained on the source cluster.

- Using upsert envelope effectively dedupes your records (since a duplicated
  update is effectively a no-op), but the initial hydration of sources takes
  longer.

### Materialized views and startup

During startup, materialized views require memory proportional to both the input
and output. When estimating required resources, consider both the startup cost
and the steady-state cost.

### Serve queries outside of Materialize

Materialize can provide value by doing calculations on writes such that reads
on these calculated values are fast.

However, if you workload requires random access to a large dataset, it may be
more feasible (because of memory and performance constraints) to serve queries
from an external system instead of Materialize.  That is, have Materialize
perform the various computations/transformations of the data and sink out to
Kafka which then feeds into anothe system, from which you can perform your
lookups.

### Legacy sized clusters

For source clusters (i.e., clusters that have no compute objects) that
are using Upsert envelopes, legacy sized clusters may provide more disk space.

{{< warning >}}
Legacy sized clusters do not allow for compute objects to spill to disk, and, in
most cases, you should not use legacy sizes.
{{< /warning >}}
