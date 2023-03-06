# Compute Introspection sources/tables/views improvements

## Summary

We have identified several issues with the sources documented
[here](https://materialize.com/docs/sql/system-catalog/mz_internal/). In
particular, some key views are missing (e.g., a way to connect channel
IDs to the operators they connect), and there are various existing ones that
have inconsistently named fields or inconsistent
semantics. Furthermore, the documentation of the various relations is
incomplete (e.g., it doesn't make clear whether each relation is
per-replica or global). 

## Goals

* Compute introspection relations should be useful and easy to understand
for internal debugging.

* They should be useful and easy to use for building external
  introspection UI.

## Non-Goals

* The relations do not need to be polished enough for external
consumption (although it's hoped they prove useful, we don't consider
it a bug if they're not). Customer-friendly introspection will be
provided in the web UI by the "external introspection" project.

* We make no attempt to maintain backwards compatibility with
  previously-existing queries, although as part of this work we will
  update the internal tools that use the introspection relations.

## Description

### Schema inconsistencies and inelegancies 

In this section, we describe the various issues that make the schema
confusing.

#### Per-worker vs. aggregated

Several relations have a `worker_id` column. Technically, the
semantics of this column are the same across all the relations: it
corresponds to the worker on which the data was measured. However, in
practice the behavior is very different, and falls into a few separate
categories, as described below.

##### Sharded relations

*Example:* `mz_arrangement_sizes`. These are relations that report
some value that is expected to differ across all workers, such that
the semantically meaningful value is (often) the sum of all the
per-worker values. For example, a user interested in the total size of
each arrangement would normally want to issue a query like `SELECT
operator_id, sum(records), sum(batches) FROM mz_arrangement_sizes
GROUP BY operator_id`.

There are also a few relations where `sum` is not the most interesting
aggregate; for example, the `min` across workers in
`mz_compute_frontiers`. "Sharded" might not be the most precise way to
describe these, but I include them in this section because the point
is basically the same.

The existence of the per-worker underlying sources is not completely
useless, e.g. because they are useful for detecting skew. However,
they are probably less commonly useful than a summed view would
be. Unforunately, this summed view does not exist for most such
relations (one counterexample is `mz_records_per_dataflow` and
`mz_records_per_dataflow_global`).

##### Duplicated relations

*Example:* `mz_dataflow_channels`. These relations report static
structural properties of dataflows. As such, they will always report
exactly the same values across all workers, except in one of two
cases:

* Our dataflow-rendering logic is buggy, and we invalidly produce
  differently-shaped dataflows on different workers, or
* Due to a race condition, where the relation is queried just after
  one worker has emitted a log message corresponding to the dataflow,
  but just before another worker has.

These cases are unlikely to be observable in practice, the first because
dataflow mismatches will usually cause unpredictable behavior
(including crashes) long before we have a chance to usefully
investigate them via introspection sources, and the second because
such race conditions will typically be very short-lived.

Thus, the argument for making global views (e.g., that simply filter
the underlying sources by worker 0) is even stronger than for the
"[Sharded relations](#sharded-relations)" case.


#### `_internal` and `raw` variants

Several of the relations are views over very similar underlying
sources whose names are suffixed with `_internal` or infixed with
`raw`, which are similar enough to be possibly confusing, but mean
something completely different. `_internal` relations are those that
count (or sum) something in the Differential Dataflow count field; the
corresponding non-`_internal` views are simple aggregates that move
that count into the data. For example, `mz_arrangement_sharing` is
defined as

``` sql
SELECT
    operator_id,
    worker_id,
    pg_catalog.count(*) AS count
FROM mz_internal.mz_arrangement_sharing_internal
GROUP BY operator_id, worker_id"
```

This dimension is applied inconsistently: not all relations that
report a count or sum in the data are derived from a corresponding
`_internal` source. 

`raw` variants, on the other hand, measure some time value in
nanoseconds, whereas their corresponding non-`raw` views convert the
times to the SQL interval type.

Confusingly, not all times are reported as intervals; for example,
`mz_scheduling_parks` has `slept_for` and `requested` columns, whose
type is `bigint` and whose unit is undocumented (in fact, it is nanoseconds).

#### Inconsistent presence of `sum` column in histograms

Some histograms include a per-bucket sum, which allows consumers to
compute average or total values precisely. Confusingly, not all
histograms report such a column (e.g. `mz_worker_compute_delays` does
not, whereas `mz_worker_raw_compute_delays` does).

#### Lack of clarity around per-replica vs. global relations

Many of the relations are logging sources that run locally in
clusters, are not persisted, and report different data depending on
the currently selected replica. Views based on these sources are also
per-replica. It is not straightforward to tell from our docs which is
the case for each relation.

Furthermore, sometimes the same logical value is reported by two
relations, one reporting `clusterd`'s view of the world, and the other
reporting `environmentd`'s. For example, `mz_compute_frontiers`
vs. `mz_cluster_replica_frontiers`. However, the names do not make it
clear what the distinction is. 

### Suggested fixes for schema inconsistencies

* All per-worker relations should have a corresponding "global" view
  where the results are aggregated across workers in the way most
  likely to be useful (for example, summing, taking the minimum, or
  restricting to worker 0). Because these global views are more likely
  to be useful than the underlying sources, they should have the basic
  name (e.g., `mz_arrangement_sharing`). The underlying source should
  have a name with `per_worker` suffixed (e.g.,
  `mz_arrangement_sharing_per_worker`). Furthermore, the per-worker
  variant should be clearly less prominent in documentation (either at
  the bottom of the page, or in a "see more" section that has to be
  clicked to expand).
* The expectations about per-worker relations should be clearly
  documented (e.g., we should clearly say whether the values for each
  worker are expected to be the same during normal operation).
* All relations that report a simple count or sum should be views based on
  an `_internal` variant that maintains the count or sum in the
  Differential diff field. We may consider renaming this, since it
  conflicts confusingly with the overall name `mz_internal`.
* `raw` variants should be removed, and the underlying dataflows
  should be in terms of intervals.
* All histogram views or sources should have a per-bucket `sum`
  column.
* All histogram views or sources should be suffixed with `_histogram`,
  to make their role more obvious. 
* All per-replica sources and views should have their names prepended
  with some string (perhaps `clusterd_`, though I'm open to
  suggestions) that makes it obvious which ones they are. Furthermore,
  the limitations of per-replica relations should be clearly
  documented.

### Possibly useful missing views

#### Channel-to-operator mapping

See [GitHub
PR](https://github.com/MaterializeInc/materialize/pull/17825).

#### Scope child to parent mapping

The only way to understand the structure of scope nesting currently is
to use the `mz_dataflow_addresses` relation and do some tedious list
manipulation. A simple view of `(id, parent_id)` would make this much
simpler.

#### Transitive object dependencies and end-to-end lag

We should add the transitive closure of `mz_object_dependencies`,
called e.g. `mz_transitive_object_dependencies`.

This will allow us to see the end-to-end frontier lag for any
dataflow.

#### More views enriched with operator names, dataflow names, etc.

(Not quite sure about this -- it's tricky to balance ease-of-use with
not blowing up the set of documented relations)

## Alternatives

None known to me

## Open questions

* Are there further views that could be useful but that we don't have
access to? We should fill this out as more become known to us (e.g.,
while debugging customer issues) 

* Is there a better name than `_internal` for the non-aggregated sources?
