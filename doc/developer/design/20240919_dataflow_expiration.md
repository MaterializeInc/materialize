# Dataflow Expiration

- Associated issues/prs: [#26029](https://github.com/MaterializeInc/materialize/issues/26029) [#29587](https://github.com/MaterializeInc/materialize/pull/29587)

## The Problem

Temporal filters currently require Materialize to maintain all future retractions
of data that is currently visible. For long windows, the retractions could be at
timestamps beyond our next scheduled restart, which is typically our weekly
DB release.

For instance, in the example below, the temporal filter in the `last_30_days`
view causes two diffs to be generated for every row inserted into `events`, the
row itself and a retraction 30 days later. However, if the replica is
restarted in the next few days, the retraction diff is never processed, making
it redundant to keep the diff waiting to be processed.

```sql
-- Create a table of timestamped events.
CREATE TABLE events (
  content TEXT,
  event_ts TIMESTAMP
);
-- Create a view of events from the last 30 seconds.
CREATE VIEW last_30_days AS
SELECT event_ts, content
FROM events
WHERE mz_now() <= event_ts + INTERVAL '30 days';

INSERT INTO events VALUES ('hello', now());

COPY (SUBSCRIBE (SELECT event_ts, content FROM last_30_days)) TO STDOUT;
1727130590201   1       2023-09-23 22:29:50.201 hello  -- now()
1729722590222   -1      2023-10-23 22:29:50.222 hello  -- now() + 30 days
```

Dataflows with large temporal windows (e.g. a year) can generate a large number
of retractions that consume memory and CPU but are never used. Instead, all
such retractions can be dropped.

## Success Criteria

When temporal filters are in use, retraction diffs associated with timestamps
beyond a set expiration time can be dropped without affecting correctness,
resulting in lower memory and CPU utilization from halving the number of
processed diffs.

## Solution Proposal

A new LaunchDarkly feature flag is introduced that specifies an _expiration
offset_ (a `Duration`). The _replica expiration_ time is computed as the offset
added to the start time of the replica. Dataflows matching certain
criteria (detailed below) are then configured with a _dataflow expiration_
derived from the replica expiration. Diffs generated in these dataflows beyond
the dataflow expiration are dropped. To ensure correctness, panic checks are
added to these dataflows that ensure that the frontier does not exceed the
dataflow expiration before the replica is restarted.

An overview of the logic used for these features is as follows:
```
# Consider the `upper` for different dataflows
if mv_view_with_constant_values:
  upper := []
else if mv_with_refresh_every:
  upper := [next_refresh()]
else:
  upper := [write_frontier()]

# The `upper` for a dataflow considering all its transitive inputs
inputs_upper := meet(for all inputs i: i_upper)

# Dataflow expiration logic
if compute_replica_expiration_offset is not set:
  dataflow_replication := []
else for dataflows of type in [materialized view, index, subscribe]:
  replica_expiration := replica_start + compute_replica_expiration_offset
  if dataflow_timeline is not EpochMilliseconds:
    # Dataflows that do not depend on any source or table are not in the
    # EpochMilliseconds timeline
    dataflow_expiration := []
  else if refresh_interval set in any transitive dependency of dataflow:
    dataflow_expiration := []
  else if inputs_upper == []:
    dataflow_expiration := []
  else if inputs_upper > expiration:
    dataflow_expiration := inputs_upper
  else:
    dataflow_expiration := replica_expiration

dataflow_until := dataflow_until.meet(dataflow_expiration)
```

Note that we only consider dataflows representing materialized views, indexes,
and subscribes. These are long-running dataflows that maintain state during
their lifetime. Other dataflows such as peeks are transient and do not need to
explicitly drop retraction diffs.

More concretely, we make the following changes:

* Introduce a new dyncfg `compute_replica_expiration_offset`.
* If the offset is configured with a non-zero value, compute
  `replica_expiration = now() + offset`. This value specifies the maximum
  time for which the replica is expected to be running. Consequently, diffs
  associated with timestamps beyond this limit do not have to be stored and can
  be dropped.
* When building a dataflow, compute `dataflow_expiration` as per the logic
  described above. If non-empty, the `dataflow_expiration` is added to the
  dataflow `until` that ensures that any diff beyond this limit is dropped in
  `mfp.evaluate()`.
* To ensure correctness, we attach checks in `Context::export_index` and
  `Context::export_sink` that panic if the dataflow frontier exceeds the
  configured `dataflow_expiration`. This is to prevent the dataflow from
  serving potentially incorrect results due to dropped data.
* On a replica restart, `replica_expiration` and `dataflow_expiration` is
  recomputed as the offset to the new start time. Any data whose timestamps
  are within the new limit are now not dropped.

## Open Questions

- What is the appropriate default expiration time?
  - Given that we currently restart replicas every week as part of the DB release
    and leaving some buffer for skipped week, 3 weeks (+1 day margin) seems like
    a good limit to start with.

## Out of Scope

Dataflow expiration is disabled for the following cases:

- Dataflows whose timeline type is not `Timeline::EpochMillis`. We rely on the
  frontier timestamp being comparable to wall clock time of the replica.
- Dataflows that transitively depend on a materialized view with a non-default
  refresh schedule. Handling such materialized views would require additional
  logic to track the refresh schedules and ensure that the dataflow expiration
