# Replica Expiration

- Associated issues/prs: [#26029](https://github.com/MaterializeInc/materialize/issues/26029) [#29587](https://github.com/MaterializeInc/materialize/pull/29587)

## The Problem

Temporal filters currently require Materialize to maintain all future retractions
of data that is currently visible. For long windows, the retractions could be at
timestamps beyond our next scheduled restart, for example during our weekly
DB releases.

For instance, in the example below, the temporal filter in the `last_30_sec`
view causes two diffs to be generated for every row inserted into `events`, the
row itself and a retraction 30 seconds later. However, if the replica is
restarted in the next 30 seconds, the retraction diff is never processed, making
it unnecessary to generate and store that extra diff.

```sql
-- Create a table of timestamped events.
CREATE TABLE events (
content TEXT,
event_ts TIMESTAMP
);
-- Create a view of events from the last 30 seconds.
CREATE VIEW last_30_sec AS
SELECT event_ts, content
FROM events
WHERE mz_now() <= event_ts + INTERVAL '30s';

INSERT INTO events VALUES ('hello', now());

COPY (SUBSCRIBE (SELECT event_ts, content FROM last_30_sec)) TO STDOUT;
1686868190714   1       2023-06-15 22:29:50.711 hello  -- now()
1686868220712   -1      2023-06-15 22:29:50.711 hello  -- now() + 30s
```

## Success Criteria

Diffs associated with timestamps beyond a set expiration time---mainly the
retractions generated in temporal filters---are dropped without affecting
correctness, resulting in lower memory utilization.

## Out of Scope

- Dataflows whose timeline type is not `Timeline::EpochMillis`. We rely on the
  frontier timestamp being comparable to wall clock time of the replica.

## Solution Proposal

We introduce a new LaunchDarkly feature flag that allows us to configure the
expiration time for replicas for each environment. When the feature flag is
enabled (non-zero offset), replicas will filter out diffs that are beyond the
expiration time. To ensure correctness, replicas will panic if their frontier
exceeds the expiration time before the replica is restarted.

More concretely, we make the following changes:

* Introduce a new dyncfg `compute_replica_expiration` to set an offset `Duration`.
* If the offset is configured with a non-zero value, compute the
  `replica_expiration` time as `now() + offset`. This value specifies the maximum
  time for which the replica is expected to be running. Consequently, diffs
  associated with timestamps beyond this limit do not have to be stored and can
  be dropped.
* We only consider dataflows with timeline kind `Timeline::EpochMillis`. For
  these dataflows, we propagate `replica_expiration` to the existing `until`
  checks in `mfp.evaluate()`, such that any data beyond the expiration time is
  filtered out.
* To ensure correctness, we add checks in `Context::export_index` to stop the
  replica with a panic if the frontier exceeds the expiration time. This is to
  prevent the replica from serving requests without any data that has
  potentially been dropped.
* If the expiration time is exceeded, the replica will panic and restart with a
  new expiration limit as an offset of the new start time. This time, any data
  whose timestamps fall within the new limit are not filtered, thus maintaining
  correctness.

## Alternatives

-

## Open Questions

- What is the appropriate default expiration time?
  - Given that we currently restart replicas every week as part of the DB release
    and leaving some buffer for skipped week, 3 weeks (+1 day margin) seems like
    a good limit to start with.
