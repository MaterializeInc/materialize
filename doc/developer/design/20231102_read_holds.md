# Read Holds

- Durable Subscribes epic: [17541](https://github.com/MaterializeInc/database-issues/issues/5094)
- Configurable compaction windows: [16701](https://github.com/MaterializeInc/database-issues/issues/4840)

## The Problem

Users are not able to use `SUBSCRIBE` without data loss.
When materialize restarts or a client connection fails for some reason,
users are not able to restart the `SUBSCRIBE` `AS OF` their most recently observed timestamp because the read frontier has usually progressed past that time.
This happens because the compaction window is 1 second for all user objects.

## Success Criteria

`SUBSCRIBE` must be usable without data loss across server restarts or client reconnections.

## Solution Proposal

Add a new `HOLD` object that acquires controller read holds on listed objects at a specific timestamp.
The `HOLD` must be manually advanced to newer timestamps by users which will release the previous and acquire a new read hold to the specified timestamp.
If the timestamp falls behind a configurable window, the `HOLD` is marked in an error state and the read holds are released.

The `HOLD` can be created by:

```
CREATE HOLD <name>
ON <object1>[, <object2> ...]
[AT <time>]
[WITH (
    MAX LAG = '3h'::INTERVAL,
)]
```

A `HOLD` can be created on indexes, materialized views, tables, and sources.
It cannot exist on sinks or unmaterialized views.
If a `HOLD` exists for a directly queryable object (i.e., not indexes) at time `t`, `SELECT` and `SUBSCRIBE` on that object `AS OF t` will successfully execute.
If a `HOLD` exists for an index, the same is true if the index satisfies the query.
A `HOLD` on any non-index object will not also create the `HOLD` on any indexes of the object.
An `AS OF` query will restrict the indexes or object it considers to those that are valid for the requested time.

The objects the `HOLD` is created on do not need to be in the same schema or database.
The `HOLD` can be created in any schema on which the user has a `CREATE` privilege (see (Alternatives)[#Alternatives] for other takes on this).
The user must have a `SELECT` privilege on all objects and a `USAGE` privilege on all affected clusters.

Terminology side note:
Both the compute and storage controllers expose a `CollectionState` with `read_capabilities` and `implied_capability`.
Although reading these are both exposed in the API, `read_capabilities` should never be read by the adapter, which should limit itself to reading only `implied_capability`.
When this document uses the term "read frontier", it is referring to the `implied_capability`.

If `AT <time>` is specified, the read hold is created at that time.
For each object in the `HOLD`, the time must be in advance of that object's read frontier *or* any other `HOLD`'s time on that object, otherwise an error occurs.
We want to support safe swapping out of one `HOLD` for another, and since we only reference a collection's `implied_capability` (which is not aware of any read holds),
it is necessary to call out specifically that other `HOLD`s times are valid for new `HOLD`s.
If `AT <time>` is not specified, Materialize automatically chooses the least valid read frontier among all objects of the `HOLD`.

`HOLD`s are destroyed by:

```
DROP HOLD <name>
```

A `DROP` will release its current read hold.
In-progress `SUBSCRIBE`s and transactions will not be affected after a `DROP`, but will continue to run as expected.
Those will still be aquiring their own read holds in addition to the `HOLD` object.

Creating a `HOLD` will increase resource consumption (mostly memory) in proportion to the amount of time it is held for.
Users must periodically advance the `HOLD` to a more recent time to allow for resources to be released.

`HOLD`s are advanced by:

```
ALTER HOLD <name> ADVANCE [TO <time>]
```

If `TO <time>` is specified, the new time will be created at that time.
For each object of the `HOLD`, the new time must be greater or equal to the lowest of all `HOLD`s (including this one) and the `implied_capability`.
(This implies that a `HOLD` can go backward in time if another `HOLD` satisfies the above condition.)
If `TO <time>` is not specified, the new time is the least common physical read frontier of all objects named in the `HOLD`.
The `HOLD` releases its current read hold and aquires a new read hold at the new time.

The `MAX LAG` option (required, but defaults to `'3h'`) is a duration.
Its upper bound is limited by a LD-configured setting.
If difference between the write frontier of the objects and the `HOLD` time is greater than `MAX LAG`, the `HOLD`'s time is automatically advanced to `write frontier - MAX HOLD`.
(Transactions and `SUBSCRIBE` are not affected, as in the `ALTER HOLD ADVANCE` case.)
`SELECT` and `SUBSCRIBE` `AS OF` a time that is before a `HOLD`'s time will return a helpful error message that a `HOLD` exists but is in advance of the `AS OF` time.

It is an error to `DROP` an object that is part of a `HOLD` unless the `DROP` is `CASCADE`.
If the `DROP` is `CASCADE`, any `HOLD`s on the dropped object are also dropped (even if the `HOLD` is also over other objects).

`HOLD`s cannot be mutated (objects added or removed) after creation, but they can be renamed in a transaction like other objects, and so can achieve the same goal.

In order to specify a `HOLD` on a new index or materialized view whose underlying object may have a long history,
`HOLD`s support a special multi-statement DDL of the form

```
BEGIN;
CREATE INDEX i ...;
CREATE HOLD ON i;
COMMIT;
```

In a transaction, exactly one `CREATE HOLD` statement is allowed to follow a `CREATE` statement if the `HOLD` names exactly and only the created object.

### Monitoring

To monitor `HOLD`s, a new `mz_catalog.mz_holds` table will be maintained with structure:

- `id text`: the `HOLD`'s id
- `schema_id text`
- `name text`
- `at uint8`: the `HOLD`'s time

A `mz_catalog.mz_hold_objects` describes objects belonging to a `HOLD`

- `hold_id text`: the `HOLD`'s id
- `on_id text`: the object's id

### Implementation of time tracking

The `HOLD` object has an associated time that must be written down somewhere on creation then updated on advancement.
The time will be stored in the catalog, so a DDL operation is performed.
Due to performance concerns of users simultaneously advancing many `HOLD`s at once, and DDLs needing to be serialized, we will batch all incoming `ADVANCE` requests.
Although these are DDL operations, this design doc does *not* propose batching all DDLs together, only `ADVANCE`s.
This should produce a natural rate limit of a few per second and scale as usage grows.
`HOLD`s must also be accounted for in `bootstrap_index_as_of` and `bootstrap_materialized_view_as_of`.

## Minimal Viable Prototype

An MVP exists at https://github.com/MaterializeInc/materialize/pull/22976.

## Alternatives

- Instead of supporting multiple objects, we could support a single object and enforce the `HOLD` is created in its schema.
Or, we could support multiple objects, but they must all be in the same schema and the `HOLD` must be in that schema as well.

- These could be called `READ HOLD`s instead of `HOLD`s.

- `SUBSCRIBES` themselves could be made durable, or we introduce `CREATE SUBSCRIPTION` that would similarly achieve this goal ((proposal)[https://github.com/MaterializeInc/database-issues/issues/5094#issuecomment-1569340739]).

- Allow users to configure the compaction window of objects.
This has problems: the window could be too short for recovery or too long and overuse resources, OOMing a cluster.
Having users routinely advance a `HOLD` should scale the extra resource use to exactly how far behind a user is.

- The stored time could be stored in one or many persist shards.
This might scale better than using the catalog, but has the downside of separating the full state of a `HOLD` from its catalog entry.

## Open questions

- Should `HOLD`s be allowed on system objects?
Allowing this could put the introspection cluster at risk, but then again so can `BEGIN; SELECT * from mz_x`.

- What are the actual resource increases for a `HOLD`?
Do indexes take more memory, and all the others only take more s3 but not more memory?
