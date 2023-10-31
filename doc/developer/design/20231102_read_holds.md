# Read Holds

- Durable Subscribes epic: [17541](https://github.com/MaterializeInc/materialize/issues/17541)
- Configurable compaction windows: [16701](https://github.com/MaterializeInc/materialize/issues/16701)

## The Problem

Users are not able to use `SUBSCRIBE` without data loss.
When materialize restarts or a client connection fails for some reason,
users are not able to restart the `SUBSCRIBE` `AS OF` their most recently observed timestamp because the read frontier has usually progressed past that time.
This happens because the compaction window is 1 second for all user objects.

## Success Criteria

`SUBSCRIBE` must be usable without data loss across server restarts or client reconnections.

## Out of Scope

<!--
What does a solution to this problem not need to address in order to be
successful?

It's important to be clear about what parts of a problem we won't be solving
and why. This leads to crisper designs, and it aids in focusing the reviewer.
-->

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
    ERROR_IF_OLDER_THAN = '24h'::INTERVAL
)]
```

A `HOLD` can be created on indexes, materialized views, tables, and sources.
It cannot exist on sinks or unmaterialized views.
If a `HOLD` exists for a directly queryable object (i.e., not indexes) at time `t`, `SELECT` and `SUBSCRIBE` on that object `AS OF t` will successfully execute.
If a `HOLD` exists for an index, the same is true if the index satisfies the query.
A `HOLD` on any non-index object will not also create the `HOLD` on those indexes.
Because this may be surprising to users, if an `AS OF` query whose plan uses an index errors because the `AS OF` is less than the index's read frontier and a `HOLD` exists on the index's object,
a helpful error message will be produced telling the user that their `HOLD` must be on the index, not the underlying object.

The objects the `HOLD` is created on do not need to be in the same schema or database.
The `HOLD` can be created in any schema on which the user has a `CREATE` privilege (see (Alternatives)[#Alternatives] for other takes on this).
The user must have a `READ` privilege on all objects.

Definitions:
- **physical read frontier**: the read frontier of an object excluding `HOLD`s and any running queries or transactions.
- **logical read frontier**: the read frontier of an object including `HOLD`s and any running queries or transactions.

If `AT <time>` is specified, the read hold is created at that time.
It is an error if that time is less than any object's logical frontier.
Due to the use of the logical frontier, a new `HOLD` can be created while at the time of an existing `HOLD` (say, because the object set of a `HOLD` needs to be changed by creating a new one).
If `AT <time>` is not specified, Materialize automatically chooses the least common physical read frontier of all objects.

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
It is an error if that time is less than any object's logical read frontier.
If `TO <time>` is not specified, the new time is the least common physical read frontier of all objects.
The `HOLD` releases its current read hold and aquires a new read hold at the new time.

The `ERROR_IF_OLDER_THAN` option (required, but defaults to `'24h'`) is a duration.
If difference between the least common physical read frontier of the objects and the `HOLD` time is greater than `ERROR_IF_OLDER_THAN`, the `HOLD`'s read hold is released and the `HOLD` enters an error state.
(Transactions and `SUBSCRIBE` are not affected, as in the `ALTER HOLD ADVANCE` case.)
In the error state, `ALTER HOLD ADVANCE` will return an error.
`SELECT` and `SUBSCRIBE` `AS OF` a time that is between the erroring `HOLD`'s time and the least common physical read frontier will return a helpful error message that the `HOLD` errored because it was not advanced.

It is an error to `DROP` an object that is part of a `HOLD` unless the `DROP` is `CASCADE`.
If the `DROP` is `CASCADE`, any `HOLD`s on the dropped object are also dropped (even if the `HOLD` is also over other objects).

### Monitoring

To monitor `HOLD`s, a new `mz_catalog.mz_holds` table will be maintained with structure:

- `id text`: the `HOLD`'s id
- `database_id text`
- `schema_id text`
- `name text`
- `on text[]`: an array of the object ids in the `HOLD`
- `at uint8`: the `HOLD`'s time

### Implementation of time tracking

The `HOLD` object has an associated time that must be written down somewhere on creation then updated on advancement.
This will use a persist shard to back it, and is thus similar to a 1-row table.
As a result of not storing it in the catalog (see Alternatives for the motivation), the `AT <time>` syntax must not be part of the `create_sql` of the `HOLD` object and must be purified away.
On coordinator bootstrap, a `HOLD` will do a 1-time fetch of its state from the shard to resume its operation.

## Minimal Viable Prototype

<!--
Build and share the minimal viable version of your project to validate the
design, value, and user experience. Depending on the project, your prototype
might look like:

- A Figma wireframe, or fuller prototype
- SQL syntax that isn't actually attached to anything on the backend
- A hacky but working live demo of a solution running on your laptop or in a
  staging environment

The best prototypes will be validated by Materialize team members as well
as prospects and customers. If you want help getting your prototype in front
of external folks, reach out to the Product team in #product.

This step is crucial for de-risking the design as early as possible and a
prototype is required in most cases. In _some_ cases it can be beneficial to
get eyes on the initial proposal without a prototype. If you think that
there is a good reason for skpiping or delaying the prototype, please
explicitly mention it in this section and provide details on why you you'd
like to skip or delay it.
-->

## Alternatives

- Instead of supporting multiple objects, we could support a single object and enforce the `HOLD` is created in its schema.
Or, we could support multiple objects, but they must all be in the same schema and the `HOLD` must be in that schema as well.

- These could be called `READ HOLD`s instead of `HOLD`s.

- `SUBSCRIBES` themselves could be made durable, or we introduce `CREATE SUBSCRIPTION` that would similarly achieve this goal ((proposal)[https://github.com/MaterializeInc/materialize/issues/17541#issuecomment-1569340739]).

- Allow users to configure the compaction window of objects.
This has problems: the window could be too short for recovery or too long and overuse resources, OOMing a cluster.
Having users routinely advance a `HOLD` should scale the extra resource use to exactly how far behind a user is.

- The stored time could be stored in the catalog instead of persist.
This would cause a full catalog transaction on any `ALTER HOLD ADVANCE` across the entire customer region, which will eventually not scale (failing our use case isolation goal) since those operations must be serially ordered (even with platform v2).

## Open questions

- Should `HOLD`s be allowed on system objects?
Allowing this could put the introspection cluster at risk, but then again so can `BEGIN; SELECT * from mz_x`.

- What are the actual resource increases for a `HOLD`?
Do indexes take more memory, and all the others only take more s3 but not more memory?

- Should we support adding or removing objects to `HOLD`s?
Removes seem like an easy yes.
Additions could be ok if the read frontier is compatible.

- Should the backing persist shard be for all `HOLD`s?

- Should the backing persist shard participate in the persist-txn shard?
Are there future scaling limitations if yes?

- Instead of a using a dedicated persist shard, should the `mz_holds` table be used to discover the previous time of a `HOLD`?
This seems dangerous because we don't use anything there as system-of-record, and we might forget in the future that we cannot wipe it.
