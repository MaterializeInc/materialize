# Replacement materialized views

Associated:
- https://github.com/MaterializeInc/materialize/pull/34032 (MVP)
- https://github.com/MaterializeInc/materialize/pull/34039 (Per-object read only mode)

## Problem

At the moment, a materialized view names a _view definition_ and the _output columns_ derived from that definition.
We cannot change one or the other independently, which means that any change to a materialized view requires dropping and recreating it.
This is inconvenient for users as changes need to cascade through the dependency graph.

We call this strong coupling between a materialized view and its definition and output columns.
This design explores an alternative in which we can decouple these concepts, allowing users to change one without needing to drop and recreate the other.
This would move us closer to a losely coupled system, which is easier to maintain and evolve over time.

## Success criteria

We allow users to change the definition and output columns of a materialized view without needing to drop and recreate it.
We preserve existing dependencies on the materialized view when doing so, and we ensure that the system remains consistent.

Changing a running materialized can cause additional work for downstream consumers.
While we cannot avoid this work, we aim to provide tools to quantify the amount of changed data.

We provide a mechanism that allows cutting over to a new definition with minimal downtime.
A stop-and-restart approach is not acceptable, unless explicitly requested by the user.

## Background

Materialized views are views that Materialize maintains and writes down durably.
They're identified by a name, have a SQL definition, and produce a set of output columns.
The name of a materialized view uniquely identifies a _shard_, which is the durable storage location for the materialized view's data.
When a materialized view is created, Materialize plans and deploys a dataflow that computes the view's contents based on its definition.

(We can rename materialized views, which merely changes the name associated with the shard.)

A materialized view has a unique and immutable catalog item ID that is valid for the lifetime of the object.
It also has a global ID that identifies the dataflow, and associates a schema (`RelationDesc`) with the shard.
The global ID associates compute and storage: it needs to be registered with persist so persist knows what schema to expect, and to reclaim durable resources when the materialized view is dropped.

When a materialized view is dropped, Materialize tears down the dataflow and reclaims the shard.

## Principles

While not specific to this project, I want to outline some principles that helped me to come up with the design.
In user interfaces, it is a principle that all operations can be undone, for example by pressing Ctrl+z.
This allows users to correct mistakes or just decide otherwise.

Similarly, Materialize offers mechanisms with potentially disastrous outcomes that cannot be undone.
For example, a cluster might become overloaded, or we seal a materialized view for all times.
Extending the "undo" principle, we split operations into a _stage_ and _apply_ operation.
The stage operation creates objects, but localizes their effect to a cluster.
Only once the user applies a change, the effect becomes global.

The following solution uses this principle to apply changes to materialized views.

## Solution proposal

We introduce the notion of a "replacement" for maintained SQL objects, starting with materialized views.
A replacement allows users to stage the change of definition and output columns of a materialized view.
The user can then inspect the replacement, and decide to apply or discard it.

We add the following SQL commands:
* `CREATE REPLACEMENT replacement_name FOR MATERIALIZED VIEW mv_name AS SELECT ...`
    Creates a replacement for the specified materialized view with the new definition.
    The usual properties for materialized views apply, such as the cluster and its options.
* `ALTER MATERIALIZED VIEW mv_name APPLY REPLACEMENT replacement_name`
    Applies the specified replacement to the materialized view.
    This updates the definition and output columns of the materialized view to match those of the replacement.
    Existing dependencies on the materialized view are preserved.
* `DROP REPLACEMENT replacement_name`
    Discards the specified replacement without applying it.

When a replacement is created, we validate that the new definition is compatible with the existing materialized view.

We provide introspection commands:
* `SHOW REPLACEMENTS`
    Lists all replacements in the system.
* `SHOW [REDACTED] CREATE REPLACEMENT <replacement_name>`
    Shows the SQL command that would recreate the specified replacement.
* The `mz_replacement_materialized_views` catalog relation, which contains metadata about all replacements for materialized views.
* The `mz_show_replacements` view and index for serving the show replacements commands.

More specifically, we change the definition of a materialized view as follows:
* A materialized view is uniquely identified by its name and catalog item ID.
* A materialized view uniquely identifies a persist shard.
* A materialized view has a current definition and output columns, identified by a unique global ID.
* A materialized view can have additional versions, each with their own unique global ID and schema.

This corresponds with switch from a strongly-coupled model to a loosely-coupled model: We switch from binding the view definition and schema, to just binding the schmea.

## Formalism

Replacing a materialized view with a new version means that at some point in time, we need to switch the contents of the materialized view from the old definition to the new definition.

Let `mv = [mv-updates, since, upper]` be a correct view of the updates in `mv`.
Now consider switching over to a new definition `mv' = [mv'-updates, since', upper']` by applying a replacement `r = [r-updates, since_r, upper_r]`.
It must be true that:
* `since_r <= upper` (the replacement can start at or before the current upper frontier of the materialized view),
* `upper < r_upper` (the replacement must be able to catch up to the current upper frontier of the materialized view).

When applying the replacement at time `upper'`, we need to ensure that:

```
mv'_updates = append(mv-updates, upper', diff(upper', mv-updates, r-updates))
```

From this moment, onwards, the materialized view `mv` will reflect the updates from `mv'`.

### Schema evolution and multiple version

When applying a replacement, we need to ensure that the new schema is compatible with the existing schema.
We define compatibility as follows:
1. The schema must be the same as the original schema,
2. Or, the schema must be a superset of the original schema (i.e., it can add new columns but cannot remove existing ones).

Schema evolution is tied to what persist considers a safe schema change.
At the moment, this is new nullable columns, but nothing else.
If persist would support more schema changes in the future, we could consider allowing them here as well.

Even if we do not change the schema, we need to register a new version of the materialized view.
A version is defined as a global ID, and a relation description.
All versions map to the same persist shard.

## Timestamp selection

Replacing a materialized view should result in a well-defined history that has definite data at each readable timestamp.
This means that we need to start the replacement materialized view at the write frontier of the existing materialized view.
The implementation needs to ensure this, and must warn or refuse to apply a replacement if this is not possible.

It can pick a later timestamp than the old write frontier, but then needs to wait for the old materialized view to reach that timestamp before the replacement can be applied.

Optionally, we could allow users to let the new materialized view start at a later timestamp and let the time jump forward as needed.
This is highly risky as it introduces gaps in the history, and we should only consider it if there is a strong use case.
The behavior should be guarded with an explicit clause, such as `WITH (FORWARD TIME)`.

### Future work: Marking spans of time as invalid

Except for some subscribes and sinks, times that have errors aren't generally readable by users.
We could use this property to mask the period that we jump forward as invalid, by emitting an error at the beginning of the jump until the end of the jump.
Specifically, when advancing time from `upper` to `upper'`, `upper` <= `upper'`, we would emit the error `[("masked interval", upper + 1, 1), ("masked interval", upper', -1)]`.
An issue with implementing this is that the materialized view dataflow would need to be aware of the replacement, which complicates the implementation.

## Minimal Viable Prototype

* Update the parser to support the above syntax.
* Implement planning and sequencing for the new commands.
* Support `SHOW REPLACEMENTS`, `SHOW CREATE REPLACEMENT` commands.
* Add catalog relations for `replacements`: `mz_replacement_materialized_views`.
* Treat a replacement as a first-class object in the catalog.
* Record replacements and state transitions in the audit log.
* Do not support schema evolution in the MVP.
* Implement "correct" timestamp selection for replacements, starting at the write frontier of the existing materialized view.
* Provide better introspection data for replacements, such as the ability to see the differences between the current and replacement definitions.
    * Surface metadata about the amount of staged changes (records, bytes) between the current and replacement definitions.
    * Document how users can observe hydration progress, or implement new ways to do so.
      Specifically, users should be able to monitor progress through the `mz_frontiers` and `mz_arrangement_sizes` introspection views.

The syntax allows users to create multiple replacements for the same target.
This has some interesting implications:
* Creating two replacements and applying them in reverse order creates versions where the more recent version has a smaller global ID than an older version.
    We're not relying on global ID's partial ordering for correctness, so this is acceptable.
* We need to check the schema once when creating the replacement, and once when applying it.
    This is to ensure that the replacement is still valid at the time of application.
* Alternatively, we could restrict to a single replacement per target at any time.
    This would simplify the implementation, but would also limit the user's ability to stage multiple changes.

## Future work

* Provide insights into the impact of applying a replacement beyond metadata:
    * Introspect the actual changes.
      For example, which rows would be added or removed.
* Automate applying a replacement once the new definition is hydrated.
* Allow replacements to jump forward in time.
* Support replacements for other maintained objects, such as upsert sources.

## Alternatives

### Hooking into `CREATE MATERIALIZED VIEW`

We could extend the existing `CREATE MATERIALIZED VIEW` command to support replacing an existing materialized view.

```
CREATE MATERIALIZED VIEW <replacement_mv_name> REPLACES <mv_name> AS SELECT ...
```

And later:

```
ALTER MATERIALIZED VIEW <mv_name> APPLY REPLACEMENT <replacement_mv_name>;
```

While this design looks appealing at first, it has several drawbacks:
* A materialized view binds a persist shard.
  Creating a new materialized view would create a new shard, and we have no mechanism to reconcile two shards.

## Multi-output materialized views

The MVP design lets the replacement write at the same shard as the original materialized view.
This comes with limitations, most importantly that we need to ensure that we don't write until cut-over time.
In turn, this prevents reads from the replacement until we're cutting over.

An alternative design is to let the replacement materialized view write to a separate shard in addition to the original materialized view's shard.
A benefit would be that the replacement can be read from immediately, allowing users to inspect the data.

Several parts of Materialize would need to be updated to support this:
* Dataflows support multiple exports, but the behavior is untested and collides with the assumption that we can identify a dataflow by a single global ID.
* The catalog would need to support multiple shards per materialized view.
* We would need to control read-write mode per dataflow export.
  This might be a positive change as the controller sends instructions per global ID, not per dataflow.
* It is unclear how we could cut over from one shard to another to reclaim storage space.
  If we don't, we'll have to pay the hydration and storage cost for all shards indefinitely.
  (We can't cut-over existing dataflows to a new shard, and the only other time we can do breaking changes is when deploying a new version of Materialize.
  It feels odd to tie a clean-up mechanism to a version deployment.)

## Open questions

<!--
What is left unaddressed by this design document that needs to be
closed out?

When a design document is authored and shared, there might still be
open questions that need to be explored. Through the design document
process, you are responsible for getting answers to these open
questions. All open questions should be answered by the time a design
document is merged.
-->
