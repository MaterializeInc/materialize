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

### Schema evolution

When applying a replacement, we need to ensure that the new schema is compatible with the existing schema.
We define compatibility as follows:
1. The schema must be the same as the original schema,
2. Or, the schema must be a superset of the original schema (i.e., it can add new columns but cannot remove existing ones).

## Minimal Viable Prototype

* Update the parser to support the above syntax.
* Implement planning and sequencing for the new commands.
* Support `SHOW REPLACEMENTS`, `SHOW CREATE REPLACEMENT` commands.
* Add catalog relations for `replacements`: `mz_replacement_materialized_views`.
* Treat a replacement as a first-class object in the catalog.
* Record replacements and state transitions in the audit log.
* Do not support schema evolution in the MVP.

The syntax allows users to create multiple replacements for the same target.
This has some interesting implications:
* Creating two replacements and applying them in reverse order creates versions where the more recent version has a smaller global ID than an older version.
    We're not relying on global ID's partial ordering for correctness, so this is acceptable.
* We need to check the schema once when creating the replacement, and once when applying it.
    This is to ensure that the replacement is still valid at the time of application.
* Alternatively, we could restrict to a single replacement per target at any time.
    This would simplify the implementation, but would also limit the user's ability to stage multiple changes.

## Future work

* Provide better introspection data for replacements, such as the ability to see the differences between the current and replacement definitions.
  * Surface metadata about the amount of staged changes (records, bytes) between the current and replacement definitions.
  * Introspect the actual changes.
    For example, which rows would be added or removed.
* Automate applying a replacement once the new definition is hydrated.

## Alternatives

<!--
What other solutions were considered, and why weren't they chosen?

This is your chance to demonstrate that you've fully discovered the problem.
Alternative solutions can come from many places, like: you or your Materialize
team members, our customers, our prospects, academic research, prior art, or
competitive research. One of our company values is to "do the reading" and
to "write things down." This is your opportunity to demonstrate both!
-->

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
