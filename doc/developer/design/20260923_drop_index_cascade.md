# Require `CASCADE` to drop an index that other dataflows read from

- Associated: [Pinned LIR design](./20260826_pinned_lir.md)

## The Problem

An index's arrangement can be imported by other dataflows on the same cluster:
materialized views, other indexes, and one-shot `SUBSCRIBE` and `SELECT`
dataflows. Which indexes a dataflow imports is an optimizer decision. It is
recorded in the dataflow's physical plan and in the compute controller, but it
is not a catalog dependency. A materialized view's `uses()` set comes from its
HIR, which references relations only. So an index's `used_by` is always empty,
the planner's dependency check never fires for indexes, and
`DROP INDEX ... CASCADE` drops nothing beyond the index itself.

Today `DROP INDEX` on an in-use index succeeds. The importers keep read holds on
the arrangement, so the compute layer never lets the index's collection compact
away. The index disappears from the catalog but keeps running as an orphan
dataflow until every importer is dropped or re-planned. The only feedback is a
`NOTICE`-level `DroppedInUseIndex` message.

Once LIR plans are pinned durably, this stops being a resource leak and becomes
a correctness problem. The importer's stored plan references the dropped index
by `GlobalId`, and that plan cannot be loaded after a restart because the
collection it imports no longer exists. Dropping an in-use index must therefore
either take its importers with it or be refused.

## Success Criteria

- `DROP INDEX` without `CASCADE` fails when a materialized view or index reads
  from the index, with an error that names the dependents and explains the
  dependency, since it is not visible in the dependent's SQL definition.
- `DROP INDEX ... CASCADE` drops the index, every dataflow that reads from it,
  and their own dependents, so that no surviving object holds a plan that
  references the dropped index.
- The rule applies uniformly to every statement that drops indexes, including
  `DROP OWNED`, `DROP SCHEMA ... CASCADE`, and `DROP CLUSTER ... CASCADE`.
- A per-environment escape hatch restores the old behavior for customers whose
  workflows depend on it, and every use of the escape hatch is visible to us.
- One-shot `SUBSCRIBE` and `SELECT` dataflows never block DDL.

## Out of Scope

- Making index imports first-class catalog dependencies. See Alternatives.
- Blocking `DROP INDEX` on in-flight `SUBSCRIBE` or `SELECT` statements. Those
  dataflows are not pinned (the pinned LIR design excludes one-shot sinks), they
  finish on their own, and holding DDL hostage to a long-running client is a
  worse failure mode than the transient orphan they cause today.
- The existing race where a `CREATE MATERIALIZED VIEW` that has been optimized
  against an index, but whose dataflow has not yet been created, races a
  `DROP INDEX`. The new check cannot see that dataflow. The creation then fails
  at dataflow creation with a missing-collection error, as it does today.
- Changing `mz_object_dependencies` or `mz_compute_dependencies`.

## Solution Proposal

The sequencer, not the planner, decides whether an index drop is allowed. It
asks the compute controller who reads from each index in the drop set, and
either refuses the drop, expands the drop set, or (with the escape hatch)
proceeds loudly.

### Vocabulary

The **compute dependents** of index `i` are the compute collections whose
`compute_dependencies` contain `i`'s `GlobalId`, as reported by
`ComputeController::collection_reverse_dependencies`. They split into:

- **Durable dependents**: non-transient ids that resolve to catalog items. These
  are materialized views and indexes.
- **Transient dependents**: one-shot dataflows. Materialized view and index
  dataflows export only under their durable `GlobalId`s (the transient view id
  the materialized view optimizer allocates is a build, not an export), so
  every transient id here is a `SUBSCRIBE` or `COPY TO` sink, a slow-path peek
  dataflow, or a system-internal introspection subscribe or metric sink.

### Semantics

| Statement | `enable_unsafe_drop_index = false` (default) | `enable_unsafe_drop_index = true` |
| --- | --- | --- |
| `DROP INDEX i` or `DROP INDEX i RESTRICT`, durable dependents exist | Error with SQLSTATE `2BP01` (`DEPENDENT_OBJECTS_STILL_EXIST`) | Drop and orphan, as today, plus a `WARNING` notice, an error-level log event, and a metric increment |
| `DROP INDEX i CASCADE`, durable dependents exist | Drop `i`, its durable dependents, and their catalog dependents, transitively. The usual `drop cascades to N other objects` notice lists them. | Same. `CASCADE` always cascades. The flag only relaxes the `RESTRICT` path. |
| Any drop where the only dependents are transient | Drop. A `NOTICE` reports how many in-flight `SUBSCRIBE`, `COPY TO`, and `SELECT` statements still read the index. | Same |
| A durable dependent is itself in the drop set | Not a blocker. `DROP SCHEMA ... CASCADE` and `DROP OWNED` already include the importer when it lives in the same schema or is owned by the same role. | Same |

Sibling indexes count too. When a relation already has an index, the optimizer
plans a new index on that relation to read from the existing arrangement, so
the newer index depends on the older one. Dropping the older index without
`CASCADE` fails while the newer one exists, and index rotation has to drop the
old index before creating its replacement. The orphaned dataflows that the old
behavior left behind in this case essentially do not exist in practice, so no
real workload depends on the old behavior and the rule stays uniform. TODO: a way for `CREATE INDEX` to avoid
reading from a specific index would remove the constraint, and is future work.

The check runs for every index in every drop set, so it is one rule rather than
a `DROP INDEX` special case. For drops of relations it is a no-op today: an
importer of an index on `t` also has a catalog dependency on `t` (or on a view
over `t`), which already blocks a `RESTRICT` drop or is included by `CASCADE`.
The rule becomes load-bearing if a future change lets an index live in a
different schema than the relation it indexes.

### Why the sequencer owns the check

The planner works from a catalog snapshot and cannot see index imports. The
compute controller is the only owner of "who reads this index", and the
sequencer already queries it to build the `DroppedInUseIndex` notice. The check
is synchronous and runs on the coordinator loop before the catalog transaction,
so it has the same consistency as the existing notice and as every other
sequencer-side validation.

RBAC is unaffected. The `DROP` RBAC rule already requires ownership of the
explicitly named objects only, never of cascaded descendants, so expanding the
drop set in the sequencer bypasses no check the planner would have made.

### Algorithm

`Coordinator::resolve_index_dependents(session, drop_ids, cascade)` returns the
possibly expanded drop set (original order preserved, expansions appended)
together with the data for notices.

```text
set      = drop_ids as a set
frontier = drop_ids
loop:
  new = for each index in frontier:
          its durable compute dependents, as CatalogItemIds, that are not in set
  if new is empty: break
  if not cascade:
    if not enable_unsafe_drop_index: return Err(IndexInUse { index, dependents })
    record a DroppedInUseIndex notice per affected index; break
  deps     = catalog.object_dependents(new)      # transitive catalog closure, includes new
  frontier = deps minus set
  set      = set union deps
```

The loop is a fixpoint because a newly added materialized view can itself be
indexed, and that index can have compute dependents of its own. Catalog
dependents of each newly added item (sinks, downstream materialized views, and
indexes) are folded in with the existing `object_dependents` walk, which is the
same one the planner uses for `CASCADE`.

After the set is final, transient dependents are collected for every index in it
and classified: ids in `active_compute_sinks` are `SUBSCRIBE` or `COPY TO`, ids
in `introspection_subscribes` are system-internal, and anything else is a
slow-path `SELECT`.

### User-facing surfaces

**Error.** `AdapterError::IndexInUse { index_name, dependents }` renders as
`cannot drop index "i": still depended upon by materialized view "mv"`, matching
the planner's `DependentObjectsStillExist` wording so clients and test matchers
treat both the same. SQLSTATE is `2BP01`. The detail says the dependents are
live dataflows that read from the index, so it cannot be dropped while they
exist. The hint offers adding `CASCADE`, or dropping the dependent objects and
recreating them once the index is gone. It names no specific statement because
`DROP OWNED` and other multi-object drops reach the same error.

**`DroppedInUseIndex` notice.** Only reachable on the flag-on `RESTRICT` path
(with `CASCADE` the dependents are gone, and with the flag off the statement
errored). Severity is raised to `WARNING`, the message names the flag, and the
sequencer also emits a `tracing::error!` event and increments
`mz_optimization_notices{notice_type="DroppedInUseIndex"}`. Only error-level
events reach Sentry as issues, and the flag is opt-in per environment, so the
volume is bounded and every orphaning is visible to us. `DROP OWNED` now bumps
the metric too.

**In-flight statements notice.** A new `NOTICE` reports the counts of
`SUBSCRIBE`, `COPY TO`, `SELECT`, and system-internal dataflows still reading
the dropped index. It says these are transient dataflows that end when their
statements finish, and that the index is maintained until then. This replaces
the long-standing TODO about peeks in the drop path.

### Feature flag

`enable_unsafe_drop_index` is a `feature_flags!` entry with `default: false` and
`enable_for_item_parsing: false`. It is deliberately not `unsafe_`-prefixed, so
it can be set per environment through the usual system-parameter channels
without unsafe mode. The coordinator reads it from the system configuration at
drop time.

The CI default is also `false`. The flag-off path is the new behavior and is
what the test suite should exercise by default. The flag-on path is covered by
dedicated tests that set the flag explicitly. The flag is listed as
uninteresting for system-parameter randomization so CI never flips it by
accident.

### Effect on tooling

dbt-materialize is essentially unaffected. Model rebuilds issue
`DROP VIEW ... CASCADE` or `DROP MATERIALIZED VIEW ... CASCADE`, which already
drop the object's indexes and every catalog dependent, and deploy cleanup uses
`DROP SCHEMA ... CASCADE` and `DROP CLUSTER ... CASCADE`. The adapter's only
plain `DROP INDEX` is a fallback branch reached when a model name collides with
an existing index. That branch gains `CASCADE` so every branch has the same
"replacing a relation drops everything built on it" contract.

## Minimal Viable Prototype

The feature is small enough that the implementation is the prototype: the
sequencer helper, the flag, the error and notices, a sqllogictest covering the
`RESTRICT`, `CASCADE`, chained, and flag-on cases, and an integration test for
the notice text and severity.

## Alternatives

**Record index imports as catalog dependencies.** Adding each dataflow's index
imports to its `uses()` set would let the planner's existing dependency check
and `object_dependents` walk handle indexes with no sequencer changes. Rejected
for now: imports are an optimizer choice that is recomputed on every replan and
on every restart, so a catalog edge would drift from the object's definition.
The edges would also surface in `mz_object_dependencies` and affect bootstrap
ordering, and the blast radius is far larger than the problem. Once pinned LIR
makes the import set durable, the import set becomes a stable property of the
object and this alternative is worth revisiting.

**`CASCADE` as acknowledgement only.** Let `CASCADE` suppress the error but
still orphan the dependents. Rejected because it does not solve the pinned-LIR
loading problem. A dependent whose stored plan references a dropped index still
cannot be loaded.

**Flag restores the old `CASCADE` no-op too.** Rejected because `CASCADE` would
then mean different things depending on a flag, and customers on the flag would
have no correct way to drop an in-use index.

**Block on transient dependents.** Rejected, see Out of Scope.

## Open questions

None at the time of writing. The decisions above (transitive `CASCADE`, flag
scoped to the `RESTRICT` path, error-level logging on the flag-on path, and
ignoring transients while reporting them) were made while designing this
change.
