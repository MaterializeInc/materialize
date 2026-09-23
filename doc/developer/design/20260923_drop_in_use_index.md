# Refuse to drop an index that other dataflows read from

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
be refused.

## Success Criteria

- `DROP INDEX` fails when a materialized view or index reads from the index,
  with an error that names the dependents and explains the dependency, since it
  is not visible in the dependent's SQL definition.
- No statement leaves an orphaned dataflow behind. The rule applies uniformly to
  every statement that drops indexes, including `DROP OWNED`,
  `DROP SCHEMA ... CASCADE`, and `DROP CLUSTER ... CASCADE`.
- `DROP INDEX ... CASCADE` keeps its existing meaning, the same as `RESTRICT`.
  Users drop the dependents themselves, or replace them with a blue/green
  deployment in production.
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

The sequencer, not the planner, decides whether a drop that includes an index is allowed. It asks the compute
controller who reads from each index in the drop set, and either refuses the
drop or (with the escape hatch) proceeds loudly. It never expands the drop set.

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
| Any drop of an index with a durable dependent outside the drop set | Error with SQLSTATE `2BP01` (`DEPENDENT_OBJECTS_STILL_EXIST`) | Drop and orphan, as today, plus a `WARNING` notice, an error-level log event, and a metric increment |
| Any drop where the only dependents are transient | Drop. A `NOTICE` reports how many in-flight `SUBSCRIBE`, `COPY TO`, and `SELECT` statements still read the index. | Same |
| A durable dependent is itself in the drop set | Not a blocker, as in `DROP INDEX a, b` or a `DROP SCHEMA ... CASCADE` that includes the importer | Same |

`CASCADE` does not change any row. On an index it has always been equivalent to
`RESTRICT`, because nothing depends on an index in the catalog, and it is not
extended to compute dependents. The dependents are invisible in every SQL
definition, so a cascade would drop objects, and their sinks, that the user had
no way to anticipate. Refusing the drop and naming the dependents lets the user
decide what to drop. In production, the supported way to replace the dependents
is a blue/green deployment.

Sibling indexes count too. When a relation already has an index, the optimizer
plans a new index on that relation to read from the existing arrangement, so
the newer index depends on the older one. Dropping the older index fails while
the newer one exists, so indexes on one relation are dropped newest first, and
index rotation has to drop the old index before creating its replacement. The
orphaned dataflows that the old behavior left behind in this case essentially
do not exist in practice, so no real workload depends on the old behavior and
the rule stays uniform. TODO: a way for `CREATE INDEX` to avoid reading from a
specific index would remove the constraint, and is future work.

The check runs for every index in every drop set, so it is one rule rather than
a `DROP INDEX` special case. Outside `DROP INDEX` it never fires today. An
importer of an index on `t` runs on the index's cluster and has a catalog
dependency on `t` (or on a view over `t`), and an index's owner is always its
relation's owner. So for drops of relations, schemas, clusters, and owned
objects, the planner's catalog dependency check already refuses the drop or its
`CASCADE` expansion already includes the importer. The rule becomes
load-bearing if a future change lets an index live in a different schema, or
have a different owner, than the relation it indexes.

### Why the sequencer owns the check

The planner works from a catalog snapshot and cannot see index imports. The
compute controller is the only owner of "who reads this index", and the
sequencer already queries it to build the `DroppedInUseIndex` notice. The check
is synchronous and runs on the coordinator loop before the catalog transaction,
so it has the same consistency as the existing notice and as every other
sequencer-side validation.

### Algorithm

`Coordinator::check_index_dependents(session, drop_ids)` makes one pass over the
indexes in `drop_ids`, with one `collection_reverse_dependencies` call per
index:

```text
set = drop_ids as a set
for each index in drop_ids:
  durable, transient = compute dependents of index
  durable = durable minus set
  if durable is not empty:
    if not enable_unsafe_drop_index: return Err(IndexInUse { index, durable })
    record a DroppedInUseIndex notice
  record transient for the in-flight notice
```

Transient dependents are classified for the notice: ids in
`active_compute_sinks` are `SUBSCRIBE` or `COPY TO`, ids in
`introspection_subscribes` are system-internal, and anything else is a
slow-path `SELECT`.

### User-facing surfaces

**In-use error.** `AdapterError::IndexInUse { index_name, dependents }` renders
as `cannot drop index "i": still depended upon by materialized view "mv"`,
matching the planner's `DependentObjectsStillExist` wording so clients and test
matchers treat both the same. SQLSTATE is `2BP01`. The detail says the
dependents are live dataflows that read from the index, so it cannot be dropped
while they exist. The hint says to drop the dependents first, then the index,
and to recreate the dependents, using a blue/green deployment in production. It
names no specific statement because `DROP OWNED` and other multi-object drops
reach the same error.

**`DroppedInUseIndex` notice.** Only reachable with the flag on. Severity is
raised to `WARNING`, the message names the flag, and the sequencer also emits a
`tracing::error!` event and increments
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
an existing index. It fails with the in-use error if another object reads from
that index.

The Console's drop dialog issues `DROP INDEX ... CASCADE`. Since `CASCADE` still
means `RESTRICT` on an index, dropping an in-use index from the Console fails
with the in-use error, which the dialog shows, and the Console needs no change.

## Minimal Viable Prototype

The feature is small enough that the implementation is the prototype: the
sequencer check, the flag, the errors and notices, a
sqllogictest for the error cases, a testdrive file asserting that no drop leaves
an orphaned dataflow in `mz_compute_dependencies`, and integration tests for
the notice text and severity.

## Alternatives

**`CASCADE` to compute dependents.** Let `DROP INDEX ... CASCADE` drop every
dataflow that reads from the index, and their catalog dependents, transitively.
Rejected because the dependents are invisible in SQL, so a cascade can drop
materialized views and sinks the user did not know were involved. Tools that
append `CASCADE` by habit, such as the Console's drop dialog, would turn an
index drop into a surprise production outage.

**Reject `DROP INDEX ... CASCADE`.** Make the planner refuse `CASCADE` on an
index, so no one can believe it drops the readers. Rejected because it breaks
scripts that pass `CASCADE` today, even for indexes with no dependents, for no
gain in safety: `CASCADE` drops nothing extra either way, and the in-use error
already explains what to do.

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

**Block on transient dependents.** Rejected, see Out of Scope.

## Open questions

None at the time of writing.
