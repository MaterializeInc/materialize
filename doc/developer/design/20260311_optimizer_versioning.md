# Resolving customer tradeoffs with multiple optimizer crates

- Associated: [#30233 optimizer release engineering](https://github.com/MaterializeInc/materialize/pull/30233),
[#8768 optimizer crate](https://github.com/MaterializeInc/database-issues/issues/8768)

## The Problem

Customers run operational workloads on Materialize.
Changes to Materialize can threaten the stability of those workloads---particularly changes in the optimizer.

To date, we have managed optimizer changes using feature flags (e.g., `enable_cast_elimination`, `enable_eager_delta_joins`).
Not every feature can be feature flagged (e.g., changing `MirRelationExpr` to hold `Repr*` types), though, and we do not have much in the way of tooling for feature flags.

It is hard for us to make changes in the optimizer that won't cause some customers to have a bad time---even if some customers have a much better time with those changes.
We need a way to change the optimizer without disrupting customer workloads.

## Success Criteria

Customers---self-hosted or cloud---will be able to qualify new optimizers before migrating to them.

Optimizer engineers will be able to develop features with confidence, namely:

  - introducing new transforms (e.g., cost-informed late materialization)
  - updating existing transforms (e.g., new join planning)
  - targeting new dataflow operators (e.g., many-to-many reduce)
  - changing AST types for HIR, MIR, or LIR (e.g., LIR many-to-many reduce, MIR window functions)

Optimizer engineers will be able to deploy hotfixes to any active optimizer using the normal weekly release.

## Out of Scope

Mztrail---testing on customer workloads---would help us predict when optimizer changes will affect customers.
(It would also help the most proactive customers, who could run tests themselves.)
While pushing in this direction is good, important work, it's a bigger bite than what's proposed here.
Moreover, it's not clear how to use mztrail in a self-managed context.

## Weighing Alternatives

- **`optimizer-versions`** Separate optimizer versions, settable per-cluster using a system-level privilege.
- **`feature-flags`** Feature flag everything, building tooling to support eng, field eng, and customers.
- **`plan-pinning`** Offer an explicit way to fix a query plan.

What are the pros and cons of each approach?

### `optimizer-versions`

Pros:

  + Fixed, known configurations.
  + Per-cluster control.
  + Forces more unified optimizer interface.
  + Ties in neatly with related ideas of "a separate optimizer process".
  + Flexible versioning: we can cut new optimizer versions as we please, and do not need to fix a support window in advance.

Cons:

  - Code duplication. (Somewhat mitigated by `git subtree`.)
  - Unknown support windows.
  - Coarse-grained offramp: you can change versions, but that's it.

### `feature-flags`

Pros:

  + The status quo (less the tooling).
  + Fine-grained control: you can offramp from old feature settings flag-by-flag. (In principle, at least.)
  + Flexible: we can create new feature flags as we plase, and we do not need to fix their support windows in advance.

Cons:

  - Feature flags apply to entire environments, not individual clusters. If different teams need different flags, they are out of luck.
  - Exponentially many configurations---we can't test every combination of flags, and flags interact.
  - Unknown support windows.

### `plan-pinning`

Pros:

  + Ties in neatly with related ideas of "production clusters".
  + Ties in neatly with related ideas of "DDIR" or some other stable, low-level interface.
  + Offers the most reliable possible experience---a fixed LIR plan would be stable even if bugfixes in MIR cause queries to change.

Cons:

  - LIR is a not a stable interface. DDIR does not actually exist.
  - No offramp.

### `query-hints`

Pros:

  + The finest-grained control.
  + Avoids/defers the need to have smart query planning.

Cons:

  - Major parser overhaul.
  - Major AST overhaul.
  - Major transform overhaul.
  - All known forms of this are brittle.
  - Hard to specify emergent properties (e.g., what to do with operators that do not syntactically appear in the query plan).

## Solution Proposal

We propose using **`optimizer-versions`**.
We see it as superior to **`feature-flags`** because we can work more flexibly (change types!) with less uncertainy (known configs!).
We see it as superior to **`plan-pinning`** because we don't need to stabilize new interfaces.
We see it as superior to **`query-hints`** because we don't want to add query hints.

[The prior design doc in #30233](https://github.com/MaterializeInc/materialize/pull/30233) proposed a Debian-like versioning scheme, with an unstable/hot branch, a testing/warm branch, and stable/cold branch.
This is overplanned: we should begin by having a stable branch and a 'future' branch, where we make more drastic changes.
We can later adopt the Debian-like scheme, or the [fully versioned approach Spanner takes](https://docs.cloud.google.com/spanner/docs/query-optimizer/versions).

The changes proposed below center around breaking off the optimizer as its own subsystem, with an `optimizer` crate (of which we may have multiple versions).
Treating the optimizer as its own system comes with its own benefits:

  - Better tracing in the optimizer, which currently hacks things to get the right outputs.
  - The possibility of process isolation for the optimizer.
  - Clearer interfaces and a better testing surface.

## Minimal Viable Prototype

This plan is a revised version of the plan from [the prior design doc in #30233](https://github.com/MaterializeInc/materialize/pull/30233).

### Versioning the `optimizer` crate

To create a new version of the `optimizer` crate, we will use `git subtree`, which should preserve features.

At first, there will be two versions: V0 (the current optimizer) and V1 (the future branch).

### The `optimizer` and `optimizer-types` crates

The `optimizer` crate will contain the definitions of HIR, MIR, and LIR, along with the HIR-to-MIR and MIR-to_LIR lowerings and the MIR-to-MIR transformations. These come from the `sql` (HIR, HIR -> MIR), `expr` (MIR), `transform` (MIR -> MIR), and `compute-types` (LIR, MIR -> LIR) crates.

The `optimizer-types` crate will have traits and definitions that are global across all three live versions of the optimizer. The AST types may change version to version, but the traits will be more stable.

These crates do _not_ include:
  + the SQL parser
  + (\*) SQL -> HIR in `sql`
  + `sequence_*` methods in adapter
  + (\*) LIR -> dataflow in `compute`

The two bullets marked (\*) above are a tricky point in the interface. SQL will _always_ lower to the latest HIR; we will have methods to convert the latest HIR to the other two versions. Similarly, LIR lowers to dataflow from the latest LIR; we will have methods to convert the other two versions to the latest.

### Supporting qualification

It would be good to link this work to the "representative workloads".
It should be easy (for us) to test the same workload with different versions of the optimizer.
We should use some kind of representative workloads to qualify making the future branch optimizer public.

### Supporting observability

Observability is how people---us and customers---know that something is working (or not).
It should be easy (for us and for customers) to see what might change in existing plans if the optimizer were to switch.

## Open questions

What should the interface of the `optimizer` crate be?

Where, precisely, should optimizer-version dispatch go? Is there an `optimizer-multiplexer` crate that picks the right version to invoke?

At what point is the V1/future optimizer available, and how? A feature flag? GA?
At what point is the V1/future optimizer the default?
We do not need hard answers to these questions, but good answers should exist.
