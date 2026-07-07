# Delta-availability fix: mirror directional delta viability at commit

## Problem

The physical eqsat pass commits a join to `DeltaQuery` only when
`CostModel::delta_join_order` (cost.rs) returns `Some`. That order finder is a
parallel reimplementation of directional join planning, and it is strictly more
restrictive on one axis: the arranged (lookup) side of every delta step must be
keyed by a plain local column.

`frontier_key_cols` (cost.rs:1361) contributes a key for an equivalence class
only when the input being added has a simple-column member in that class
(`!local_simple.is_empty()`, line 1390). The frontier side already accepts a
complex expression, but the local side does not. So when the only equated member
an input contributes is an expression, the class yields no key, the step is a
cross, and `delta_join_order` returns `None`. The join then commits differential.

Outer-join lowering produces exactly this shape. A null-pad join key such as
`#5{person2id} = case when (#4) IS NULL then null else #3{person2id} end`
equates a plain column on one input with a CASE expression over another input.
The CASE-side input has no simple-column member in the class, so eqsat declines
delta. Directional arranges that input on the CASE expression and keeps the
delta join.

### Measured

Probe on `outer_join_simplification.slt` (DELTAORDER, 2026-07-06): 7 of the
3-plus-input joins get `delta_join_order = None` and commit differential (3 with
3 inputs, 4 with 4 inputs). The directional base renders 5 of those as delta. 23
other joins in the file commit delta and survive. Both microlp (at 6904598c87)
and HiGHS produce 0 delta here: this gap is in the shared cost.rs order finder,
not the solver.

This is the delta-availability gap at its true locus. It is the arc's own
regression: at true base the cycle-aware ILP bailed on cyclic joins to greedy,
which kept these deltas; the cycle-aware fix made the ILP run, and the commit
path's restrictive `delta_join_order` then declined them. It is the req-2
JI-parity stop-and-report item, now root-caused.

## Constraints (from the fix ruling)

1. Mirror directional, do not parallel-invent. Reuse directional's actual
   viability and order logic. A second independent "can this be delta" check is
   how this gap returns.
2. Keep the SP-B1 guards: `num_inputs <= 2 -> differential` (the k=1 delta
   renderer panic), the `n >= 32` mask bail, the constant-singleton bail, and
   the empty-non-start-key differential bail.
3. Parity target is base's 5, not the probe's 7. Directional declines 2 of the 7
   itself. Matching directional is parity. Restoring all 7 overshoots into
   unverified territory. Verify by re-running DELTAORDER post-fix and diffing
   against base per join.
4. After the fix, separate the HiGHS-incremental sub-case (chbench 12->11,
   ldbc_bi 36->35). If a delta loss survives this fix, that is the genuine
   count-tie question and the tie-break ruling reactivates, handled per-instance
   with evidence.
5. One integration at the end: HiGHS plus this fix together, single settle and
   regen, one audit with all three lenses (arity, arrangement count, join-type
   transitions), then the formal E0. Not two integration passes.

## Why the fix is not "just the viability check"

eqsat's `JoinStep.key_cols` is `BTreeSet<usize>` (column indices). Directional's
delta key is `Vec<MirScalarExpr>` (expressions). Making the local side accept an
expression key therefore cannot stop at `frontier_key_cols`: the key has to be
carried through `JoinStep`, `commit_delta_query`, and `delta_new_arrangements`,
all of which are column-indexed today. Extending them to expressions in place
would be the parallel-invention path constraint 1 forbids.

The commit path already delegates rendering to directional
(`implement_arrangements`, `permute_order`, `install_lifted_mfp` in
join_commit.rs). Only order and key selection is still eqsat's own. So the
faithful fix delegates that last piece too.

## Design: unify `Rel::Join` onto the existing `Rel::WcoJoin` commit path

There are two eqsat join commit paths in `raise.rs`:

* `Rel::WcoJoin` (raise.rs:290) already delegates to directional's
  `plan_join_min_arrangements` (raise.rs:321). That helper plans both
  differential and delta via directional's planners, applies the `n <= 2`
  guard, and returns delta iff `delta_new <= diff_new`, else differential. It
  handles expression keys because directional does. This is the correct path.
* `Rel::Join` (raise.rs:164) goes through `commit_join` (raise.rs:386), which
  uses eqsat's column-only `delta_join_order`. This is the broken path.

Approach A, in its final form: route `commit_join` through the same
`plan_join_min_arrangements` that `Rel::WcoJoin` already uses. Keep
`commit_join`'s pre-planning guards (the constant-singleton bail, the
equivalence canonicalization for build-profile stability), then hand the
canonicalized join and the `per_input` availability to
`plan_join_min_arrangements` and return its result. This deletes
`delta_join_order`, `commit_delta_query`, `commit_differential`, and the whole
manual delta/differential dance from the eqsat side. eqsat contributes only the
WcoJoin-vs-not decision (already made by the e-graph) and the guards.

`plan_join_min_arrangements` is already `pub` (join_implementation.rs:636), so no
visibility change is needed. The earlier spec draft called for exposing
`delta_queries::plan`/`optimize_orders`; that is unnecessary given this helper.

Fixed default features. The commit path drives the planner with a fixed
`OptimizerFeatures::default()`, matching the `Rel::WcoJoin` path. So
`enable_join_prioritize_arranged` does not steer eqsat's join ordering, and the
delta choice is always min-arrangements (the eager rule applied unconditionally).
This is intentional. The physical eqsat pass is a distinct optimizer, not a
re-run of `JoinImplementation` under its flags. It is also parity-safe for the
golden corpus: the harness forces `enable_eager_delta_joins = true`
(misc/python/materialize/mzcompose/__init__.py) and leaves
`enable_join_prioritize_arranged = false` (its default), so
`OptimizerFeatures::default()` reproduces the directional baseline exactly. Once
the flags are dead on this path, the `prioritize_arranged` and `eager_delta`
fields of `NativeJoinFlags` are removed, leaving only the commit toggle.

Approach C (rejected): extend `frontier_key_cols`, `JoinStep`,
`commit_delta_query`, and `delta_new_arrangements` to carry `MirScalarExpr`
keys. This is a second expression-keyed delta planner living beside
directional's. Constraint 1 forbids it.

### Phase-2 net-of-shared and cross-source asymmetry: both subsumed

The two concerns raised at the gate dissolve structurally, they do not need
separate handling:

* Cross-source asymmetry. `plan_join_min_arrangements` plans BOTH the
  differential and the delta strategy against the SAME `available` baseline
  (join_implementation.rs:654 and 669), then compares `delta_new <= diff_new`.
  There is no eqsat-differential-order vs directional-delta-order cross-source
  comparison. One planner, one availability baseline, both counts derived from
  it. The decision cannot be biased by mismatched baselines.
* Phase-2 net-of-shared credit. eqsat's old `commit_join` reconstructed the
  net-of-shared credit by hand (crediting the differential order's built
  arrangements before counting delta's). That hand credit existed only because
  eqsat planned the two strategies separately. Directional's planners count new
  arrangements reuse-aware against `available` directly, so the net-of-shared
  accounting is subsumed: delta's `delta_new` already excludes any arrangement
  the shared `available` provides. Delegation does not lose the credit, it
  replaces a hand reconstruction with directional's native accounting.

### Prototype result (load-bearing assumption confirmed)

A throwaway prototype (early-return in `commit_join` through
`plan_join_min_arrangements`) was built and `outer_join_simplification.slt`
rewritten. Result: base 5 delta / 8 differential, prototype 5 delta / 8
differential, and per join the same five CASE-expr null-pad joins commit delta
in both, every base-differential stays differential. Exactly base's 5, not the
probe's 7 (directional declines the same 2), so no overshoot. The load-bearing
assumption (directional's planner fed raise-time-derived inputs reproduces
directional's decisions) holds: the later-pipeline MFP/lifting-state difference
did not change the delta/differential outcomes here.

### What stays eqsat's, what moves into directional

After full delegation, eqsat's `commit_join` keeps only two things: the
constant-singleton bail (load-bearing, `join_scalars` drops a singleton and would
misalign `per_input`) and the equivalence canonicalization into `canon_equivs`
(build-profile stability). Everything else about the delta/differential choice
moves inside `plan_join_min_arrangements`:

* The `num_inputs <= 2 -> differential` guard (constraint 2) lives in the helper
  (join_implementation.rs:663).
* The delta-vs-differential decision (`delta_new <= diff_new`) is the helper's own
  rule (join_implementation.rs:678), applied unconditionally (the eager rule), so
  eqsat no longer reads `eager_delta`.
* The old empty-non-start-key / `find_bound_expr` bail is not needed. It guarded
  eqsat's own hand-derived start keys. Directional never hand-derives: its order
  search aligns the start key and returns `Err` when it cannot, which the reroute
  maps to `None` and the bare join. That malformed-plan class cannot arise.
* The `n >= 32` mask bail belonged to eqsat's `delta_join_order` and goes with it.
  Directional has its own width handling and is the same code production runs.

### Deleted, not kept

Delegation makes the entire eqsat-side commit-planning machinery dead, and it is
deleted: the whole `eqsat/join_commit.rs` module (`commit_delta_query`,
`commit_differential`, `delta_new_arrangements`, `differential_new_arrangements`),
and in cost.rs `delta_join_order`, `binary_join_order`, `frontier_key_cols`,
`JoinOrder`, `JoinStep`, and their tests. An earlier draft of this section said to
keep `frontier_key_cols` because `binary_join_order` used it. That is wrong.
`binary_join_order` is itself deleted, so `frontier_key_cols` loses its only
production caller and is deleted too. Kept, because they still have callers on the
extraction-cost path: `join_key_cols_for_input`, `terms_cost`, `key_cols_arranged`,
`delta_join_terms`.

## Scope and blast radius

* Commit-path only. `delta_join_order` is not called in ILP extraction cost, so
  extraction goldens move only through the delta-vs-differential commit choice.
* Bounded to joins where delta becomes newly viable (expression-keyed lookups).
  Files expected to move: outer_join_simplification, ldbc_bi, ldbc_bi_eager,
  variadic_outer_join, and any other outer-join-heavy golden. The move is
  differential -> delta, the parity-restoring direction.

## Verification

1. Re-run the DELTAORDER probe post-fix on outer_join_simplification. Expect the
   None count to drop from 7 toward the joins directional also keys. Diff the
   rendered delta joins per-join against base: target is base's 5, and no join
   that base renders differential should become delta (constraint 3).
2. Three-lens audit on the full moved set (arity, arrangement count, join-type
   transitions) against base. Join-type lens must show delta counts moving toward
   base, not past it.
3. Separate the HiGHS-incremental sub-case (constraint 4): after this fix, if
   chbench or ldbc_bi still show a HiGHS-vs-base delta loss, that residue is the
   count-tie question, evaluated per instance.
4. Unit tests: replace the two `delta_join_order` tests with tests asserting the
   commit path now commits delta for an expression-keyed (CASE null-pad) 3-input
   join, and still commits differential for a genuinely disconnected join and for
   `num_inputs <= 2`.

## Integration (constraint 5)

Land this fix on the spike branch on top of HiGHS + K=30. Then a single joint
regen and settle for HiGHS + delta fix, one three-lens audit, one formal E0. No
intermediate integration of HiGHS alone.

## Spec gate

Approved (2026-07-06): Approach A, in its final form of unifying `Rel::Join`'s
`commit_join` onto the existing `Rel::WcoJoin` path via
`plan_join_min_arrangements`. No visibility change needed (already `pub`). The
phase-2 and cross-source concerns are subsumed by same-baseline planning, not
handled separately. Prototype confirmed exact parity (base's 5). Proceed to SDD.

## SDD task outline

1. Route `commit_join` through `plan_join_min_arrangements`: keep the
   constant-singleton bail and equivalence canonicalization, replace the
   `delta_join_order` / `commit_delta_query` / `commit_differential` block with
   the delegated call, return its result (`Err` -> `None` -> bare join fallback).
   Thread the real `OptimizerFeatures` if it changes behavior versus
   `default()`; the `WcoJoin` path uses `default()`, so match it unless a test
   shows a difference.
2. Delete `delta_join_order` and its unit tests after a caller check, then delete
   everything the reroute makes dead: `binary_join_order` and `frontier_key_cols`
   (its only caller was `binary_join_order`), `JoinOrder`/`JoinStep`, the whole
   `eqsat/join_commit.rs` module (`commit_delta_query`, `commit_differential`,
   `delta_new_arrangements`, `differential_new_arrangements`), and their tests.
   Keep the extraction-cost-path helpers that still have callers:
   `join_key_cols_for_input`, `terms_cost`, `key_cols_arranged`,
   `delta_join_terms`. Confirm each keep/delete with a repo-wide grep.
3. Add commit-path tests: an expression-keyed (CASE null-pad) 3-input join
   commits delta, a disconnected join stays differential, `num_inputs <= 2`
   stays differential.
4. Verification: DELTAORDER-style re-probe or direct golden diff on
   outer_join_simplification (target base's 5, per join), then the three-lens
   audit on the full moved set in the joint regen.

## Integration (unchanged)

One joint regen and settle for HiGHS + K=30 + this delta fix on the spike
branch, one three-lens audit against base, one formal E0.
