# Fix: eqsat demand_pushdown defeats maintained-index reuse

## Problem

`raise::demand_pushdown` (called at `src/transform/src/eqsat.rs:193`) runs the
production `Demand` + `ProjectionPushdown::default()` over the raised *logical*
plan to acquire column liveness the e-graph search cannot express.

In the logical phase joins are still `Unimplemented`, so their inputs are bare
`Get` nodes with no `ArrangeBy` wrapper.
`ProjectionPushdown` narrows a shared `Let` binding to the union of demand from
its uses (PP Let arm).
When that binding reaches a global `Get` backed by a maintained index, the
narrowing pushes a `Project` below the index `Get`, so the binding is no longer
the full-width relation the index materializes.
The physical phase then full-scans, projects, and builds a fresh arrangement
instead of reusing the maintained one (+1 arrangement, lookup to full scan).

Production is safe without index knowledge purely by timing: it only narrows in
the *physical* phase, where each join input is wrapped in `ArrangeBy` and
`ProjectionPushdown` refuses to push a `Project` past an `ArrangeBy`
(`src/transform/src/movement/projection_pushdown.rs:465`).
That `ArrangeBy` barrier keeps the maintained full-width arrangement intact.
eqsat's logical-phase narrowing lacks the barrier.

Minimal repro (`scratchpad/repro.sql`): a relation with an index, read at two
join sites (a shared CTE) where downstream demands only a column subset.
eqsat-off keeps `cte l0 = ArrangeBy(ReadIndex dim)` (reuse).
eqsat-on produces `cte l0 = ArrangeBy(Project(Filter(ReadIndex dim)))`
(full scan, +1 arrangement).

## Fix strategy

Make `demand_pushdown` reproduce the physical-phase `ArrangeBy` barrier in the
logical phase, but only where an index actually covers the join key (so a
legitimate narrowing past a non-covering index is unaffected).

For each `Join` input in the raised plan, if the input (after stripping leading
Map/Filter/Project wrappers and resolving a leading local `Get` through its
binding) reaches a global `Get g` such that `available[g]` contains an index
whose key column set equals this input's join key column set, wrap that input in
a sentinel `ArrangeBy` before running `ProjectionPushdown`, then remove the
sentinel afterward.

The sentinel `ArrangeBy` carries an empty key list (`keys = vec![]`), which a
genuine eqsat-extracted `ArrangeBy` never has (an arrangement is always keyed by
at least one expression).
That makes the inserted barriers unambiguously identifiable for removal and
avoids touching any pre-existing `ArrangeBy` that the e-graph extracted as a
real arrangement.
`ProjectionPushdown` reads only the `ArrangeBy` input (it ignores the key list),
so the empty key is purely a marker.

This is the logical phase only (`commit_wcoj == false`).
The physical phase already uses `ProjectionPushdown::skip_joins()` and the
`Demand` step is omitted there, so no barrier is installed when `commit_wcoj`.

## Files

* Modify: `src/transform/src/eqsat/raise.rs`
  * `demand_pushdown` signature gains `available: &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>`.
  * New private helpers (same file):
    * `install_index_reuse_barriers(expr, available)` — wrap qualifying join inputs in empty-key `ArrangeBy`.
    * `remove_index_reuse_barriers(expr)` — splice out every empty-key `ArrangeBy`.
    * `mir_join_key_cols_for_input(offset, input_arity, equivalences)` — MIR analog of `cost::join_key_cols_for_input`.
    * `input_reaches_covering_index(input, join_key_cols, available, bindings)` — strip MFP wrappers, follow a leading local `Get` via `bindings`, reach a global `Get`, test index-key-set == join-key-set.
* Modify: `src/transform/src/eqsat.rs:193` — pass `&available_for_raise` to `demand_pushdown`.

## Changes, in order

1. **Add the MIR join-key helper.**
   Mirror `cost::join_key_cols_for_input` over `MirScalarExpr` equivalences:
   for input `i` at column `offset` with `input_arity`, collect local column
   indices appearing in an equivalence class that also references a column
   outside `[offset, offset+input_arity)`.

2. **Add `input_reaches_covering_index`.**
   Build a `bindings: BTreeMap<LocalId, &MirRelationExpr>` of `Let`/`LetRec`
   binding values once per `demand_pushdown` call.
   Given a join input: strip leading `Map`/`Filter`/`Project` (a `Project`
   shifts columns, so stop crediting through it — match the conservative rule in
   `raise::input_arrangement_keys`), and if the base is `Get { Id::Local }`
   resolve once through `bindings` and strip again.
   If the base is `Get { Id::Global(g) }`, return true iff some key in
   `available[g]` has a plain-column set equal to `join_key_cols`.

3. **Add `install_index_reuse_barriers`.**
   Visit every `Join { inputs, equivalences }`.
   For each input at its running `offset`, compute `join_key_cols`; skip if
   empty.
   If `input_reaches_covering_index`, replace the input in place with
   `ArrangeBy { input: Box::new(taken), keys: vec![] }`.
   Recurse into children (including the wrapped inputs' subtrees) so nested
   joins are handled.

4. **Add `remove_index_reuse_barriers`.**
   Post-order visit: replace any `ArrangeBy { input, keys }` with `keys.is_empty()`
   by its `*input`.

5. **Wire into `demand_pushdown`.**
   After the `Demand` step and before `ProjectionPushdown`, in the
   `!commit_wcoj` branch only, call `install_index_reuse_barriers(&mut work, available)`.
   After `ProjectionPushdown` succeeds, call `remove_index_reuse_barriers(&mut work)`.
   Keep the clone-and-adopt-on-success structure; barriers live only on `work`.

6. **Update the call site** at `eqsat.rs:193` to pass `&available_for_raise`.

## Verification

* `cargo check -p mz-transform --tests` (authoritative).
* `cargo fmt`, `bin/lint`, `cargo clippy -p mz-transform`.
* Minimal repro via running environmentd: eqsat-on plan must match eqsat-off
  for `scratchpad/repro.sql` (l0 reuses `dim_id`, no full scan, no extra
  arrangement).
* Re-run the eqsat unit/validation tests: `bin/cargo-test -p mz-transform eqsat`.
* Re-regen the two tracked goldens and confirm reuse restored:
  `catalog_server_explain.slt` (`mz_frontiers_ind`) and
  `singlereplica_attribution_sources.slt` (`u13`) revert to lookup, -1 arrangement.
* Re-run `normalize_lets.slt`: hunk-1 `edges` hoist (no index) must be unchanged
  (barrier only fires on covering-index Gets), hunk-2 potato index reuse intact.
* Full EXPLAIN-golden re-regen (single invocation) and audit the diff:
  expect the 2 tracked queries to improve and no new regressions.

## Risks / open points

* Empty-key `ArrangeBy` must survive a `ProjectionPushdown` pass transiently.
  Validate; if PP or a debug assertion rejects it, fall back to a non-empty key
  plus a tracked-node set instead of the empty-key sentinel.
* Coverage uses exact key-set equality (matches `cost::arrange_by_oracle_covered`).
  A join key that is a strict subset/superset of every index key is treated as
  not covered, so narrowing proceeds (acceptable: reuse was not possible anyway).
* `LetRec` bindings: resolution through `bindings` must include `LetRec` values;
  recursive `Get`s resolve to their binding like `Let`. Barriers inside a
  recursive scope are still valid (they only influence PP demand, then are
  removed).
