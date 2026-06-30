# SP-B1: Cost-model-native join commit + emitter — Design

> **Status:** spec, ready for implementation planning. Part of the umbrella
> project "subsume `JoinImplementation` in the eqsat physical optimizer"
> (approach **B**, cost-model-native). This is sub-project **SP-B1**; later
> sub-projects add the cardinality axis (SP-B2), selectivity axis (SP-B3), and
> residual JI parity (SP-B4).

## Goal

Make the eqsat physical pass commit **every** join — `Rel::Join` (acyclic
multi-way and binary), not just the already-committed `Rel::WcoJoin` (cyclic) —
to a filled `JoinImplementation` whose strategy and order are chosen by the
eqsat cost model. A committed implementation makes `fixpoint_join_impl`
(`JoinImplementation`) a no-op for eqsat-produced joins, which (a) activates the
already-built join-key spelling selector and (b) removes the
variable-outer-join (VOJ) cross product, because JI's `Unimplemented`-guarded
`canonicalize_equivalences` no longer runs on the committed join to revert the
local spelling.

Gated by a new flag, default on in-branch for A/B measurement.

## Background: why the selector is inert today

The prior cycle (`docs/superpowers/specs/2026-06-29-eqsat-delta-join-cost-design.md`,
landed but inert) built a delta-aware keyed-ness cost (`CostModel::delta_join_terms`,
`cost.rs`) and a spelling selector (`select_join_spelling`,
`eqsat/join_spelling.rs`) that re-spells a join's foreign key (`#0`) to a local
one (`#2`) at extraction, so a delta path stays keyed instead of crossing.

It is inert end-to-end because of the pipeline order
(`doc/developer/design/20260630_joinimplementation_internals.md`):

1. The eqsat physical pass extracts a plan. For the VOJ, an acyclic multi-way
   `LEFT JOIN`, extraction produces a `Rel::Join` (the cyclic-only
   `join_to_wcoj` rule, `rules/relational.rewrite:323-327`, never fires).
2. `raise.rs:134-146` lowers `Rel::Join` to a bare `MirRelationExpr::join_scalars`
   with `implementation = Unimplemented`. **No commit.**
3. Downstream, `fixpoint_join_impl` runs `JoinImplementation`. Its `action`
   matches `Unimplemented`, so it calls `canonicalize_equivalences`
   (`join_implementation.rs:168-182`), which is **not idempotent** and reverts
   the selector's local `#2` spelling back to the foreign `#0` representative.
4. JI then delta-plans (eager-delta on) the join with the foreign spelling →
   the t3-driven delta path cross-products the full `t1`:
   `%2 » %0:t1[×] » %1[#0]K`.

In contrast, `Rel::WcoJoin` is committed: `raise.rs:246-283` calls
`crate::join_implementation::plan_join_min_arrangements`, which fills the
implementation (`DeltaQuery` or `Differential`); JI then skips it (`action`
only re-plans `Unimplemented` and, with eager-delta off, `Differential`).

**The fix:** route `Rel::Join` through a commit step too. The committed
implementation is not `Unimplemented`, so JI's `canonicalize_equivalences` guard
(`matches!(implementation, Unimplemented)`) does not fire, the local spelling
survives, and the cross disappears.

## Decisions (resolved in brainstorming)

1. **Approach B — cost-model-native.** The join **order** comes from the eqsat
   cost model's own orderers, not from JI's `optimize_orders` (the greedy
   max-min `JoinInputCharacteristics` planner). That planner policy is the
   dependency being removed.
2. **Acyclic → differential.** The cyclic-only `join_to_wcoj` gate stays
   untouched. Acyclic joins commit as `Differential`; cyclic joins keep the
   existing `WcoJoin → DeltaQuery` path. Rationale: the `join_to_wcoj` doc
   warns that giving acyclic joins a delta alternative makes the AGM `Cost`
   over-pick delta (the keyed-ness axis is intentionally **not** in `Cost`), so
   "let `Cost` pick delta vs differential" would regress a swath of acyclic
   joins. For the VOJ, **differential already avoids the cross** (findings:
   "eager-delta OFF → differential → no cross"), so no delta-for-acyclic path is
   needed to fix the flagship.
3. **Reuse JI's mechanical lowering helpers.** The emitter builds the
   `Differential` order tuples natively from the cost-model order, then calls
   JI's `implement_arrangements` + `install_lifted_mfp` (the ArrangeBy-wrapping
   and MFP plumbing). These are mechanism, not the removed planner policy, and
   are how the `WcoJoin` commit already works.
4. **Flag default on in-branch** for A/B testing; production rollout deferred.
5. **Flag-gated skip in JI** so a committed `Differential` is not re-touched by
   JI's eager-delta-off upgrade, keeping the eqsat plan final.
6. **Do not `canonicalize_equivalences` on committed joins.** Obtained for free:
   the existing JI guard fires only on `Unimplemented`. The emitter must not
   call it either.

## Architecture

```
extraction (build.rs / extract.rs)
  └─ select_join_spelling  (prior cycle; gated enable_eqsat_delta_join_cost)
        → Rel::Join carries the LOCAL-spelled equivalences
raise.rs  Rel::Join arm  (physical phase, commit_wcoj = true)
  ├─ cost.rs: model.binary_join_order(inputs, equivalences)  ← on the IR (Rel/EScalar)
  └─ NEW: eqsat::join_commit::commit_differential(raised_join, order, per_input)
        ├─ build JoinImplementation::Differential((start, key, None), rest) tuples
        └─ reuse: implement_arrangements + install_lifted_mfp
        → MirRelationExpr::Join { implementation: Differential(..), .. }
fixpoint_join_impl  (JoinImplementation)
  └─ NEW guard: skip committed Differential when flag on  → no-op
  └─ Unimplemented guard on canonicalize_equivalences never fires → spelling kept
```

## Components

### Component 1 — Order surfacing (`src/transform/src/eqsat/cost.rs`)

The orderers already compute the winning left-deep order but discard it,
returning only the degree multiset. Add a sibling that returns the order.

- **New `pub(crate)` method**, e.g.:
  ```rust
  pub(crate) fn binary_join_order(
      &self,
      inputs: &[Rel],
      equivalences: &[Vec<EScalar>],
  ) -> Option<JoinOrder>
  ```
  where
  ```rust
  pub(crate) struct JoinOrder {
      /// Inputs in left-deep order; `steps[0]` is the starting input. Each
      /// carries the local key columns of that input used at its join step
      /// (`steps[0]`'s key is the start arrangement used by the first edge).
      pub steps: Vec<JoinStep>,
  }
  pub(crate) struct JoinStep {
      /// Global input index (position in the join's `inputs`).
      pub input: usize,
      /// Column indices local to `input` forming this step's arrangement key.
      pub key_cols: BTreeSet<usize>,
  }
  ```
  Both args are the eqsat IR (`&[Rel]`, `&[Vec<EScalar>]`) — the form available
  in the `Rel::Join` arm of `raise.rs` **before** raising — not MIR. The order
  is computed there and passed to the emitter; the local key-column indices are
  position-stable across `raise` (raising an input does not reorder its
  columns).
- The order is the same one `binary_join_terms` already selects via
  `DpSub`/`Dpccp`/`DphypBushy` (`cost.rs:720-724`, defs at `989-1154`).
  Implement by extending the chosen orderer to record backpointers (which input
  is appended at each DP step) alongside its cost, then reconstruct the order
  from the optimal cell. The existing `binary_join_terms` return value and all
  its current callers/costs are **unchanged** (pure addition), so no plan moves
  from this component alone.
- `key_cols` for each step: the columns of that input that are equated, via some
  `equivalences` class, to a column of an already-placed input (for `steps[0]`,
  to the input it first joins with). Reuse the existing `join_key_cols_for_input`
  (`cost.rs:877-902`) restricted to the current frontier (an equivalence class
  contributes only when it spans the frontier and the input being placed).
- `None` is returned when no connected left-deep order exists (disconnected
  join graph); the caller falls back to leaving the join `Unimplemented`.

### Component 2 — Differential emitter (new `src/transform/src/eqsat/join_commit.rs`)

```rust
pub(crate) fn commit_differential(
    join: MirRelationExpr,                       // raised, Unimplemented
    order: JoinOrder,                            // computed from the IR by the cost model
    available: &[Vec<Vec<MirScalarExpr>>],       // per-input arrangements
) -> Option<MirRelationExpr>
```

- Returns `None` (caller falls back to the bare `Unimplemented` join) when:
  the expr is not a `Join` or `implement_arrangements` fails. (`binary_join_order`
  returning `None` is handled in `raise.rs` before the emitter is called.)
- Steps:
  1. Translate `JoinOrder` (`steps[0]` = start, `steps[1..]` = rest) into the
     `JoinImplementation::Differential` shape:
     `((start, Some(start_key), None), Vec<(input, key, None)>)` where each
     `key: Vec<MirScalarExpr>` is built from the step's `key_cols` as local
     `MirScalarExpr::column(c)` references, the same coordinate convention JI uses
     (`join_implementation.rs:814-822`). `JoinInputCharacteristics` slots are
     `None` (EXPLAIN-only; not produced cost-model-side in SP-B1).
  2. Call `implement_arrangements(inputs, available, order_tuples.iter())`
     (`join_implementation.rs:944-1032`) to wrap inputs needing arrangements in
     `ArrangeBy` and lift MFPs where keys are already available.
  3. Assign `*implementation = JoinImplementation::Differential(start, rest)`.
  4. Call `install_lifted_mfp(&mut join, lifted_mfp)`
     (`join_implementation.rs:1048-1095`).
  5. Return `Some(join)`.
- **Visibility change:** `implement_arrangements` and `install_lifted_mfp` are
  currently `super`-visible inside `join_implementation.rs`. Make them
  `pub(crate)` so the emitter can call them. No behavioral change.
- The emitter does **not** call `differential::plan`, `delta_queries::plan`,
  `optimize_orders`, or `canonicalize_equivalences`.

### Component 3 — Routing (`src/transform/src/eqsat/raise.rs`)

In the `Rel::Join` arm (`raise.rs:134-146`):

- Logical phase (`!commit_wcoj`): unchanged — emit the bare
  `join_scalars(..)` (`Unimplemented`).
- Physical phase (`commit_wcoj`): mirror the `WcoJoin` arm (`246-283`):
  ```rust
  // `inputs` (Vec<Rel>) and `equivalences` (Vec<Vec<EScalar>>) are the IR form
  // in scope in this arm, before raising — exactly what the cost model needs.
  let model = CostModel::with_available(available.clone());   // GlobalId-keyed map
  let order = model.binary_join_order(inputs, equivalences);
  let raised_inputs: Vec<MirRelationExpr> = inputs.iter().map(|r| raise(r, scope)).collect();
  let join = MirRelationExpr::join_scalars(
      raised_inputs.clone(),
      equivalences.iter().map(|c| resolve(c)).collect(),
  );
  if !commit_wcoj || !features.enable_eqsat_native_join_commit {
      return join;
  }
  let Some(order) = order else { return join; };
  let per_input = per_input_available(&raised_inputs, available);
  crate::eqsat::join_commit::commit_differential(join.clone(), order, &per_input)
      .unwrap_or(join)
  ```
  Thread the flag (`features`) into `raise` the same way `available` /
  `commit_wcoj` are threaded today. The `CostModel` is built locally from the
  already-threaded `available` map (`CostModel::with_available`); the order is
  computed on the IR `inputs`/`equivalences` before they are raised.
- Cyclic joins are unaffected: if extraction chose `WcoJoin`, the `WcoJoin` arm
  commits delta; if extraction chose `Join` for a cyclic join (its `Cost` was
  ≤ the `WcoJoin`'s), it commits differential here. Either is by the cost
  model's choice.

### Component 4 — JI no-op parity (`src/transform/src/join_implementation.rs`)

- `DeltaQuery` already survives JI untouched (cyclic path; no change).
- Committed `Differential` would otherwise be re-touched by JI's eager-delta-off
  upgrade path (`action` matches `Unimplemented | Differential(..)`). Add a
  guard at the top of `action`:
  ```rust
  if features.enable_eqsat_physical_optimizer
      && features.enable_eqsat_native_join_commit
      && matches!(implementation, Differential(..))
  {
      return Ok(());
  }
  ```
  Gating on **both** flags scopes the skip to runs where the only filled
  `Differential`s reaching JI are eqsat's, so JI's own second-pass upgrades for
  non-eqsat configs are unaffected. This is the sanctioned "skip JI for eqsat"
  edit; it does not relocate any planning *policy* into JI.
- `canonicalize_equivalences` (`168-182`) needs no change: its `Unimplemented`
  guard already excludes committed joins.

### Component 5 — Flag plumbing

New optimizer feature flag `enable_eqsat_native_join_commit`, default value
`true` (in-branch), wired at every site the prior cycle's
`enable_eqsat_delta_join_cost` flag touches:

- `src/sql/src/session/vars/definitions.rs` — the system variable.
- `src/repr/src/optimize.rs` — the `optimizer_feature_flags!` macro entry **and**
  the `From<&SystemVars> for OptimizerFeatures` binding.
- Every **exhaustive** `OptimizerFeatureOverrides` construction (no `..`):
  `src/sql/src/plan/statement/dml.rs:620` and
  `src/sql/src/plan/statement/ddl.rs:4936`. (`ddl.rs:4846` uses `..` — safe.)

Follow `enable_eqsat_delta_join_cost` as the template; see
`feature-flags-on-by-default-in-tests` (CI/test default on).

## Data flow (the VOJ, end to end, both flags on)

1. `select_join_spelling` (gated `enable_eqsat_delta_join_cost`) makes the
   extracted `Rel::Join` carry the **local** spelling (`…else #2 end`).
2. `raise.rs` `Rel::Join` arm → `commit_differential`.
3. `binary_join_order` over the local-spelled equivalences finds a **keyed**
   left-deep order (no forced cross; the local key lets each step probe an
   arrangement).
4. The emitter builds `Differential(..)` with local-coordinate keys, wraps
   inputs via `implement_arrangements`, installs the lifted MFP.
5. `fixpoint_join_impl`: the new guard skips the committed `Differential`; the
   `canonicalize_equivalences` guard never fires. Spelling preserved.
6. Result: `type=differential`, single keyed path, **no cross**. Flag-on now
   differs from flag-off (which still crosses).

## Error handling

- All failure modes in the emitter degrade to leaving the join `Unimplemented`
  (return `None` → `unwrap_or(join)`), so JI handles it exactly as today. No new
  panics; no `expect` on planning.
- `binary_join_order` returns `None` for disconnected graphs rather than
  fabricating a cross order.
- The visibility change and the JI guard are behind the flag; with the flag off,
  behavior is byte-identical to today.

## Testing

- **cost.rs unit tests (order surfacing):** the VOJ 3-input fixture
  (`get("t1",2), get("t2",3), get("t3",3)`) with the local spelling — assert
  `binary_join_order` returns a fully keyed order (every step's `key_cols`
  non-empty, no cross step). With the foreign spelling, assert the order still
  validates (may contain a cross), proving robustness.
- **join_commit.rs unit test (emitter):** feed a known `JoinOrder` + a bare
  `Join` and assert the resulting `JoinImplementation::Differential` has the
  expected `(start, key)` and `rest` tuple shape with local-coordinate keys,
  and that inputs needing arrangements are wrapped in `ArrangeBy`.
- **slt:** upgrade `test/sqllogictest/transform/eqsat_delta_join_cost.slt` so
  the VOJ asserts the cross is **gone** with both flags on
  (`%2 » %0:t1[×]` absent; a keyed differential path present), and that flag-off
  still crosses — i.e. flag-on ≠ flag-off, replacing the prior cycle's
  "inert / byte-identical" assertion.
- **Corpus A/B (in the plan, not a unit test):** capture
  `EXPLAIN ... WITH(join implementations)` (or the optimized slt corpus) before
  and after, flag-on vs flag-off, and record the plan diff. SP-B1 **expects**
  ordering movement (the eqsat DP optimizes the AGM-degree objective; JI
  optimizes `JoinInputCharacteristics` — cardinality/selectivity parity is
  SP-B2/B3). The flag + the diff is the safety net, not zero movement.

## Global constraints

- **Both gates:** the VOJ fix requires `enable_eqsat_physical_optimizer`,
  `enable_eqsat_delta_join_cost` (the selector), and
  `enable_eqsat_native_join_commit` (this cycle) all on. Tests must enable all
  three.
- **Flag-off = today:** with `enable_eqsat_native_join_commit` off, every
  changed path is bypassed; behavior is byte-identical to current `main`.
- **No `Cost` / comparator changes.** Do not modify the `Cost` struct,
  `cmp_memory_first`, `cmp_time_first`, or promote keyed-ness into `Cost`. The
  acyclic-→-differential decision exists precisely to avoid needing that.
- **No new planner in JI and no fix inside JI.** The only JI edits are the
  flag-gated `Differential` skip guard and the two `pub(crate)` visibility
  changes. No planning policy moves into JI.
- **Do not edit `doc/developer/generated/`.**
- **No customer names** in any committed file.

## Out of scope (later sub-projects)

- SP-B2: cardinality estimates in the cost-model ordering objective.
- SP-B3: `FilterCharacteristics` selectivity in the ordering objective.
- SP-B4: local/let-bound arrangement awareness (G3), `IndexedFilter` visibility
  (G6), delta-for-acyclic via keyed-ness-in-`Cost`, and the final canonicalize
  audit (G4) so JI is provably a no-op for every eqsat join shape.

## Pointers

- Commit path today: `eqsat/raise.rs:246-283` (WcoJoin), `134-146` (Join);
  `join_implementation.rs:636-681` (`plan_join_min_arrangements`).
- Cost model: `eqsat/cost.rs` — `binary_join_terms:696-725`,
  `delta_join_terms:747-848`, `join_key_cols_for_input:877-902`, orderers
  `989-1154`, `Cost:85-107`, comparators `114-140`.
- Lowering mechanism: `join_implementation.rs` — `implement_arrangements:944-1032`,
  `install_lifted_mfp:1048-1095`, Differential emit `814-822`.
- `JoinImplementation` enum: `src/expr/src/relation.rs:3218-3261`.
- Selector (prior cycle): `eqsat/join_spelling.rs`.
- Subsume map: `doc/developer/design/20260630_joinimplementation_internals.md`.
- Findings: `doc/developer/design/20260629_eqsat_join_cost_findings.md`.
- Memory: `eqsat-joinimpl-undoes-spelling`, `eqsat-local-vs-global-cost-model`.
