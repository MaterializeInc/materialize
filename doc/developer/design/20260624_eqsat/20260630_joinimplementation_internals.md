# JoinImplementation Internals: What eqsat Must Subsume

**Purpose.** A reference map of every decision and side-effect owned by
`JoinImplementation` (`src/transform/src/join_implementation.rs`), structured
as a checklist for the future project of making the eqsat physical optimizer
subsume / bypass it ("skip JI for eqsat-produced plans").

This is read-only design archaeology. No source was modified to produce it.

---

## 0. Background: what JI is and where it sits

`JoinImplementation` is the physical join planner for MIR.  It runs inside a
fixpoint loop called `fixpoint_join_impl` in `Optimizer::physical_optimizer`
(`src/transform/src/lib.rs:900-904`).  The fixpoint loop contains only JI; the
limit is 100 iterations.

Pipeline position in `physical_optimizer` (`lib.rs:845-934`):

```
fixpoint_physical_01            ← EquivalencePropagation, Demand, Projections,
                                   LiteralLifting, FoldConstants, CoalesceCase
PhysicalEqSatTransform          ← gated on enable_eqsat_physical_optimizer
LiteralConstraints              ← must precede JI (see §6)
fixpoint_join_impl              ← JoinImplementation only
CanonicalizeMfp
RelationCSE(inline_mfp=false)
ProjectionPushdown(skip_joins)  ← gated on enable_projection_pushdown_after_relation_cse
CanonicalizeMfp                 ← same gate
CaseLiteralTransform            ← gated
fold_constants_fixpoint
```

JI traverses the plan in **pre-order** (`action_recursive`,
`join_implementation.rs:90-127`), accumulating let-bound arrangement knowledge
as it descends (local `ArrangeBy`s and `Reduce` group keys are registered in
`IndexMap`, lines 98-116).  For each `Join` node encountered it calls `action`.

---

## Responsibility 1 — Equivalence Canonicalization

### (a) What it decides
JI normalises the `equivalences` field of a `Join` node the first time it
visits that node.  The goal is to collapse multi-expression equivalence classes
to their simplest canonical form and to merge classes that share members.

### (b) Exact location
`join_implementation.rs:168-182`

```rust
if matches!(implementation, Unimplemented) {
    mz_expr::canonicalize::canonicalize_equivalences(
        equivalences,
        input_types.iter().map(|t| &t.column_types),
    );
}
```

`canonicalize_equivalences` lives at `src/expr/src/relation/canonicalize.rs:39-103`.

### (c) Inputs needed
- The `equivalences: Vec<Vec<MirScalarExpr>>` already on the `Join` node.
- Per-input column types (for `reduce` and null propagation inside the function).
- The function is gated: **only when `implementation == Unimplemented`**.

### (d) What it emits
The `equivalences` field is mutated in-place.  Each class is simplified to
use the least-complex expression as the canonical representative (by
`rank_complexity`), the classes are sorted/deduped, and overlapping classes are
merged via union-find (`canonicalize_equivalence_classes`, line 102).

### (e) The non-idempotency problem (why the guard exists)
The comment at lines 169-177 explains: `canonicalize_equivalences` is **not
idempotent**.  If it were called again on already-canonicalized equivalences, it
might produce a different form.  Since the fixpoint loop runs JI multiple times,
and since a second run may or may not keep the newly-computed plan (it might
discard a new Differential plan in favour of upgrading to a Delta plan), calling
it on the second pass could leave `equivalences` inconsistent with the previously
emitted plan.  The `Unimplemented` guard is therefore load-bearing.

**Eqsat note.** The logical eqsat pass already canonicalizes scalar expressions
inside the e-graph.  The physical pass inherits those canonicalizations.  If
eqsat-produced plans bypass JI, they still need `equivalences` to be in the
form the cost model and arrangement-selection code expect.  The physical eqsat
extraction does not currently call `canonicalize_equivalences`; a bypass would
need to either call it on the extracted MIR or guarantee the e-graph's own
normal form is compatible.

---

## Responsibility 2 — Delta-Query vs. Differential Decision

### (a) What it decides
Whether a multi-input join is executed as a delta-query dataflow or as a
left-deep sequence of binary differential joins.

### (b) Exact location
`join_implementation.rs:362-523` (the decision block in `action`).

Key sub-functions:
- `delta_queries::plan` — `join_implementation.rs:698-773`
- `differential::plan` — `join_implementation.rs:796-933`

### (c) Inputs needed
- `inputs.len()` — binary joins (≤ 2) are **always** differential (lines 373, 409-429).
- `available_arrangements[i]` — pre-existing arrangement keys per input.
- `unique_keys[i]`, `cardinalities[i]`, `filters[i]` — for order scoring.
- Feature flags:
  - `features.enable_eager_delta_joins` — changes the comparison predicate
    (line 491).
  - `features.enable_join_prioritize_arranged` — affects the ordering heuristic
    used during planning.
- The existing `implementation` field: if already `Differential`, JI considers
  upgrading it to a Delta join (but only if it needs zero new arrangements when
  `enable_eager_delta_joins` is false, line 378).

### (d) What it emits
One of:
- `JoinImplementation::DeltaQuery(...)` — when delta is chosen.
- `JoinImplementation::Differential(...)` — otherwise.
- The existing implementation is left unchanged if neither condition fires.

### (e) Decision predicate summary
```
inputs.len() <= 2          → always Differential
delta_new_arrangements == 0               → always DeltaQuery   (both modes)
enable_eager_delta_joins &&
  delta_new_arrangements <= diff_new      → DeltaQuery          (eager mode)
else                                       → Differential        (first run)
                                           → keep old plan       (second run)
```

### (f) Eqsat note
The eqsat cost model (`src/transform/src/eqsat/cost.rs`) already contains a
WcoJoin vs. binary-join comparison that parallels this decision.  `PhysicalEqSatTransform`
calls `optimize_with_availability` with `commit_wcoj=true`, committing the
WcoJoin choice to a `DeltaQuery` in the extracted MIR (see §7 for the interlock).
For a full bypass, the eqsat cost model must cover:
- the exact "0 new arrangements" threshold vs. the eager-delta inequality,
- the binary join exception,
- the upgrade-from-Differential path (second JI run).

---

## Responsibility 3 — Join-Order Search

### (a) What it decides
For each possible starting input, JI computes the best order to join the
remaining inputs.  For `Differential` it then selects one starting point (and
thus one total order) via a max-min heuristic.  For `DeltaQuery` all N starting
points are kept.

### (b) Exact location
`optimize_orders` — `join_implementation.rs:1112-1136`
`Orderer::optimize_order_for` — `join_implementation.rs:1217-1362`
`Orderer::order_input` — `join_implementation.rs:1370-1462`
Differential order selection — `join_implementation.rs:847-893`

### (c) Inputs needed
- `equivalences` — to determine which columns become "bound" as inputs are
  placed, and therefore which arrangement keys become viable.
- `available_arrangements[i]` — tells the orderer which keys are pre-arranged.
- `unique_keys[i]` — used to compute `JoinInputCharacteristics::unique_key`.
- `cardinalities[i]: Option<usize>` — from `DerivedBuilder` analysis
  (`Cardinality::with_stats`, lines 282-293), gated on
  `enable_cardinality_estimates`.
- `filters[i]: FilterCharacteristics` — built from: the input's own MFP
  filters, `IndexedFilter` membership, filters below an `ArrangeBy` above the
  input, and predicates that `PredicatePushdown` would push from above the join
  (lines 234-296).
- `enable_join_prioritize_arranged` — selects between `JoinInputCharacteristicsV1`
  (no `not_cross` field) and `JoinInputCharacteristicsV2` (adds `not_cross`
  above key_length in priority) (`relation.rs:3313-3453`).

### (d) Algorithm
The orderer maintains a `BinaryHeap<(JoinInputCharacteristics, key, input)>`.
Starting from a fixed input, it calls `order_input(start)`, which marks the
input as placed, activates equivalences now fully supported, updates `bound[rel]`
for each other input that references those equivalences, and pushes newly-viable
arrangements (and cross-join fall-backs) onto the heap.  The outer loop pops the
highest-priority candidate, places it, and repeats until all inputs are ordered.

The Differential path then picks the starting point that maximises the
**worst** `JoinInputCharacteristics` across the chain — the max-min selection
(`join_implementation.rs:860-875`).  Ties are broken by the lexicographic order
of the full characteristic vector ("pushes bad stuff later").

### (e) What it emits
A `Vec<(JoinInputCharacteristics, Vec<MirScalarExpr>, usize)>` per starting
point.  Each element is `(score, arrangement_key_in_local_coords, input_index)`.

### (f) Eqsat note
This is the **hardest gap**.  The current eqsat cost model assigns a per-node
cost to the extracted tree without simulating the greedy search or the max-min
selection.  Subsuming this requires the cost model to:
1. Consider all N! orderings (or a good proxy) to reproduce max-min behaviour.
2. Propagate `FilterCharacteristics` and cardinality signals into that ordering
   comparison.
3. Handle the "sum vs. max" semantics of the Differential arrangement-count
   cost (the orderer charges `k-2` intermediate arrangements plus the cost of
   the chosen starting point).

---

## Responsibility 4 — Per-Input Arrangement/Key Selection and Reuse

### (a) What it decides
Which arrangement key to use for each input in the chosen plan, and whether an
`ArrangeBy` node needs to be inserted (new arrangement built at runtime) or an
existing one can be reused.

### (b) Exact location
Collection of available arrangements — `join_implementation.rs:296-358`
`Orderer::order_input` (activates viable arrangements) — lines 1403-1434
`implement_arrangements` — `join_implementation.rs:944-1032`

### (c) Sources of available arrangements
The available arrangements for each input are collected in `action` (lines
298-358):

| Input shape | Source |
|---|---|
| `Get { id }` | `indexes.get(id)` — global indexes via `IndexOracle` plus local let-bound arrangements via `IndexMap::local` |
| `ArrangeBy { keys }` | keys listed on the node, plus any `Get` keys beneath it |
| `Reduce { group_key }` | columns `0..group_key.len()` form one arrangement key |
| `Join { implementation: IndexedFilter(id, ..) }` | the underlying index's keys |

After collection, two filters are applied (lines 326-358):
1. **Projection filter**: key columns that are projected away by an MFP
   surrounding the input are removed.  Keys that reference removed columns are
   dropped entirely.
2. **Equivalence filter**: only arrangement keys whose every component appears
   in some join equivalence class are kept (comment at lines 347-358 notes this
   is currently overly conservative).

Keys are permuted to reflect local column positions after the MFP projection.

### (d) What it emits
`implement_arrangements` (lines 944-1032):
- Strips any existing `ArrangeBy` wrapper from each input.
- If all needed keys for an input are already in `available_arrangements[i]`,
  lifts the input's MFP (filter+map+project) so the arrangement sits directly
  on the un-filtered input (enabling arrangement reuse).
- Otherwise, wraps the input in `ArrangeBy { keys: needed }`.

Returns the combined lifted MFP and a per-input projection permutation
(`lifted_projections`), which the caller uses to patch the order vectors.

### (e) Eqsat note
The eqsat cost model at `src/transform/src/eqsat/cost.rs:339+` and `cost.rs:402+`
already reads `available` (a `BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>` built
by `build_availability` in `transform.rs:183-209`).  It suppresses the
arrangement-build memory term for indexed inputs.  What it does NOT currently
model:
- Local (let-bound) `ArrangeBy` and `Reduce`-key arrangements.
- The MFP-lifting step that allows arrangement reuse when an MFP sits between
  the `Get` and the join.
- The equivalence-based key-restriction filter.

---

## Responsibility 5 — MFP Lifting and Post-Join Reinstallation

### (a) What it decides
Whether predicates and projections from inside a join input can be "lifted"
above the join to enable arrangement reuse, and where the combined lifted MFP
is installed.

### (b) Exact location
`implement_arrangements` — `join_implementation.rs:944-1032` (lifting)
`install_lifted_mfp` — `join_implementation.rs:1048-1095` (reinstallation)
`permute_order` — `join_implementation.rs:1097-1110` (key patching)

### (c) Mechanism
If **all** needed arrangement keys for input `i` are already in
`available_arrangements[i]`, `implement_arrangements` extracts the input's
non-error MFP via `MapFilterProject::extract_non_errors_from_expr_mut`, removes
any existing `ArrangeBy`, and permutes the needed keys through the lifted
projection.

After the arrangement keys are settled, `install_lifted_mfp`:
1. Permutes `equivalences` entries that reference columns now remapped by the
   lifted projection (`lines 1051-1071`).
2. Resolves column references that now point into the lifted `map` expressions
   (inlining, `lines 1063-1071`).
3. Calls `get_canonicalizer_map` to rewrite expressions in the lifted `map` and
   `filter` using the canonical equivalence-class representative, deduplicating
   predicates that were lifted from multiple inputs but that refer to the same
   equivalence class member (`lines 1084-1091`).
4. Wraps the join in `map(combined_map).filter(combined_filter).project(project)`
   (`line 1093`).

### (d) Eqsat note
The eqsat extraction does not perform MFP lifting.  The logical eqsat pass's
`demand_pushdown` inside `raise` partially subsumes demand narrowing, but not
this specific arrangement-reuse lifting pattern.  A bypass would either need to
perform this rewriting in extraction or treat the whole lifted form as a
separate e-node shape.

---

## Responsibility 6 — Emitted Plan Form

### (a) What it decides
The exact value written to `MirRelationExpr::Join::implementation`.

### (b) Exact location
`src/expr/src/relation.rs:3218-3288` (enum definition)
`differential::plan` — sets `JoinImplementation::Differential` at
`join_implementation.rs:919-922`
`delta_queries::plan` — sets `JoinImplementation::DeltaQuery` at
`join_implementation.rs:762`

### (c) Differential form
```rust
JoinImplementation::Differential(
    (start_index: usize, Option<start_key: Vec<MirScalarExpr>>, Option<JoinInputCharacteristics>),
    Vec<(input_index: usize, key: Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)>,
)
```
- `start_index` + `start_key` identify the starting arrangement (the "probe"
  side of the first binary join).
- The `Vec` contains the remaining inputs in join order, each with the
  arrangement key used to look them up.
- All keys are in **local** column coordinates (after MFP lifting).
- The `JoinInputCharacteristics` values are for `EXPLAIN` only.

### (d) DeltaQuery form
```rust
JoinImplementation::DeltaQuery(
    Vec<                            // one path per starting input (N paths total)
        Vec<(usize, Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)>
    >
)
```
- Path `i` starts from input `i` (implicit) and lists the subsequent inputs in
  join order with their arrangement keys.
- All keys are in local column coordinates.

### (e) IndexedFilter form
```rust
JoinImplementation::IndexedFilter(coll_id: GlobalId, idx_id: GlobalId,
                                   index_key: Vec<MirScalarExpr>, constants: Vec<Row>)
```
This is set by `LiteralConstraints`, **not JI**.  JI skips nodes already in
this state.

### (f) Eqsat note
The eqsat extraction must produce the same struct shapes to avoid a separate
lowering step.  The `DeltaQuery` shape (produced by `plan_as_delta_query`,
`join_implementation.rs:591-619`) is already used by the physical eqsat pass via
`commit_wcoj`.  The `Differential` shape is what JI produces for binary and
non-delta multi-way joins — this is not yet produced by eqsat extraction.

---

## Responsibility 7 — Sequencing Dependencies

### (a) What must run before JI

| Precondition | Why | Location of comment |
|---|---|---|
| `LiteralConstraints` before JI | JI lifts `Filter` away from `Get` nodes; LC must have already converted literal filters to `IndexedFilter` joins before that happens | `lib.rs:852-853` |
| No `RelationCSE` between LC and JI | CSE could move an `IndexedFilter` behind a `Get`, making LC's work invisible to JI | `lib.rs:854-855` |
| Last `RelationCSE` before JI must use `inline_mfp = true` | So JI sees MFPs inlined rather than behind a `Get` | `lib.rs:856`, `lib.rs:986-988` |
| `LiteralLifting` must precede JI | LL sometimes creates `Unimplemented` joins (even after a prior LL run in the logical optimizer) | `lib.rs:857-859` |
| `EquivalencePropagation` must NOT share a fixpoint loop with JI | EP might invalidate a join plan; if co-looped, JI might run unboundedly | `lib.rs:860-866` |
| Same caveat for `FoldConstants`, `Demand`, `LiteralLifting` | Same oscillation risk | `lib.rs:867-868` |
| `ProjectionPushdown` (inside `fixpoint_physical_01`) must run before `fixpoint_join_impl` | PP panics on filled-in `JoinImplementation` when run in `include_joins` mode; it must finish before JI fills them in | `eqsat/transform.rs:62-67` |

### (b) What must run after JI
- `CanonicalizeMfp` — tidies the MFPs JI installed.
- `RelationCSE(inline_mfp=false)` — CSE after JI to share common sub-expressions.
- `ProjectionPushdown(skip_joins)` — can push projections behind Gets created by
  CSE without touching filled-in join implementations.

### (c) What eqsat already folds in (logical pass)
The logical eqsat `raise` module runs:
- `coalesce_mfp` — corresponding to `CanonicalizeMfp`.
- `demand_pushdown` — corresponding to parts of `Demand`.
- MFP fusion rules — corresponding to `fusion::Fusion`.

These are internal to saturation and not the same as the production passes.

### (d) Eqsat note for bypass
A bypass of `fixpoint_join_impl` for eqsat-produced plans must guarantee:
1. `LiteralConstraints` still runs before the eqsat physical pass (already the
   case: it runs between `PhysicalEqSatTransform` and `fixpoint_join_impl`).
2. The extracted MIR already has a filled-in `implementation` field (not
   `Unimplemented`), so JI's `action` no-ops on it.
3. `EquivalencePropagation` must not re-run after the physical eqsat extraction
   (it does not: it is inside `fixpoint_physical_01` which precedes eqsat).

---

## Responsibility 8 — WcoJoin→DeltaQuery Interlock with PhysicalEqSatTransform

### (a) What it decides
When the eqsat physical optimizer is active, it "commits" the WcoJoin choice
by lowering the extracted `WcoJoin` node to a fully-specified
`JoinImplementation::DeltaQuery` in the MIR output.  JI is then expected to
skip those joins.

### (b) Exact location
`PhysicalEqSatTransform::actually_perform_transform` — `eqsat/transform.rs:139-173`

> "Unlike the logical pass, this calls `optimize_with_availability`
> (`commit_wcoj=true`) so the e-graph's WcoJoin choice is lowered to a live
> DeltaQuery with an index-aware cost model."
> — `eqsat/transform.rs:164-166`

> "The committed `DeltaQuery` survives `JoinImplementation` because that
> transform only replans `Unimplemented` and `Differential` joins."
> — `eqsat/transform.rs:129-130`

The placement comment in `lib.rs:892-896`:

> "Physical eqsat placement: joins are still `Unimplemented` here (the
> `ProjectionPushdown` inside `fixpoint_physical_01` that panics on
> filled-in implementations has already run). `optimize` commits the WcoJoin
> choice to a `DeltaQuery`; `JoinImplementation` downstream skips `DeltaQuery`,
> so the decision is preserved."

### (c) Mechanism in JI
`join_implementation.rs:143-164` — the `action` method matches only on:
```rust
implementation @ (Unimplemented | Differential(..))
```
A `DeltaQuery` or `IndexedFilter` node does not match this pattern, so `action`
returns `Ok(())` without touching it.  This is the explicit interlock.

### (d) What is NOT yet handled by the interlock
- `Differential` joins for which eqsat chose the binary-join form: the physical
  eqsat pass currently produces `Unimplemented` joins from its logical pass
  (`EqSatTransform`, `transform.rs:64-68`) and only `DeltaQuery` from the
  physical pass (via `commit_wcoj`).  Binary joins and non-WcoJoin multi-way
  joins still fall through to JI.
- The eqsat cost model covers `WcoJoin` and `Join` (binary) forms.  There is
  no eqsat equivalent of `Differential` for arbitrary linear-chain joins;
  that form is still fully JI's domain.

### (e) Eqsat note
The current arrangement is a **partial bypass**: eqsat handles WcoJoin→DeltaQuery;
JI handles everything else.  A full bypass would need eqsat to also produce
`Differential` forms (including the per-input arrangement keys and
characteristics), at which point JI's `action` would no-op for all eqsat-produced joins.

---

## Gap List for eqsat to Skip JI Completely

The following concrete pieces are missing before `fixpoint_join_impl` can be
skipped for all eqsat-produced plans without losing any decision JI makes today.

### G1 — Differential plan production (hardest)
Eqsat extraction currently produces `WcoJoin` (→`DeltaQuery`) and plain binary
`Join` forms.  It does not produce the `Differential(start, key, rest[])` form
for multi-way linear chains.  Subsuming this requires:
- A new extraction path that chooses a linear order for non-WcoJoin multi-way
  joins, assigns arrangement keys, and emits `JoinImplementation::Differential`.
- The cost model must compare this against `DeltaQuery` with the same criteria
  JI uses (§2 decision predicate), including the binary-join exception.
- **Difficulty: high.** The greedy max-min order search (§3) is the core
  heuristic; replacing it with a cost-model-driven extraction is non-trivial
  because the orderer's input characteristics (unique keys, filter selectivities,
  cardinalities) are inputs to JI that the e-graph must carry or recompute.

### G2 — MFP lifting (medium)
The `implement_arrangements` + `install_lifted_mfp` pipeline (§5) is not
represented in eqsat.  Without it, the extracted MIR may lack the MFP-lifting
step that enables arrangement reuse.  Subsuming it in extraction requires either:
- Running `implement_arrangements` as a post-extraction pass on eqsat-produced
  joins, or
- Teaching eqsat about the MFP-lift pattern as a rule and choosing to apply it
  during extraction.

### G3 — Local / let-bound arrangement awareness (medium)
`IndexMap` (§4, `join_implementation.rs:529-576`) tracks `ArrangeBy` and
`Reduce` nodes on let-bound values as the traversal descends.  The eqsat cost
model's `build_availability` only queries the global `IndexOracle`; it does not
walk `Let` bindings to find local arrangements.  If an eqsat-produced plan
references a let-bound `ArrangeBy`, the cost model may over-charge the
arrangement build cost and prefer a sub-optimal plan.

### G4 — Equivalence canonicalization (low–medium)
If eqsat bypasses JI, `canonicalize_equivalences` will not be called on the
extracted plan's `Join` nodes.  Equivalence classes that are already canonical
inside the e-graph may be spelled differently in the MIR output.  This matters
because downstream passes (`install_lifted_mfp`'s `get_canonicalizer_map`, and
any later `EquivalencePropagation` runs) rely on the canonical form.  Fix:
call `canonicalize_equivalences` once on the eqsat-extracted plan before the
rest of the physical pipeline.

### G5 — `FilterCharacteristics` and cardinality propagation (low)
The orderer builds per-input `FilterCharacteristics` by inspecting the MFP
above each join input and simulating predicate pushdown from above the join
(§3c).  The eqsat cost model does not replicate this propagation for its join
ordering.  For `WcoJoin`, the cost model currently uses `size_degree` terms;
for a true ordering decision it would need to replicate the `FilterCharacteristics`
scoring and the cardinality analysis (`Cardinality::with_stats`).

### G6 — `IndexedFilter` visibility (low)
JI recognises `IndexedFilter` joins as arranged inputs (§4c, `join_implementation.rs:317-324`)
and uses their index keys for join ordering.  The physical eqsat pass seeds the
e-graph with `IndexedFilterSeed` values (`collect_indexed_filter_seeds`,
`transform.rs:230-293`), but the cost model's arrangement-suppression logic may
not cover all `IndexedFilter` shapes that JI would recognise.  Audit needed.

---

## Appendix: Feature Flags Relevant to JI

| Flag (in `OptimizerFeatures`) | Effect on JI |
|---|---|
| `enable_eager_delta_joins` | When true: JI runs only once per join (no upgrade attempt); delta is chosen whenever `delta_new <= diff_new`. |
| `enable_join_prioritize_arranged` | Selects `JoinInputCharacteristicsV2` (adds `not_cross` field before `key_length` in ordering priority). |
| `enable_cardinality_estimates` | Enables `DerivedBuilder`-based cardinality estimates as a signal in `JoinInputCharacteristics`. |
| `enable_eqsat_physical_optimizer` | Gates `PhysicalEqSatTransform`; when active, eqsat commits WcoJoin→DeltaQuery before JI runs. |
| `enable_eqsat_delta_join_cost` | Gates the delta-join cost term in the eqsat cost model. |
| `enable_eqsat_ilp_extraction` | Gates ILP-based extraction in the eqsat physical pass. |
