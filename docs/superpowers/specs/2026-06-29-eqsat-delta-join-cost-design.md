# Eqsat delta-aware join cost + join-key spelling candidates — design

> **Status:** approved design (brainstorm output), revised after review. Next
> step: `writing-plans`. Builds on the investigation in
> `doc/developer/design/20260624_eqsat/20260629_eqsat_join_cost_findings.md` (findings + a
> probe that de-risked the cost math). This spec turns those findings into a
> buildable, gated, end-to-end change.

## Goal

Make the eqsat optimizer emit the **local** join-key spelling so a
variable-outer-join (VOJ) no longer plans a cross product. The enabling change
is a delta-aware join-cost function with a **keyed-ness axis** (forced-cross
count) that the join-key **spelling resolver** uses to choose the local spelling
over the canonicalized-foreign one. Gated behind a new feature flag (default
**on** in-branch, for easy A/B testing).

## Background (why the cross happens)

```sql
CREATE TABLE t1(f0 int, f1 int);
CREATE TABLE t2(f2 int, f3 int);
CREATE TABLE t3(f4 int, f5 int);
SELECT * FROM t1 LEFT JOIN t2 ON (f0 = f2) LEFT JOIN t3 ON (f2 = f4);
```

With `enable_eager_delta_joins` on, the t3-driven delta path is
`%2 » %0:t1[×] » %1[#0]K` — a cross product over the full `t1`.

Root cause: a **shared** transform (`canonicalize_equivalences` /
`EquivalencePropagation`, used by both the legacy and eqsat paths) collapses the
`{f0, f2}` equivalence class to the **foreign** representative `#0` (t1).
Downstream `JoinImplementation` then has only `#0` to key on for the t3 step, so
it cannot build a local arrangement and falls back to the cross. The
semantically-identical **local** spelling `#2` would let it build a local
arrangement.

Ground truth (from the investigation, captured via
`EXPLAIN ... WITH(join implementations)`):

- eqsat ON ≡ eqsat OFF, byte-identical: eqsat does **not** help today — the `#0`
  spelling is baked in upstream, and SP4b deliberately excluded Join keys from
  colored resolution.
- `enable_eager_delta_joins` OFF → a differential single-path plan, no cross
  (the bug is delta-path-specific).
- Writing the query as `... ON (f3 = f4)` keeps a local spelling and the planner
  builds a local arrangement — confirming the trigger is purely the key
  spelling.

## Why the cost model is the lever (three walls)

All three obstacles are in the **cost model's axes**, not the rewrite search:

1. Predicate-simp folds are arrangement-neutral → invisible to memory/time.
2. The scalar colored extractor can't discriminate the spelling: `#0` and `#2`
   are both single column refs with identical `scalar_expr_cost`, and the
   current `binary_join_terms` actually rates the `#0` (cross) spelling
   *cheaper* (more cross-input equality constraints → tighter AGM bound).
3. AGM (worst-case output size) cannot tell a non-key equi-join (N²) from a
   cross product (N²). The cross's real cost is arrangement-structural
   (broadcast via an empty-key arrangement), invisible to an output-size bound.

The probe (see findings doc) validated the fix: a **keyed-ness axis**
(forced-cross count) compared **before** the degree vector. Crucial nuance: on
the degree vector *alone* the cross spelling looks cheaper
(`#0=[2,1,1] < #2=[2,2,2]`), so keyed-ness must be **primary**, not a tiebreak.

## Architecture

### The consumer is the spelling resolver, not node selection

The delta-aware cost is consumed at exactly **one** point: the join-key
**spelling resolver** in extraction. Node selection (the ILP / greedy extractor)
is **not** a consumer, by two independent facts:

- The default extraction objective `ArrangementCount` (`objective.rs`) compares
  `arrangements` → `time` → `nodes` *directly*; it never calls
  `Cost::cmp_memory_first`/`cmp_time_first`. Only the legacy `PeakDegree` and
  `TimeFirst` objectives delegate to those comparators. So a `crosses` field on
  `Cost` would be invisible to the default objective.
- Even if it were wired into `ArrangementCount`, node selection is structurally
  cross-blind: a cross's empty-key broadcast and a keyed step **both count as 1
  arrangement** in `collect_memory`, so the objective ties them. This is the same
  wall the findings doc named (AGM/arrangement-count can't see the cross).

Therefore: `crosses` is **not** added to `Cost`, and the shared comparators
(`cmp_memory_first`, `cmp_time_first`) and `Cost`'s fields are left **unchanged**
— preserving `PeakDegree`/`TimeFirst` and the `engine.rs` faster-but-heavier
*recommendation*, which both call those comparators. The keyed-ness axis lives
only in `delta_join_terms`' return tuple and is compared only inside the
resolver.

### Data flow

```
eqsat Equivalences analysis fact for the join e-class (carries #0 ≡ #2 ≡ #4)
  → extraction resolves a join's keys
    → resolver enumerates spellings from that fact's classes
      → for each, builds resolved equivalences with a directed #0→#2 substitution
        → scores each with delta_join_terms → (crosses, degrees)
          → picks the spelling minimizing (crosses, then degrees)
            → emits the local spelling (#2)
              → downstream JoinImplementation builds a local arrangement → no cross
```

`JoinImplementation` is **unchanged**: once eqsat emits `#2`, it builds the
local arrangement on its own.

**Candidate source (resolved by spike, 2026-06-29).** The local spelling is
**not** lost by production canonicalization and does **not** need re-minting.
`EquivalenceClasses::minimize`/`reduce_child` structurally cannot drop a distinct
column member (a bare `Column` hits the catch-all `_ => {}`; refresh/tidy only
dedup identical exprs and keep `len > 1` classes), so the class retains all
spellings (`[#0, #2, #4]`); the reducer merely *maps* `#2 → #0`. The eqsat
`Equivalences` analysis (`src/transform/src/eqsat/analysis/equivalences.rs`)
recomputes this fact from structure: its Join arm folds each input's permuted
equivalences with the join's own equivalences, so the `t1⋈t2` step (one input to
the t3 join) carries `#0 ≡ #2` *intra-input*, identity-permuted into the t3-join
fact. So the candidate spellings are read directly from the analysis fact's
`classes`; the colored extractor is **not** the source.

## Components

### Component A — `delta_join_terms` scoring function (`src/transform/src/eqsat/cost.rs`)

A standalone scoring function (a `CostModel` method, reusing the existing
`Hypergraph` / `agm_degree_subset_memo` machinery). It is **not** wired into
`cost()` or `Cost`; its only caller is the resolver (Component B).

`delta_join_terms(inputs, equivalences) -> (crosses: usize, degrees: Vec<f64>)`:

- Sum over **per-driver-rooted** left-deep paths (a delta join maintains one
  path per input; `binary_join_terms` picks a single best order and roots away
  from the cross, so it never sees it).
- **Delta-connectivity:** a class can *key* the step that extends `frontier` by
  input `j` only if **all** of the class's inputs are within `frontier ∪ {j}`
  *and* it spans the boundary (touches both `frontier` and `j`). A class that
  also touches a not-yet-joined input cannot key the step (plain hypergraph
  connectivity over-counts and misses this).
- A step with no keying class is a **forced cross**. Count it into `crosses`
  **only when neither side of the forced product is a constant** — i.e. both the
  broadcast frontier and the joined input have size-degree > `EPS`. A broadcast
  of a constant/single-row side is effectively free and must not dominate a
  genuine degree blowup (see test `cheap_cross_does_not_dominate_blowup`).
- **Drop each path's final full-join term** (the streamed output, identical
  across spellings — mirrors what `collect_memory_into` already does with
  `terms.pop()`). The remaining `degrees` are the intermediate-arrangement
  degrees, sorted descending.

### Component B — join-key spelling candidates at extraction (`src/transform/src/eqsat/egraph/build.rs`, `src/transform/src/eqsat/extract.rs`)

- **Candidate source: the eqsat `Equivalences` analysis fact, not the colored
  extractor.** At the Join e-class, run/read the `Equivalences` analysis
  (recomputed from structure — robust to `enable_eq_classes_withholding_errors`,
  which routes a *different* `extract_equivalences` we must avoid) and read the
  fact's `classes` for the candidate spellings. The two genuinely new pieces:
  (1) a **join-level selector** scored by `delta_join_terms` — `resolve_scalar_colored`'s
  scalar-local `(scalar_expr_cost, support, name_key)` cannot pick `#2` over `#0`
  (they tie), so the selector is mandatory and new; (2) **threading** — the
  analysis fact must be made available at the resolution point in extraction,
  where it is absent today, and `delta_join_terms` operates on the resolved
  `Rel.equivalences` (`&[Vec<EScalar>]`), not the analysis map.
- **Substitution direction.** The built-in `reducer()` points `#2 → #0` (toward
  the canonical rep — the wrong way). The selector builds a **directed**
  `BTreeMap { #0 → #2 }` and applies `ExpressionReducer::reduce_expr`, which
  descends into `If`'s then/else (so the VOJ key
  `case when #4 IS NULL then null else #0 end` becomes `… else #2 end`).
- **Flag-gating.** The whole selector path is gated by the flag (Component C):
  with the flag off, Join keys keep today's `resolve_equivalences` output, so
  flag-off is byte-identical to today — not merely "scoring skipped."
- **Comparison rule (the single consumer of the keyed-ness axis):** the resolver
  compares candidate spellings of *the same join* by the raw
  `(crosses, degrees)` tuple returned by `delta_join_terms` — `crosses`
  ascending first, then `cmp_vecs` over the descending `degrees` vectors —
  **without** constructing a `Cost` or touching the shared comparators. Because
  all candidates join the same inputs, this is a per-join choice among spellings,
  never a plan-wide cross count. It emits the local spelling `#2`,
  counteracting the upstream canonicalize-to-`#0`.
- **Enumeration is a joint search and must be bounded.** Whether a step is a
  forced cross depends on the *joint* spelling assignment (each class's spelling
  decides which input it attaches to, hence connectivity), so per-class
  independent minimization is unsound — the search space is the product over
  multi-spelling classes (`m^k` for `k` such classes; VOJ is a trivial `2×2`).
  v1 strategy: **exhaustive search guarded by a small-`k` cap** (e.g. fall back
  to today's single canonical spelling when `k` or the product exceeds a fixed
  bound), so a wide join with many aliased classes cannot blow up extraction
  time. The cap value and fallback are fixed in the plan; the cap being hit is
  logged, not silent.
- **Empirical gate (caveat 1).** All committed goldens that show
  `Join on=(#0 = #2 = #4)` are *inner* joins; the VOJ is a LEFT JOIN, lowered to
  `inner ∪ antijoin` with null-padding `If`s (the `case when #4 IS NULL …` key is
  itself the null-pad artifact). The mechanism is shape-independent, but *which*
  e-class carries `#0 ≡ #2` and the exact column offsets must be confirmed with a
  live `EXPLAIN` + `Equivalences`-fact dump on the precise VOJ query. The plan's
  **first Component-B task is that dump**, converting the last unknown during
  execution rather than blocking on it.
- **Source robustness (caveat 2).** Use the structural eqsat `Equivalences`
  analysis (recomputed from structure) as the candidate source, *not* the
  persisted equivalences field's reducer, so the path is robust to
  `enable_eq_classes_withholding_errors` (which uses a different
  `extract_equivalences`, `eqprop.rs`).
- **WcoJoin is unchanged for v1.** A WcoJoin is not left-deep and cannot force a
  binary cross, so `delta_join_terms` does not apply; its key resolution keeps
  the current path. (Cross/keyed-ness for WcoJoin is a follow-up.)
- **Node selection (ILP) is unchanged for v1.** The spelling is resolved
  post-selection. Known boundary: if the corpus surfaces a case where node
  *selection* (not spelling) trades a cross for fewer arrangements, adding a
  keyed-ness tier to node selection is the follow-up — and would require solving
  the arrangement-count cross-blindness noted above.

### Component C — feature flag `enable_eqsat_delta_join_cost`

A flag is a **multi-site** addition; all three sites are required:

1. **System var** (`src/sql/src/session/vars/definitions.rs`): define
   `enable_eqsat_delta_join_cost`, default **true** (in-branch; primary use is
   A/B testing), near `enable_eqsat_physical_optimizer`.
2. **Optimizer feature** (`src/repr/src/optimize.rs`): add
   `enable_eqsat_delta_join_cost: bool` to the `optimizer_feature_flags!` macro
   block (alongside `enable_eqsat_physical_optimizer`). **The macro generates two
   structs — `OptimizerFeatures` and `OptimizerFeatureOverrides` — and every
   *exhaustive* (no `..`) use of `OptimizerFeatureOverrides` must be updated or
   the build breaks.** The plan must `grep "OptimizerFeatureOverrides {"` and fix
   each; known exhaustive sites today are `src/sql/src/plan/statement/dml.rs`
   (construction), `src/sql/src/plan/statement/ddl.rs` (a construction *and* a
   `let OptimizerFeatureOverrides { .. }` destructure). Treat the grep, not this
   list, as authoritative.
3. **Binding** (`definitions.rs`, the `OptimizerFeatures`-construction site that
   already maps `enable_eqsat_physical_optimizer: vars.enable_eqsat_physical_optimizer()`):
   bind the new feature from the new var.

When the flag is **off**: the resolver does not call `delta_join_terms` and
keeps the current non-colored join-key path — byte-identical to today. This is
the A/B baseline.

## Testing & validation

### Unit tests (`cost.rs` `mod tests`)

- `delta_join_local_spelling_beats_cross`: build the reproduction recipe — 3
  inputs `t1(2), t2b(3), t3b(3)`; `class1 = {#0, #2}`;
  `class2 = {#5, col(X) Eq col(4)}` with X=0 (cross) and X=2 (local). Assert
  `delta_join_terms` yields `crosses = 1` for X=0 and `crosses = 0` for X=2, and
  that the tuple comparison ranks the X=2 spelling cheaper.
- `keyed_ness_is_primary_over_degree`: assert the resolver's tuple comparison
  ranks `(crosses: 0, degrees: [2,2,2])` below `(crosses: 1, degrees: [2,1,1])`
  — locks in that keyed-ness beats a *smaller* degree vector.
- `cheap_cross_does_not_dominate_blowup`: a spelling whose only "cross" is a
  broadcast of a constant/single-row side must **not** be counted as a forced
  cross, so it does not strictly dominate a keyed spelling that carries a large
  degree. Guards the constant-side gate.
- The existing `cost.rs`/`objective.rs` `mod tests` suites are untouched and must
  still pass; because `Cost` and the comparators are unchanged, this change
  cannot perturb them.

### Resolver test (`build.rs` / `extract.rs`)

- Extraction of the VOJ join emits the **local** spelling: assert the resolved
  t3-step key references the local input's column, not the foreign `#0`.

### End-to-end (sqllogictest)

- Promote the investigation's `voj_groundtruth.slt` into the tree (anonymized,
  no customer references). With `enable_eqsat_delta_join_cost` +
  `enable_eager_delta_joins` on, `EXPLAIN ... WITH(join implementations)` shows
  **no `[×]`** cross. With the flag off, the plan is byte-identical to today.

### Corpus / regression discipline (merge blocker)

- Before merge, A/B the flag across the optimizer plan goldens and review every
  plan that moves. Even though the change is resolver-scoped, choosing a
  different join-key spelling shifts downstream arrangement choices, so plans
  *will* move. Each moved plan must be a defensible improvement or neutral. This
  is the primary risk and gets an explicit review gate in the implementation
  plan.

### Commands

- Unit/golden: `bin/cargo-test -p mz-transform` (nextest); rewrite goldens with
  `REWRITE=1`.
- slt: `COCKROACH_URL=postgres://root@localhost:26257
  METADATA_BACKEND_URL=postgres://root@localhost:26257 bin/sqllogictest
  --optimized -- [--rewrite-results] <files>`.

## Out of scope (follow-ups)

- **Removing `JoinImplementation`** from the eqsat optimizer (make
  `PhysicalEqSatTransform` fully commit join implementation so
  `fixpoint_join_impl` is unnecessary for eqsat plans). This spec keeps
  `JoinImplementation` and only changes which spelling eqsat hands it.
- **Cross-awareness in node selection.** Making the ILP / `ArrangementCount`
  objective see the cross requires both a new objective tier *and* solving the
  arrangement-count cross-blindness (a broadcast and a keyed step both count as
  one arrangement). Only needed if the corpus shows a node-selection-level
  cross/arrangement trade-off.
- **Charging the cross a memory term** (the empty-key broadcast arrangement). It
  cannot be a size-based degree (a broadcast and an index over the same relation
  have equal size), so it would be its own structural term — deferred. The
  constant-side gate in Component A is the cheap interim guard against a trivial
  cross being over-penalized.
- **WcoJoin keyed-ness** beyond the binary/delta-join scope above.

## Pointers

- Cost model: `src/transform/src/eqsat/cost.rs` — `Cost`, `cmp_memory_first`,
  `cmp_time_first`, `cost`, `binary_join_terms`, `collect_memory_into` (the
  `terms.pop()` that drops the streamed output), `join_key_cols_for_input`,
  `agm_degree_subset_memo`, `Hypergraph::build`.
- Extraction objectives: `src/transform/src/eqsat/objective.rs` —
  `ArrangementCount` (default; compares `arrangements` directly), `PeakDegree`,
  `TimeFirst`. Recommendation comparison: `src/transform/src/eqsat/engine.rs`.
- Candidate source: `src/transform/src/eqsat/analysis/equivalences.rs` (the
  `Equivalences` analysis; its Join arm folds permuted-input + own equivalences;
  the per-e-class fact whose `classes` enumerate the spellings) and the shared
  `src/transform/src/analysis/equivalences.rs` (`EquivalenceClasses`,
  `ExpressionReducer::reduce_expr`, the `reducer()` that points `#2 → #0`).
  Avoid the `enable_eq_classes_withholding_errors` path's `extract_equivalences`
  in `src/transform/src/analysis/equivalences/eqprop.rs`.
- Spelling resolution: `src/transform/src/eqsat/egraph/build.rs` —
  `resolve_equivalences` (Join today), the greedy `extract_with` Join arm;
  `src/transform/src/eqsat/extract.rs` — `IlpExtractor::build_selected` Join
  handling (the second extraction path that must also be threaded).
- Pipeline: `src/transform/src/lib.rs` — logical `EqSatTransform`,
  `PhysicalEqSatTransform`, `fixpoint_join_impl`.
- Flags: `src/sql/src/session/vars/definitions.rs`
  (`enable_eqsat_physical_optimizer`, `enable_eager_delta_joins`);
  `src/repr/src/optimize.rs` (`optimizer_feature_flags!`).
- Investigation: `doc/developer/design/20260624_eqsat/20260629_eqsat_join_cost_findings.md`.
