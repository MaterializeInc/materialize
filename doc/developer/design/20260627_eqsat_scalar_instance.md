# Equality saturation: scalar as a second instance of the generic core (SP2a)

- Associated: CLU-137; `doc/developer/design/20260627_eqsat_generic_core.md` (SP1, the
  generic core this builds on); `doc/developer/design/20260625_eqsat_scalar_expressions.md`
  (the scalar engine being migrated).

> File and line references were grounded on 2026-06-27 against branch
> `claude/mir-equality-optimizer-sodbej`. Re-verify before relying on a specific
> line; they drift.

> This is the design for **Sub-Project 2a (SP2a)** of the effort to unify the
> relational and scalar equality-saturation engines. SP2a is a self-contained,
> **behavior-neutral** refactor: it makes the scalar engine run on SP1's generic
> `EGraph<L>` core, deleting the duplicated scalar e-graph substrate, while
> keeping the scalar rules, saturation loop, lower/raise bridges, and tree-size
> cost exactly as they are today.

## Re-scoping of SP2

SP1's design described **SP2** as a single sub-project that both made scalar a
second instance *and* ported the 21 scalar rules to the declarative rewrite DSL
(genericizing the DSL/codegen/matcher/applier stack). Grounding that against the
code showed it bundles two separable concerns of very different size and risk:

1. **Substrate** — making scalar a second instance of the generic *core*
   (`Language` + analyses + saturation + cost/extraction). Small, mechanical,
   behavior-neutral.
2. **Rule authoring** — porting the 21 hand-written scalar rules to the
   declarative DSL, which forces genericizing the entire DSL grammar, `build.rs`
   codegen, matcher, applier, and the `CompiledRule`-coupled saturation loop, and
   extending the DSL with const-eval / variadic-set / analysis-gate constructs.
   Large, and the relational `saturate(&CompiledRuleSet)` is welded to the DSL
   machinery, so hand-written scalar rules cannot reuse it.

SP2 is therefore split:

- **SP2a (this document)** — scalar on the generic core, keeping hand-written
  rules. Behavior-neutral.
- **SP2b (future)** — port the scalar rules to the declarative DSL with
  `sorry`-stubbed Lean theorems; genericize the DSL/codegen/matcher/applier and
  the saturation loop over `Language`. This is where the declarative-only
  end-state and the rule-machinery genericization land.

The overarching vision (colored e-graph runtime integration; SP3 colored
mechanism; SP4 multi-sort integration) is unchanged — see the SP1 design's
"Where this fits".

## The problem

After SP1, `src/transform/src/eqsat/` still holds two e-graph **cores**:

- the generic `core::EGraph<L>` (union-find, hashcons/memo, congruence closure
  via `rebuild`, `GraphData`, the `Analysis<L>` batch driver), with the
  relational engine as its sole instance (`EGraph<RelLang>`); and
- the standalone `scalar/egraph.rs::ScalarEGraph` — a near-duplicate of the same
  substrate (its own `uf`, `classes`, `hashcons`, `find`, `add`, `union`,
  `rebuild`) carried only because the scalar engine predates the generic core.

This is exactly the duplication SP1 was built to remove. SP2a removes it: scalar
runs on `core::EGraph<L>` as a second `Language` instance.

The scalar engine's non-core pieces stay as-is:

- `SNode` (`scalar/node.rs`) — the decomposed scalar e-node.
- `ClassAnalysis { could_error: bool, literal: Option<(Result<Row, EvalError>,
  ReprColumnType)> }` with `make`/`merge` (`scalar/analysis.rs`) — two
  conservative upper-bound fields, maintained **incrementally** today (computed
  on every `add`, merged on every `union`).
- the 21 hand-written rules (`scalar/rules.rs`), typed
  `fn(&mut ScalarEGraph, &SNode) -> Vec<Id>`.
- `lower`/`raise` bridges (`scalar/lower.rs`, `scalar/raise.rs`) with a
  tree-size (e-node count) cost and fixpoint extraction.
- the `canonicalize` / `canonicalize_predicates` entry points (`scalar.rs`).

## Goal and non-goals

**Goal.** Delete `ScalarEGraph`'s duplicated substrate; run the scalar engine on
`core::EGraph<ScalarLang>`; keep observable behavior identical.

**Non-goals (deferred to SP2b / later):**

- Porting any scalar rule to the declarative DSL.
- Genericizing the DSL, `build.rs` codegen, matcher, applier, or the
  `CompiledRule`-based relational saturation loop.
- Changing the scalar cost model (stays tree-size) or the analysis lattice
  (stays `could_error` + `literal`).
- Any colored / multi-sort work.

## Design

### Scalar `Language`

A new instance of SP1's `Language` trait:

```rust
// src/transform/src/eqsat/scalar/lang.rs (new)
pub struct ScalarLang;

impl Language for ScalarLang {
    type Node = SNode;
    type Sym = ScalarSym;
    type GraphData = ScalarGraphData;

    fn children(node: &SNode) -> Vec<Id> { node.children() }
    fn map_children(node: &SNode, f: impl Fn(Id) -> Id) -> SNode { node.map_children(f) }
    fn symbol(node: &SNode) -> ScalarSym { /* discriminant + func identity */ }

    fn on_add(data: &mut ScalarGraphData, id: Id, node: &SNode, get: &dyn Fn(Id) -> Id) {
        let a = analysis::make(node, &data.analysis, get);
        data.analysis.insert(id, a);
    }
    fn on_union(data: &mut ScalarGraphData, winner: Id, loser: Id) {
        if let (Some(w), Some(l)) = (data.analysis.remove(&winner), data.analysis.remove(&loser)) {
            data.analysis.insert(winner, analysis::merge(w, l));
        }
        // (loser's entry is removed; winner retains the merged analysis)
    }
}
```

- **`ScalarSym`** is a discriminant enum mirroring `SNode`'s variants, carrying
  the function identity for calls (`Unary(UnaryFunc)`, `Binary(BinaryFunc)`,
  `Variadic(VariadicFunc)`, plus `Column`/`Literal`/`Unmaterializable`/`If`). It
  exists only to satisfy `Language::symbol`; the symbol index is consumed by the
  matcher, which SP2a does not use. (`symbol` returning a coarse discriminant is
  acceptable — `index()` is not on the scalar hot path in SP2a.)
- **`ScalarGraphData { col_types: Vec<ReprColumnType>, analysis: HashMap<Id,
  ClassAnalysis> }`** — the two members of today's `ScalarEGraph` that are not
  the generic core, relocated behind accessors. This mirrors SP1 moving the
  relational `available` oracle into `RelGraphData`.

### Incremental-analysis hooks on the core

The scalar engine maintains `ClassAnalysis` incrementally (always current,
including through congruence closure). The generic core does not — the relational
engine runs analyses as a periodic batch driver (`run_analysis`) and does not
want per-`add`/`union` analysis work. To preserve scalar's exact behavior without
burdening relational, the `Language` trait gains two hooks with **default no-op
bodies**:

```rust
pub trait Language {
    type Node; type Sym; type GraphData;
    fn children(node: &Self::Node) -> Vec<Id>;
    fn map_children(node: &Self::Node, f: impl Fn(Id) -> Id) -> Self::Node;
    fn symbol(node: &Self::Node) -> Self::Sym;

    /// Maintain GraphData-resident analysis when a NEW class is created for `node`
    /// (not fired on a hashcons hit). `get` resolves a child Id to its canonical
    /// root. Default: no-op.
    fn on_add(_data: &mut Self::GraphData, _id: Id, _node: &Self::Node, _get: &dyn Fn(Id) -> Id) {}

    /// Merge analysis when `loser`'s class folds into `winner`. Default: no-op.
    fn on_union(_data: &mut Self::GraphData, _winner: Id, _loser: Id) {}
}
```

Core wiring (the only changes to `core.rs` behavior):

- **`add`** — after creating a class for a genuinely new node (memo miss), call
  `L::on_add(&mut data, id, node, &|c| find(c))`.
- **`union(a, b)`** — after the winner/loser decision, call
  `L::on_union(&mut data, winner, loser)`.
- **`rebuild`** — its congruence merges already route through `union`, so
  `on_union` fires for them automatically (keeping every class's analysis entry
  populated through congruence closure). The core's `rebuild` is otherwise
  unchanged; it does **not** recompute analysis.

`RelLang` keeps the defaults (no behavior change; the relational batch
`Analysis<L>` driver is untouched). `ScalarLang` overrides them, delegating to
the existing `analysis::make`/`analysis::merge` logic verbatim.

**The two-phase analysis, faithfully.** Today's `ScalarEGraph` maintains analysis
two ways: incrementally (`make` on `add`, `merge` on `union`) *and*, at the end of
every `rebuild`, by **clearing and recomputing the whole analysis as a monotone
least-fixpoint** over the post-congruence class layout (`egraph.rs:248–295`). The
fixpoint pass is what makes constant-folding's self-referential classes sound; the
incremental pass keeps analysis current *between* rebuilds (during rule
application, where rules read the analysis of freshly added classes).

The migration preserves both, split by where each belongs:

- The **incremental** pass becomes the `on_add` / `on_union` hooks above.
- The **fixpoint recompute** becomes a scalar-side free function
  `recompute_analysis(eg: &mut EGraph<ScalarLang>)` that clears `data.analysis`,
  seeds every class to the merge identity, and iterates `make`/`merge` to a
  fixpoint over the core's `pub(crate)` class layout. It is **not** a core hook
  (the core's `rebuild` stays analysis-agnostic for relational); instead the
  scalar `saturate` driver calls it immediately after each `eg.rebuild()` — the
  same program point the recompute occupies inside `ScalarEGraph::rebuild` today.
  This is a near-verbatim lift of `egraph.rs:248–295`.

**Behavior-neutrality contract.** The migration is sound iff (a) the core fires
the hooks at the same logical points and in the same winner/loser direction as
today's `ScalarEGraph::{add, union}`, and (b) `recompute_analysis` runs at the
same point (right after congruence closure) as today's in-`rebuild` recompute. The
88 existing scalar tests probe `could_error`/`literal` after unions and round-trip
`raise(lower(e)) == e`, so they are the precise gate on this contract.

**Behavior-neutrality contract.** The migration is sound iff the core fires the
hooks at the same logical points, and in the same winner/loser direction, as
today's `ScalarEGraph::{add, union, rebuild}`. The 88 existing scalar tests probe
`could_error`/`literal` after unions and round-trip `raise(lower(e)) == e`, so
they are the precise gate on this contract.

### Saturate driver

Today's `ScalarEGraph::saturate` becomes a free function over the generic graph;
its loop body is lifted verbatim:

```rust
// src/transform/src/eqsat/scalar/egraph.rs
pub fn saturate(eg: &mut EGraph<ScalarLang>) -> usize
```

Up to `MAX_ITERS = 100`; each round: `eg.rebuild()` then
`recompute_analysis(eg)`, stop if e-node count exceeds `MAX_ENODES = 600`,
snapshot read-only `(class, node)` pairs bounded by `MATCH_LIMIT = 1_000`, apply
all 21 rules, break on fixpoint. The `rebuild()` + `recompute_analysis()` pair
reproduces exactly what `ScalarEGraph::rebuild` did as one method. Within the
apply phase, rules read current analysis through a `ScalarGraphData` accessor
exactly as they read `eg.analysis(id)` today (entries kept populated by the
`on_add`/`on_union` hooks).

### Minimal-churn type alias (SP1 strategy)

`ScalarEGraph` is retained **as a type alias**, exactly as SP1 kept
`pub type EGraph = core::EGraph<RelLang>`:

```rust
// src/transform/src/eqsat/scalar/egraph.rs
pub type ScalarEGraph = crate::eqsat::core::EGraph<ScalarLang>;
pub use crate::eqsat::core::Id; // was `pub type Id = usize;` — now the core's Id
```

Because the alias keeps the name and the method names (`add`/`find`/`union`/
`rebuild`/`nodes`/`analysis`/`col_types`) all resolve, the rule type
(`fn(&mut ScalarEGraph, &SNode) -> Vec<Id>`) and **all of `rules.rs`,
`lower.rs`, `raise.rs`, `node.rs`, and `analysis.rs`'s non-test code are
unchanged**. The only call sites that change are in `scalar.rs`: the e-graph
construction (`with_col_types` is gone) and the two `.saturate()` method calls
(now the free `egraph::saturate(&mut eg)`).

The methods the alias must resolve come from two places:

- **Core (generic), as `pub(crate)`:** `nodes(id) -> Vec<L::Node>` (canonical-root
  node set), `class_ids() -> Vec<Id>`, `node_count() -> usize`, `data() ->
  &L::GraphData`, `data_mut() -> &mut L::GraphData`. (`add`/`find`/`union`/
  `rebuild` already exist and are `pub`.)
- **Scalar inherent impl on `EGraph<ScalarLang>`** (legal: same crate as
  `EGraph`): `analysis(id) -> &ClassAnalysis` (reads
  `self.data().analysis[&self.find(id)]`) and `col_types() -> &[ReprColumnType]`
  (reads `self.data().col_types`).

### lower / raise

Unchanged. They reference `ScalarEGraph` (now the alias) and call
`egraph.add`/`find`/`nodes`, which all resolve. The tree-size cost (`node_cost`
= 1 + Σ child costs), the least-fixpoint `compute_costs`, and the memoized
`build`/`reconstruct` (including And/Or operand sorting) are untouched.

### Entry points

`scalar::canonicalize(&MirScalarExpr, &[ReprColumnType]) -> MirScalarExpr` and
`canonicalize_predicates(...)` keep their signatures. Internally `canonicalize`
constructs `EGraph::<ScalarLang>::default()`, seeds `col_types` via
`eg.data_mut().col_types = col_types.to_vec()` (replacing
`ScalarEGraph::with_col_types`), then `lower → egraph::saturate(&mut eg) →
raise` as today.

## What is deleted

`scalar/egraph.rs`'s `ScalarEGraph` **struct** and its substrate methods
(`new`/`with_col_types`/`find`/`new_class`/`canon_node`/`add`/`union`/`rebuild`/
`analysis`/`nodes`/`node_count`/`class_ids`/`saturate`, the `uf`/`classes`/
`hashcons`/`analysis`/`col_types` fields). The name `ScalarEGraph` survives as a
type alias (above). The file retains the scalar-specific free `saturate` driver,
the `recompute_analysis` free function (lifted from the recompute tail of the old
`rebuild`, `egraph.rs:248–295`), the budget constants, and the scalar inherent
`analysis`/`col_types` accessors. `col_types` and `analysis` now live in
`ScalarGraphData`.

## File structure

| File | Change |
|---|---|
| `eqsat/core.rs` | Add `on_add`/`on_union` default methods to `Language`; fire them from `add`/`union` (and thus `rebuild`). Add `pub(crate)` `nodes(id)`/`class_ids()`/`node_count()`/`data()`/`data_mut()` accessors on `EGraph<L>`. Relational unaffected (defaults). |
| `eqsat/scalar/lang.rs` *(new)* | `ScalarLang`, `ScalarSym`, `ScalarGraphData`, `impl Language for ScalarLang` (incl. `on_add`/`on_union` → `analysis::make`/`merge`). |
| `eqsat/scalar.rs` | Add `mod lang;`. `canonicalize` builds `EGraph::<ScalarLang>::default()` + seeds `col_types` + calls free `saturate`; `canonicalize_predicates` unchanged. Signatures unchanged. |
| `eqsat/scalar/egraph.rs` | Delete `ScalarEGraph` struct + substrate methods; add `pub type ScalarEGraph = EGraph<ScalarLang>` + `pub use core::Id`; keep `saturate`/`recompute_analysis` as free fns + budget consts; add scalar inherent `analysis`/`col_types` accessors. |
| `eqsat/scalar/analysis.rs` | `ClassAnalysis` + `make`/`merge` unchanged (now invoked via hooks). Tests unchanged (use the `ScalarEGraph` alias). |
| `eqsat/scalar/rules.rs` | Non-test code unchanged (uses the `ScalarEGraph` alias; `Rule` type and rule bodies untouched). Two test sites change `eg.saturate()` (method) → `egraph::saturate(&mut eg)` (free fn). |
| `eqsat/scalar/lower.rs`, `raise.rs`, `node.rs` | Unchanged (use the `ScalarEGraph` alias / re-exported `Id`). |

## Testing — behavior-neutral acceptance

1. **88 scalar unit tests pass unchanged in intent** (12 analysis + 66 rules + 10
   module). The `ScalarEGraph` alias and re-exported `Id` keep almost all test
   code identical; the only edits are mechanical `eg.saturate()` (method) →
   `egraph::saturate(&mut eg)` (free fn) at the handful of tests that drive
   saturation directly (two in `rules.rs`, the `assert_round_trip` helper in
   `scalar.rs`). No assertion changes. The two direct-`rebuild` analysis tests
   pass via the `on_union` hook; the rules tests route through
   `canonicalize`/`saturate`, which gets `recompute_analysis`. These are the
   precise gate on the hook migration.
2. **`bin/cargo-test -p mz-transform eqsat`** — full eqsat suite green
   (relational 226 + scalar), confirming the core hook additions did not perturb
   the relational engine.
3. **slt goldens unchanged** — run the transform-suite slt with the scalar-eqsat
   flag in its CI/test-on state, no `--rewrite`. The known `case_literal`
   list-mode artifact (passes standalone) is the only tolerated diff, as in SP1.
4. **No `REWRITE` / `cargo insta accept`** anywhere. A behavior-neutral refactor
   that needs a golden rewrite has, by definition, changed behavior.

## Risks

- **Hook timing drift.** If the core fires `on_add`/`on_union` at points that
  differ from `ScalarEGraph` (e.g. firing `on_add` on a hashcons hit, or
  reversing winner/loser), analysis values diverge. Mitigation: the migration
  lifts the call sites 1:1, and the 88 tests directly assert post-union analysis.
- **`symbol` coarseness.** `ScalarSym` is only used to satisfy the trait; if a
  future SP2b matcher relies on it, it must be revisited then. Out of scope here.
- **Accessor visibility creep.** The new `EGraph<L>` accessors are `pub(crate)`
  and exist for the scalar saturate driver/rules; keep them minimal.

## Open questions

None blocking. SP2b's DSL/codegen genericization and the saturation-loop
unification are explicitly deferred and will get their own design.
