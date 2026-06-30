# SP4a Multi-sort E-graph Fusion (context-free) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make scalar expressions first-class e-classes inside the relational e-graph — replace `ENode`'s opaque `EScalar` payloads with scalar-class `Id` references over one combined-`Language` e-graph — behavior-neutrally.

**Architecture:** One `core::EGraph<CombinedLang>` where `Node = CNode = Rel(ENode) | Scalar(SNode)`, a single shared `Id` space, and `GraphData = CombinedData` carrying both sides' state plus a write-once `escalar: HashMap<Id, EScalar>` cache. `ENode`'s scalar payload fields become `Id` references that the core canonicalizes (via `children`/`map_children`); relational rule/analysis/cost/extraction code reads scalar facts by `Id` through the cache. Lower interns scalars (the same `reduce`-canonical exprs as today); raise reads the cache. Scalar classes are inert (no scalar rules, no unions, no colors), so each holds exactly one expr and the round-trip is the identity.

**Tech Stack:** Rust; `mz-transform` crate; the existing `core::EGraph<L>` / `Language` / `Analysis<L>` substrate (SP1); the scalar `SNode` / scalar `lower` / scalar `raise` (SP2a); `#[mz_ore::test]`; nextest via `bin/cargo-test`; `bin/sqllogictest --optimized`.

## Global Constraints

- **Behavior-neutral.** With `enable_eqsat_optimizer` at its default (on), all plan goldens and slt output must be **byte-identical** to pre-SP4a. No `REWRITE`, no `cargo insta accept` — *unchanged* is the assertion. (The one tolerated artifact is the pre-existing `case_literal` list-mode slt case carried since SP1/SP2a, which must still pass standalone.)
- **`EScalar` survives.** It remains the `Rel` tree payload (`ir.rs` unchanged) and becomes the cached fact value. Only `ENode`'s scalar fields flip to `Id`. `Rel`-based analysis helpers (`rec_analyze`, `rel_keys`, `rel_non_negative`, `rel_monotonic`, `letrec_local_facts`) stay on `EScalar` and are not touched by the flip.
- **Scalars inert.** No scalar rewrite rules, no scalar unions, no colors in SP4a. Each scalar class therefore holds exactly one interned expr.
- **Keying invariant (the critical correctness rule).** Every extraction / cost / CSE comparison or hash that today keys on `EScalar` content MUST resolve `Id`→`EScalar` through the cache, so keys are byte-identical to today's. Tie-break order and CSE grouping must be provably unchanged.
- **No new feature flag.** SP4a runs under the existing `enable_eqsat_optimizer`.
- **Repo conventions.** No `mod.rs` files (use `analysis.rs` beside `analysis/`, mirroring `colored.rs`/`colored/`). Third-party versions only via `[workspace.dependencies]`. `clippy -p mz-transform --all-targets -- -D warnings` is CI and must stay clean. Tests use `#[mz_ore::test]`, never bare `#[test]`. Do not edit anything under `doc/developer/generated/`.
- **Test commands.** Unit/datadriven: `bin/cargo-test -p mz-transform <filter>` (nextest; sets the metadata-store env). slt: `bin/sqllogictest --optimized -- <paths>`. Build the binary once; allow ≥10 min for first build.
- **Spec.** `doc/developer/design/20260628_eqsat_multisort_fusion_sp4a.md` is the source of truth; this plan implements it.

## File Structure

| File | Responsibility | SP4a change |
|---|---|---|
| `src/transform/src/eqsat/egraph.rs` | `ENode`, `Sym`, `RelGraphData`, `RelLang`, `EGraph` alias, `add_rel` (relational lower), the payload reducer helpers, extraction `Demand`. | Define `CNode`/`CSym`/`CombinedLang`/`CombinedData`; flip `ENode` scalar fields to `Id`; add `scalar_children`/`relational_children`; repoint the `EGraph` alias; rewrite `add_rel` to intern scalars + populate the cache; add the `escalar` accessor + scalar-interning helper. |
| `src/transform/src/eqsat/scalar/lower.rs` | Intern a `MirScalarExpr` into a scalar e-graph. | Generalize the intern target so it can write into `EGraph<CombinedLang>`. |
| `src/transform/src/eqsat/scalar/raise.rs` | Reconstruct a `MirScalarExpr` from a scalar class. | Generalize the read target so it can read scalar classes from `EGraph<CombinedLang>` (used SP4b-forward; in SP4a raise reads the cache, but the round-trip corpus exercises the generalized scalar raise to lock it in). |
| `src/transform/src/eqsat/matcher.rs` | `Binding` enum + scalar helpers (`cols`, `is_col`, `permute_cols`, column-bound filters, `into_*`). | `Binding` scalar variants hold `Id`/`Vec<Id>`; helpers resolve via the cache; `permute_cols` interns the permuted expr. |
| `src/transform/src/eqsat/analysis.rs` (+ new `analysis/`) | The `Analysis` impls (`NonNeg`, `Monotonic`, `Keys`, `Equivalences`, `ConstantColumns`) and `Rel`-based recursion helpers. | Re-type `make` impls to `CombinedLang`, scalar nodes → `bottom()`, read scalar facts via `Ctx`; split into per-analysis modules (Task 2). |
| `src/transform/src/eqsat/cost.rs` | Relational cost model. | Read scalar content via the cache; do not count scalar classes as relational nodes. |
| `src/transform/src/eqsat/extract.rs` | Relational extraction / raise to `Rel`. | Resolve scalar `Id` children via the cache; apply the keying invariant to tie-breaks. |
| `src/transform/src/eqsat/cse.rs` | Extraction-time CSE. | Apply the keying invariant to structural keys/hashes. |
| `src/transform/src/eqsat/engine.rs`, `transform.rs` | Saturation driver, `Transform` entry points. | Re-type to `EGraph<CombinedLang>`; match via the relational view. |

---

## Task 1: The fusion flip — combined language, `ENode` `EScalar`→`Id`, lower/raise/cache, consumers re-typed

This is the foundational, compilation-coupled task: the `ENode` field-type change breaks every consumer, so the combined language, the lower/raise cache bridge, and every ENode-consumer update land together. Its deliverable is a tree that **compiles, round-trips, and passes the existing engine unit tests** — NOT yet the full goldens gate (that is Task 3, after the keying safeguard is verified).

**Files:**
- Modify: `src/transform/src/eqsat/egraph.rs` (ENode, language, `add_rel`, accessors, cache)
- Modify: `src/transform/src/eqsat/scalar/lower.rs`, `scalar/raise.rs` (generalize intern/read target)
- Modify: `src/transform/src/eqsat/matcher.rs`, `analysis.rs`, `cost.rs`, `extract.rs`, `cse.rs`, `engine.rs`, `transform.rs` (consumers)
- Test: inline `#[cfg(test)]` modules in `egraph.rs` (cache + dispatch + round-trip)

**Interfaces:**
- Produces:
  - `pub enum CNode { Rel(ENode), Scalar(SNode) }`
  - `pub enum CSym { Rel(Sym), Scalar(ScalarSym) }`
  - `pub struct CombinedLang;` with `impl core::Language for CombinedLang { type Node = CNode; type Sym = CSym; type GraphData = CombinedData; }`
  - `pub struct CombinedData { pub rel: RelGraphData, pub scalar: ScalarGraphData, escalar: HashMap<Id, EScalar> }` with `pub fn escalar(&self, id: Id) -> &EScalar` and `pub(crate) fn set_escalar(&mut self, id: Id, e: EScalar)`
  - `pub type EGraph = core::EGraph<CombinedLang>;` (repointed alias — call sites keep using `EGraph`)
  - `impl ENode { pub fn relational_children(&self) -> Vec<Id>; pub fn scalar_children(&self) -> Vec<Id>; }`
  - `EGraph::intern_scalar(&mut self, expr: &MirScalarExpr, lit: Option<bool>) -> Id` (interns + registers the cache fact, returns the scalar class)
- Consumes: SP1 `core::{EGraph, Language, Id, Index}`; SP2a `ScalarLang`, `ScalarGraphData`, `SNode`, `scalar::lower`, `scalar::raise`, `ScalarSym`; `EScalar` from `ir.rs` (unchanged).

### Step group A — combined language scaffolding

- [ ] **A1: Add `relational_children`/`scalar_children` to `ENode` and flip the scalar field types.**

In `egraph.rs`, change every `ENode` scalar field from `EScalar` to `Id` per the spec §3.1 table:
```rust
// before: Map { input: Id, scalars: Vec<EScalar> }
// after:
Map { input: Id, scalars: Vec<Id> },
Filter { input: Id, predicates: Vec<Id> },
FlatMap { input: Id, func: TableFunc, exprs: Vec<Id> },
Reduce { input: Id, group_key: Vec<Id>, aggregates: Vec<AggregateExpr>, monotonic: bool, expected_group_size: Option<u64> },
ArrangeBy { input: Id, key: Vec<Id> },
ArrangeByMany { input: Id, keys: Vec<Vec<Id>> },
IndexedFilter { input: Id, predicates: Vec<Id>, committed: Box<MirRelationExpr> },
Join { inputs: Vec<Id>, equivalences: Vec<Vec<Id>> },
WcoJoin { inputs: Vec<Id>, equivalences: Vec<Vec<Id>> },
```
Rename the existing `children()` to `relational_children()` (relational inputs only — body unchanged). Add:
```rust
impl ENode {
    /// Scalar e-class references this node carries, in a fixed order:
    /// Map.scalars; Filter/IndexedFilter.predicates; FlatMap.exprs;
    /// Reduce.group_key; ArrangeBy.key; ArrangeByMany.keys (flattened);
    /// Join/WcoJoin.equivalences (flattened). Empty for nodes with none.
    pub fn scalar_children(&self) -> Vec<Id> {
        match self {
            ENode::Map { scalars, .. } => scalars.clone(),
            ENode::Filter { predicates, .. }
            | ENode::IndexedFilter { predicates, .. } => predicates.clone(),
            ENode::FlatMap { exprs, .. } => exprs.clone(),
            ENode::Reduce { group_key, .. } => group_key.clone(),
            ENode::ArrangeBy { key, .. } => key.clone(),
            ENode::ArrangeByMany { keys, .. } => keys.iter().flatten().copied().collect(),
            ENode::Join { equivalences, .. }
            | ENode::WcoJoin { equivalences, .. } => equivalences.iter().flatten().copied().collect(),
            _ => Vec::new(),
        }
    }
}
```
Extend `ENode::map_children` (private) to remap scalar `Id`s as well as relational inputs (every scalar field's `Id`s mapped through `f`, preserving order and grouping).

- [ ] **A2: Define `CNode`, `CSym`, `CombinedData`, `CombinedLang`.**

In `egraph.rs` (top-level, near `RelLang`):
```rust
use crate::eqsat::scalar::lang::{ScalarGraphData, ScalarSym};
use crate::eqsat::scalar::node::SNode;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CNode { Rel(ENode), Scalar(SNode) }

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum CSym { Rel(Sym), Scalar(ScalarSym) }

#[derive(Default)]
pub struct CombinedData {
    pub rel: RelGraphData,
    pub scalar: ScalarGraphData,
    escalar: HashMap<Id, EScalar>,
}
impl CombinedData {
    /// The cached EScalar fact for a scalar class. Panics if `id` is not a
    /// registered scalar class (a relational-code bug).
    pub fn escalar(&self, id: Id) -> &EScalar {
        self.escalar.get(&id).expect("scalar class has a cached EScalar fact")
    }
    pub(crate) fn set_escalar(&mut self, id: Id, e: EScalar) { self.escalar.insert(id, e); }
}

pub struct CombinedLang;
impl Language for CombinedLang {
    type Node = CNode;
    type Sym = CSym;
    type GraphData = CombinedData;

    fn children(node: &CNode) -> Vec<Id> {
        match node {
            CNode::Rel(e) => {
                let mut c = e.relational_children();
                c.extend(e.scalar_children());
                c
            }
            CNode::Scalar(s) => s.children(),
        }
    }
    fn map_children(node: &CNode, f: impl Fn(Id) -> Id) -> CNode {
        match node {
            CNode::Rel(e) => CNode::Rel(e.map_children(&f)),
            CNode::Scalar(s) => CNode::Scalar(s.map_children(f)),
        }
    }
    fn symbol(node: &CNode) -> CSym {
        match node {
            CNode::Rel(e) => CSym::Rel(e.sym()),
            CNode::Scalar(s) => CSym::Scalar(ScalarLang::symbol(s)),
        }
    }
    fn on_add(data: &mut CombinedData, id: Id, node: &CNode, get: &dyn Fn(Id) -> Id) {
        match node {
            CNode::Rel(_) => RelLang::on_add(&mut data.rel, id, /* see note */ unreachable_rel(), get),
            CNode::Scalar(s) => ScalarLang::on_add(&mut data.scalar, id, s, get),
        }
    }
    fn on_union(data: &mut CombinedData, winner: Id, loser: Id) {
        // Scalars never union in SP4a; route to the scalar analysis defensively,
        // and to the relational side (currently a no-op) for relational unions.
        ScalarLang::on_union(&mut data.scalar, winner, loser);
    }
}
```
> Note for the implementer: `RelLang::on_add` is a no-op default (relational analysis is batch-driven, not hook-driven), so the `Rel` arm of `on_add` should simply do nothing rather than fabricate an `ENode`. Write the `Rel` arm as `CNode::Rel(_) => {}`. The `unreachable_rel()` placeholder above is illustrative only — do **not** introduce it. The scalar `on_add`/`on_union` keep maintaining `ScalarGraphData.analysis` exactly as SP2a does; cache population happens in `intern_scalar` (A3), not in the hook, because the hook lacks the `EScalar` `lit` fact.

`map_children` for `ENode` must accept `impl Fn(Id) -> Id` by reference (`e.map_children(&f)`); confirm the existing signature is `fn map_children(&self, f: impl Fn(Id) -> Id)` and that passing `&f` satisfies it (a `&F` where `F: Fn` is itself `Fn`).

- [ ] **A3: Add `intern_scalar` and repoint the `EGraph` alias.**

Replace `pub type EGraph = core::EGraph<RelLang>;` with `pub type EGraph = core::EGraph<CombinedLang>;` and `pub(crate) type Index = core::Index<CombinedLang>;`. Add:
```rust
impl EGraph {
    /// Intern a scalar expression (the same reduced form used today) as a
    /// scalar e-class and register its EScalar fact in the cache. Returns the
    /// scalar class id.
    pub fn intern_scalar(&mut self, escalar: &EScalar) -> Id {
        let id = crate::eqsat::scalar::lower::lower_into(self, &escalar.expr);
        self.data_mut().set_escalar(id, escalar.clone());
        id
    }
}
```
(`lower_into` is added in B1. `add` on the combined graph hash-conses, so identical scalars share a class — registering the cache for an already-present class is idempotent because the `EScalar` is identical.)

- [ ] **A4: Run unit tests for the language scaffolding.**

Add to a `#[cfg(test)] mod combined_tests` in `egraph.rs`:
```rust
#[mz_ore::test]
fn scalar_children_and_map_children_round_trip() {
    // A Filter ENode with two scalar Id children maps both through f.
    let n = ENode::Filter { input: 0, predicates: vec![5, 6] };
    assert_eq!(n.relational_children(), vec![0]);
    assert_eq!(n.scalar_children(), vec![5, 6]);
    let mapped = CombinedLang::map_children(&CNode::Rel(n.clone()), |x| x + 10);
    match mapped { CNode::Rel(ENode::Filter { input, predicates }) => {
        assert_eq!(input, 10); assert_eq!(predicates, vec![15, 16]);
    } _ => panic!("shape preserved") }
    assert_eq!(CombinedLang::children(&CNode::Rel(n)), vec![0, 5, 6]);
}
```
Run: `bin/cargo-test -p mz-transform combined_tests::scalar_children_and_map_children_round_trip`
Expected: PASS.

### Step group B — lower / raise / cache bridge

- [ ] **B1: Generalize scalar `lower` to a combined-graph intern.**

In `scalar/lower.rs`, add a `lower_into` that interns `CNode::Scalar` into `EGraph<CombinedLang>`, factoring the recursion to share with the existing `lower` (keep `lower` working on `ScalarEGraph` for the standalone scalar canonicalizer used elsewhere). The decomposition match is identical; only the `egraph.add(node)` target differs:
```rust
use crate::eqsat::egraph::{CNode, EGraph as CombinedEGraph};

/// Intern `expr` into the combined relational+scalar e-graph, returning the
/// scalar e-class of its root. Mirrors `lower`, wrapping each `SNode` in
/// `CNode::Scalar`.
pub fn lower_into(egraph: &mut CombinedEGraph, expr: &MirScalarExpr) -> Id {
    let node = /* identical SNode construction as `lower`, recursing via lower_into */;
    egraph.add(CNode::Scalar(node))
}
```
> Keep the SNode construction DRY: extract a helper `fn snode_of(expr, mut intern_child: impl FnMut(&MirScalarExpr) -> Id) -> SNode` used by both `lower` and `lower_into`, or duplicate the small match if the borrow checker makes the closure awkward (the match is 7 arms). Prefer the helper.

- [ ] **B2: Rewrite `add_rel` (the relational lower) to intern scalars.**

In `egraph.rs`, `add_rel` builds an `ENode` from a `Rel`. At every site that previously copied an `EScalar` (or `Vec<EScalar>` / `Vec<Vec<EScalar>>`) into the `ENode`, intern instead and store the `Id`:
```rust
// before: Rel::Filter { input, predicates } => ENode::Filter {
//     input: self.add_rel(input), predicates: predicates.clone() }
// after:
Rel::Filter { input, predicates } => {
    let input = self.add_rel(input);
    let predicates = predicates.iter().map(|s| self.intern_scalar(s)).collect();
    ENode::Filter { input, predicates }
}
```
Apply the same pattern to `Map.scalars`, `FlatMap.exprs`, `Reduce.group_key`, `ArrangeBy.key`, `ArrangeByMany.keys` (intern per inner vec), `IndexedFilter.predicates`, `Join.equivalences` / `WcoJoin.equivalences` (intern per inner vec). Then `self.add(CNode::Rel(node))` (every `ENode` constructed by `add_rel` is wrapped in `CNode::Rel`). The `IndexedFilterSeed.predicates: Vec<EScalar>` seed path likewise interns to `Vec<Id>` when seeding the node.
> Borrow note: `intern_scalar` takes `&mut self`, so collect child relational ids and intern scalars before constructing the `ENode`, not inside the struct literal while another `&self` borrow is live.

- [ ] **B3: Rewrite relational raise (`extract.rs`) to read scalars from the cache.**

Where extraction reconstructs a `Rel` from an `ENode`, each scalar `Id` field becomes the cached `EScalar`:
```rust
// before: ENode::Filter { input, predicates } => Rel::Filter {
//     input: <recurse>, predicates: predicates.clone() }
// after (predicates: Vec<Id>):
ENode::Filter { input, predicates } => Rel::Filter {
    input: Box::new(<recurse on input>),
    predicates: predicates.iter().map(|id| graph.data().escalar(*id).clone()).collect(),
}
```
Apply to every `ENode` scalar field. (Relational children still recurse through extraction; scalar children resolve via the cache — no scalar extraction recursion in SP4a.)

- [ ] **B4: Round-trip test through the combined graph.**

Add to `egraph.rs` tests (or `extract.rs`):
```rust
#[mz_ore::test]
fn add_rel_then_extract_is_identity_for_scalar_bearing_nodes() {
    // Build a Rel with a Filter[#0 = 1] over a Get, lower into the combined
    // graph, rebuild, extract, and assert structural identity.
    let rel = /* Filter { predicates: [EScalar::plain(#0 = lit 1)], input: Get } */;
    let mut eg = EGraph::new();
    let root = eg.add_rel(&rel);
    eg.rebuild();
    let out = crate::eqsat::extract::extract(&eg, root); // existing extract entry
    assert_eq!(out, rel, "scalar payloads round-trip through Id + cache");
}
```
Run: `bin/cargo-test -p mz-transform add_rel_then_extract_is_identity_for_scalar_bearing_nodes`
Expected: PASS.

### Step group C — consumers: matcher, analysis `make`, cost, cse, engine

- [ ] **C1: `matcher.rs` — `Binding` holds `Id`s; helpers resolve via the cache.**

Change the `Binding` scalar variants to hold ids:
```rust
Predicates(Vec<Id>), Scalars(Vec<Id>), Equivalences(Vec<Vec<Id>>),
GroupKey(Vec<Id>), FlatMapExprs(Vec<Id>),
```
Rewrite the helpers to take the graph/data and resolve:
- `cols`/`is_col`/the column-bound filters (`cols < b`, `cols >= b`) call `data.escalar(id).cols()` / `.is_col()`.
- `permute_cols`: `let e = data.escalar(id); let permuted = e.permute_cols(f)?; let new_id = graph.intern_scalar(&permuted); ...` — returns new `Id`s.
- `into_predicates`/`into_scalars`/`into_equivalences`/`into_group_key`/`into_flatmap_exprs` return `Vec<Id>` / `Vec<Vec<Id>>` (interning where they currently build `EScalar::plain(column(c))` — intern that EScalar and return its id).
Thread the combined graph (or `&mut EGraph`) into the binding operations that intern; pure-read helpers take `&CombinedData`. The DSL-generated rule code calls these helpers, so keep the method names stable.

**Decision (2026-06-28): Option A — matcher in the `Id` domain now** (the SP4b-aligned path; the human chose it over a smaller Option B that kept `Binding` on `EScalar` and changed only the two codegen boundaries). This expands C1 with two sub-steps the original draft understated:

- [ ] **C1a: Re-type the column-mutating helpers and scalar-reading `cond_*` to thread `eg`/`data`.** `shift_payload`/`remap_payload`/`swap_equivs` take `&mut EGraph` (resolve `Id`→`EScalar` via the cache, permute, `intern_scalar` the result). Every scalar-reading condition helper (`cond_all_columns`, `any_false`, `no_false`, `no_error`, `all_true`, `produces_key`, `has_inner_equiv`, `uses_only_input`, `cols_in_range`, and any other that reads `EScalar`) takes `&mut EGraph`/`&CombinedData` and resolves via the cache. The column-manipulation *logic* is byte-identical to today.
- [ ] **C1b: `let`-hoist nested payloads in codegen.** Restructure `build*/codegen.rs` `pexpr_expr` (and `cond_expr` where it threads scalars) so nested payload-helper calls become sequential `let` bindings: `let a = shift_payload(eg, …); let b = remap_payload(eg, e, &a); …`. This is the borrow fix for the now-`&mut eg` helpers; nested `&mut eg` does not borrow-check (two-phase borrows do not save the nested free-fn form). If `pexpr_expr` must change shape (emit statements + a final expr rather than one expr), that is in-scope.

- [ ] **C2: `analysis.rs` — re-type `make` impls to `CombinedLang`, scalar facts via `Ctx`.**

For each `impl Analysis<RelLang> for {NonNeg, Monotonic, Keys, Equivalences, ConstantColumns}`:
- change to `impl Analysis<CombinedLang>`;
- `make(&self, node: &CNode, get, ctx)` matches `CNode::Rel(e) => <today's body using e>`, `CNode::Scalar(_) => self.bottom()`;
- where the body reads `EScalar` from a scalar field (e.g. `Keys` reading `group_key` columns, `Equivalences` reading join equivalences, `ConstantColumns` building `ConstCols = BTreeMap<usize, EScalar>`), resolve `Id`→`EScalar` through a scalar-fact accessor passed in `Ctx<'a>`. Extend each analysis's `Ctx` to carry `&CombinedData` (or a `&dyn Fn(Id)->&EScalar` view) alongside its existing arity provider.
- `ConstantColumns`' `ConstCols` domain keeps `EScalar` values (resolve the constant's `Id`→`EScalar` via `Ctx` in `make`, store the `EScalar`).
The `Rel`-based helpers (`rec_analyze`, `rel_keys`, `rel_non_negative`, `rel_monotonic`, `letrec_local_facts`) operate on `Rel` and stay on `EScalar` — do not change them.
> The `Analysis::Ctx<'a>: Copy` GAT already exists for per-run context ("relational: an arity provider; scalar later: column types"). Adding the cache view to each `Ctx` is the mechanism the spec calls for.

- [ ] **C3: `cost.rs` — read scalar content via the cache; don't count scalar classes.**

At every `EScalar` read in the cost model, resolve via `data.escalar(id)`. Ensure cost iterates the relational extraction tree (it already does), not raw combined classes, so scalar e-classes are not counted as relational nodes. No cost number changes.

- [ ] **C4: `cse.rs` — keying invariant.**

Where CSE hashes/compares relational subtrees that include scalar payloads, resolve `Id`→`EScalar` so the structural key equals today's `EScalar`-content key. (See Task 3 for the dedicated safeguard test; this step is the implementation.)

- [ ] **C5: `engine.rs` / `transform.rs` — relational view for matching.**

The saturation driver iterates classes/nodes to feed the `Sym`-bucketed matcher. With `Node = CNode`, provide a relational view: iterate classes, for each `CNode::Rel(e)` yield `(id, e, Sym)`; skip `CNode::Scalar(_)`. The matcher index becomes keyed by `CSym::Rel(Sym)`; the generated rule code matches relational symbols only. No scalar rules are registered, so scalar classes are inert during saturation. Keep the public `Transform` impls (`EqSatTransform`, `PhysicalEqSatTransform`) signatures unchanged.

- [ ] **C6: Build, clippy, and run the existing engine unit + datadriven tests.**

Run:
```
bin/cargo-test -p mz-transform eqsat
cargo clippy -p mz-transform --all-targets -- -D warnings
```
Expected: the workspace compiles; all existing `eqsat::*` unit + datadriven tests pass (they assert engine behavior, which is unchanged); clippy clean. If any datadriven golden under `mz-transform` drifts, STOP — that signals a keying or interning regression to fix here before proceeding (do not `REWRITE`).

- [ ] **C7: Commit Task 1.**
```bash
git add src/transform/src/eqsat
git commit -m "eqsat SP4a: fuse scalars into the relational e-graph as Id-referenced e-classes (flip)"
```

---

## Task 2: Split `analysis.rs` into per-analysis modules

Pure code-movement, separable and independently reviewable now that the analyses are re-typed.

**Files:**
- Create: `src/transform/src/eqsat/analysis/nonneg.rs`, `monotonic.rs`, `keys.rs`, `equivalences.rs`, `constant_columns.rs`, `recursion.rs` (the `Rel`-based `rec_analyze`/`LocalFacts`/`letrec_local_facts`/`rel_*` helpers)
- Modify: `src/transform/src/eqsat/analysis.rs` (becomes a thin root: `mod` declarations + `pub use` re-exports preserving the existing public surface)

**Interfaces:**
- Consumes: the re-typed analyses from Task 1.
- Produces: identical public paths (`crate::eqsat::analysis::{NonNeg, Monotonic, Keys, KeySet, Key, Equivalences, ConstantColumns, ConstCols, Direction, rec_analyze, LocalFacts, letrec_local_facts, is_superkey, rel_keys, rel_non_negative, rel_monotonic}`) via re-export, so no other file changes.

- [ ] **Step 1: Move each analysis to its module verbatim.**
Move `NonNeg` → `analysis/nonneg.rs`, `Monotonic` → `monotonic.rs`, `Keys`/`Key`/`KeySet`/`join_keys`/`is_superkey`/`rel_keys` → `keys.rs`, `Equivalences` → `equivalences.rs`, `ConstantColumns`/`ConstCols` → `constant_columns.rs`, and `Direction`/`rec_analyze`/`RecAnalysis`/`LocalFacts`/`letrec_local_facts`/`rel_non_negative`/`rel_monotonic` → `recursion.rs`. Carry each item's doc comments and tests with it. Add `use` imports each module needs at the top.

- [ ] **Step 2: Make `analysis.rs` a thin root.**
```rust
mod constant_columns;
mod equivalences;
mod keys;
mod monotonic;
mod nonneg;
mod recursion;

pub use constant_columns::{ConstCols, ConstantColumns};
pub use equivalences::Equivalences;
pub use keys::{is_superkey, rel_keys, Key, KeySet, Keys};
pub use monotonic::Monotonic;
pub use nonneg::NonNeg;
pub use recursion::{letrec_local_facts, rec_analyze, rel_monotonic, rel_non_negative, Direction, LocalFacts};
```
(Match the exact set of currently-public items; re-export everything other files import. Per repo convention there is NO `analysis/mod.rs` — the root stays `analysis.rs` beside the `analysis/` dir.)

- [ ] **Step 3: Build, clippy, test.**
Run:
```
bin/cargo-test -p mz-transform eqsat::analysis
cargo clippy -p mz-transform --all-targets -- -D warnings
```
Expected: compiles; the analysis tests (moved with their impls) pass; clippy clean. No behavior change — this is movement only.

- [ ] **Step 4: Commit.**
```bash
git add src/transform/src/eqsat/analysis.rs src/transform/src/eqsat/analysis
git commit -m "eqsat SP4a: split analysis.rs into per-analysis modules"
```

---

## Task 3: Keying safeguard + behavior-neutral gate

Verify and lock in the keying invariant, then run the full behavior-neutral gate (the deliverable). Adds the round-trip corpus, the dedicated keying test, and the optional measurement harness.

**Files:**
- Test: `src/transform/src/eqsat/extract.rs` (or a new `#[cfg(test)]` module): round-trip corpus + keying safeguard
- Create (optional): a `#[ignore]`d measurement test (in `egraph.rs` or `extract.rs`)

**Interfaces:**
- Consumes: Task 1's `add_rel`/`extract`, `CombinedData::escalar`, the cse keying from C4.

- [ ] **Step 1: Round-trip corpus test.**
Write a test that builds one `Rel` per scalar-bearing `ENode` kind (Map, Filter, FlatMap, Reduce, Join, WcoJoin, ArrangeBy, ArrangeByMany, IndexedFilter), lowers each into a fresh `EGraph`, `rebuild()`s, extracts, and asserts structural equality with the input:
```rust
#[mz_ore::test]
fn round_trip_corpus_all_scalar_bearing_nodes() {
    for rel in scalar_bearing_corpus() { // helper returning Vec<Rel>
        let mut eg = EGraph::new();
        let root = eg.add_rel(&rel);
        eg.rebuild();
        assert_eq!(crate::eqsat::extract::extract(&eg, root), rel, "round-trip: {rel}");
    }
}
```
Run: `bin/cargo-test -p mz-transform round_trip_corpus_all_scalar_bearing_nodes` → PASS.

- [ ] **Step 2: Keying safeguard test.**
Construct a plan with two cost-equal alternatives whose extraction selection depends on scalar ordering (e.g. a class holding two `Map`s with the same cost but differently-ordered scalar payloads), union them, `rebuild()`, and assert extraction picks the SAME representative as the pre-fusion `EScalar`-`Ord` would (i.e. the tie-break resolves through the cache). Encode the expected pick explicitly:
```rust
#[mz_ore::test]
fn extraction_tiebreak_resolves_scalars_through_cache() {
    // two cost-equal forms in one class; the deterministic winner must be the
    // one whose resolved EScalars are Ord-smaller, identical to pre-fusion.
    /* build, union, rebuild, extract, assert the chosen scalars == expected */
}
```
Run it → PASS. If it fails, fix the comparator in `extract.rs`/`cse.rs` to resolve `Id`→`EScalar` (do not change the test's expectation).

- [ ] **Step 3: Optional measurement harness (non-gating).**
```rust
#[mz_ore::test]
#[ignore] // measurement; run with: bin/cargo-test -p mz-transform -- --run-ignored ignored-only scalar_sharing
fn scalar_sharing() {
    // lower a representative plan corpus; report distinct scalar classes vs
    // total scalar references (the CSE ratio SP4b will exploit).
}
```

- [ ] **Step 4: Full behavior-neutral gate.**
Run the relational transform datadriven/golden tests and a representative slt sweep with eqsat at its default:
```
bin/cargo-test -p mz-transform
bin/sqllogictest --optimized -- test/sqllogictest/transform.slt test/sqllogictest/joins.slt test/sqllogictest/select.slt
```
Expected: all pass, output byte-identical to pre-SP4a (the only tolerated artifact is the known `case_literal` list-mode case, which must pass standalone). No `REWRITE`/`insta accept`. If a golden drifts, the cause is a keying/interning regression in Task 1 — fix there, do not rewrite goldens.

- [ ] **Step 5: Full slt confirmation.**
Run the broader slt suite to confirm neutrality:
```
bin/sqllogictest --optimized -- $(git -C . ls-files 'test/sqllogictest/*.slt' | head -n 60)
```
Expected: unchanged pass/fail profile vs. a pre-SP4a baseline run. Record the counts in the SDD ledger.

- [ ] **Step 6: Commit.**
```bash
git add src/transform/src/eqsat
git commit -m "eqsat SP4a: round-trip corpus, keying safeguard, scalar-sharing measurement; behavior-neutral gate green"
```

---

## Self-Review

**Spec coverage:**
- §2 architecture (one `EGraph<CombinedLang>`, `CNode`/`CSym`, no core changes) → Task 1 A2.
- §3.1 `ENode` field flip + `scalar_children`/`relational_children` → Task 1 A1.
- §3.2 `CombinedLang::children`/`map_children`/`symbol` covering both sorts → Task 1 A1/A2.
- §3.3 `CombinedData` + `escalar` cache + `on_add`/`on_union` dispatch → Task 1 A2/A3.
- §4 lower/raise/intern (reuse scalar lower; raise reads cache) → Task 1 B1–B3.
- §5.1 matcher port → Task 1 C1; §5.2 analyses re-typed via `Ctx` → C2; §5.3 analysis split → Task 2; §5.4 engine/transform relational view → C5.
- §6.1/6.2 cost/extraction selection → Task 1 B3/C3; §6.3 keying invariant → C4 + Task 3 Step 2.
- §7 conventions (no `mod.rs`, RelLang removal, no new flag) → Global Constraints + Task 1 A3/Task 2.
- §8 gate (round-trip corpus, parity, keying test, goldens/slt, measurement) → Task 3.
- §9 risks → mitigated across Task 1 (cache keeps logic stable) and Task 3 (keying test, gate).

**Placeholder scan:** The `unreachable_rel()` token in A2 is explicitly called out as illustrative-only with the concrete instruction to write `CNode::Rel(_) => {}`; no other placeholders. Mass-edit steps give the transformation rule + a representative before/after + the exhaustive field/site list (grounded in the per-file `EScalar` grep counts), which is the executable form for an in-place refactor; the implementer reads the real files via the SDD brief.

**Type consistency:** `CNode`/`CSym`/`CombinedLang`/`CombinedData`/`escalar`/`intern_scalar`/`lower_into`/`relational_children`/`scalar_children` names are used identically across Tasks 1–3. `EGraph` alias repointed once (A3); all consumers keep the `EGraph` name. `Binding` scalar variants → `Id` consistently (C1). Analyses → `Analysis<CombinedLang>` consistently (C2), re-exported unchanged (Task 2).

**Known structural note (for the SDD pre-flight):** Task 1 is large and compilation-coupled — the `ENode` field flip breaks every consumer simultaneously, so it cannot be split into independently-compiling subtasks. The step groups (A scaffolding → B bridge → C consumers) are ordered for the implementer but land in one commit; Tasks 2 and 3 are the cleanly separable, independently-gateable follow-ons. If the Task 1 implementer reports BLOCKED on size, the controller should re-dispatch with a more capable model rather than artificially splitting (intermediate states do not compile).
