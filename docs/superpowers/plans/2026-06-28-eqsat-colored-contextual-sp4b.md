# SP4b Colored Contextual Equalities — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the eqsat Phase-2a payload rewriting and re-home the eqsat `Equivalences` analysis onto a colored e-graph mechanism: per-scope contextual scalar equalities, with the rewritten spellings interned pre-freeze and selected by color-aware extraction.

**Architecture:** Run context-free saturation, then on the still-mutable graph derive each relational class's output-equivalence set (the existing lattice), intern each Filter/Map payload's `reduce_escalar` spelling as a base term, and record the equality `original ≅ reduced` in that scope's color. Freeze, build a colored union-find layer, and make extraction resolve Filter/Map payloads under `color(input)` by reading the `escalar` of the cheapest colored-class member. A temporary `EquivMode` switch keeps the old path runnable for a differential, then the old path is deleted.

**Tech Stack:** Rust; the `mz-transform` crate; the SP4a combined e-graph (`EGraph<CombinedLang>`); the SP3b colored layer (`ColoredEGraph`, `ColoredUnionFind`); `EquivalenceClasses`/`ExpressionReducer` from `crate::analysis::equivalences`.

## Global Constraints

- Spec: `doc/developer/design/20260624_eqsat/20260628_eqsat_colored_contextual_sp4b.md`. Every task implicitly includes its requirements.
- One worktree/branch: `claude/mir-equality-optimizer-sodbej`, worktree `.claude/worktrees/mir-equality-optimizer`. Run all commands from the worktree root.
- **Test commands (mz-test skill):** unit tests `bin/cargo-test -p mz-transform <filter>`; a single slt file `bin/sqllogictest --optimized -- <path>`; regenerate slt `bin/sqllogictest --optimized -- --rewrite-results <path>`. Rust tests use `#[mz_ore::test]`. Clippy gate: `cargo clippy -p mz-transform --all-targets -- -D warnings`.
- **Behavior neutrality through Task 6:** Tasks 1–6 must leave optimizer output unchanged (`EquivMode::Phase2a` is the default), so the full slt suite and goldens stay green. The behavior change is isolated to Task 7.
- **Application scope:** colored equalities are applied **only** to Filter predicates and Map scalars (Phase-2a's reach). Do **not** rewrite Join/Reduce/ArrangeBy key payloads.
- **No permanent feature flag.** `EquivMode` is temporary dev scaffolding, removed in Task 8.
- **Reuse, don't reimplement:** term creation reuses `reduce_escalar` (with its `max_col` guard); derivation reuses the per-operator arms from the eqsat `Equivalences` analysis and `EquivalenceClasses`/`ExpressionReducer`; unsatisfiability reuses `EquivalenceClasses::unsatisfiable()`.
- **`doc/developer/generated/` is READ-ONLY** (do not touch).
- Locate symbols by **name (grep)**, not the line numbers in this plan — Task 1 moves code between files.

### Key existing symbols (verified, pre-split locations)

- `egraph.rs`: `pub fn intern_scalar(&mut self, escalar: &EScalar) -> Id` (627); `fn intern_scalars(&mut self, &[EScalar]) -> Vec<Id>` (636); `CombinedData::escalar(&self, id: Id) -> &EScalar` (443); `resolve_scalars(&self, ids: &[Id]) -> Vec<EScalar>` (1233); `resolve_equivalences(&self, &[Vec<Id>]) -> Vec<Vec<EScalar>>` (1238); `arrangements_of(&self, node: &ENode) -> Vec<(Id, Vec<usize>)>` (1171); `pub fn extract_with(&self, root, model, objective) -> Option<Rel>` (1806); `cond_unsatisfiable(&self, an: &Analyses, id: Id) -> bool` (1398); `pub(crate) struct Analyses` (1514) with field `eq: HashMap<Id, Option<EquivalenceClasses>>`; `pub fn saturate(&mut self, rules, max_iters, facts) -> usize` (1535) — Phase 2a block ~1674–1735; `fn reduce_escalar(escalar: &EScalar, reducer: &BTreeMap<MirScalarExpr, MirScalarExpr>, max_col: usize) -> (bool, EScalar)` (2114); `fn rewrite_escalars(&mut self, node: &ENode, reducer, filter_input_reducer)` (2169); `const MAX_ENODES: usize = 600` (60); `const MAX_EQUIVALENCES_ANALYSIS_ITERS: usize = 4` (91).
- `core.rs`: `find(&self, id) -> Id` (99); `nodes(&self, id) -> Vec<L::Node>` (208); `class_ids(&self) -> Vec<Id>` (217); `uf_len(&self) -> usize` (230); `lookup(&self, node: &L::Node) -> Option<Id>` (238); `run_analysis_bounded<A: Analysis<L>>(...)` (281).
- `analysis/equivalences.rs`: `pub struct Equivalences` (31); `impl Analysis<CombinedLang> for Equivalences` (35) with `type Domain = Option<EquivalenceClasses>`, `fn make(&self, node: &CNode, get: &dyn Fn(Id)->Domain, ctx) -> Domain` (43), `fn merge(&self, a, b) -> Domain` (239). The per-operator arms read scalar ids via `ctx.data.escalar(id).expr`.
- `colored.rs`: `ColoredEGraph<'b, L>::new(base: &'b EGraph<L>)`, `new_color(&mut self, parent: Option<ColorId>) -> ColorId`, `find(&mut self, c: ColorId, x: Id) -> Id`, `union(&mut self, c, x, y) -> bool`; `ColorId(pub(crate) usize)`. `colored/union_find.rs`: `find_local`, `union_local`, `remove`.
- `engine.rs`: `Optimizer::optimize(&self, plan: Rel) -> Outcome` (147); fragment saturate+extract arms in `optimize_node_with_alt` (190) and `optimize_node` (235), plus scope helpers `optimize_scope`/`optimize_around_scopes`. Extraction calls: `self.extractor.extract(&eg, root, &self.model, self.objective.as_ref())` and `eg.extract_with(root, &self.model, &TimeFirst)`.
- `dsl.rs`: `Cond::Unsatisfiable { rel: String }` (285). Codegen/grammar arms for `Unsatisfiable` live in `build/codegen.rs` and `build/grammar.rs` (grep `Unsatisfiable`). Rules: `rules/relational.rewrite` `rule empty_false_filter` (278, KEEP), `rule collapse_unsatisfiable` (287, DELETE).

---

### Task 1: Split `egraph.rs` into an `egraph/` module (pure movement)

**Files:**
- Create: `src/transform/src/eqsat/egraph/node.rs`, `combined.rs`, `build.rs`, `saturate.rs`
- Modify: `src/transform/src/eqsat/egraph.rs` (becomes the module root: `mod` decls + `pub use` re-exports + residual glue)

**Interfaces:**
- Consumes: nothing.
- Produces: identical public surface. Every path that previously named `crate::eqsat::egraph::X` still resolves (`X` re-exported from the root). No consumer file outside `egraph/` changes.

This is a **pure refactor**: no behavior change, no signature change. Split by responsibility:
- `node.rs` — `pub enum ENode`, `pub enum Sym`, `impl ENode` (incl. `relational_children`, `scalar_children`, `map_children`).
- `combined.rs` — `pub enum CNode`, `pub enum CSym`, `pub struct RelGraphData`, `pub struct CombinedData` (+ `escalar`/`set_escalar` accessors), `pub struct CombinedLang` + its `Language` impl, `pub type EGraph`.
- `build.rs` — the `impl EGraph` block(s) for construction/interning: `add_rel`, `intern_scalar`, `intern_scalars`, `resolve_scalars`, `resolve_equivalences`, `set_available`, `seed_indexed_filters`.
- `saturate.rs` — `pub(crate) struct Analyses`, `saturate`, the analysis-driver glue, `cond_unsatisfiable`, `reduce_escalar`, `reduce_escalar_list`, `rewrite_escalars`, and the Phase-2a loop. (Extraction `extract_with` may stay in the root or move to `build.rs`; keep it wherever minimizes churn.)
- `egraph.rs` root — `pub mod`/`mod` decls, `pub use self::node::*; pub use self::combined::*;` etc., the doc comment, constants (`MAX_ENODES`, `MAX_EQUIVALENCES_ANALYSIS_ITERS`), and the `#[cfg(test)] mod tests`.

- [ ] **Step 1: Establish the baseline.** Run `bin/cargo-test -p mz-transform` and record the pass count (expected: all pass). This is the invariant for the whole task.

- [ ] **Step 2: Create the module files and move declarations.** Move each group above into its file. Use `use super::*;` or explicit `use crate::eqsat::...` imports per file. Keep visibility modifiers exactly as they were (`pub`, `pub(crate)`, private). In the root, add `mod node; mod combined; mod build; mod saturate;` and re-export every previously-public item so external paths are unchanged.

- [ ] **Step 3: Compile.** Run `cargo check -p mz-transform`. Fix only import/visibility errors (no logic edits). Expected: clean.

- [ ] **Step 4: Run the full crate tests.** Run `bin/cargo-test -p mz-transform`. Expected: identical pass count to Step 1.

- [ ] **Step 5: Clippy.** Run `cargo clippy -p mz-transform --all-targets -- -D warnings`. Expected: clean.

- [ ] **Step 6: Commit.**
```bash
git add src/transform/src/eqsat/egraph.rs src/transform/src/eqsat/egraph/
git commit -m "eqsat SP4b: split egraph.rs into egraph/ modules (pure movement)"
```

**Fallback:** if a clean split proves to entangle `EGraph` impl blocks unsafely (e.g. private helpers shared across groups that resist a clean cut), split only `node.rs` + `combined.rs` (the data types) and leave the `impl EGraph` blocks in the root. Report the narrower split in the task report.

---

### Task 2: Colored-class member enumeration accessor

**Files:**
- Modify: `src/transform/src/eqsat/colored.rs`
- Test: in `colored.rs` (extend `mod color_tree_tests`)

**Interfaces:**
- Consumes: `ColoredEGraph` (`new`, `new_color`, `find`, `union`), `ColorId`.
- Produces: `pub(crate) fn colored_class_members(&mut self, c: ColorId, x: Id) -> Vec<Id>` on `impl<'b, L: Language> ColoredEGraph<'b, L>` — all **base** class ids whose layered `find(c, ·)` equals `find(c, x)`. Returned ids are canonical base ids (`base.find(y) == y`), sorted ascending for determinism.

- [ ] **Step 1: Write the failing test.** Add to `color_tree_tests` in `colored.rs`:
```rust
#[mz_ore::test]
fn colored_class_members_groups_base_ids() {
    let (eg, ids) = fixed_base();
    let [a, b, x, ..] = ids;
    let mut ceg = ColoredEGraph::new(&eg);
    let c = ceg.new_color(None);
    // Before any union, each base id is alone in its colored class.
    assert_eq!(ceg.colored_class_members(c, a), vec![a]);
    // After a≅b, both appear (sorted), and x is unaffected.
    ceg.union(c, a, b);
    let mut ab = vec![a, b];
    ab.sort();
    assert_eq!(ceg.colored_class_members(c, a), ab);
    assert_eq!(ceg.colored_class_members(c, b), ab);
    assert_eq!(ceg.colored_class_members(c, x), vec![x]);
}
```

- [ ] **Step 2: Run it to confirm it fails.** Run `bin/cargo-test -p mz-transform colored_class_members_groups_base_ids`. Expected: FAIL (method not found).

- [ ] **Step 3: Implement the accessor.** Add to the `impl<'b, L: Language> ColoredEGraph<'b, L>` block in `colored.rs`:
```rust
/// All base class ids in the same colored class as `x` under color `c`
/// (i.e. layered `find(c, ·)` equal to `find(c, x)`). Returns canonical
/// base ids, sorted ascending. Colored delta ids (≥ `base.uf_len()`) are not
/// returned — SP4b's candidates are always base terms.
pub(crate) fn colored_class_members(&mut self, c: ColorId, x: Id) -> Vec<Id> {
    let rep = self.find(c, x);
    let mut out: Vec<Id> = self
        .base
        .class_ids()
        .into_iter()
        .filter(|&y| self.base.find(y) == y)
        .filter(|&y| self.find(c, y) == rep)
        .collect();
    out.sort_unstable();
    out
}
```

- [ ] **Step 4: Run the test.** Run `bin/cargo-test -p mz-transform colored_class_members_groups_base_ids`. Expected: PASS.

- [ ] **Step 5: Clippy + commit.** Run `cargo clippy -p mz-transform --all-targets -- -D warnings`, then:
```bash
git add src/transform/src/eqsat/colored.rs
git commit -m "eqsat SP4b: ColoredEGraph::colored_class_members accessor"
```

---

### Task 3: `colored_derive` — per-class merged output-equivalence facts

**Files:**
- Create: `src/transform/src/eqsat/colored_derive.rs`
- Modify: `src/transform/src/eqsat.rs` (add `pub(crate) mod colored_derive;`)
- Test: in `colored_derive.rs`

**Interfaces:**
- Consumes: `EGraph`, the eqsat `Equivalences` analysis (`make`/`merge`) and `EquivalenceClasses`, `run_analysis_bounded` or an equivalent driver, `MAX_EQUIVALENCES_ANALYSIS_ITERS`.
- Produces:
```rust
pub(crate) type EqFact = Option<EquivalenceClasses>;
/// Per-(canonical relational class) output-equivalence fact, merged across all
/// e-nodes in the class to a bounded fixpoint — identical to what the eqsat
/// `Equivalences` analysis computes today.
pub(crate) fn derive_facts(eg: &EGraph) -> std::collections::HashMap<Id, EqFact>;
/// A class is empty iff its fact is `None` (absorbing) or its
/// `EquivalenceClasses` is `unsatisfiable()`.
pub(crate) fn fact_is_empty(fact: &EqFact) -> bool;
```

Compute the same per-class fact the analysis produces. The simplest faithful implementation reuses the existing analysis driver: run `Equivalences` via the same path `saturate` uses (`run_analysis_bounded` with `MAX_EQUIVALENCES_ANALYSIS_ITERS`), then read out the per-class map. This guarantees the "merge across all nodes + bounded fixpoint" semantics (Global Constraint: reuse, don't reimplement).

- [ ] **Step 1: Write failing tests.** Create `colored_derive.rs` with a test module that builds small `Rel`s, adds them to an `EGraph`, saturates with an **empty** ruleset (so only the input nodes exist), and checks facts. Use the existing test helpers in `egraph.rs`/`analysis/equivalences.rs` tests as a model for constructing `Rel` and reading equivalence classes.
```rust
#[cfg(test)]
mod tests {
    use super::*;
    // (helpers: build a Filter(#0 = #1) over a Get with arity 2, etc.)

    #[mz_ore::test]
    fn filter_eq_derives_column_equivalence() {
        // Filter predicate `#0 = #1` ⇒ the Filter class's fact equates #0 and #1.
        let (eg, filter_class) = filter_eq_fixture();
        let facts = derive_facts(&eg);
        let ec = facts[&eg.find(filter_class)].as_ref().expect("non-empty");
        assert!(ec.reducer().values().any(|_| true), "has a reducer");
        // #0 and #1 are in one class:
        assert_eq!(ec_canon(ec, col(0)), ec_canon(ec, col(1)));
    }

    #[mz_ore::test]
    fn contradiction_is_empty() {
        // Filter `#0 = 1` then `#0 = 2` ⇒ unsatisfiable ⇒ fact_is_empty.
        let (eg, top) = contradictory_filter_fixture();
        let facts = derive_facts(&eg);
        assert!(fact_is_empty(&facts[&eg.find(top)]));
    }

    #[mz_ore::test]
    fn merge_across_two_nodes_in_a_class() {
        // A class holding two equivalent e-nodes whose individual facts differ:
        // the derived fact is their lattice merge, not one node's.
        let (eg, class) = two_node_class_fixture();
        let facts = derive_facts(&eg);
        let ec = &facts[&eg.find(class)];
        assert_eq!(ec, &expected_merged_fact());
    }
}
```
(Implementers: write the three fixtures using the same `Rel`/`MirScalarExpr` construction the analysis tests use; `ec_canon`/`col` are tiny local helpers — `col(i) = MirScalarExpr::column(i)`, `ec_canon` looks up the representative via `ec.reducer()`.)

- [ ] **Step 2: Run to confirm failure.** Run `bin/cargo-test -p mz-transform colored_derive`. Expected: FAIL (module/functions absent).

- [ ] **Step 3: Implement `derive_facts` + `fact_is_empty`.**
```rust
use std::collections::HashMap;
use crate::analysis::equivalences::EquivalenceClasses;
use crate::eqsat::analysis::equivalences::Equivalences;
use crate::eqsat::egraph::{EGraph, Id};

pub(crate) type EqFact = Option<EquivalenceClasses>;

/// Per-canonical-class output-equivalence fact, computed by the eqsat
/// `Equivalences` analysis (merge across all class nodes, bounded fixpoint).
pub(crate) fn derive_facts(eg: &EGraph) -> HashMap<Id, EqFact> {
    // Run the same analysis the saturation loop runs, and collect its per-class
    // result. Reuse `run_analysis_bounded` so the merge/fixpoint semantics are
    // identical to production (Global Constraint: reuse, don't reimplement).
    // Construct the `Equivalences` analysis exactly as `saturate` does — grep its
    // construction there (it initializes the `locals` field for recursion); do
    // NOT assume a `Default` impl. For a Let-free fragment `locals` is empty.
    let analysis = /* same `Equivalences { locals: ... }` saturate builds */;
    eg.run_analysis_bounded(&analysis, MAX_EQUIVALENCES_ANALYSIS_ITERS)
}

/// `None` (absorbing empty) or a contradictory `EquivalenceClasses`.
pub(crate) fn fact_is_empty(fact: &EqFact) -> bool {
    match fact {
        None => true,
        Some(ec) => ec.unsatisfiable(),
    }
}
```
If `run_analysis_bounded` does not already return a `HashMap<Id, Domain>`, add a thin `pub(crate)` wrapper in `egraph.rs`/`core.rs` that runs the analysis and returns the per-class map (the saturation loop already materializes this into `Analyses.eq`; expose the same computation). Import `MAX_EQUIVALENCES_ANALYSIS_ITERS` (re-export it `pub(crate)` from `egraph` if needed).

- [ ] **Step 4: Run the tests.** Run `bin/cargo-test -p mz-transform colored_derive`. Expected: PASS.

- [ ] **Step 5: Clippy + commit.**
```bash
git add src/transform/src/eqsat/colored_derive.rs src/transform/src/eqsat.rs
git commit -m "eqsat SP4b: colored_derive::derive_facts (per-class merged equivalences)"
```

---

### Task 4: `colored_derive` — reduced-spelling interning, scopes, unsat

**Files:**
- Modify: `src/transform/src/eqsat/colored_derive.rs`; expose `reduce_escalar` (or a thin reuse wrapper) from `egraph` as `pub(crate)`.
- Test: in `colored_derive.rs`

**Interfaces:**
- Consumes: `derive_facts`/`fact_is_empty` (Task 3); `reduce_escalar` (egraph, made `pub(crate)`); `EGraph::intern_scalar`, `CombinedData::escalar`; `EquivalenceClasses::reducer()`; the relational arity helper used by Phase-2a (`input` arity).
- Produces:
```rust
/// One distinct contextual scope: the scalar-id equalities valid there.
pub(crate) struct ScopeEqualities { pub unions: Vec<(Id, Id)> }
pub(crate) struct DerivedScopes {
    /// Deduped distinct equality-sets (one future color each).
    pub scopes: Vec<ScopeEqualities>,
    /// Canonical relational class -> index into `scopes` (absent ⇒ black/no color).
    pub class_scope: std::collections::HashMap<Id, usize>,
    /// Canonical relational classes that denote the empty relation.
    pub empty_classes: std::collections::HashSet<Id>,
    /// For each (canonical relational class, payload slot), the original payload
    /// `Id` and the interned reduced-spelling `Id` to be unioned in the class's
    /// color. Used by extraction (Task 6) only via `scopes`; kept for tests.
    pub reductions: Vec<(Id, Id)>,
}
/// Runs on the MUTABLE graph (interns reduced spellings) BEFORE freeze.
pub(crate) fn derive(eg: &mut EGraph) -> DerivedScopes;
```

Algorithm of `derive(eg)`:
1. `let facts = derive_facts(eg);`
2. `empty_classes = { canon class : fact_is_empty(fact) }`.
3. For each canonical relational class with a non-empty fact `ec`:
   - For each **Filter** e-node in the class: for each predicate `Id`, apply `reduce_escalar(escalar, input_ec.reducer(), input_arity)` where `input_ec` is `facts[input]`'s reducer (the **input's** equivalences, `max_col = input_arity`). If changed, `intern_scalar(reduced)` → `reduced_id`; record union `(predicate_id, reduced_id)` for this class's scope.
   - For each **Map** e-node in the class: for each scalar at position `pos`, apply `reduce_escalar(escalar, ec.reducer(), input_arity + pos)` (the class's own reducer; `max_col` reproduces the forward-reference guard). If changed, intern → record union.
   - Skip all other node kinds (Join/Reduce/etc.) — Phase-2a never rewrote them.
4. Group the recorded unions by class into per-class equality-sets; **dedup** identical equality-sets into `scopes` (a `HashMap<Vec<(Id,Id)>-sorted-key, usize>`); fill `class_scope`.

Because interning happens here (mutation) and the colored layer is built only after freeze (Task 5), `derive` records ids that the caller re-canonicalizes through `base.find` after `rebuild()`.

- [ ] **Step 1: Write failing tests.**
```rust
#[mz_ore::test]
fn map_scalar_reduced_to_column_is_interned_and_unioned() {
    // input has #0; filter establishes #0 = f(#1) above; a Map computes f(#1).
    // derive() must intern `#0` spelling-equivalent and record (f(#1)-id, #0-id).
    let (mut eg, map_class) = map_redundant_compute_fixture();
    let d = derive(&mut eg);
    let scope = &d.scopes[d.class_scope[&eg.find(map_class)]];
    assert!(!scope.unions.is_empty(), "a reduction was recorded");
    // The reduced spelling is now a base class with an escalar entry:
    for &(_orig, reduced) in &scope.unions {
        let _ = eg.data().escalar(reduced); // must not panic
    }
}

#[mz_ore::test]
fn map_self_reference_is_rejected_by_max_col() {
    // A map scalar at pos that would rewrite to its own output column must NOT
    // be reduced (max_col guard), so no union is recorded for it.
    let (mut eg, map_class) = map_self_reference_fixture();
    let d = derive(&mut eg);
    assert!(d.class_scope.get(&eg.find(map_class)).is_none()
        || d.scopes[d.class_scope[&eg.find(map_class)]].unions.is_empty());
}

#[mz_ore::test]
fn identical_scopes_dedup_to_one() {
    let (mut eg, _) = two_filters_same_equality_fixture();
    let d = derive(&mut eg);
    // Two classes with the same equality-set share one scope index.
    let distinct: std::collections::HashSet<_> = d.class_scope.values().collect();
    assert!(distinct.len() < d.class_scope.len() || d.class_scope.len() <= 1);
}

#[mz_ore::test]
fn contradiction_recorded_in_empty_classes() {
    let (mut eg, top) = contradictory_filter_fixture();
    let d = derive(&mut eg);
    assert!(d.empty_classes.contains(&eg.find(top)));
}
```

- [ ] **Step 2: Run to confirm failure.** Run `bin/cargo-test -p mz-transform colored_derive`. Expected: FAIL.

- [ ] **Step 3: Make `reduce_escalar` reusable.** In `egraph` (post-split: `saturate.rs`), change `fn reduce_escalar` to `pub(crate) fn reduce_escalar` and re-export it `pub(crate) use` from the `egraph` root so `colored_derive` can call it. Do not change its body.

- [ ] **Step 4: Implement `derive`.** Implement the algorithm above in `colored_derive.rs`. Use `eg.nodes(class)` to enumerate e-nodes, match `CNode::Rel(ENode::Filter { input, predicates })` / `ENode::Map { input, scalars }`, resolve payload `Id`→`EScalar` via `eg.data().escalar(id)`, compute `input` arity via the same helper Phase-2a/`make` uses (grep the arity accessor used in `analysis/equivalences.rs`), call `reduce_escalar`, `eg.intern_scalar(&reduced)`, and accumulate. Dedup scopes with a sorted-key `HashMap`.

- [ ] **Step 5: Run the tests.** Run `bin/cargo-test -p mz-transform colored_derive`. Expected: PASS. Also run the full crate (`bin/cargo-test -p mz-transform`) to confirm no regression (Phase-2a still default).

- [ ] **Step 6: Clippy + commit.**
```bash
git add src/transform/src/eqsat/colored_derive.rs src/transform/src/eqsat/egraph*
git commit -m "eqsat SP4b: colored_derive::derive (reduced-spelling interning, scopes, unsat)"
```

---

### Task 5: Build the colored layer and thread it through extraction (behind `EquivMode`)

**Files:**
- Modify: `src/transform/src/eqsat/colored_derive.rs` (layer builder), `extract.rs` (`Extractor` trait + impls), `engine.rs` (`Optimizer` field + fragment arms), `egraph.rs` (`extract_with` signature).
- Test: in `colored_derive.rs` (layer build) + an engine smoke test.

**Interfaces:**
- Produces:
```rust
pub(crate) struct ColoredLayer<'b> {
    pub ceg: ColoredEGraph<'b, CombinedLang>,
    pub color_of: std::collections::HashMap<Id, ColorId>, // canon rel class -> color
    pub empty_classes: std::collections::HashSet<Id>,
}
/// Build after freeze: create one color per scope, apply (re-canonicalized) unions.
pub(crate) fn build_colored_layer<'b>(base: &'b EGraph, scopes: DerivedScopes) -> ColoredLayer<'b>;

// extract.rs
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EquivMode { Phase2a, Colored }   // temporary; deleted in Task 8
```
- `Extractor::extract` and `EGraph::extract_with` gain a `colored: Option<&mut ColoredLayer<'_>>` parameter (None ⇒ today's behavior). `Optimizer` gains a `equiv_mode: EquivMode` field defaulting to `EquivMode::Phase2a`, set via a builder `with_equiv_mode`.

**Behavior:** with `EquivMode::Phase2a` (the default), the colored layer is **not built** and `None` is threaded — output is byte-identical to today. The colored path is wired but dormant. (Color-aware *resolution* itself is Task 6.)

- [ ] **Step 1: Write the failing layer test.**
```rust
#[mz_ore::test]
fn build_colored_layer_applies_unions() {
    let (mut eg, _) = filter_eq_fixture_mut();
    let scopes = derive(&mut eg);
    eg.rebuild();
    let layer = build_colored_layer(&eg, scopes);
    // Some color exists and merges the recorded pair.
    assert!(!layer.color_of.is_empty());
}
```

- [ ] **Step 2: Confirm failure.** Run `bin/cargo-test -p mz-transform build_colored_layer_applies_unions`. Expected: FAIL.

- [ ] **Step 3: Implement `build_colored_layer`.**
```rust
pub(crate) fn build_colored_layer<'b>(base: &'b EGraph, scopes: DerivedScopes) -> ColoredLayer<'b> {
    let mut ceg = ColoredEGraph::new(base);
    let color_ids: Vec<ColorId> = scopes.scopes.iter().map(|_| ceg.new_color(None)).collect();
    for (i, scope) in scopes.scopes.iter().enumerate() {
        for &(a, b) in &scope.unions {
            // Re-canonicalize through the frozen base after rebuild().
            ceg.union(color_ids[i], base.find(a), base.find(b));
        }
    }
    let color_of = scopes
        .class_scope
        .into_iter()
        .map(|(cls, idx)| (base.find(cls), color_ids[idx]))
        .collect();
    let empty_classes = scopes.empty_classes.into_iter().map(|c| base.find(c)).collect();
    ColoredLayer { ceg, color_of, empty_classes }
}
```

- [ ] **Step 4: Add `EquivMode` and thread the parameter (dormant).** Add `EquivMode` to `extract.rs`. Add `colored: Option<&mut ColoredLayer<'_>>` to `Extractor::extract`, both impls, and `EGraph::extract_with`; at every existing call site pass `None`. Add the `equiv_mode` field + `with_equiv_mode` builder to `Optimizer` (default `Phase2a`). In the fragment arms of `optimize_node_with_alt`/`optimize_node` (and the scope helpers if they extract), leave the colored layer unbuilt when `equiv_mode == Phase2a`.

- [ ] **Step 5: Compile + full tests.** Run `cargo check -p mz-transform`, then `bin/cargo-test -p mz-transform`. Expected: all pass, output unchanged (Phase2a default, `None` threaded).

- [ ] **Step 6: Clippy + commit.**
```bash
git add src/transform/src/eqsat/
git commit -m "eqsat SP4b: build_colored_layer + thread EquivMode/ColoredLayer (dormant, Phase2a default)"
```

---

### Task 6: Color-aware Filter/Map resolution + empty-fold (under `EquivMode::Colored`)

**Files:**
- Modify: `egraph.rs` (color-aware resolution helpers + the build path that emits Filter/Map payloads), `engine.rs` (build the colored layer in fragment arms when `Colored`), `colored_derive.rs` (scalar cost helper).
- Test: in `colored_derive.rs`/`egraph.rs` unit tests + an engine test asserting a colored plan differs only where expected.

**Interfaces:**
- Consumes: `ColoredLayer`, `colored_class_members` (Task 2), `CombinedData::escalar`.
- Produces:
```rust
/// Tree-size cost of a scalar expr (node count); lower is cheaper.
pub(crate) fn scalar_cost(expr: &mz_expr::MirScalarExpr) -> usize;
/// Cheapest `EScalar` spelling of payload `id` under `color`, among its colored-
/// class base members. Falls back to `base.data().escalar(id)` when no color.
pub(crate) fn resolve_scalar_colored(
    base: &EGraph, layer: &mut ColoredLayer<'_>, color: ColorId, id: Id,
) -> EScalar;
```

**Behavior:** when `equiv_mode == Colored`, each Let-free fragment, after `seed_indexed_filters`, builds the colored layer (`derive` must run *before* the freeze that the extractor reads — see ordering note) and threads `Some(&mut layer)` into extraction. During `build_rel`, when emitting a **Filter** predicate, resolve it under `color_of[filter.input]`; when emitting a **Map** scalar, resolve under `color_of[map.input]`; all other scalar payloads use the unchanged `escalar` cache. If the class being built is in `empty_classes`, emit an empty `Constant` of the class arity instead.

**Ordering note (critical):** `derive(eg)` mutates the graph (interns spellings) and must run **before** the graph is frozen for extraction. In each fragment arm: `saturate` → `seed_indexed_filters` → (if `Colored`) `let scopes = colored_derive::derive(&mut eg); eg.rebuild(); let mut layer = build_colored_layer(&eg, scopes);` → extract with `Some(&mut layer)`. The `TimeFirst` alternate extraction (`extract_with`) shares the same `layer`.

- [ ] **Step 1: Write the failing scalar-cost + resolution tests.**
```rust
#[mz_ore::test]
fn scalar_cost_counts_nodes() {
    assert!(scalar_cost(&col(0)) < scalar_cost(&add(col(0), col(1))));
}

#[mz_ore::test]
fn resolve_colored_picks_cheaper_member() {
    // Under a color where f(#1) ≅ #0, resolving f(#1) yields the column #0.
    let (mut eg, payload_id, map_input_class) = redundant_compute_fixture_mut();
    let scopes = derive(&mut eg);
    eg.rebuild();
    let mut layer = build_colored_layer(&eg, scopes);
    let color = layer.color_of[&eg.find(map_input_class)];
    let got = resolve_scalar_colored(&eg, &mut layer, color, eg.find(payload_id));
    assert!(got.is_col().is_some(), "resolved to the bare column #0");
}
```

- [ ] **Step 2: Confirm failure.** Run `bin/cargo-test -p mz-transform resolve_colored_picks_cheaper_member`. Expected: FAIL.

- [ ] **Step 3: Implement `scalar_cost` and `resolve_scalar_colored`.**
```rust
pub(crate) fn scalar_cost(expr: &mz_expr::MirScalarExpr) -> usize {
    let mut n = 0;
    expr.visit_pre(|_| n += 1);
    n
}

pub(crate) fn resolve_scalar_colored(
    base: &EGraph, layer: &mut ColoredLayer<'_>, color: ColorId, id: Id,
) -> EScalar {
    let members = layer.ceg.colored_class_members(color, base.find(id));
    members
        .into_iter()
        .map(|m| base.data().escalar(m).clone())
        .min_by_key(|e| (scalar_cost(&e.expr), e.name_key()))
        .unwrap_or_else(|| base.data().escalar(base.find(id)).clone())
}
```
(`name_key()` is the deterministic tie-breaker already used by `set_escalar`.)

- [ ] **Step 4: Wire color-aware emission into `build_rel`.** In the extractor's `build_rel` (egraph.rs), thread the `Option<&mut ColoredLayer>` down. For Filter predicates and Map scalars, when a layer is present and `color_of` has the relevant input class, replace the `resolve_scalars`/per-id `escalar` lookup with `resolve_scalar_colored(self, layer, color, id)`. For the empty-fold: at the top of building a class's `Rel`, if `layer.empty_classes.contains(&self.find(class))`, return an empty `Constant` of the class's arity (use the arity helper; SP4a M1.1 — guard `arity()` for relational classes). Keep all non-Filter/Map payloads on the plain cache.

- [ ] **Step 5: Build the layer in the fragment arms when `Colored`.** In `engine.rs` `optimize_node_with_alt` and `optimize_node` (and scope helpers that extract), implement the ordering note: build the layer when `self.equiv_mode == EquivMode::Colored`, thread `Some(&mut layer)` into both the primary `extract` and the `TimeFirst` `extract_with`.

- [ ] **Step 6: Add an engine smoke test (mode-gated).**
```rust
#[mz_ore::test]
fn colored_mode_rewrites_redundant_map_compute() {
    // Same plan optimized in Phase2a vs Colored: the Colored plan replaces a
    // redundant recomputation with the equal column established by a Filter.
    let plan = redundant_map_plan();
    let p2a = optimize_with_mode(plan.clone(), EquivMode::Phase2a);
    let col = optimize_with_mode(plan, EquivMode::Colored);
    // Both are valid; the colored one is no larger.
    assert!(rel_node_count(&col) <= rel_node_count(&p2a));
}
```
(`optimize_with_mode` is a test helper that builds an `Optimizer` with `with_equiv_mode`; `rel_node_count` walks the `Rel`.)

- [ ] **Step 7: Run tests.** Run `bin/cargo-test -p mz-transform colored`. Expected: PASS. Then full `bin/cargo-test -p mz-transform` — still green because the **default** is Phase2a (the new tests opt into Colored explicitly).

- [ ] **Step 8: Clippy + commit.**
```bash
git add src/transform/src/eqsat/
git commit -m "eqsat SP4b: color-aware Filter/Map resolution + empty-fold (EquivMode::Colored)"
```

---

### Task 7: Differential, flip default to `Colored`, regenerate & review goldens

**Files:**
- Modify: `engine.rs`/`eqsat.rs` (default `EquivMode` → `Colored`), `test/sqllogictest/**` (regenerated goldens).
- Artifact: a differential report committed under the SDD ledger directory.

**Interfaces:**
- Consumes: everything above. After this task, the default optimizer path is colored.

- [ ] **Step 1: Capture the differential.** With both modes available, run the EXPLAIN-bearing slt corpus under each mode and diff per file. Practical approach: run the full slt suite once on the current (Phase2a) binary to confirm green; then temporarily flip the default to `Colored` in a scratch build and run `bin/sqllogictest --optimized -- <file>` across the EXPLAIN-bearing files (the SP4a sweep list — `git grep -l 'EXPLAIN' test/sqllogictest`), collecting the failing (changed) files. Write the list + a one-line justification per changed file (improvement / sound-neutral) to `.superpowers/sdd/sp4b-differential.md`.

- [ ] **Step 2: Review every delta.** For each changed EXPLAIN, confirm the new plan is an improvement or sound-neutral (a contextual substitution or empty-fold), exactly as the spec gate requires. STOP and report if any delta is a regression (a worse plan or a semantic change) — that is a real bug, not a golden to regenerate.

- [ ] **Step 3: Flip the default.** Change `Optimizer`'s default `equiv_mode` to `EquivMode::Colored` (remove the `Phase2a` default; keep the enum + `with_equiv_mode` until Task 8).

- [ ] **Step 4: Regenerate goldens.** For each changed file, `bin/sqllogictest --optimized -- --rewrite-results <file>`. Evaluate **per-file standalone** (SP4a list-mode caveat). Re-run each standalone to confirm it passes.

- [ ] **Step 5: Full suite + unit tests.** Run `bin/cargo-test -p mz-transform` and the slt suite. Expected: green.

- [ ] **Step 6: Commit (two commits).**
```bash
git add .superpowers/sdd/sp4b-differential.md && git commit -m "eqsat SP4b: differential capture (colored vs Phase-2a)"
git add src/transform/src/eqsat/ test/sqllogictest/ && git commit -m "eqsat SP4b: flip default to colored equivalences + regenerate goldens (name/plan deltas reviewed)"
```

---

### Task 8: Delete Phase-2a, the eqsat `Equivalences` analysis, unsat machinery, and `EquivMode`

**Files:**
- Modify: `egraph.rs`/`saturate.rs` (Phase-2a block, `rewrite_escalars`, `reduce_escalar_list`, `filter_input_reducer`, `Analyses.eq`, `cond_unsatisfiable`), `analysis.rs` + `analysis/equivalences.rs` (delete the eqsat analysis impl), `dsl.rs`/`build/grammar.rs`/`build/codegen.rs` (`Cond::Unsatisfiable`), `rules/relational.rewrite` (`collapse_unsatisfiable`), `extract.rs`/`engine.rs` (`EquivMode` scaffolding).

**Interfaces:** none new. Net: colored path is the only path; `reduce_escalar` survives (used by `colored_derive`).

- [ ] **Step 1: Delete the Phase-2a saturation block** in `saturate` and the helpers used **only** there: `rewrite_escalars`, `reduce_escalar_list`, the `filter_input_reducer` plumbing, and the `Analyses.eq` field + its computation. **Keep `reduce_escalar`** (now used by `colored_derive`). Keep `empty_false_filter`.

- [ ] **Step 2: Delete `cond_unsatisfiable`** and the `Cond::Unsatisfiable` machinery: the `dsl.rs` variant (`Unsatisfiable { rel }`), the `build/grammar.rs` production, the `build/codegen.rs` arms (grep `Unsatisfiable`), and the `rule collapse_unsatisfiable` in `rules/relational.rewrite`. Regenerate any build.rs-driven artifacts by `cargo check` (the grammar/codegen are build-time). Do **not** delete `empty_false_filter` or `union_cancel`.

- [ ] **Step 3: Delete the eqsat `Equivalences` analysis impl** in `analysis/equivalences.rs` and its registration in `analysis.rs` (`mod` + `pub use Equivalences`). `colored_derive` no longer depends on the `Analysis` *impl* if Task 3 inlined the driver; if Task 3 reused `Equivalences`, move the minimal `make`/`merge` arms it needs into `colored_derive` first, then delete. Keep `crate::analysis::equivalences::{EquivalenceClasses, ExpressionReducer}`.

- [ ] **Step 4: Remove `EquivMode`.** Delete the enum, the `with_equiv_mode` builder, and the `Option`/branch plumbing — the colored layer is now always built in the fragment arms. Simplify `Extractor::extract`/`extract_with` to take `colored: &mut ColoredLayer<'_>` (no longer `Option`), since black scopes are represented by `color_of` misses, not a `None` layer. (If any non-fragment caller cannot supply a layer, keep a trivial empty layer helper rather than `Option`.)

- [ ] **Step 5: Compile + full tests + slt.** `cargo check -p mz-transform`; `bin/cargo-test -p mz-transform`; slt suite. Expected: green, no golden changes (behavior already flipped in Task 7).

- [ ] **Step 6: Clippy.** `cargo clippy -p mz-transform --all-targets -- -D warnings`. Expected: clean (no dead-code warnings from the removed paths).

- [ ] **Step 7: Commit.**
```bash
git add -A src/transform/
git commit -m "eqsat SP4b: delete Phase-2a + eqsat Equivalences analysis + unsat machinery + EquivMode scaffolding"
```

---

## Notes for the executor

- **Model guidance:** Task 1 (movement) and Task 2 (small accessor) → cheap/standard model. Tasks 3–6, 8 → capable model (subtle reuse + threading + deletions). Task 7 → capable model (gate judgment on every plan delta).
- **The differential (Task 7) is the behavior gate.** A delta that is not an improvement or sound-neutral is a bug to fix, not a golden to accept.
- **SP4a SDD lesson:** background agents reliably diagnose but stall before implementing — verify real git/file progress, nudge explicitly to execute, take over mechanical work (golden regeneration, name-only verification) if an agent stalls.
- **Deferred to SP4c (do not build here):** colored congruence closure, `add_colored` conclusions, `extract_colored`, color-aware key/arrangement reasoning, hierarchical colors, colored saturation, SP4a M1.1/M1.2.
