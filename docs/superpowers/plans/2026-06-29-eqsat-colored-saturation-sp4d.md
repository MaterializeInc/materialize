# SP4d — eqsat colored saturation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-run a tagged subset of rewrite rules inside contextual colors so contextual equalities compose with the full rewrite system, with all conclusions confined to the colored overlay and surfaced at extraction.

**Architecture:** Abstract the codegen-emitted graph operations behind a `MatchGraph`/`ApplyGraph` trait pair; the base `EGraph` implements them byte-identically, and a `ColoredView` over `ColoredEGraph` at a `ColorId` implements them color-aware (reads canonicalize through `find(c,·)`/`canon(c,·)`; writes go to `add_colored(c,·)`). A new colored driver runs the tagged rules per color over a hierarchical color forest, closing congruence (incrementally, with `remove`-in-`close`), and extraction picks the cheapest colored conclusion per class via `extract_colored`.

**Tech Stack:** Rust; the eqsat crate `mz-transform` (`src/transform/src/eqsat/`); build-time rule codegen (`src/transform/build/`); `chumsky` grammar; nextest via `bin/cargo-test`; datadriven `*.spec`; `bin/sqllogictest`.

## Global Constraints

- **Black-layer byte-identity (P1):** the base `EGraph` routed through the new traits must produce identical saturation output and identical goldens. P1 lands with **zero** golden movement. (spec §4.1, §8)
- **Colored soundness by confinement + exact conds:** colored conclusions live only in their color's delta (`add_colored`), never mutate the base or escape to siblings; the colored rule subset is restricted to rules whose side conditions are **color-exact** (payload/arity/structure only) — analysis-gated conds (NonNeg/Keys/Monotonic) are excluded. A build-time assertion enforces that a `colored`-tagged rule uses only color-exact conditions. (spec §4.2, §5.A)
- **`Rel::node_count()` stays relational-only** (it is the CSE ordering key, `cse.rs:77,361`); only colored conclusion selection may use a tree-size colored cost. (inherited from SP4c)
- **Reuse the real cost model:** the final base-vs-colored extraction choice uses `cost::CostModel` (`cost.rs`) unchanged; the colored `CostModel<L>` (`colored/extract.rs`) is used only to pick the cheapest *colored* spelling per class (tree size), mirroring SP4b `resolve_scalar_colored`. (spec §5.E, Out-of-scope: color-aware arrangement cost)
- **Build alongside, then switch:** colored saturation is wired in and validated against a captured corpus differential before any golden is regenerated; goldens are reviewed per-file-standalone. (spec §7)
- **Only P5 (extraction + wiring) may move goldens.** Any earlier task moving a golden is a regression to investigate. (spec §8)
- **Per task:** `cargo clippy -p mz-transform --all-targets -- -D warnings` clean; tests via `bin/cargo-test -p mz-transform <filter>` (nextest, NOT `cargo test`); datadriven rewrite via `REWRITE=1 bin/cargo-test -p mz-transform run_tests`; slt via `bin/sqllogictest --optimized -- [--rewrite-results] <file>`; timeout ≥600000ms.

## Constants (mirror from `egraph.rs` / `core.rs`)

- `MAX_ENODES` = 600 (`egraph.rs:56`) — base saturation e-node budget; reuse as the colored delta-node budget.
- `MATCH_LIMIT` = 1_000 (`egraph.rs:64`), `INITIAL_BAN_LEN` = 4 (`egraph.rs:68`) — per-rule match cap + backoff; reuse for the colored driver.
- `MAX_EQUIVALENCES_ANALYSIS_ITERS` = 4 (`egraph.rs:84`) — bound for the Equivalences analysis (used by `derive_facts`).
- New: `COLORED_MAX_ITERS` = 4 — per-color colored-saturation fixpoint cap (define beside `MAX_ENODES`).

## File Structure

- **Create** `src/transform/src/eqsat/egraph/view.rs` — the `MatchGraph` (read) and `ApplyGraph` (write) traits and the base-`EGraph` impls. One responsibility: the graph-operation surface the codegen targets.
- **Create** `src/transform/src/eqsat/colored/view.rs` — `ColoredView<'a,'b>` wrapping `&mut ColoredEGraph` + `ColorId` + a per-view `delta_escalar` cache + a `ColoredIndex`; impls of `MatchGraph`/`ApplyGraph`.
- **Create** `src/transform/src/eqsat/colored/saturate.rs` — `colored_saturate(...)` driver (the colored analogue of `egraph::saturate`).
- **Modify** `src/transform/build/codegen.rs` — emit generic `find`/`apply` against the traits.
- **Modify** `src/transform/build/grammar.rs`, `src/transform/src/eqsat/dsl.rs` — the `colored` rule attribute.
- **Modify** `src/transform/src/eqsat/rules.rs` — `colored` field on `CompiledRule`; `CompiledRuleSet::colored_rules()`.
- **Modify** `src/transform/src/eqsat/rules/relational.rewrite` — tag the targeted subset.
- **Modify** `src/transform/src/eqsat/matcher.rs` — payload helpers + free-fn conds generalized over the traits.
- **Modify** `src/transform/src/eqsat/colored/congruence.rs` — `remove`-in-`close` (P3) + incremental dirty-tracking (P4).
- **Modify** `src/transform/src/eqsat/colored_derive.rs` — hierarchical color forest (P3); colored-conclusion enumeration for extraction (P5).
- **Modify** `src/transform/src/eqsat/extract.rs` — colored relational candidates in `build_rel` (P5).
- **Modify** `src/transform/src/eqsat/engine.rs` — insert `colored_saturate` at the 3 call sites (P5).
- **Modify** `src/transform/src/eqsat/egraph.rs` / `colored.rs` — module declarations, `COLORED_MAX_ITERS`.

---

# Phase P1 — trait refactor, byte-identical

## Task 1: Define `MatchGraph` / `ApplyGraph` traits + base `EGraph` impl

**Files:**
- Create: `src/transform/src/eqsat/egraph/view.rs`
- Modify: `src/transform/src/eqsat/egraph.rs` (add `mod view;` and re-exports)
- Test: in `view.rs` `#[cfg(test)]`

**Interfaces:**
- Consumes: existing `EGraph` inherent methods — `rel_class_ids(&self)->Vec<Id>`, `rel_class_nodes(&self,Id)->Vec<&ENode>`, `arity(&self,Id)->usize`, `data(&self)->&CombinedData`, `add(&mut self,CNode)->Id`, `intern_scalar(&mut self,&EScalar)->Id`, `union(&mut self,Id,Id)->bool`, `binding_arities(&self,&EBindings)->BTreeMap<String,usize>`, `column_types(&self,Id)->Option<Vec<ReprColumnType>>`, `find(&self,Id)->Id`; the `cond_*` instance methods (`saturate.rs:109-249`); `Index = HashMap<Sym, Vec<(Id,ENode)>>` (`combined.rs:178`).
- Produces: traits `MatchGraph` and `ApplyGraph` (exact methods below) and `impl MatchGraph for BaseView<'_>` / `impl ApplyGraph for EGraph`.

The base read side needs both the graph and the prebuilt `Index` (sym lookup) and `Analyses`. Wrap them: `pub(crate) struct BaseView<'a> { pub eg: &'a EGraph, pub index: &'a Index, pub an: &'a Analyses }`.

- [ ] **Step 1: Write the failing test** (`view.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::eqsat::egraph::{EGraph, CNode, ENode};
    use crate::eqsat::ir::EScalar;
    use mz_expr::MirScalarExpr;

    #[mz_ore::test]
    fn base_view_matches_inherent_methods() {
        let mut eg = EGraph::new();
        // Build Filter(#0 = #0)(Get-like leaf) via a constant input.
        let leaf = eg.add(CNode::Rel(ENode::Constant { card: 1, arity: 1, col_types: None }));
        let p = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(0)));
        let filt = eg.add(CNode::Rel(ENode::Filter { predicates: vec![p], input: leaf }));
        eg.rebuild();
        let index = eg.rel_index();
        let an = Analyses::default();
        let view = BaseView { eg: &eg, index: &index, an: &an };
        // The trait read methods agree with the inherent ones.
        assert_eq!(view.rel_class_ids(), eg.rel_class_ids());
        assert_eq!(view.arity(filt), eg.arity(filt));
        assert_eq!(
            view.rel_class_nodes(filt).len(),
            eg.rel_class_nodes(filt).len()
        );
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bin/cargo-test -p mz-transform base_view_matches_inherent_methods`
Expected: FAIL — `view` module / `MatchGraph` / `BaseView` not found.

- [ ] **Step 3: Implement the traits and base impls** (`view.rs`)

Define the two traits to cover exactly the codegen surface. `MatchGraph` carries the read/match surface; `ApplyGraph` the write surface. The `cond_*` are methods so the colored impl can override them; free-fn conds (`cond_all_true` etc.) take an escalar resolver, added in Task 2.

```rust
use std::collections::BTreeMap;
use crate::eqsat::core::Id;
use crate::eqsat::egraph::{Analyses, CNode, CombinedData, EGraph, ENode, Index};
use crate::eqsat::egraph::node::Sym;
use crate::eqsat::ir::EScalar;
use crate::eqsat::matcher::Payload;
use mz_repr::ReprColumnType; // adjust to the actual column-type path used by column_types

/// Read/match surface the generated `find` functions call. Implemented by the
/// base layer (`BaseView`) byte-identically and by `ColoredView` color-aware.
pub(crate) trait MatchGraph {
    fn rel_class_ids(&self) -> Vec<Id>;
    fn rel_class_nodes(&self, id: Id) -> Vec<ENode>;
    fn nodes_by_sym(&self, sym: Sym) -> Vec<(Id, ENode)>;
    fn arity(&self, id: Id) -> usize;
    /// Resolve a scalar id to its EScalar (base cache or colored delta cache).
    fn escalar(&self, id: Id) -> EScalar;
    // Color-exact conditions (graph/payload/arity only):
    fn cond_uses_only_input(&self, p: &Payload, rel: Id) -> bool;
    fn cond_cols_in_range(&self, p: &Payload, lo: i64, hi: i64) -> bool;
    fn cond_all_columns(&self, p: &Payload) -> bool;
    fn cond_any_false(&self, p: &Payload) -> bool;
    fn cond_no_false(&self, p: &Payload) -> bool;
    fn cond_no_error(&self, p: &Payload) -> bool;
    fn cond_all_true(&self, p: &Payload) -> bool;
    fn cond_identity_projection(&self, p: &Payload, rel: Id) -> bool;
    fn cond_is_rel_empty(&self, id: Id) -> bool;
    fn cond_not_rel_empty(&self, id: Id) -> bool;
    fn cond_has_three_or_more_inputs(&self, root: Id) -> bool;
    fn cond_is_binary_join(&self, root: Id) -> bool;
    fn cond_join_is_cyclic(&self, root: Id) -> bool;
    fn cond_has_inner_equiv(&self, p: &Payload, boundary: i64) -> bool;
    fn cond_reads_indexed_global(&self, id: Id) -> bool;
    fn cond_produces_key(&self, rel: Id, key: &Payload) -> bool;
    // Analysis-gated conditions: present for uniform codegen, NEVER reached for
    // colored rules (enforced at build time, Task 4). Base delegates; colored
    // returns false (sound: the rule simply does not fire).
    fn cond_non_negative(&self, id: Id) -> bool;
    fn cond_monotonic(&self, id: Id) -> bool;
    fn cond_is_unique_key(&self, p: &Payload, id: Id) -> bool;
}

/// Write surface the generated `apply` functions call.
pub(crate) trait ApplyGraph {
    fn add_rel(&mut self, node: ENode) -> Id;
    fn intern_scalar(&mut self, e: &EScalar) -> Id;
    fn arity_of_binding(&self, id: Id) -> usize;
    fn escalar(&self, id: Id) -> EScalar;
    fn column_types(&self, id: Id) -> Option<Vec<ReprColumnType>>;
}

pub(crate) struct BaseView<'a> {
    pub eg: &'a EGraph,
    pub index: &'a Index,
    pub an: &'a Analyses,
}

impl<'a> MatchGraph for BaseView<'a> {
    fn rel_class_ids(&self) -> Vec<Id> { self.eg.rel_class_ids() }
    fn rel_class_nodes(&self, id: Id) -> Vec<ENode> {
        self.eg.rel_class_nodes(id).into_iter().cloned().collect()
    }
    fn nodes_by_sym(&self, sym: Sym) -> Vec<(Id, ENode)> {
        self.index.get(&sym).cloned().unwrap_or_default()
    }
    fn arity(&self, id: Id) -> usize { self.eg.arity(id) }
    fn escalar(&self, id: Id) -> EScalar { self.eg.data().escalar(id).clone() }
    fn cond_uses_only_input(&self, p: &Payload, rel: Id) -> bool {
        crate::eqsat::egraph::cond_uses_only_input(self.eg.data(), p, self.eg.arity(rel))
    }
    fn cond_cols_in_range(&self, p: &Payload, lo: i64, hi: i64) -> bool {
        crate::eqsat::egraph::cond_cols_in_range(self.eg.data(), p, lo, hi)
    }
    fn cond_all_columns(&self, p: &Payload) -> bool {
        crate::eqsat::egraph::cond_all_columns(self.eg.data(), p)
    }
    fn cond_any_false(&self, p: &Payload) -> bool {
        crate::eqsat::egraph::cond_any_false(self.eg.data(), p)
    }
    fn cond_no_false(&self, p: &Payload) -> bool {
        crate::eqsat::egraph::cond_no_false(self.eg.data(), p)
    }
    fn cond_no_error(&self, p: &Payload) -> bool {
        crate::eqsat::egraph::cond_no_error(self.eg.data(), p)
    }
    fn cond_all_true(&self, p: &Payload) -> bool {
        crate::eqsat::egraph::cond_all_true(self.eg.data(), p)
    }
    fn cond_identity_projection(&self, p: &Payload, rel: Id) -> bool {
        crate::eqsat::egraph::cond_identity_projection(p, self.eg.arity(rel))
    }
    fn cond_is_rel_empty(&self, id: Id) -> bool { self.eg.cond_is_rel_empty(id) }
    fn cond_not_rel_empty(&self, id: Id) -> bool { self.eg.cond_not_rel_empty(id) }
    fn cond_has_three_or_more_inputs(&self, root: Id) -> bool {
        self.eg.cond_has_three_or_more_inputs(root)
    }
    fn cond_is_binary_join(&self, root: Id) -> bool { self.eg.cond_is_binary_join(root) }
    fn cond_join_is_cyclic(&self, root: Id) -> bool { self.eg.cond_join_is_cyclic(root) }
    fn cond_has_inner_equiv(&self, p: &Payload, boundary: i64) -> bool {
        self.eg.cond_has_inner_equiv(p, boundary)
    }
    fn cond_reads_indexed_global(&self, id: Id) -> bool {
        self.eg.cond_reads_indexed_global(id)
    }
    fn cond_produces_key(&self, rel: Id, key: &Payload) -> bool {
        self.eg.cond_produces_key(self.an, rel, key)
    }
    fn cond_non_negative(&self, id: Id) -> bool { self.eg.cond_non_negative(self.an, id) }
    fn cond_monotonic(&self, id: Id) -> bool { self.eg.cond_monotonic(self.an, id) }
    fn cond_is_unique_key(&self, p: &Payload, id: Id) -> bool {
        self.eg.cond_is_unique_key(self.an, p, id)
    }
}

impl ApplyGraph for EGraph {
    fn add_rel(&mut self, node: ENode) -> Id { self.add(CNode::Rel(node)) }
    fn intern_scalar(&mut self, e: &EScalar) -> Id { EGraph::intern_scalar(self, e) }
    fn arity_of_binding(&self, id: Id) -> usize { self.arity(id) }
    fn escalar(&self, id: Id) -> EScalar { self.data().escalar(id).clone() }
    fn column_types(&self, id: Id) -> Option<Vec<ReprColumnType>> { self.column_types(id) }
}
```

Note: confirm the exact `cond_*` visibility — they are currently `pub(crate)` methods on `EGraph` (`saturate.rs`) and free fns re-exported via `egraph.rs`; if any are private, widen to `pub(crate)`. Confirm the `column_types` return type from `build.rs:444` and use it verbatim.

- [ ] **Step 4: Run test to verify it passes**

Run: `bin/cargo-test -p mz-transform base_view_matches_inherent_methods`
Expected: PASS.

- [ ] **Step 5: Clippy**

Run: `cargo clippy -p mz-transform --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/transform/src/eqsat/egraph/view.rs src/transform/src/eqsat/egraph.rs
git commit -m "SP4d P1: MatchGraph/ApplyGraph traits + base EGraph impl"
```

## Task 2: Generalize payload helpers and free-fn conds over the traits

**Files:**
- Modify: `src/transform/src/eqsat/matcher.rs` (`map_scalar_ids`, `map_payload_cols`, `shift_payload`, `remap_payload`, `swap_equivs`, and `cols_of_payload`/`equivs_inner`/`equivs_outer` which read `&CombinedData`)
- Test: `matcher.rs` `#[cfg(test)]` (extend existing)

**Interfaces:**
- Consumes: `ApplyGraph` (Task 1) for write helpers; `MatchGraph::escalar` for read helpers.
- Produces: `shift_payload<G: ApplyGraph>(g: &mut G, p, k)`, `remap_payload<G: ApplyGraph>(g, p, outs)`, `swap_equivs<G: ApplyGraph>(g, p, a, b)`; `cols_of_payload`/`equivs_inner`/`equivs_outer` take `&dyn Fn(Id)->EScalar` (an escalar resolver) instead of `&CombinedData`.

Today these take `&mut EGraph` / `&CombinedData` (matcher.rs:159-360). Change the write helpers' first param to `&mut G` and route `graph.data().escalar(id)` through `MatchGraph`/`ApplyGraph`'s `escalar`. Change the data-reading helpers to take a resolver closure so both base and colored callers supply ids→EScalar. Keep behavior identical: a no-op permutation re-interns the same expr (base) or the same colored spelling (colored).

- [ ] **Step 1: Write the failing test** — adapt the existing `shift_rewrites_columns` test to call through `ApplyGraph`:

```rust
#[mz_ore::test]
fn shift_via_apply_graph_trait() {
    use crate::eqsat::egraph::view::ApplyGraph;
    let mut eg = EGraph::new();
    let p = pred(&mut eg, &[2, 3]);
    let out = shift_payload(&mut eg as &mut dyn ApplyGraph, p, -2).unwrap();
    assert_eq!(pred_cols(&eg, &out), vec![Some(0), Some(1)]);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform shift_via_apply_graph_trait`
Expected: FAIL — signature mismatch / `ApplyGraph` not used by `shift_payload`.

- [ ] **Step 3: Implement** — change `map_scalar_ids`, `map_payload_cols`, `shift_payload`, `remap_payload`, `swap_equivs` to be generic over `G: ApplyGraph` (resolve via `g.escalar(id)`, intern via `g.intern_scalar`). Change `cols_of_payload`, `equivs_inner`, `equivs_outer` to take `escalar: &dyn Fn(Id) -> EScalar`. Update the codegen call sites in Task 3.

- [ ] **Step 4: Run to verify it passes** (and the existing matcher tests)

Run: `bin/cargo-test -p mz-transform matcher::`
Expected: PASS (all matcher tests).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/src/eqsat/matcher.rs
git commit -m "SP4d P1: payload helpers + data conds generic over graph traits"
```

## Task 3: Emit generic `find`/`apply` from codegen; drive base saturate through `BaseView`

**Files:**
- Modify: `src/transform/build/codegen.rs` (`emit_find` ~411, `emit_apply` ~722, `cond_expr` ~300-357, `ix_expr` ~290-298, `child` ~245, payload-helper emission ~474-540, `CompiledRule` table emission ~742-770, the `find`/`apply` field types)
- Modify: `src/transform/src/eqsat/rules.rs` (`CompiledRule::find`/`apply` field types become generic-friendly fn pointers)
- Modify: `src/transform/src/eqsat/egraph/saturate.rs` (`saturate` builds a `BaseView` and calls rules through it)
- Test: byte-identity via the full suite (this is the P1 gate)

**Interfaces:**
- Consumes: `MatchGraph`/`ApplyGraph` (Task 1), generic helpers (Task 2).
- Produces: generated `fn find_NAME<G: MatchGraph>(g: &G, an: &Analyses, limit: usize) -> (Vec<EBindings>, bool)` and `fn apply_NAME<G: ApplyGraph>(g: &mut G, b: &EBindings) -> Result<Id, String>`. `CompiledRule.find`/`apply` become monomorphized fn pointers over the base view plus a registry the colored driver can call (see note).

**Monomorphization note:** Rust fn pointers cannot be generic. Two emission targets per rule:
1. `find_NAME_base(g: &BaseView, an, limit)` and `apply_NAME_base(g: &mut EGraph, b)` — concrete, stored in `CompiledRule` (unchanged field types, so `saturate` and all existing callers are untouched aside from constructing a `BaseView`).
2. For tagged rules only, also emit `find_NAME_colored(g: &mut ColoredView, an, limit)` and `apply_NAME_colored(g: &mut ColoredView, b)` and store them in a parallel `ColoredRule { name, find_colored, apply_colored }` table (Task 4/6 consume it).

Emit both by generating the rule body once into a helper that takes `&impl MatchGraph` is **not** possible across the fn-pointer boundary; instead, emit the body twice with the two concrete graph types. Keep the body generation (`body`/`child`/`guard`/`tmpl_stmts`) parameterized by the graph identifier string (`"g"`) and the graph type so the two emissions share one code path. The colored emissions are added in Task 6; **Task 3 emits only the `_base` variants** and proves byte-identity.

- [ ] **Step 1: Snapshot current goldens** (baseline for byte-identity)

Run: `bin/cargo-test -p mz-transform 2>&1 | tail -30`
Expected: record pass count (e.g. 345/345) as the baseline.

- [ ] **Step 2: Change `emit_find`/`emit_apply` to emit `_base` variants over `BaseView`**

In `emit_find` change the signature template to:
```rust
"fn find_{}_base(g: &crate::eqsat::egraph::view::BaseView, an: &Analyses, limit: usize) -> (Vec<EBindings>, bool) {{\n"
```
Replace root ranging `eg.rel_class_ids()` → `g.rel_class_ids()`; `index.get(&Sym::X)` → `g.nodes_by_sym(Sym::X)` (returns owned `Vec<(Id,ENode)>`; adapt the `for (root_id, root_node) in ...` loop to iterate the owned vec and bind `let root_id = root_id;`). In `child` (line 245) `eg.rel_class_nodes({class})` → `g.rel_class_nodes({class})`. In `cond_expr` map every `eg.cond_*`/free-fn call to the `g.cond_*` trait methods (e.g. `cond_all_true(eg.data(), &pl)` → `g.cond_all_true(&pl)`; `eg.arity(r)` → `g.arity(r)`). In `ix_expr` `eg.arity(...)` → `g.arity(...)`.

In `emit_apply` change to:
```rust
"fn apply_{}_base(g: &mut EGraph, b: &EBindings) -> Result<Id, String> {{\n let arities = eg_binding_arities(g, b);\n ..."
```
where `eg.add(CNode::Rel(...))` → `g.add_rel(...)`; `eg.binding_arities(b)` → `g.binding_arities(b)` (EGraph already has it; for the colored variant a trait method supplies it). Payload helpers `shift_payload(eg, ...)` → `shift_payload(g, ...)`, `cols_of_payload(eg.data(), ...)` → `cols_of_payload(&|id| g.escalar(id), ...)`, etc.

Update the `CompiledRule` table emission to reference `find_NAME_base` / `apply_NAME_base`.

- [ ] **Step 3: Update `saturate` to build a `BaseView`** (`saturate.rs:406,425`)

```rust
let view = crate::eqsat::egraph::view::BaseView { eg: self, index: &index, an: &analyses };
let (matches, hit_limit) = (rule.find)(&view, &analyses, MATCH_LIMIT + 1);
```
Wait — `rule.find` mutates nothing in Phase 1, but `view` borrows `self` immutably while Phase 2 needs `&mut self`. Phase 1 already collects matches into `pending` before any mutation (saturate.rs:397-414), so the immutable `view` is dropped before Phase 2. Keep the two-phase split. For `apply`, call `(compiled[qi].apply)(self, &b)` unchanged (apply takes `&mut EGraph`).

- [ ] **Step 4: Run the full suite — byte-identity gate**

Run: `bin/cargo-test -p mz-transform`
Expected: PASS, **same count as Step 1 baseline**, zero golden diffs.

Run: `REWRITE=1 bin/cargo-test -p mz-transform run_tests && git diff --stat src/transform`
Expected: **no `.spec` changes**.

- [ ] **Step 5: EXPLAIN slt spot-check** (a representative file)

Run: `bin/sqllogictest --optimized -- test/sqllogictest/transform/literal_lifting.slt`
Expected: PASS, no diff. (Pick any 2-3 transform EXPLAIN files.)

- [ ] **Step 6: Clippy + commit**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/build/codegen.rs src/transform/src/eqsat/rules.rs src/transform/src/eqsat/egraph/saturate.rs
git commit -m "SP4d P1: codegen emits base find/apply over BaseView (byte-identical)"
```

---

# Phase P2 — flat-color colored driver + rule tagging

## Task 4: Add the `colored` rule attribute (DSL → codegen → rules table)

**Files:**
- Modify: `src/transform/build/grammar.rs` (rule header ~583-600, after the `phase` parse)
- Modify: `src/transform/src/eqsat/dsl.rs` (`Rule` struct ~344-353: add `pub colored: bool`)
- Modify: `src/transform/build/codegen.rs` (derive + emit `colored` into the table; build-time assertion)
- Modify: `src/transform/src/eqsat/rules.rs` (`CompiledRule` gains `pub(crate) colored: bool`; add `CompiledRuleSet::colored_rules()`)
- Modify: `src/transform/src/eqsat/rules/relational.rewrite` (tag the targeted subset)
- Test: `rules.rs` `#[cfg(test)]`

**Interfaces:**
- Produces: `CompiledRuleSet::colored_rules(&self) -> Vec<&'static CompiledRule>` returning the tagged subset; build-time guarantee that a colored rule's conds are color-exact.

The targeted subset to tag (`relational.rewrite`, names verbatim): predicate-simplification — `drop_true_filter`, `empty_false_filter`; filter-structural — `merge_filters`, `push_filter_through_map`, `push_filter_past_flatmap`, `push_filter_through_negate`, `push_filter_through_threshold`, `push_filter_past_project`, `push_filter_into_join_first`, `push_filter_into_join_second`, `distribute_filter_union`, `distribute_filter_union_nary`. (Confirm each exists by name; tag only those whose conds are color-exact.)

Color-exact cond set (from the EGraph-surface map): all free-fn conds (`all_true`/`any_false`/`no_false`/`no_error`/`all_columns`/`uses_only_input`/`cols_in_range`/`identity_projection`), plus `not_rel_empty`/`is_rel_empty`/`has_three_or_more_inputs`/`is_binary_join`/`reads_indexed_global`/`has_inner_equiv`/`produces_key`/`join_is_cyclic`. **Excluded** (analysis-gated): `non_negative`, `monotonic`, `is_unique_key`.

- [ ] **Step 1: Write the failing test** (`rules.rs`)

```rust
#[mz_ore::test]
fn colored_rules_are_tagged_subset() {
    let set = all();
    let names: std::collections::BTreeSet<_> =
        set.colored_rules().iter().map(|r| r.name).collect();
    assert!(names.contains("drop_true_filter"));
    assert!(names.contains("merge_filters"));
    assert!(names.contains("push_filter_through_map"));
    // An analysis-gated rule must never be colored.
    assert!(!names.iter().any(|n| *n == "some_key_rule")); // replace with a real key/monotonic rule name
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform colored_rules_are_tagged_subset`
Expected: FAIL — `colored_rules` not found.

- [ ] **Step 3: Implement the attribute and the build-time assertion**

Grammar (`grammar.rs`): after `.then(kw("phase").ignore_then(phasekw).or_not())` add `.then(kw("colored").or_not().map(|o| o.is_some()))` and thread `colored` into the `Rule { .. }` map. (Order the parse so `colored` follows `phase`.)

`dsl.rs`: add `pub colored: bool` to `Rule`; default `false` everywhere a `Rule` is constructed in tests.

`codegen.rs`: in the table emission, add `colored: {}` from `r.colored`. Add a build-time assertion loop:
```rust
const COLOR_EXACT: &[&str] = &["all_true","any_false","no_false","no_error","all_columns",
  "uses_only_input","cols_in_range","identity_projection","not_rel_empty","is_rel_empty",
  "has_three_or_more_inputs","is_binary_join","reads_indexed_global","has_inner_equiv",
  "produces_key","join_is_cyclic"];
for r in rules {
    if r.colored {
        for c in &r.conds {
            assert!(cond_is_color_exact(c),
                "colored rule `{}` uses non-color-exact condition {:?}", r.name, c);
        }
    }
}
```
where `cond_is_color_exact` matches the `Cond::*` variants against the excluded analysis trio (`NonNegative`/`Monotonic`/`IsUniqueKey`) → false, all else → true.

`rules.rs`: add `pub(crate) colored: bool` to `CompiledRule`; add:
```rust
pub(crate) fn colored_rules(&self) -> Vec<&'static CompiledRule> {
    self.rules.iter().copied().filter(|r| r.colored).collect()
}
```

`relational.rewrite`: add the `colored` keyword to each tagged rule header (after `phase` if present, else after `doc`/before the pattern).

- [ ] **Step 4: Run to verify it passes**

Run: `bin/cargo-test -p mz-transform colored_rules_are_tagged_subset`
Expected: PASS.

- [ ] **Step 5: Full suite stays byte-identical** (tagging adds a field, no behavior change yet)

Run: `bin/cargo-test -p mz-transform`
Expected: PASS, same count; zero golden diffs.

- [ ] **Step 6: Clippy + commit**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/build/grammar.rs src/transform/build/codegen.rs src/transform/src/eqsat/dsl.rs src/transform/src/eqsat/rules.rs src/transform/src/eqsat/rules/relational.rewrite
git commit -m "SP4d P2: colored rule attribute + colored_rules() + build-time color-exact assertion"
```

## Task 5: `ColoredView` — `MatchGraph`/`ApplyGraph` over `ColoredEGraph` at a color

**Files:**
- Create: `src/transform/src/eqsat/colored/view.rs`
- Modify: `src/transform/src/eqsat/colored.rs` (`mod view;`)
- Test: `colored/view.rs` `#[cfg(test)]`

**Interfaces:**
- Consumes: `ColoredEGraph::{find,union,canon,visible_nodes,colored_class_members,add_colored}` (`colored.rs`/submodules); `MatchGraph`/`ApplyGraph` (Task 1).
- Produces: `ColoredView<'a,'b>` with `fn new(ceg: &'a mut ColoredEGraph<'b, CombinedLang>, color: ColorId, base: &'b EGraph) -> Self`; a `ColoredIndex = HashMap<Sym, Vec<(Id, ENode)>>` built from `visible_nodes(color)` under `canon`; impls of both traits.

```rust
pub(crate) struct ColoredView<'a, 'b> {
    pub ceg: &'a mut ColoredEGraph<'b, CombinedLang>,
    pub color: ColorId,
    pub base: &'b EGraph,
    /// EScalar for colored-delta scalar ids created during colored saturation
    /// (base ids resolve via base.data()). Populated on intern_scalar.
    pub delta_escalar: HashMap<Id, EScalar>,
    /// Per-color sym index over canon(color, ·) of visible_nodes(color).
    pub index: HashMap<Sym, Vec<(Id, ENode)>>,
}
```

Key implementation points:
- **`index` build:** for each `(id, node)` in `ceg.visible_nodes(color)`, if `CNode::Rel(e)`, push `(ceg.find(color, id), canon-resolved e)` under `e.sym()`. Build once per driver round (Task 6 rebuilds after each `close`).
- **`rel_class_ids`:** canonical-under-color reps of base rel classes plus colored rel delta classes — derive from the `index` values' ids (dedup).
- **`rel_class_nodes(id)`:** `index` entries whose canonical id equals `ceg.find(color, id)`.
- **`arity(id)`:** `base.try_arity(rep)` for a base member of the colored class (arity is congruence-invariant); fall back through `colored_class_members`.
- **`escalar(id)`:** `self.delta_escalar.get(&id).cloned().unwrap_or_else(|| base.data().escalar(id).clone())`.
- **color-exact conds:** reuse the existing free fns, supplying a resolver `|id| self.escalar(id)` for payload reads and `self.arity` for arities; structural conds (`not_rel_empty` etc.) read the colored class (a class is empty iff its color is unsat — Task 6/derive supplies that, else delegate to a base member).
- **analysis-gated conds:** return `false` (never reached for tagged rules; the build-time assertion guarantees it).
- **`ApplyGraph::add_rel`:** `self.ceg.add_colored(self.color, CNode::Rel(node))`.
- **`ApplyGraph::intern_scalar`:** lower the EScalar's expr to an `SNode` (via `crate::eqsat::scalar::lower::lower_into` against a colored sink — or hash-cons the scalar tree through `add_colored` for each `CNode::Scalar`), obtain a delta id, insert `delta_escalar.insert(id, e.clone())`, return id. (If `lower_into` requires `&mut EGraph`, add a small colored lowering helper that walks the expr and `add_colored`s each scalar node; the EScalar is already in hand for the cache.)

- [ ] **Step 1: Write the failing test** (`colored/view.rs`)

```rust
#[mz_ore::test]
fn colored_view_reads_canonicalize_under_color() {
    let mut eg = EGraph::new();
    let leaf = eg.add(CNode::Rel(ENode::Constant { card: 1, arity: 2, col_types: None }));
    let c0 = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(0)));
    let c1 = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(1)));
    let _ = leaf;
    eg.rebuild();
    let mut ceg = ColoredEGraph::new(&eg);
    let color = ceg.new_color(None);
    ceg.union(color, eg.find(c0), eg.find(c1)); // #0 ≅_c #1
    let view = ColoredView::new(&mut ceg, color, &eg);
    // Under the color, #0 and #1 canonicalize to the same id.
    assert_eq!(view.ceg.find(color, eg.find(c0)), view.ceg.find(color, eg.find(c1)));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform colored_view_reads_canonicalize_under_color`
Expected: FAIL — `ColoredView` not found.

- [ ] **Step 3: Implement `ColoredView` + `ColoredIndex` + both trait impls** as above.

- [ ] **Step 4: Run to verify it passes**

Run: `bin/cargo-test -p mz-transform colored_view`
Expected: PASS.

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/src/eqsat/colored/view.rs src/transform/src/eqsat/colored.rs
git commit -m "SP4d P2: ColoredView MatchGraph/ApplyGraph over a color"
```

## Task 6: Colored rule driver `colored_saturate`

**Files:**
- Create: `src/transform/src/eqsat/colored/saturate.rs`
- Modify: `src/transform/src/eqsat/colored.rs` (`mod saturate;`), `src/transform/src/eqsat/egraph.rs` (define `COLORED_MAX_ITERS`)
- Modify: `src/transform/build/codegen.rs` (emit `find_NAME_colored`/`apply_NAME_colored` for tagged rules; add `find_colored`/`apply_colored` to a `ColoredRule` table)
- Modify: `src/transform/src/eqsat/rules.rs` (`ColoredRule { name, find_colored, apply_colored }`; `colored_rules()` returns `&'static ColoredRule`)
- Test: `colored/saturate.rs` `#[cfg(test)]`

**Interfaces:**
- Consumes: `ColoredView` (Task 5), `colored_rules()` (Task 4), the emitted colored fn pointers.
- Produces: `pub(crate) fn colored_saturate(layer: &mut ColoredLayer<'_>, base: &EGraph, rules: &[ColoredRule]) -> usize` running each color's tagged subset to a bounded fixpoint; and `CompiledRuleSet::colored_rules_static(&self) -> &'static [ColoredRule]` returning the tagged colored-fn-pointer table (distinct from Task 4's `colored_rules()`, which returns `&'static CompiledRule`s for identification/tests).

Driver structure (per color, parent-first via `layer.ceg` color order):
```rust
pub(crate) fn colored_saturate(layer: &mut ColoredLayer<'_>, base: &EGraph,
                               rules: &[ColoredRule]) -> usize {
    let mut total = 0;
    for &color in &layer.all_colors_parent_first() {       // Task 7 supplies order; flat = decl order
        for _ in 0..COLORED_MAX_ITERS {
            let mut view = ColoredView::new(&mut layer.ceg, color, base); // builds the per-round index
            let mut pending: Vec<(usize, EBindings)> = Vec::new();
            let an = Analyses::default();                  // colored rules read no analyses
            for (qi, rule) in rules.iter().enumerate() {
                let (ms, _hit) = (rule.find_colored)(&mut view, &an, MATCH_LIMIT + 1);
                for b in ms.into_iter().take(MATCH_LIMIT) { pending.push((qi, b)); }
            }
            let mut changed = false;
            let mut equalities: Vec<(Id, Id)> = Vec::new();
            for (qi, b) in pending {
                if let Ok(new_id) = (rules[qi].apply_colored)(&mut view, &b) {
                    equalities.push((new_id, b.root));
                }
                // delta-node budget guard
                if view.ceg.colored_node_count(color) > MAX_ENODES { break; }
            }
            // Apply unions + close congruence in the color.
            let metrics = layer.ceg.close(color, &equalities);
            if metrics.induced_merges > 0 || metrics.applied_equalities > 0 { changed = true; }
            total += 1;
            if !changed { break; }
        }
    }
    total
}
```
Notes: `find_colored` takes `&mut ColoredView` because building matches may need `ceg.find`/`canon` (which are `&mut self` on `ColoredEGraph`). `apply_colored` adds colored nodes (mutates). The match-then-apply split avoids borrow conflicts within a round. `colored_node_count(color)` is a small accessor to add on `ColoredEGraph` (count of the color's delta nodes); `all_colors_parent_first()` returns flat-color order in P2 (replaced by forest pre-order in P3).

Codegen for colored variants: emit `fn find_NAME_colored(g: &mut ColoredView, an: &Analyses, limit) -> (Vec<EBindings>, bool)` and `fn apply_NAME_colored(g: &mut ColoredView, b) -> Result<Id, String>` using the **same body generator** as the base variants but with the colored graph type and `&mut` receiver for find (since colored reads mutate the layered UF). Only emit these for `r.colored` rules.

- [ ] **Step 1: Write the failing test** — a contextual simplification produces a colored conclusion (`drop_true_filter` under a context that makes a predicate true):

```rust
#[mz_ore::test]
fn colored_saturate_simplifies_under_context() {
    // Build Filter(#0 = #1)(leaf arity 2); under color #0 ≅ #1 the predicate
    // (#0 = #1) reduces; drop_true_filter then removes the filter in the color.
    let mut eg = EGraph::new();
    let leaf = eg.add(CNode::Rel(ENode::Constant { card: 1, arity: 2, col_types: None }));
    let eqp = eg.intern_scalar(&EScalar::plain(
        MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), mz_expr::BinaryFunc::Eq)));
    let filt = eg.add(CNode::Rel(ENode::Filter { predicates: vec![eqp], input: leaf }));
    eg.rebuild();
    // One flat color asserting #0 ≅ #1.
    let mut layer = /* build a ColoredLayer with one color unioning escalar(#0),escalar(#1) */;
    let n = colored_saturate(&mut layer, &eg, &CompiledRuleSet::all().colored_rules_static());
    assert!(n > 0);
    // Under the color, `leaf` is reachable as a colored conclusion of `filt`'s class.
    let members = layer.ceg.colored_class_members(layer.color_of[&eg.find(filt)], eg.find(filt));
    assert!(members.contains(&eg.find(leaf)));
}
```
(Adjust helper construction to the real `ColoredLayer` builder; the assertion is that the filter's class colored-merges with its input under the context.)

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform colored_saturate_simplifies_under_context`
Expected: FAIL — `colored_saturate` not found.

- [ ] **Step 3: Implement** the driver, the colored codegen variants, `ColoredRule` table, `colored_node_count`, `COLORED_MAX_ITERS`.

- [ ] **Step 4: Run to verify it passes**

Run: `bin/cargo-test -p mz-transform colored_saturate`
Expected: PASS.

- [ ] **Step 5: Full suite still byte-identical** (driver not yet wired into engine)

Run: `bin/cargo-test -p mz-transform`
Expected: PASS, same count, zero golden diffs.

- [ ] **Step 6: Clippy + commit**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/src/eqsat/colored/saturate.rs src/transform/src/eqsat/colored.rs src/transform/src/eqsat/egraph.rs src/transform/build/codegen.rs src/transform/src/eqsat/rules.rs
git commit -m "SP4d P2: colored_saturate driver + colored rule codegen variants"
```

---

# Phase P3 — hierarchical colors + remove-in-close

## Task 7: Hierarchical color forest from the fact lattice

**Files:**
- Modify: `src/transform/src/eqsat/colored_derive.rs` (`build_colored_layer` ~124; add forest derivation)
- Modify: `src/transform/src/eqsat/colored.rs` (`new_color(parent)` already supports a parent; add `all_colors_parent_first()` / expose color order)
- Test: `colored_derive.rs` `#[cfg(test)]`

**Interfaces:**
- Consumes: `DerivedScopes { scopes: Vec<ScopeEqualities>, class_scope, empty_classes }` (colored_derive.rs:87).
- Produces: `build_colored_layer` builds a forest: colors = distinct equality-sets; `parent(C)` = a maximal proper subset (deterministic tie-break: most equalities, then canonical `Vec<(Id,Id)>` order); each child color created with `new_color(Some(parent))` and applied only its **delta** equalities.

Derivation (replace the flat loop at build_colored_layer:126-133):
```rust
// scopes[i].unions are sorted-deduped equality-sets (Id,Id).
// 1. Order scopes by ascending |unions| so parents precede children.
let mut order: Vec<usize> = (0..scopes.scopes.len()).collect();
order.sort_by_key(|&i| (scopes.scopes[i].unions.len(), scopes.scopes[i].unions.clone()));
// 2. For each scope, find parent = the already-placed scope whose unions ⊂ this,
//    maximal by |unions|, tie-broken by the canonical order above.
let mut color_ids = vec![None; scopes.scopes.len()];
for &i in &order {
    let mine: BTreeSet<(Id,Id)> = scopes.scopes[i].unions.iter().copied().collect();
    let mut parent: Option<usize> = None;
    for &j in &order {
        if j == i || color_ids[j].is_none() { continue; }
        let theirs: &Vec<(Id,Id)> = &scopes.scopes[j].unions;
        if theirs.len() < mine.len() && theirs.iter().all(|e| mine.contains(e)) {
            // proper subset; keep the maximal (order is ascending, so last wins)
            parent = Some(j);
        }
    }
    let pcolor = parent.map(|j| color_ids[j].unwrap());
    let cid = ceg.new_color(pcolor);
    color_ids[i] = Some(cid);
    // Apply only the delta equalities (those not in the parent chain).
    let parent_eqs: BTreeSet<(Id,Id)> = match parent {
        Some(j) => scopes.scopes[j].unions.iter().copied().collect(),
        None => BTreeSet::new(),
    };
    for &(a, b) in &scopes.scopes[i].unions {
        if !parent_eqs.contains(&(a, b)) {
            ceg.union(color_ids[i].unwrap(), base.find(a), base.find(b));
        }
    }
}
```
(`color_of`/`empty_classes` mapping unchanged, using `color_ids[idx].unwrap()`.)

- [ ] **Step 1: Write the failing test** — three nested scopes ∅⊂{e1}⊂{e1,e2} form a chain:

```rust
#[mz_ore::test]
fn color_forest_is_inclusion_ordered() {
    // Construct DerivedScopes with scopes [{e1,e2}, {e1}] (out of order) and
    // assert the built layer makes {e1,e2}'s color a child of {e1}'s color.
    // ... build base eg with scalar ids a,b,c so e1=(a,b), e2=(a,c) ...
    let layer = build_colored_layer(&eg, scopes);
    let child = layer.color_for_unions(&[e1, e2]);
    let parent = layer.color_for_unions(&[e1]);
    assert_eq!(layer.ceg.parent_of(child), Some(parent));
}
```
(Add small test-only accessors `color_for_unions` / `parent_of` if needed, or assert via `chain_top_down(child)` containing `parent`.)

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform color_forest_is_inclusion_ordered`
Expected: FAIL.

- [ ] **Step 3: Implement** the forest derivation + color order accessor.

- [ ] **Step 4: Run to verify it passes; full suite byte-identical**

Run: `bin/cargo-test -p mz-transform color_forest_is_inclusion_ordered && bin/cargo-test -p mz-transform`
Expected: PASS; same count; zero golden diffs (forest not yet feeding extraction).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/src/eqsat/colored_derive.rs src/transform/src/eqsat/colored.rs
git commit -m "SP4d P3: hierarchical color forest from the fact lattice"
```

## Task 8: `remove`-in-`close`

**Files:**
- Modify: `src/transform/src/eqsat/colored/congruence.rs` (`close` ~85)
- Modify: `src/transform/src/eqsat/colored/union_find.rs` (already has `remove`)
- Test: `colored/congruence.rs` `#[cfg(test)]`

**Interfaces:**
- Consumes: `ColoredUnionFind::remove` (union_find.rs:69), `chain_top_down`.
- Produces: `close` drops, from a child color's delta, any edge an ancestor color already provides (so the child's delta stays minimal).

In `close`, after applying equalities and reaching the congruence fixpoint, for each colored-class edge in this color's delta, if the same pair is already merged in the parent chain (`chain_top_down(color)` minus `color`), call `self.colors[color].uf.remove(id)` for the redundant local rep. The class membership (via the layered `find`) is unchanged because the parent still provides the edge; only the duplicate child-local edge is removed.

- [ ] **Step 1: Write the failing test** — a child edge subsumed by the parent is removed but membership is preserved:

```rust
#[mz_ore::test]
fn remove_in_close_prunes_redundant_child_edge() {
    let mut eg = EGraph::new();
    let a = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(0)));
    let b = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(1)));
    eg.rebuild();
    let mut ceg = ColoredEGraph::new(&eg);
    let parent = ceg.new_color(None);
    ceg.close(parent, &[(eg.find(a), eg.find(b))]);
    let child = ceg.new_color(Some(parent));
    // Re-assert the same edge in the child; close should remove it as redundant.
    ceg.close(child, &[(eg.find(a), eg.find(b))]);
    // Membership preserved (inherited from parent) ...
    assert_eq!(ceg.find(child, eg.find(a)), ceg.find(child, eg.find(b)));
    // ... but the child's local delta no longer stores the edge.
    assert!(ceg.color_delta_is_empty(child)); // small test accessor
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform remove_in_close_prunes_redundant_child_edge`
Expected: FAIL.

- [ ] **Step 3: Implement** the redundancy check + `remove` call in `close`; add `color_delta_is_empty` test accessor.

- [ ] **Step 4: Run to verify it passes; full colored suite green**

Run: `bin/cargo-test -p mz-transform colored::`
Expected: PASS (the SP3b oracle/congruence tests must still agree).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/src/eqsat/colored/congruence.rs src/transform/src/eqsat/colored/union_find.rs
git commit -m "SP4d P3: remove-in-close prunes ancestor-provided child edges"
```

---

# Phase P4 — incremental dirty-tracking congruence

## Task 9: Dirty-set incremental `close`

**Files:**
- Modify: `src/transform/src/eqsat/colored/congruence.rs` (`close` ~85)
- Test: `colored/congruence.rs` `#[cfg(test)]`

**Interfaces:**
- Produces: `close` re-examines only nodes whose canonical children changed since the last batch, to a fixpoint; a `#[cfg(debug_assertions)]` check that the incremental fixpoint equals the full-pass fixpoint.

Maintain a `dirty: VecDeque<Id>` seeded with the nodes touched by the applied equalities (the classes whose reps changed). Process a node by recomputing `canon(color, node)` and merging congruent nodes; when a merge changes a rep, enqueue the parents of the affected classes. Keep the existing full-pass loop behind `#[cfg(any(test, debug_assertions))]` and assert the two reach the same partition.

- [ ] **Step 1: Write the failing test** — incremental == full-pass on a seeded congruence:

```rust
#[mz_ore::test]
fn incremental_close_equals_full_pass() {
    // Build a base with f(x) and f(y) nodes; union x≅y in a color; closing must
    // merge f(x)≅f(y). Run close (incremental) and assert the resulting
    // partition equals a from-scratch full-pass close on a clone.
    // (Use the existing oracle helper in colored/oracle.rs to compare partitions.)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform incremental_close_equals_full_pass`
Expected: FAIL (until the incremental path + accessor exist) — or PASS trivially if full-pass is still the only path; in that case first introduce the incremental path so the assertion is meaningful.

- [ ] **Step 3: Implement** the dirty-set incremental closure + the debug-assertion parity check.

- [ ] **Step 4: Run to verify it passes; full colored + oracle suite**

Run: `bin/cargo-test -p mz-transform colored::`
Expected: PASS.

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/src/eqsat/colored/congruence.rs
git commit -m "SP4d P4: incremental dirty-tracking congruence in close"
```

---

# Phase P5 — extraction integration + wiring + goldens

## Task 10: Colored relational candidates in extraction

**Files:**
- Modify: `src/transform/src/eqsat/extract.rs` (`build_rel` — where `resolve_scalar_colored` is called)
- Modify: `src/transform/src/eqsat/colored_derive.rs` (add a per-class colored-conclusion enumerator)
- Test: `extract.rs` `#[cfg(test)]`

**Interfaces:**
- Consumes: `ColoredEGraph::extract_colored<C: colored::CostModel<L>>` (colored/extract.rs:29); the real `cost::CostModel` (cost.rs:338).
- Produces: in `build_rel`, when expanding a relational class `id` under `color_of[id]`, the candidate set includes the **cheapest colored conclusion** for that class (a colored delta e-node), costed in the final choice by the real `cost::CostModel` on the built subtree.

Approach (mirrors SP4b `resolve_scalar_colored`, lifted to relational):
- Add `fn colored_rel_conclusion(layer, base, color, id) -> Option<ENode>` that runs `layer.ceg.extract_colored(color, &TreeSizeColoredCost)` once per color (cache per color), returning the lowest-tree-size colored e-node for `id`'s colored class if it differs from the base nodes. `TreeSizeColoredCost` is a `colored::CostModel<CombinedLang>` scoring `node_count`-style (relational nodes count 1 + children), the colored analogue of SP4b's scalar `scalar_expr_cost` selection.
- In `build_rel`, when choosing the best e-node for `id`, append the colored conclusion (if any) to the candidate e-nodes; the existing real-cost selection then picks base-or-colored by the true cost. This retires `extract_colored` (now production-called) while keeping the final decision on `cost::CostModel`.

- [ ] **Step 1: Write the failing test** — extraction prefers a colored conclusion when it is cheaper:

```rust
#[mz_ore::test]
fn extraction_picks_colored_conclusion_when_cheaper() {
    // Filter(#0 = #1)(leaf) with a color #0 ≅ #1 and colored_saturate run so the
    // filter's class has a colored conclusion == leaf (filter removed). Extraction
    // under that color must return the bare leaf (cheaper) not the Filter.
    // ... build eg + layer, run colored_saturate, then extract_with(root,...,Some(&mut layer)) ...
    let rel = eg.extract_with(root, &model, &ArrangementCount, Some(&mut layer)).unwrap();
    assert!(matches!(rel, Rel::Constant { .. })); // filter elided under context
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform extraction_picks_colored_conclusion_when_cheaper`
Expected: FAIL.

- [ ] **Step 3: Implement** `TreeSizeColoredCost`, `colored_rel_conclusion`, and the `build_rel` candidate extension.

- [ ] **Step 4: Run to verify it passes**

Run: `bin/cargo-test -p mz-transform extraction_picks_colored`
Expected: PASS.

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/src/eqsat/extract.rs src/transform/src/eqsat/colored_derive.rs
git commit -m "SP4d P5: colored relational conclusions as extraction candidates"
```

## Task 11: Wire `colored_saturate` into the engine (alongside; differential capture)

**Files:**
- Modify: `src/transform/src/eqsat/engine.rs` (3 sites: ~234, ~300, ~450; insert after `build_colored_layer`, before `extract`)
- Test: `engine.rs` `#[cfg(test)]` differential helper

**Interfaces:**
- Consumes: `colored_saturate` (Task 6), `ColoredLayer` (now hierarchical), the extended `extract_with` (Task 10).
- Produces: each site runs `colored_saturate(&mut layer, &eg, self.rules.colored_rules_static())` after `build_colored_layer` and before `extract`; close-all already ran in build_colored_layer (Task 7) or is invoked here.

At each site replace:
```rust
let mut layer = build_colored_layer(&eg, scopes);
```
with:
```rust
let mut layer = build_colored_layer(&eg, scopes);
colored_saturate(&mut layer, &eg, self.rules.colored_rules_static());
```
(`colored_rules_static()` returns `&'static [ColoredRule]` filtered to tagged, from Task 6; reachable via `self.rules` like the base rule set.)

- [ ] **Step 1: Capture the corpus differential (alongside, before goldens move)**

Add a test/debug path or a one-off harness that, for the eqsat datadriven corpus, records the extracted plan with colored saturation ON vs OFF (e.g. a `MZ_EQSAT_COLORED_SAT=0/1` env switch read once at engine construction). Run `REWRITE=0 bin/cargo-test -p mz-transform run_tests` with both settings and diff. **Do not regenerate yet.**

Run: `bin/cargo-test -p mz-transform run_tests 2>&1 | tail -40`
Expected: a list of `.spec` cases whose output changes (the differential to justify in Task 12).

- [ ] **Step 2: Implement the wiring** at all 3 sites.

- [ ] **Step 3: Run unit + colored tests (not goldens yet)**

Run: `bin/cargo-test -p mz-transform --lib`
Expected: PASS (unit tests; golden `.spec`/slt addressed in Task 12).

- [ ] **Step 4: Clippy + commit (goldens deliberately not yet regenerated)**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/src/eqsat/engine.rs
git commit -m "SP4d P5: wire colored_saturate into the engine (alongside)"
```

## Task 12: Justify differential, regenerate goldens, headline test, final gate

**Files:**
- Modify: eqsat `*.spec` goldens (`src/transform/tests/...`) and transform EXPLAIN slt under `test/sqllogictest/transform/` as the differential dictates
- Test: a headline `#[mz_ore::test]` asserting the filter-placement case is recovered at the eqsat-output level

**Interfaces:**
- Consumes: the differential list (Task 11 Step 1).

- [ ] **Step 1: Write the headline test** — the SP4b filter-placement case now resolves at saturation time:

```rust
#[mz_ore::test]
fn colored_saturation_recovers_filter_placement() {
    // The SP4b-regressed case: a tautology Filter(#0=#0) above a join under a
    // context where #0=#0 holds. With colored saturation, the eqsat-extracted
    // plan no longer carries the redundant filter above the join.
    // Build the exact plan from the eqsat.spec case; extract; assert the filter
    // is elided / pushed in the eqsat output (NOT relying on downstream norm).
}
```
(Recover the exact input from the `eqsat.spec` case named in the SP4b ledger; assert on the eqsat-extracted `Rel`.)

- [ ] **Step 2: Run it — confirm the capability**

Run: `bin/cargo-test -p mz-transform colored_saturation_recovers_filter_placement`
Expected: PASS (this is the §1 success criterion).

- [ ] **Step 3: Regenerate datadriven goldens and review each per-file**

Run: `REWRITE=1 bin/cargo-test -p mz-transform run_tests`
Then: `git diff src/transform/tests` — review every moved case against the differential list; each must be a justified consequence of a contextual rewrite now firing at saturation time (filter elided/pushed under a valid context). Anything else is a regression to investigate before proceeding.

- [ ] **Step 4: Regenerate any moved EXPLAIN slt per-file-standalone**

For each transform slt the differential touches:
Run: `bin/sqllogictest --optimized -- --rewrite-results test/sqllogictest/transform/<file>.slt`
Then re-run without `--rewrite-results` to confirm it passes standalone; review the diff.

- [ ] **Step 5: Full suite + clippy final gate**

Run: `bin/cargo-test -p mz-transform`
Expected: PASS (all unit + datadriven).
Run: `cargo clippy -p mz-transform --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add -A src/transform test/sqllogictest/transform
git commit -m "SP4d P5: regenerate goldens for colored saturation + headline filter-placement test"
```

---

## Final whole-branch review

After Task 12, dispatch the final whole-branch review (most capable model) over the range from the P1 base commit to HEAD. Focus areas: (1) P1 byte-identity held until P5; (2) colored conclusions never mutate the base or escape their color; (3) the build-time color-exact assertion actually fires on a tagged analysis-gated rule; (4) `extract_colored`/`add_colored`/`close`/`close_all`/`remove` are all production-reachable; (5) every moved golden is justified by a contextual rewrite, none structural-only; (6) `Rel::node_count()` untouched. Then use superpowers:finishing-a-development-branch.

## Self-review notes (plan vs spec)

- **Spec §5.A (trait abstraction):** Tasks 1-3. **§5.A rule tagging:** Task 4. **§5.B colored driver:** Tasks 5-6. **§5.C hierarchical forest + remove:** Tasks 7-8. **§5.D incremental congruence:** Task 9. **§5.E color-threaded relational extraction:** Task 10. **Pipeline wiring (§5 step 6-7):** Tasks 11-12.
- **§4.1 byte-identity:** gated at Task 3 Step 4 and re-checked at Tasks 4/6/7 (zero golden movement until P5).
- **§4.2 soundness:** build-time color-exact assertion (Task 4) + confinement (ColoredView writes only `add_colored`, Task 5).
- **§8 only P5 moves goldens:** Tasks 1-9 each assert zero golden diff; only Task 12 regenerates.
- **Open implementation detail flagged for implementers:** the exact `build_rel` candidate-insertion point (Task 10) and the colored scalar lowering inside `ColoredView::intern_scalar` (Task 5) are read-then-implement against the live files; signatures and approach are fixed here, the few surrounding lines are confirmed in-file.
