# SP2b Slice 1: the scalar-DSL seam (go/no-go) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove end-to-end that one scalar rewrite rule (`not_not`) can be written declaratively, compiled by the existing build-time codegen into a `CompiledRule` over `EGraph<CombinedLang>`, matched and applied inside the single combined e-graph, and extracted with byte-identical results to the old standalone `EGraph<ScalarLang>` engine.

**Architecture:** Extend the one relational rewrite-DSL grammar and its codegen with a fixed-func scalar-unary pattern (Approach A, one grammar over `CNode`). Scalar-rooted rules compile into a separate `SCALAR_COMPILED_RULES` static so they never fire in the relational saturation pass. A new scalar saturate driver runs those rules on a `CombinedLang` graph seeded by a scalar lower, with a ported determinism-parity extractor. A differential test asserts the new path equals the old engine on a committed corpus. This is the go/no-go: if `not_not` cannot reach extraction identity here, the one-grammar-over-CNode approach is reassessed having ported only one rule.

**Tech Stack:** Rust, the `mz-transform` crate, a `chumsky`-based build-time grammar, datadriven tests, Lean 4 (theorem emission, `sorry`-stubbed).

## Global Constraints

- **Behavior-neutral, strict gate.** No `--rewrite`, no `cargo insta accept`. The new path must reproduce the old engine's output exactly.
- **`ScalarLang` the type stays.** `combined.rs` delegates to it for `CNode::Scalar`. Do not delete `ScalarLang`, `SNode`, `ScalarGraphData`, `scalar/analysis.rs`. This slice deletes nothing (delete-last is slice 7).
- **Production untouched.** The three production callers of `crate::eqsat::scalar::canonicalize_predicates` (`predicate_pushdown.rs`, `literal_constraints.rs`, `fusion/filter.rs`) stay on the old `EGraph<ScalarLang>` engine. This slice adds a NEW path used only by the differential test.
- **Relational grammar byte-unchanged.** `relational.rewrite` is not edited. All codegen changes are additive scalar arms.
- **Scalar saturate bounds are copied constants, mirroring `scalar/egraph.rs`:** `MAX_ENODES = 600`, `MATCH_LIMIT = 1_000`, `MAX_ITERS = 100`.
- **Determinism parity:** the ported extractor copies the old tie-break `ca.cmp(&cb).then_with(|| a.cmp(b))` (cost then `SNode::Ord`) and the And/Or operand `sort()` verbatim.
- **Cheap checks before every commit:** `bin/fmt` and `cargo check -p mz-transform`.
- **Test command (mz-test skill):** unit tests via `bin/cargo-test -p mz-transform <filter>`; slt via `bin/sqllogictest --optimized -- test/sqllogictest/transform/`.

---

## File Structure

**New files:**
- `src/transform/src/eqsat/rules/scalar.rewrite`: scalar rule source (one grammar, separate file for human organization by sort).
- `src/transform/src/eqsat/scalar_extract.rs`: determinism-parity scalar extractor over `EGraph<CombinedLang>`, exposes `pub fn raise(eg, id)`.
- `src/transform/src/eqsat/scalar_saturate.rs`: scalar saturate driver + `canonicalize_combined` entry, over `EGraph<CombinedLang>`, using copied bounds and `SCALAR_COMPILED_RULES`.
- `src/transform/tests/testdata/eqsat_scalar_corpus`: committed differential corpus fixture (datadriven).
- `src/transform/tests/eqsat_scalar_parity.rs`: the differential harness test.

**Modified files:**
- `src/transform/src/eqsat/dsl.rs`: add `Pat::SUnary { func, input }`.
- `src/transform/build/grammar.rs`: parse `Unary[<func>](child)` into `Pat::SUnary`.
- `src/transform/build/codegen.rs`: scalar arms in `sym_name`/`Matcher::node`/`Matcher::child`/`find_stmts`; a scalar-func-keyword table; split emitted rules into `COMPILED_RULES` (relational-root) and `SCALAR_COMPILED_RULES` (scalar-root).
- `src/transform/build.rs`: read `scalar.rewrite` in addition to `relational.rewrite`, concatenate rule lists.
- `src/transform/src/eqsat/egraph/view.rs`: scalar methods on `MatchGraph`; `scalar_index` field on `BaseView`; impls.
- `src/transform/src/eqsat/egraph/combined.rs`: `scalar_index()` builder + `add_scalar`/`lower_scalar` helpers on `EGraph<CombinedLang>`.
- `src/transform/src/eqsat/rules.rs`: expose `SCALAR_COMPILED_RULES` via a `scalar_all()` accessor.
- `src/transform/src/eqsat/lean.rs`: scalar denotation dispatch + `emit_rule` scalar arm (sorry).
- `src/transform/lean/MirRewrite/Semantics.lean`: bounded scalar denotation for `not`.
- `src/transform/src/eqsat.rs` (or the eqsat mod file): register the new modules `scalar_extract`, `scalar_saturate`.

---

## Interfaces (cross-task contract)

- `MatchGraph` gains: `fn scalar_class_nodes(&self, id: Id) -> Vec<SNode>;` and `fn nodes_by_scalar_sym(&self, sym: ScalarSym) -> Vec<(Id, SNode)>;`
- `BaseView` gains field `scalar_index: &'a ScalarIndex` where `pub(crate) type ScalarIndex = HashMap<ScalarSym, Vec<(Id, SNode)>>;`
- `EGraph<CombinedLang>` (alias `EGraph`) gains: `pub(crate) fn scalar_index(&self) -> ScalarIndex;`, `pub(crate) fn lower_scalar(&mut self, expr: &MirScalarExpr) -> Id;`
- `scalar_extract::raise(eg: &EGraph, id: Id) -> MirScalarExpr` (pinned public API for slices 4-6).
- `scalar_saturate::saturate(eg: &mut EGraph) -> usize` and `scalar_saturate::canonicalize_combined(expr: &MirScalarExpr, col_types: &[ReprColumnType]) -> MirScalarExpr`.
- `rules::scalar_all() -> CompiledRuleSet` (backed by `SCALAR_COMPILED_RULES`).
- Codegen: a scalar rule is one whose `rule.lhs` root is a scalar `Pat` variant (`Pat::SUnary` in this slice); such rules are emitted into `SCALAR_COMPILED_RULES`, all others into `COMPILED_RULES` (unchanged behavior for relational rules).

---

### Task 1: Scalar match methods on the view surface

**Files:**
- Modify: `src/transform/src/eqsat/egraph/view.rs`
- Modify: `src/transform/src/eqsat/egraph/combined.rs`
- Test: `src/transform/src/eqsat/egraph/view.rs` (inline `#[cfg(test)]`)

**Interfaces:**
- Produces: `MatchGraph::scalar_class_nodes`, `MatchGraph::nodes_by_scalar_sym`; `BaseView.scalar_index` field; `type ScalarIndex`; `EGraph::scalar_index()`.
- Consumes: existing `EGraph` (`= core::EGraph<CombinedLang>`), `CNode`, `SNode`, `ScalarSym`, `core::Id`.

- [ ] **Step 1: Add the `ScalarIndex` type and `EGraph::scalar_index()` builder in `combined.rs`**

After the `Index` alias (`combined.rs:176`), add:

```rust
/// Match index for scalar e-nodes, bucketed by scalar operator symbol. Built
/// per saturation round by the scalar saturate driver, mirroring the relational
/// `Index`. Kept separate so the relational matcher never sees scalar nodes and
/// vice versa.
pub(crate) type ScalarIndex = std::collections::HashMap<ScalarSym, Vec<(Id, SNode)>>;
```

Add an `impl EGraph` method (in `combined.rs`, near the type alias):

```rust
impl EGraph {
    /// Bucket every scalar e-node by its `ScalarSym`. One entry per (class, node).
    pub(crate) fn scalar_index(&self) -> ScalarIndex {
        let mut idx: ScalarIndex = std::collections::HashMap::new();
        for id in self.class_ids() {
            for node in self.nodes(id) {
                if let CNode::Scalar(s) = node {
                    idx.entry(ScalarLang::symbol(&s))
                        .or_default()
                        .push((id, s));
                }
            }
        }
        idx
    }
}
```

- [ ] **Step 2: Add scalar methods to the `MatchGraph` trait**

In `view.rs`, inside `pub(crate) trait MatchGraph { ... }` (after `escalar`, view.rs:34), add:

```rust
    /// The scalar e-nodes of class `id` (empty if the class holds no scalar nodes).
    fn scalar_class_nodes(&self, id: Id) -> Vec<SNode>;
    /// Every scalar e-node whose operator symbol is `sym`, as `(class, node)`.
    fn nodes_by_scalar_sym(&self, sym: ScalarSym) -> Vec<(Id, SNode)>;
```

Add the imports at the top of `view.rs`:

```rust
use crate::eqsat::egraph::combined::ScalarIndex;
use crate::eqsat::scalar::lang::ScalarSym;
use crate::eqsat::scalar::node::SNode;
```

- [ ] **Step 3: Add the `scalar_index` field to `BaseView` and implement the two methods**

Change the `BaseView` struct (view.rs:71-75) to:

```rust
pub(crate) struct BaseView<'a> {
    pub eg: &'a EGraph,
    pub index: &'a Index,
    pub scalar_index: &'a ScalarIndex,
    pub an: &'a Analyses,
}
```

In `impl<'a> MatchGraph for BaseView<'a>`, add:

```rust
    fn scalar_class_nodes(&self, id: Id) -> Vec<SNode> {
        self.eg
            .nodes(id)
            .into_iter()
            .filter_map(|n| match n {
                crate::eqsat::egraph::combined::CNode::Scalar(s) => Some(s),
                _ => None,
            })
            .collect()
    }

    fn nodes_by_scalar_sym(&self, sym: ScalarSym) -> Vec<(Id, SNode)> {
        self.scalar_index.get(&sym).cloned().unwrap_or_default()
    }
```

(`EGraph::nodes` is `pub(crate)`; `class_ids` too. Confirm both are reachable from `view.rs`/`combined.rs`, which are in the same crate.)

- [ ] **Step 4: Fix the existing `BaseView` constructor in `saturate.rs`**

`saturate.rs` builds `BaseView { eg, index, an }` (saturate.rs:399-403). Add the new field so the relational pass compiles with an empty scalar index (scalar rules do not run in the relational pass):

Just before the `let view = ...BaseView {` line, add:

```rust
            let scalar_index = crate::eqsat::egraph::combined::ScalarIndex::new();
```

and add `scalar_index: &scalar_index,` to the struct literal.

- [ ] **Step 5: Write the failing test**

Add to `view.rs` a `#[cfg(test)]` module:

```rust
#[cfg(test)]
mod scalar_view_tests {
    use super::*;
    use crate::eqsat::egraph::combined::{CNode, EGraph};
    use crate::eqsat::scalar::node::SNode;
    use crate::eqsat::scalar::lang::ScalarSym;
    use mz_expr::{UnaryFunc, MirScalarExpr};

    #[mz_ore::test]
    fn scalar_index_buckets_unary() {
        let mut eg = EGraph::new();
        let x = eg.add(CNode::Scalar(SNode::Column(
            0,
            mz_ore::treat_as_equal::TreatAsEqual(None),
        )));
        let not = eg.add(CNode::Scalar(SNode::CallUnary {
            func: UnaryFunc::Not(mz_expr::func::Not),
            expr: x,
        }));
        let idx = eg.scalar_index();
        let unary = idx.get(&ScalarSym::Unary).cloned().unwrap_or_default();
        assert_eq!(unary.len(), 1);
        assert_eq!(unary[0].0, eg.find(not));
    }
}
```

- [ ] **Step 6: Run the test, expect FAIL then PASS**

Run: `bin/cargo-test -p mz-transform scalar_index_buckets_unary`
Expected first: compile error / FAIL until Steps 1-4 are in. After: PASS.

- [ ] **Step 7: Commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/src/eqsat/egraph/view.rs src/transform/src/eqsat/egraph/combined.rs src/transform/src/eqsat/egraph/saturate.rs
git commit -m "eqsat: scalar match methods + scalar index on the combined view"
```

---

### Task 2: `Pat::SUnary` AST variant + grammar

**Files:**
- Modify: `src/transform/src/eqsat/dsl.rs`
- Modify: `src/transform/build/grammar.rs`
- Test: `src/transform/build/grammar.rs` (inline `#[cfg(test)]`, or a datadriven parse test if one exists)

**Interfaces:**
- Produces: `Pat::SUnary { func: String, input: Box<Pat> }`. `func` is the scalar-func keyword text (e.g. `"not"`), resolved to a concrete `UnaryFunc` by codegen (Task 3).
- Consumes: existing `Pat` enum, chumsky grammar combinators (`kw`, `bracket_ident`, `pat`).

- [ ] **Step 1: Add the variant to `Pat`**

In `dsl.rs`, inside `pub enum Pat`, add (after `RelVar(String)`):

```rust
    /// A scalar unary call with a FIXED function, e.g. `Unary[not](x)`. `func`
    /// is the scalar-func keyword text, resolved to a concrete `UnaryFunc` by
    /// codegen. Func-metavar binding (a bound `UnaryFunc`) is a later-slice
    /// capability and is a distinct variant when it lands.
    SUnary { func: String, input: Box<Pat> },
```

- [ ] **Step 2: Parse it in the grammar**

In `grammar.rs`, in the `pat` recursive parser (alongside the `filter` etc. arms, near grammar.rs:304), add:

```rust
    let sunary = kw("Unary")
        .ignore_then(bracket_ident())
        .then(pat.clone())
        .map(|(func, input)| Pat::SUnary {
            func,
            input: Box::new(input),
        });
```

Add `sunary` to the `choice((...))` list (grammar.rs:382-385), before `relvar`.

- [ ] **Step 3: Write the failing parse test**

Add a test that `parse` accepts a one-rule `scalar.rewrite`-style source with `Unary[not](Unary[not](x)) => x`:

```rust
#[test]
fn parses_scalar_unary_nested() {
    let src = "rule not_not { Unary[not](Unary[not](x)) => x }";
    let rules = crate::grammar::parse(src).expect("parses");
    assert_eq!(rules.len(), 1);
    match &rules[0].lhs {
        crate::dsl::Pat::SUnary { func, input } => {
            assert_eq!(func, "not");
            assert!(matches!(**input, crate::dsl::Pat::SUnary { .. }));
        }
        other => panic!("expected SUnary, got {other:?}"),
    }
}
```

(Place it where existing grammar tests live; if none, add a `#[cfg(test)] mod tests` in `grammar.rs`. `grammar::parse` returns `Result<Vec<Rule>, _>` per `build.rs:50`.)

- [ ] **Step 4: Run the test**

Because `grammar.rs` is a build-script module, run it via the build-script's own test target if present, otherwise verify by triggering a build that parses a scalar rule in Task 8. If the build module has no test harness, assert this in Task 8's compile instead and note it here.

Run: `cargo check -p mz-transform` (the build script parses; a grammar error fails the build).
Expected: builds (the new arm parses; nothing emits it yet).

- [ ] **Step 5: Commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/src/eqsat/dsl.rs src/transform/build/grammar.rs
git commit -m "eqsat dsl: Pat::SUnary fixed-func scalar unary pattern + grammar"
```

---

### Task 3: Codegen scalar match arms + `SCALAR_COMPILED_RULES` split

**Files:**
- Modify: `src/transform/build/codegen.rs`
- Test: exercised by Task 8's build + Task 9 harness (codegen has no standalone test target).

**Interfaces:**
- Produces: generated `find_not_not_base` that enumerates scalar-unary roots and matches nested `SNode::CallUnary { func: UnaryFunc::Not(_), .. }`; a `SCALAR_COMPILED_RULES` static; `apply_not_not_base` reuses the existing `Tmpl::RelVar` path (no new apply arm).
- Consumes: `Pat::SUnary`, `Matcher`, `find_stmts`, `emit_compiled`, `ScalarSym`, `SNode`.

- [ ] **Step 1: Add a scalar-func keyword resolver**

Near `sym_name` (codegen.rs:99), add a helper mapping the func keyword to the Rust match-pattern text for a fixed `UnaryFunc`:

```rust
/// The Rust pattern text matching a fixed scalar `UnaryFunc` by keyword. Extend
/// this table as scalar rules reference more unary functions.
fn unary_func_pat(func: &str) -> String {
    match func {
        "not" => "mz_expr::UnaryFunc::Not(_)".to_string(),
        other => panic!("unknown scalar unary func keyword: {other}"),
    }
}
```

- [ ] **Step 2: Scalar root enumeration in `find_stmts`**

In `find_stmts` (codegen.rs:496), the `_ =>` arm computes `sym_name(&rule.lhs)` and loops `for (root_id, root_node) in g.nodes_by_sym(Sym::{sym})`. Add a scalar branch BEFORE that arm handles a scalar root. Change the `match &rule.lhs` to add:

```rust
        Pat::SUnary { .. } => {
            s.push_str(
                "for (root_id, root_node) in g.nodes_by_scalar_sym(crate::eqsat::scalar::lang::ScalarSym::Unary) {\n",
            );
            s.push_str("let root_id = root_id;\n");
            s.push_str("let root_node = &root_node;\n");
            m.node(&rule.lhs, "root_node");
            for stmt in &m.stmts {
                s.push_str(stmt);
                s.push('\n');
            }
            s.push_str(&body(rule, &m, mode));
            for _ in 0..m.open_braces {
                s.push_str("}\n");
            }
            s.push_str("}\n");
        }
```

- [ ] **Step 3: Scalar arm in `Matcher::node`**

In `Matcher::node` (codegen.rs:140), add before the `Pat::RelVar(_) => unreachable!(...)` arm:

```rust
            Pat::SUnary { func, input } => {
                let c = self.fresh.id();
                let fpat = unary_func_pat(func);
                self.stmts.push(format!(
                    "let SNode::CallUnary {{ func: {fpat}, expr: e{c} }} = {node} else {{ continue }};"
                ));
                self.child(input, &format!("e{c}"));
            }
```

- [ ] **Step 4: Scalar operator children in `Matcher::child`**

`Matcher::child` (codegen.rs:284) scans relational child classes via `g.rel_class_nodes`. A scalar operator child must scan scalar nodes. Change the `_ =>` arm to branch on whether the child pattern is scalar:

```rust
        _ => {
            let c = self.fresh.id();
            let scalar = matches!(pat, Pat::SUnary { .. });
            if scalar {
                self.stmts
                    .push(format!("for n{c} in g.scalar_class_nodes({class}) {{"));
            } else {
                self.stmts
                    .push(format!("for n{c} in g.rel_class_nodes({class}) {{"));
            }
            self.stmts.push(format!("let n{c} = &n{c};"));
            self.open_braces += 1;
            self.node(pat, &format!("n{c}"));
        }
```

(Note: `scalar_class_nodes` returns `Vec<SNode>` by value, so `let n{c} = &n{c};` reborrows for match ergonomics, same as the relational path.)

- [ ] **Step 5: Split emitted rules into two statics**

In `emit_compiled` (codegen.rs:838), classify each rule by root sort and emit two tables. Add a helper:

```rust
fn is_scalar_rule(r: &Rule) -> bool {
    matches!(r.lhs, Pat::SUnary { .. })
}
```

Change `emit_compiled` to emit `COMPILED_RULES` from `rules.iter().filter(|r| !is_scalar_rule(r))` and a second block `SCALAR_COMPILED_RULES` from `rules.iter().filter(|r| is_scalar_rule(r))`. Both use the identical `CompiledRule { ... find: find_NAME_base, apply: apply_NAME_base }` literal already emitted; only the partition and the static name differ. The `find`/`apply` function bodies are emitted for ALL rules (unchanged loop at codegen.rs:840-845). Emit:

```rust
    s.push_str(
        "/// Every scalar-sort rewrite rule, compiled to `find`/`apply` functions.\n\
         pub(crate) static SCALAR_COMPILED_RULES: &[CompiledRule] = &[\n",
    );
    // ... same per-rule literal loop, filtered to is_scalar_rule ...
    s.push_str("];\n");
```

- [ ] **Step 6: Verify via build + a smoke check**

Cannot fully test until Task 8 adds the rule. After Task 8, `cargo check -p mz-transform` must compile the generated `find_not_not_base`/`apply_not_not_base` and the `SCALAR_COMPILED_RULES` static. Add a note in the commit that Task 8 is the exercising build.

- [ ] **Step 7: Commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/build/codegen.rs
git commit -m "eqsat codegen: scalar-unary match arms + SCALAR_COMPILED_RULES split"
```

---

### Task 4: CombinedLang scalar lower

**Files:**
- Modify: `src/transform/src/eqsat/egraph/combined.rs`
- Test: inline `#[cfg(test)]` in `combined.rs`

**Interfaces:**
- Produces: `EGraph::lower_scalar(&mut self, expr: &MirScalarExpr) -> Id`, lowers a `MirScalarExpr` into `CNode::Scalar` classes, reusing `scalar::lower::snode_of`.
- Consumes: `scalar::lower::snode_of` (already `pub(crate)`, lower.rs:25), `CNode::Scalar`, `SNode`.

- [ ] **Step 1: Add `lower_scalar`**

In `combined.rs`, in the `impl EGraph` block, add:

```rust
    /// Lower a `MirScalarExpr` into this combined e-graph as `CNode::Scalar`
    /// classes, returning the root scalar class. Reuses `scalar::lower::snode_of`
    /// so the decomposition is identical to the standalone scalar lower.
    pub(crate) fn lower_scalar(&mut self, expr: &mz_expr::MirScalarExpr) -> Id {
        let node = crate::eqsat::scalar::lower::snode_of(expr, |child| self.lower_scalar(child));
        self.add(CNode::Scalar(node))
    }
```

- [ ] **Step 2: Write the failing test (round-trip identity via a no-rule graph)**

```rust
    #[mz_ore::test]
    fn lower_scalar_roundtrips_without_rules() {
        use mz_expr::{MirScalarExpr, UnaryFunc};
        let e = MirScalarExpr::column(0).call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let mut eg = EGraph::new();
        let root = eg.lower_scalar(&e);
        // With no rules run, the raised expr equals the input (Task 5 provides raise).
        let out = crate::eqsat::scalar_extract::raise(&eg, root);
        assert_eq!(out, e);
    }
```

(This test depends on Task 5's `raise`. If executing strictly in order, write the test now and let it fail to compile until Task 5; that is the intended TDD ordering. Alternatively move this assertion into Task 5's commit. The subagent driving this task should keep the `lower_scalar` unit assertion minimal, e.g. assert the root class holds a `CNode::Scalar(SNode::CallUnary{..})`, and defer the round-trip to Task 5.)

Minimal standalone assertion (no dependency on Task 5):

```rust
    #[mz_ore::test]
    fn lower_scalar_builds_scalar_nodes() {
        use mz_expr::{MirScalarExpr, UnaryFunc};
        let e = MirScalarExpr::column(0).call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let mut eg = EGraph::new();
        let root = eg.lower_scalar(&e);
        assert!(eg.nodes(root).iter().any(|n| matches!(
            n,
            CNode::Scalar(SNode::CallUnary { .. })
        )));
    }
```

- [ ] **Step 3: Run the test**

Run: `bin/cargo-test -p mz-transform lower_scalar_builds_scalar_nodes`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/src/eqsat/egraph/combined.rs
git commit -m "eqsat: lower_scalar into the combined e-graph"
```

---

### Task 5: Determinism-parity scalar extractor on CombinedLang

**Files:**
- Create: `src/transform/src/eqsat/scalar_extract.rs`
- Modify: the eqsat module file to add `pub(crate) mod scalar_extract;`
- Test: inline `#[cfg(test)]` in `scalar_extract.rs`

**Interfaces:**
- Produces: `pub fn raise(eg: &EGraph, id: Id) -> MirScalarExpr` (pinned public API for slices 4-6). Internals `node_cost`/`compute_costs`/`build`/`reconstruct` ported from `scalar/raise.rs`, reading `CNode::Scalar` nodes.
- Consumes: `EGraph` (`= core::EGraph<CombinedLang>`), `CNode`, `SNode`, `core::Id`.

- [ ] **Step 1: Port the extractor, verbatim tie-break and And/Or sort**

Create `scalar_extract.rs`. Port `scalar/raise.rs` (see the source at `scalar/raise.rs:30-183`), changing only how a class's scalar nodes are read: use a local helper `scalar_nodes(eg, id) -> Vec<SNode>` that filters `eg.nodes(id)` to `CNode::Scalar`. Keep `node_cost`, `compute_costs`, `build` (with `ca.cmp(&cb).then_with(|| a.cmp(b))`), and `reconstruct` (with the `if matches!(func, VariadicFunc::And(_) | VariadicFunc::Or(_)) { operands.sort(); }` block) byte-for-byte in logic.

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Determinism-parity scalar extractor over the combined e-graph.
//!
//! A verbatim port of `scalar/raise.rs`'s bottom-up tree-size extraction, reading
//! `CNode::Scalar` nodes from `EGraph<CombinedLang>`. The tie-break
//! (`cost` then `SNode::Ord`) and the And/Or operand `sort()` are copied exactly
//! so extraction is byte-identical to the standalone scalar engine.

use std::collections::{HashMap, HashSet};

use mz_expr::{MirScalarExpr, VariadicFunc};

use crate::eqsat::core::Id;
use crate::eqsat::egraph::combined::{CNode, EGraph};
use crate::eqsat::scalar::node::SNode;

/// The scalar e-nodes of `id`'s class.
fn scalar_nodes(eg: &EGraph, id: Id) -> Vec<SNode> {
    eg.nodes(id)
        .into_iter()
        .filter_map(|n| match n {
            CNode::Scalar(s) => Some(s),
            _ => None,
        })
        .collect()
}

/// Reconstruct the min-cost `MirScalarExpr` for the class of `id`.
pub fn raise(eg: &EGraph, id: Id) -> MirScalarExpr {
    let costs = compute_costs(eg, id);
    let mut memo: HashMap<Id, MirScalarExpr> = HashMap::new();
    build(eg, id, &costs, &mut memo)
}

fn node_cost(eg: &EGraph, node: &SNode, costs: &HashMap<Id, usize>) -> Option<usize> {
    let mut total = 1usize;
    for child in node.children() {
        total = total.saturating_add(*costs.get(&eg.find(child))?);
    }
    Some(total)
}

fn compute_costs(eg: &EGraph, id: Id) -> HashMap<Id, usize> {
    let mut reachable: Vec<Id> = Vec::new();
    let mut seen: HashSet<Id> = HashSet::new();
    let mut stack = vec![eg.find(id)];
    while let Some(rep) = stack.pop() {
        if !seen.insert(rep) {
            continue;
        }
        reachable.push(rep);
        for node in scalar_nodes(eg, rep) {
            for child in node.children() {
                stack.push(eg.find(child));
            }
        }
    }
    let mut costs: HashMap<Id, usize> = HashMap::new();
    loop {
        let mut changed = false;
        for &rep in &reachable {
            let best = scalar_nodes(eg, rep)
                .iter()
                .filter_map(|node| node_cost(eg, node, &costs))
                .min();
            if let Some(best) = best {
                if costs.get(&rep).is_none_or(|&cur| best < cur) {
                    costs.insert(rep, best);
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    costs
}

fn build(
    eg: &EGraph,
    id: Id,
    costs: &HashMap<Id, usize>,
    memo: &mut HashMap<Id, MirScalarExpr>,
) -> MirScalarExpr {
    let rep = eg.find(id);
    if let Some(expr) = memo.get(&rep) {
        return expr.clone();
    }
    let nodes = scalar_nodes(eg, rep);
    let best = nodes
        .into_iter()
        .filter(|node| node_cost(eg, node, costs).is_some())
        .min_by(|a, b| {
            let ca = node_cost(eg, a, costs).expect("filtered to finite cost");
            let cb = node_cost(eg, b, costs).expect("filtered to finite cost");
            ca.cmp(&cb).then_with(|| a.cmp(b))
        })
        .expect("non-empty class with a finite-cost node");
    let expr = reconstruct(eg, &best, costs, memo);
    memo.insert(rep, expr.clone());
    expr
}

fn reconstruct(
    eg: &EGraph,
    node: &SNode,
    costs: &HashMap<Id, usize>,
    memo: &mut HashMap<Id, MirScalarExpr>,
) -> MirScalarExpr {
    match node {
        SNode::Column(index, name) => {
            MirScalarExpr::Column(*index, mz_ore::treat_as_equal::TreatAsEqual(name.0.clone()))
        }
        SNode::Literal(lit, typ) => MirScalarExpr::Literal(lit.clone(), typ.clone()),
        SNode::CallUnmaterializable(func) => MirScalarExpr::CallUnmaterializable(func.clone()),
        SNode::CallUnary { func, expr } => MirScalarExpr::CallUnary {
            func: func.clone(),
            expr: Box::new(build(eg, *expr, costs, memo)),
        },
        SNode::CallBinary { func, expr1, expr2 } => MirScalarExpr::CallBinary {
            func: func.clone(),
            expr1: Box::new(build(eg, *expr1, costs, memo)),
            expr2: Box::new(build(eg, *expr2, costs, memo)),
        },
        SNode::CallVariadic { func, exprs } => {
            let mut operands: Vec<MirScalarExpr> =
                exprs.iter().map(|e| build(eg, *e, costs, memo)).collect();
            if matches!(func, VariadicFunc::And(_) | VariadicFunc::Or(_)) {
                operands.sort();
            }
            MirScalarExpr::CallVariadic {
                func: func.clone(),
                exprs: operands,
            }
        }
        SNode::If { cond, then, els } => MirScalarExpr::If {
            cond: Box::new(build(eg, *cond, costs, memo)),
            then: Box::new(build(eg, *then, costs, memo)),
            els: Box::new(build(eg, *els, costs, memo)),
        },
    }
}
```

- [ ] **Step 2: Register the module**

In the eqsat module file (the one with `pub mod egraph;` / `pub mod scalar;`, check `src/transform/src/eqsat.rs`), add `pub(crate) mod scalar_extract;`.

- [ ] **Step 3: Write the failing test (round-trip identity, no rules)**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::eqsat::egraph::combined::EGraph;
    use mz_expr::{MirScalarExpr, UnaryFunc};

    #[mz_ore::test]
    fn raise_roundtrips_lowered_scalar() {
        let e = MirScalarExpr::column(0).call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let mut eg = EGraph::new();
        let root = eg.lower_scalar(&e);
        assert_eq!(raise(&eg, root), e);
    }
}
```

- [ ] **Step 4: Run the test**

Run: `bin/cargo-test -p mz-transform raise_roundtrips_lowered_scalar`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/src/eqsat/scalar_extract.rs src/transform/src/eqsat.rs
git commit -m "eqsat: determinism-parity scalar extractor on the combined e-graph"
```

---

### Task 6: Scalar saturate driver on CombinedLang

**Files:**
- Create: `src/transform/src/eqsat/scalar_saturate.rs`
- Modify: eqsat module file (`pub(crate) mod scalar_saturate;`)
- Modify: `src/transform/src/eqsat/rules.rs` (add `scalar_all()`)
- Test: inline `#[cfg(test)]` in `scalar_saturate.rs`

**Interfaces:**
- Produces: `scalar_saturate::saturate(eg: &mut EGraph) -> usize`, `scalar_saturate::canonicalize_combined(expr, col_types) -> MirScalarExpr`, `rules::scalar_all() -> CompiledRuleSet`.
- Consumes: `SCALAR_COMPILED_RULES` (Task 3), `BaseView` with `scalar_index` (Task 1), `EGraph::scalar_index`/`lower_scalar` (Tasks 1, 4), `scalar_extract::raise` (Task 5), scalar analysis recompute.

- [ ] **Step 1: Expose the scalar rule set**

In `rules.rs`, after `all()` (rules.rs:133), add:

```rust
/// The scalar-sort rewrite rules (run only by the scalar canonicalizer, never in
/// the relational saturation pass).
pub(crate) fn scalar_all() -> CompiledRuleSet {
    CompiledRuleSet {
        rules: SCALAR_COMPILED_RULES.iter().collect(),
    }
}
```

- [ ] **Step 2: Port the scalar analysis recompute to CombinedLang**

The scalar analysis lives in `CombinedData.scalar` (`ScalarGraphData.analysis`). Port `scalar/egraph.rs::recompute_analysis` (egraph.rs:94-145) to operate on `EGraph<CombinedLang>`, reading `CNode::Scalar` nodes and writing `eg.data_mut().scalar.analysis`. Place it in `scalar_saturate.rs` as `fn recompute_analysis(eg: &mut EGraph)`. Reuse `crate::eqsat::scalar::analysis::{make, merge, ClassAnalysis}`. Iterate scalar classes only (a class with no scalar node has no scalar analysis entry).

- [ ] **Step 3: Write the saturate driver (copied bounds, scalar rule set)**

```rust
// Copyright header (as in other files).

//! Scalar equality-saturation over the combined e-graph.
//!
//! The scalar canonicalizer runs its rules through the one CombinedLang
//! machinery. The bounds are copied from the standalone `scalar/egraph.rs`
//! (600 / 1_000 / 100) so the fixpoint reached here is identical to the old
//! engine's, which the differential test gates on. Scalar rules run only here,
//! never in the relational `EGraph::saturate` pass.

use mz_expr::MirScalarExpr;
use mz_repr::ReprColumnType;

use crate::eqsat::core::Id;
use crate::eqsat::egraph::combined::{CNode, EGraph};
use crate::eqsat::egraph::view::BaseView;
use crate::eqsat::rules;
use crate::eqsat::scalar::node::SNode;
use crate::eqsat::scalar_extract;

const MAX_ENODES: usize = 600;
const MATCH_LIMIT: usize = 1_000;
const MAX_ITERS: usize = 100;

// recompute_analysis(eg: &mut EGraph) from Step 2 goes here.

/// Saturate the scalar rules over `eg`, returning the iteration count.
pub(crate) fn saturate(eg: &mut EGraph) -> usize {
    let ruleset = rules::scalar_all();
    let compiled = ruleset.rules();
    let mut iters = 0;
    for _ in 0..MAX_ITERS {
        iters += 1;
        eg.rebuild();
        recompute_analysis(eg);
        let n_nodes: usize = eg.class_ids().iter().map(|&id| eg.nodes(id).len()).sum();
        if n_nodes > MAX_ENODES {
            break;
        }

        // Phase 1 (read-only): collect matches. Scalar rules do not read the
        // relational analyses, so an empty `Analyses` and empty relational
        // `Index` are passed; the scalar index is the live surface.
        let scalar_index = eg.scalar_index();
        let index = crate::eqsat::egraph::combined::Index::new();
        let analyses = crate::eqsat::egraph::saturate::Analyses::default();
        let mut pending: Vec<(usize, crate::eqsat::egraph::saturate::EBindings)> = Vec::new();
        {
            let view = BaseView {
                eg,
                index: &index,
                scalar_index: &scalar_index,
                an: &analyses,
            };
            for (qi, rule) in compiled.iter().enumerate() {
                let (matches, _hit) = (rule.find)(&view, &analyses, MATCH_LIMIT + 1);
                for b in matches.into_iter().take(MATCH_LIMIT) {
                    pending.push((qi, b));
                }
            }
        }

        // Phase 2 (mutate): apply and union.
        let mut changed = false;
        for (qi, b) in pending {
            if let Ok(new_id) = (compiled[qi].apply)(eg, &b) {
                if eg.union(new_id, b.root) {
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    eg.rebuild();
    iters
}

/// Canonicalize a scalar expression through the combined machinery. This is the
/// SP2b end-state entry; production still uses the old engine until slice 7.
pub(crate) fn canonicalize_combined(
    expr: &MirScalarExpr,
    col_types: &[ReprColumnType],
) -> MirScalarExpr {
    let mut eg = EGraph::new();
    eg.data_mut().scalar.col_types = col_types.to_vec();
    let root = eg.lower_scalar(expr);
    saturate(&mut eg);
    scalar_extract::raise(&eg, root)
}
```

Notes for the implementer:
- `Analyses`, `EBindings`, and `Index` must be reachable. `Analyses` needs a `Default` (it is constructed field-by-field in `saturate.rs`; if it has no `#[derive(Default)]`, add one, all four fields are `HashMap`). `Index` alias is in `combined.rs`.
- `col_types` is set on `data_mut().scalar.col_types` (field is `pub(crate)`, lang.rs:44). Not read by `not_not` but required by the entry's contract for later slices.
- `MATCH_LIMIT` truncation and the ban logic from the relational `saturate` are intentionally omitted: the old scalar driver had no backoff, and matching its behavior is the parity requirement.

- [ ] **Step 4: Register the module**

Add `pub(crate) mod scalar_saturate;` to the eqsat module file. Confirm `saturate::{Analyses, EBindings}` and `combined::Index` are `pub(crate)`; widen visibility minimally if needed (add `pub(crate)` to `Analyses` / derive `Default`).

- [ ] **Step 5: Write the failing test (needs Task 8's rule to actually rewrite)**

A no-op smoke test now (no scalar rules compiled until Task 8, so `scalar_all()` is empty and `canonicalize_combined` is identity):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::{MirScalarExpr, UnaryFunc};

    #[mz_ore::test]
    fn canonicalize_combined_is_identity_without_rules() {
        // Until not_not is compiled (Task 8), the scalar rule set is empty and
        // this is the identity. After Task 8, a dedicated test asserts the
        // rewrite (Task 9 differential harness).
        let e = MirScalarExpr::column(0).call_unary(UnaryFunc::Not(mz_expr::func::Not));
        assert_eq!(canonicalize_combined(&e, &[]), e);
    }
}
```

- [ ] **Step 6: Run the test**

Run: `bin/cargo-test -p mz-transform canonicalize_combined_is_identity_without_rules`
Expected: PASS (identity while the scalar rule set is empty).

- [ ] **Step 7: Commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/src/eqsat/scalar_saturate.rs src/transform/src/eqsat/rules.rs src/transform/src/eqsat.rs src/transform/src/eqsat/egraph/saturate.rs
git commit -m "eqsat: scalar saturate driver + canonicalize_combined on the combined e-graph"
```

---

### Task 7: Add `not_not` as a scalar rule; wire `scalar.rewrite` into the build

**Files:**
- Create: `src/transform/src/eqsat/rules/scalar.rewrite`
- Modify: `src/transform/build.rs`
- Test: build compiles `find_not_not_base`/`apply_not_not_base`; behavior verified in Task 8.

**Interfaces:**
- Produces: the compiled `not_not` scalar rule in `SCALAR_COMPILED_RULES`.
- Consumes: grammar `Pat::SUnary` (Task 2), codegen scalar arms + split (Task 3).

- [ ] **Step 1: Write the rule**

Create `scalar.rewrite`:

```
rule not_not {
    doc "Not(Not(x)) = x"
    Unary[not](Unary[not](x)) => x
}
```

- [ ] **Step 2: Read `scalar.rewrite` in `build.rs` and concatenate**

In `build.rs::main` (build.rs:39), after reading `relational.rewrite` and before codegen, read the scalar file and concatenate the parsed rule lists:

```rust
    let scalar_path = Path::new(&manifest).join("src/eqsat/rules/scalar.rewrite");
    println!("cargo:rerun-if-changed={}", scalar_path.display());
    let scalar_src = fs::read_to_string(&scalar_path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", scalar_path.display()));
    let scalar_rules = grammar::parse(&scalar_src).unwrap_or_else(|errs| {
        let mut msg = format!("failed to parse {}:\n", scalar_path.display());
        for e in errs {
            msg.push_str(&format!("  {e}\n"));
        }
        panic!("{msg}");
    });

    let mut rules = rules;
    rules.extend(scalar_rules);
```

(`rules` is the `Vec<Rule>` from `grammar::parse(&src)` at build.rs:50. Make it `mut` or rebind as above. `codegen::emit(&rules)` then sees both sorts and partitions them by root.)

- [ ] **Step 3: Build and confirm the generated code compiles**

Run: `cargo check -p mz-transform`
Expected: builds. The generated `$OUT_DIR/eqsat_rules.rs` now defines `find_not_not_base`, `apply_not_not_base`, and a `SCALAR_COMPILED_RULES` with one entry.

- [ ] **Step 4: Assert the rule rewrites (integration smoke test)**

Add to `scalar_saturate.rs` tests:

```rust
    #[mz_ore::test]
    fn not_not_rewrites_via_combined() {
        use mz_expr::{MirScalarExpr, UnaryFunc};
        let not = |e: MirScalarExpr| e.call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let x = MirScalarExpr::column(0);
        let e = not(not(x.clone()));
        assert_eq!(canonicalize_combined(&e, &[]), x);
    }
```

Run: `bin/cargo-test -p mz-transform not_not_rewrites_via_combined`
Expected: PASS (the double-negation collapses to the column).

- [ ] **Step 5: Commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/src/eqsat/rules/scalar.rewrite src/transform/build.rs src/transform/src/eqsat/scalar_saturate.rs
git commit -m "eqsat: not_not as a declarative scalar rule, compiled to the combined engine"
```

---

### Task 8: Differential harness + corpus fixture

**Files:**
- Create: `src/transform/tests/testdata/eqsat_scalar_corpus`
- Create: `src/transform/tests/eqsat_scalar_parity.rs`
- Test: the harness itself.

**Interfaces:**
- Consumes: `scalar_saturate::canonicalize_combined` (new path), `scalar::canonicalize` (old oracle, scalar.rs:36). Both must be reachable from an integration test. If `scalar::canonicalize` is not `pub`, add `pub(crate)` and a thin `#[cfg(test)]`-reachable wrapper, or move the harness to a `#[cfg(test)]` module inside the crate (see Step 3).

- [ ] **Step 1: Build the corpus fixture**

For slice 1, the corpus is the set of scalar expressions that exercise `not_not`, plus the three-axis coverage stubs (ties, could_error, non-nullable) that later slices populate. Commit a small hand-authored fixture now, expanded by the collection procedure in later slices. Format: one expression per record, with column types. Minimal content:

```
# eqsat scalar differential corpus (slice 1 seed).
# Format: `expr` line, `types` line (comma-separated ReprScalarType names), blank separator.
# Slice 1 covers not_not; ties/could_error/type axes are seeded here and grown per slice.
expr: not(not(#0))
types: bool

expr: not(not(not(#0)))
types: bool

expr: #0
types: bool
```

(The exact serialization is the harness's own parser, Step 2. Keep it a plain, hand-parseable format. NOTE: full collection via instrumented production callers is a later-slice task per the spec; slice 1 seeds a hand-authored set sufficient to exercise not_not.)

- [ ] **Step 2: Write the harness (differential + coverage assertion)**

Create `eqsat_scalar_parity.rs`. Parse the fixture, and for each `(expr, types)` assert `canonicalize_combined(expr, types) == old_canonicalize(expr, types)`. Build the `MirScalarExpr` values directly in Rust for slice 1 (a tiny constructor keyed off the fixture lines), since a full expr parser is unneeded for the seed set. Include the coverage assertion scaffold:

```rust
// The harness asserts, for the rules ported so far, that the combined path
// equals the old scalar engine. Slice 1: not_not only.
#[mz_ore::test]
fn scalar_parity_not_not() {
    use mz_expr::{MirScalarExpr, UnaryFunc};
    let not = |e: MirScalarExpr| e.call_unary(UnaryFunc::Not(mz_expr::func::Not));
    let x = MirScalarExpr::column(0);
    let cases = vec![
        not(not(x.clone())),
        not(not(not(x.clone()))),
        x.clone(),
    ];
    for e in cases {
        let new = mz_transform::eqsat::scalar_saturate::canonicalize_combined(&e, &[]);
        let old = mz_transform::eqsat::scalar::canonicalize(&e, &[]);
        assert_eq!(new, old, "parity failed for {e:?}");
    }
}
```

(If `eqsat` / `scalar_saturate` / `canonicalize` are `pub(crate)`, this external integration test cannot see them. In that case place the harness as a `#[cfg(test)] mod` inside `src/transform/src/eqsat/scalar_saturate.rs` and read the committed fixture via `include_str!("../../tests/testdata/eqsat_scalar_corpus")`. Prefer the in-crate placement to avoid widening visibility.)

Coverage assertion (grows per slice; slice 1 asserts the not_not axis is present):

```rust
#[mz_ore::test]
fn corpus_covers_slice1() {
    // Slice 1 requires at least one double-negation case. Later slices add
    // tie / could_error / type-context coverage assertions here.
    let corpus = include_str!("testdata/eqsat_scalar_corpus");
    assert!(corpus.contains("not(not("), "corpus must exercise not_not");
}
```

- [ ] **Step 3: Run the harness, expect PASS**

Run: `bin/cargo-test -p mz-transform scalar_parity_not_not corpus_covers_slice1`
Expected: PASS. If it FAILS on any case, that is the slice-1 go/no-go trigger, do not adjust goldens, report it.

- [ ] **Step 4: Commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/tests/testdata/eqsat_scalar_corpus src/transform/tests/eqsat_scalar_parity.rs
git commit -m "eqsat: differential parity harness + corpus seed for not_not"
```

---

### Task 9: Lean scalar denotation skeleton + theorem emission

**Files:**
- Modify: `src/transform/lean/MirRewrite/Semantics.lean`
- Modify: `src/transform/src/eqsat/lean.rs`
- Test: `cargo run -p mz-transform --example gen-lean` regenerates `Generated.lean` with `rule_not_not`.

**Interfaces:**
- Consumes: the DSL `Rule` (with `Pat::SUnary` / `Tmpl::RelVar`), `lean.rs::emit_rule` / `translate_pat` / `translate_tmpl` / `choose_proof`.
- Produces: a `ScalarExpr` denotation for `not` in `Semantics.lean` and a `rule_not_not` theorem in the generated Lean.

- [ ] **Step 1: Add a bounded scalar denotation**

In `Semantics.lean`, add a minimal `ScalarExpr` inductive and a `denoteS` function covering only `not` and a variable, sufficient for the `not_not` theorem to typecheck:

```lean
inductive ScalarExpr where
  | var : Nat → ScalarExpr
  | notE : ScalarExpr → ScalarExpr

def denoteS (env : Nat → Bool) : ScalarExpr → Bool
  | ScalarExpr.var n => env n
  | ScalarExpr.notE e => not (denoteS env e)
```

- [ ] **Step 2: Emit a scalar theorem for `SUnary`/`RelVar`**

In `lean.rs`, extend `translate_pat` and `translate_tmpl` with arms for `Pat::SUnary { func: "not", input }` (emit `ScalarExpr.notE (...)`) and the scalar metavar (emit `ScalarExpr.var`), and `choose_proof` to prove `not_not` via `simp [denoteS]` (double negation), else `sorry`. The theorem shape:

```lean
theorem rule_not_not : ∀ (env : Nat → Bool) (x : ScalarExpr),
    denoteS env (ScalarExpr.notE (ScalarExpr.notE x)) = denoteS env x := by
  intro env x; simp [denoteS]
```

Emit it into `Generated.lean` alongside the relational theorems.

- [ ] **Step 3: Regenerate and confirm**

Run: `cargo run -p mz-transform --example gen-lean`
Expected: `src/transform/lean/MirRewrite/Generated.lean` gains `theorem rule_not_not ...` and still contains all relational theorems.

- [ ] **Step 4: Confirm no permanent-sorry regression**

`not_not` is provable, so it must NOT carry `-- PERMANENT SORRY`. Grep:

Run: `grep -c "PERMANENT SORRY" src/transform/lean/MirRewrite/Generated.lean`
Expected: `0` (no builtin appliers exist until slice 4).

- [ ] **Step 5: Commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/lean/MirRewrite/Semantics.lean src/transform/lean/MirRewrite/Generated.lean src/transform/src/eqsat/lean.rs
git commit -m "eqsat lean: scalar denotation + rule_not_not theorem"
```

---

### Task 10: Slice-1 gate (go/no-go) + regression sweep

**Files:**
- Test only.

- [ ] **Step 1: Full mz-transform unit suite green**

Run: `bin/cargo-test -p mz-transform eqsat`
Expected: all eqsat tests PASS (relational + the new scalar tests). This confirms the additive codegen did not perturb the relational engine.

- [ ] **Step 2: Differential parity (the go/no-go criterion)**

Run: `bin/cargo-test -p mz-transform scalar_parity_not_not`
Expected: PASS. `canonicalize_combined == old canonicalize` on every not_not case. A failure here is the go/no-go trigger: STOP, do not adjust output, report that the one-grammar-over-CNode seam cannot reach extraction identity on `not_not`.

- [ ] **Step 3: Relational golden regression (no rewrite)**

Run: `bin/sqllogictest --optimized -- test/sqllogictest/transform/`
Expected: no diffs. Confirms the codegen split and the new scalar arms did not change the relational engine's output. Do NOT pass `--rewrite`.

- [ ] **Step 4: Termination check**

Confirm `canonicalize_combined` terminates within the copied bounds on the corpus (no test hangs; `not_not` saturation converges). This is implicitly covered by Step 2 completing, note it explicitly in the task's review.

- [ ] **Step 5: Final commit (gate record)**

```bash
git commit --allow-empty -m "eqsat: slice-1 seam go/no-go PASS (not_not parity + relational goldens clean)"
```

---

## Self-Review

**Spec coverage (against `2026-07-01-eqsat-sp2b-scalar-dsl-port-design.md`, slice-1 row):**
- Scalar `Pat` AST (2.1) → Task 2. Scalar `Tmpl` deferred (not_not RHS is a metavar; construction first fires in slice 2), consistent with the spec's "introduce shapes, exercise when a rule needs them".
- Codegen scalar arms (2.2) → Task 3.
- Scalar view methods (2.3) → Task 1.
- Scalar extractor with determinism parity (2.6) + pinned `pub fn raise` → Task 5.
- CombinedLang canonicalize path (lower, saturate, scalar-extract) → Tasks 4, 6.
- Differential harness + corpus fixture (one work unit) → Task 8.
- Lean scalar denotation skeleton + emit dispatch (sorry) → Task 9.
- Slice-1 gate (extraction identity + termination; relational goldens) → Task 10.
- `re-homed call_scalar_type` is NOT in slice 1 (no type-context rule here); it lands in slice 5. The spec's slice-1 machinery mentions it for API-pinning, but `not_not` needs only `raise`. `raise` is pinned in Task 5; `call_scalar_type` re-home is deferred to slice 5. Flagged as an intentional narrowing, not a gap.

**Placeholder scan:** No TBD/TODO-as-work. Every code step shows code. Two tasks (2, 3) are exercised by a later task's build rather than a standalone test because build-script modules have no unit-test target; this is stated explicitly, not hidden.

**Type consistency:** `ScalarIndex = HashMap<ScalarSym, Vec<(Id, SNode)>>` (Task 1) is consumed identically in `BaseView` (Task 1) and `scalar_saturate` (Task 6). `raise(eg, id)` signature identical in Tasks 5, 6. `canonicalize_combined(expr, col_types)` identical in Tasks 6, 7, 8. `SCALAR_COMPILED_RULES` (Task 3) consumed by `scalar_all()` (Task 6).

**Open risk to verify during execution:** `Matcher::child`'s scalar branch (Task 3 Step 4) assumes a scalar operator child is detected by `matches!(pat, Pat::SUnary { .. })`. When more scalar `Pat` variants land (slices 2+), that check must widen to all scalar variants, noted for those slices, correct for slice 1 (SUnary is the only scalar variant).
