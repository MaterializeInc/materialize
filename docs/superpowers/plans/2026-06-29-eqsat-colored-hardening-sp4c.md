# SP4c — eqsat Colored Runtime Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Solidify the SP4b colored runtime — make the cost model scalar-aware, add a total `try_arity`, and clear the deferred micro-cleanups (M1.2/M2/M6/M7/M8) — with no new capability.

**Architecture:** Four small, independent changes to existing eqsat modules. Only Task 1 (scalar-aware joint cost) is behavior-affecting and may move goldens; Tasks 2–4 are behavior-neutral perf/robustness/doc cleanups.

**Tech Stack:** Rust, `mz-transform` crate, eqsat e-graph (`src/transform/src/eqsat/`).

## Global Constraints

- **Only Task 1 may move goldens.** Any golden moving under Tasks 2–4 is a regression to investigate, not to regenerate.
- Behavior gate per task: `bin/cargo-test -p mz-transform` (unit + datadriven `*.spec`) must pass; `cargo clippy -p mz-transform --all-targets -- -D warnings` clean.
- For any golden that moves (expected only in Task 1): regenerate and review **per-file standalone** with `bin/sqllogictest --optimized -- <file>`.
- Use a bash timeout of at least 600000 ms for cargo test/build commands (first build is slow).
- `Rel::node_count()` (`ir.rs`) must stay relational-only — it is the CSE ordering key (`cse.rs:77`, `cse.rs:361`). Only `Cost.nodes` becomes scalar-aware.
- Do not touch `.superpowers/sdd/` except writing your own report file; do not `git add` it.
- Do not edit anything under `doc/developer/generated/` (read-only, agent-managed).

---

### Task 1: Scalar-aware joint cost

Make the cost model see scalar expression size, via one shared scalar-cost function used by both `Cost.nodes` and the colored substitution. Today `Rel::node_count()` counts only relational nodes, so replacing a Map scalar `#1 + 1` with the equal column `#0` does not change `Cost.nodes`.

**Files:**
- Modify: `src/transform/src/eqsat/ir.rs` — add `scalar_expr_cost` (relocated from `colored_derive`) and `Rel::scalar_node_count`.
- Modify: `src/transform/src/eqsat/cost.rs:354` — make `Cost.nodes` scalar-aware.
- Modify: `src/transform/src/eqsat/colored_derive.rs` — delete the private `scalar_cost`, repoint `resolve_scalar_colored` and the two tests to the shared fn.

**Interfaces:**
- Produces: `pub(crate) fn scalar_expr_cost(expr: &MirScalarExpr) -> usize` (in `ir.rs`); `Rel::scalar_node_count(&self) -> usize` (in `ir.rs`).
- Consumes: existing `Rel::node_count`, `Rel::children`, `EScalar.expr`, `CostModel::cost`.

- [ ] **Step 1: Write the failing test for `scalar_node_count`**

In `src/transform/src/eqsat/ir.rs`, inside the existing `#[cfg(test)] mod tests` block (find it with `grep -n "mod tests" src/transform/src/eqsat/ir.rs`; if there is none, create `#[cfg(test)] mod scalar_cost_tests { use super::*; ... }` at end of file), add:

```rust
#[mz_ore::test]
fn scalar_node_count_sums_payloads_and_recurses() {
    use mz_expr::{BinaryFunc, func};
    use mz_repr::{Datum, ReprScalarType};
    // `#1 + 1` is a 3-node scalar (Add, column, literal); `#0` is 1 node.
    let lit1 = MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64);
    let compute = MirScalarExpr::column(1).call_binary(lit1, BinaryFunc::AddInt64(func::AddInt64));
    let get = Rel::Get { name: "r".to_string(), arity: 2 };
    let map_compute = Rel::Map {
        scalars: vec![EScalar::plain(compute.clone())],
        input: Box::new(get.clone()),
    };
    let map_col = Rel::Map {
        scalars: vec![EScalar::plain(MirScalarExpr::column(0))],
        input: Box::new(get.clone()),
    };
    // Get carries no scalar payload.
    assert_eq!(get.scalar_node_count(), 0);
    // Map[#1 + 1] = 3; Map[#0] = 1.
    assert_eq!(map_compute.scalar_node_count(), 3);
    assert_eq!(map_col.scalar_node_count(), 1);
    // Recursion: Filter over the computing Map sums both levels (filter pred #0 = 1 is 3).
    let pred = MirScalarExpr::column(0)
        .call_binary(MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64), BinaryFunc::Eq(func::Eq));
    let filt = Rel::Filter {
        predicates: vec![EScalar::plain(pred)],
        input: Box::new(map_compute.clone()),
    };
    assert_eq!(filt.scalar_node_count(), 3 + 3);
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `bin/cargo-test -p mz-transform scalar_node_count_sums_payloads_and_recurses`
Expected: FAIL — `no method named scalar_node_count` (and `scalar_expr_cost` not yet defined).

- [ ] **Step 3: Add `scalar_expr_cost` to `ir.rs`**

Place it as a free `pub(crate)` fn near `Rel::node_count` (around `ir.rs:518`). It is the exact body relocated from `colored_derive::scalar_cost`:

```rust
/// Tree-size cost of a scalar expr (node count); lower is cheaper. The single
/// canonical scalar cost, shared by the cost model's `nodes` tie-breaker
/// ([`crate::eqsat::cost::CostModel::cost`]) and the colored substitution's
/// cheapest-spelling choice ([`crate::eqsat::colored_derive::resolve_scalar_colored`]).
pub(crate) fn scalar_expr_cost(expr: &MirScalarExpr) -> usize {
    let mut n = 0;
    expr.visit_pre(|_| n += 1);
    n
}
```

- [ ] **Step 4: Add `Rel::scalar_node_count` to `ir.rs`**

Inside `impl Rel`, directly after `node_count`:

```rust
/// Total scalar tree-size carried by this node and its relational subtree.
///
/// Sums [`scalar_expr_cost`] over every scalar payload — `EScalar` payloads via
/// `.expr`, and the one bare `MirScalarExpr` payload (`TopKShape.limit`)
/// directly — then recurses over relational children. `Project.outputs` (`Col`),
/// `Constant` rows (literals), and `TopKShape.group_key`/`order_key`
/// (`Col`/`ColumnOrder`) carry no scalar trees and are not counted.
///
/// Used only by [`crate::eqsat::cost::CostModel::cost`] to make the `nodes`
/// tie-breaker scalar-aware. Deliberately NOT folded into [`Self::node_count`],
/// which is the CSE ordering key (`cse.rs`) and must stay relational-only.
pub fn scalar_node_count(&self) -> usize {
    let here: usize = match self {
        Rel::Map { scalars, .. } => scalars.iter().map(|s| scalar_expr_cost(&s.expr)).sum(),
        Rel::Filter { predicates, .. } | Rel::IndexedFilter { predicates, .. } => {
            predicates.iter().map(|s| scalar_expr_cost(&s.expr)).sum()
        }
        Rel::Join { equivalences, .. } | Rel::WcoJoin { equivalences, .. } => {
            equivalences.iter().flatten().map(|s| scalar_expr_cost(&s.expr)).sum()
        }
        Rel::FlatMap { exprs, .. } => exprs.iter().map(|s| scalar_expr_cost(&s.expr)).sum(),
        Rel::ArrangeBy { key, .. } => key.iter().map(|s| scalar_expr_cost(&s.expr)).sum(),
        Rel::ArrangeByMany { keys, .. } => {
            keys.iter().flatten().map(|s| scalar_expr_cost(&s.expr)).sum()
        }
        Rel::Reduce { group_key, aggregates, .. } => {
            group_key.iter().map(|s| scalar_expr_cost(&s.expr)).sum::<usize>()
                + aggregates.iter().map(|a| scalar_expr_cost(&a.expr)).sum::<usize>()
        }
        Rel::TopK { shape, .. } => shape.limit.as_ref().map_or(0, scalar_expr_cost),
        _ => 0,
    };
    here + self.children().iter().map(|c| c.scalar_node_count()).sum::<usize>()
}
```

- [ ] **Step 5: Run the test to confirm it passes**

Run: `bin/cargo-test -p mz-transform scalar_node_count_sums_payloads_and_recurses`
Expected: PASS.

- [ ] **Step 6: Write the failing cost test**

In `src/transform/src/eqsat/cost.rs`, inside its `#[cfg(test)] mod tests` block, add:

```rust
#[mz_ore::test]
fn cost_nodes_is_scalar_aware() {
    use mz_expr::{BinaryFunc, MirScalarExpr, func};
    use mz_repr::{Datum, ReprScalarType};
    use crate::eqsat::ir::{EScalar, Rel};
    let lit1 = MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64);
    let compute = MirScalarExpr::column(1).call_binary(lit1, BinaryFunc::AddInt64(func::AddInt64));
    let get = Rel::Get { name: "r".to_string(), arity: 2 };
    let map_compute = Rel::Map {
        scalars: vec![EScalar::plain(compute)],
        input: Box::new(get.clone()),
    };
    let map_col = Rel::Map {
        scalars: vec![EScalar::plain(MirScalarExpr::column(0))],
        input: Box::new(get),
    };
    let model = CostModel::new();
    // Same relational structure; the cheaper scalar spelling has fewer cost nodes.
    assert!(
        model.cost(&map_col).nodes < model.cost(&map_compute).nodes,
        "Cost.nodes must reflect scalar size",
    );
}
```

- [ ] **Step 7: Run it to confirm it fails**

Run: `bin/cargo-test -p mz-transform cost_nodes_is_scalar_aware`
Expected: FAIL — the two costs have equal `.nodes` (both Map+Get = 2).

- [ ] **Step 8: Make `Cost.nodes` scalar-aware**

In `src/transform/src/eqsat/cost.rs`, in `CostModel::cost` (the `Cost { ... }` literal around line 354), change:

```rust
            nodes: rel.node_count(),
```

to:

```rust
            // Scalar-aware (SP4c): the `node_count` term is the relational tree
            // size (also the CSE ordering key, kept relational-only); the
            // `scalar_node_count` term adds scalar payload size so the tie-breaker
            // prefers cheaper scalar spellings directly.
            nodes: rel.node_count() + rel.scalar_node_count(),
```

- [ ] **Step 9: Run the cost test to confirm it passes**

Run: `bin/cargo-test -p mz-transform cost_nodes_is_scalar_aware`
Expected: PASS.

- [ ] **Step 10: Repoint `colored_derive` at the shared fn and delete the duplicate**

In `src/transform/src/eqsat/colored_derive.rs`:

1. Delete the private `scalar_cost` fn (the `pub(crate) fn scalar_cost(expr: &MirScalarExpr) -> usize { ... }` with its doc comment, around lines 146–156).
2. Add `scalar_expr_cost` to the `egraph`/`ir` imports. The existing import is `use crate::eqsat::ir::EScalar;` — change it to `use crate::eqsat::ir::{EScalar, scalar_expr_cost};`.
3. In `resolve_scalar_colored`, change the `min_by_key` key from `scalar_cost(&e.expr)` to `scalar_expr_cost(&e.expr)`.
4. In the test module: the import line `use super::{ ... scalar_cost, ... }` — remove `scalar_cost` from it and instead `use crate::eqsat::ir::scalar_expr_cost;` inside the test module. Update the two test bodies that call `scalar_cost(` to `scalar_expr_cost(`: `scalar_cost_counts_nodes` (rename the assertions only; keep the test name or rename to `scalar_expr_cost_counts_nodes`) and `resolve_colored_equal_cost_prefers_lower_column_index`.

- [ ] **Step 11: Run the colored_derive tests**

Run: `bin/cargo-test -p mz-transform colored_derive`
Expected: PASS (all `colored_derive` unit tests).

- [ ] **Step 12: Run the full crate suite and capture golden moves**

Run: `bin/cargo-test -p mz-transform 2>&1 | tail -40`
Expected: unit tests pass. Datadriven `*.spec` goldens may report diffs — that is expected for this task. If a `*.spec` golden moved, regenerate with `REWRITE=1 bin/cargo-test -p mz-transform run_tests` and **review the diff** (it should be a tie-break-driven plan change, e.g. a cheaper scalar spelling chosen; no arity change, no result change).

- [ ] **Step 13: Regenerate and review slt EXPLAIN goldens**

Find which EXPLAIN slt files moved, then review and regenerate each. List the candidate files with `ls test/sqllogictest/explain/*.slt`, and run the whole directory once to see which fail (a fail = changed output):

Run: `for f in test/sqllogictest/explain/*.slt; do bin/sqllogictest --optimized -- "$f" >/dev/null 2>&1 || echo "CHANGED: $f"; done`

For each `CHANGED` file: inspect the diff (`bin/sqllogictest --optimized -- "$f"` shows expected-vs-actual), regenerate with `bin/sqllogictest --optimized -- --rewrite-results "$f"`, then review `git diff "$f"`.
Expected: only tie-break-driven plan changes (scalar spellings / node-count-driven choices). Document each moved file and why in the task report. No structural/arity/result changes.

- [ ] **Step 14: Clippy**

Run: `cargo clippy -p mz-transform --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 15: Commit**

```bash
git add src/transform/src/eqsat/ir.rs src/transform/src/eqsat/cost.rs src/transform/src/eqsat/colored_derive.rs
git add -A test/sqllogictest src/transform/tests   # any regenerated goldens
git commit -m "SP4c: scalar-aware joint cost (shared scalar_expr_cost)"
```

---

### Task 2: `try_arity` robustness (M1.1)

Add a total `try_arity(id) -> Option<usize>` (the public face of the existing `arity_guarded`); make `arity()` the asserting wrapper that delegates. Gives future SP4d colored-saturation code (which iterates mixed relational/scalar classes) a non-panicking primitive, and turns the documented precondition into an actual safe API.

**Files:**
- Modify: `src/transform/src/eqsat/egraph/build.rs:363-376` (the `arity` fn) — add `try_arity`, rewrap `arity`.

**Interfaces:**
- Consumes: existing `arity_guarded(&self, id: Id, visiting: &mut HashSet<Id>) -> Option<usize>`, `rel_class_nodes`.
- Produces: `pub(crate) fn try_arity(&self, id: Id) -> Option<usize>`.

- [ ] **Step 1: Write the failing tests**

In `src/transform/src/eqsat/egraph/build.rs`, inside its `#[cfg(test)] mod tests` block (find with `grep -n "mod tests" src/transform/src/eqsat/egraph/build.rs`), add. (Use the existing test idioms in that module for constructing an `EGraph`; `intern_scalar` and `add_rel` are the in-module patterns — see `colored_derive.rs` tests for `intern_scalar` usage.)

```rust
#[mz_ore::test]
fn try_arity_some_for_relational_class() {
    let mut eg = EGraph::new();
    let r = eg.add_rel(&Rel::Get { name: "r".to_string(), arity: 3 });
    eg.rebuild();
    assert_eq!(eg.try_arity(r), Some(3));
}

#[mz_ore::test]
fn try_arity_none_for_scalar_class() {
    let mut eg = EGraph::new();
    let sid = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(0)));
    eg.rebuild();
    assert_eq!(eg.try_arity(sid), None, "a scalar class has no arity");
}

#[mz_ore::test]
#[should_panic(expected = "well-defined arity")]
fn arity_panics_on_scalar_class() {
    let mut eg = EGraph::new();
    let sid = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(0)));
    eg.rebuild();
    let _ = eg.arity(sid);
}
```

Ensure the test module imports `Rel`, `EScalar`, and `MirScalarExpr` (add `use` lines if missing, mirroring `colored_derive.rs` tests).

- [ ] **Step 2: Run them to confirm failure**

Run: `bin/cargo-test -p mz-transform try_arity`
Expected: FAIL — `no method named try_arity`.

- [ ] **Step 3: Add `try_arity` and rewrap `arity`**

In `src/transform/src/eqsat/egraph/build.rs`, replace the `arity` fn (lines ~363–376) with:

```rust
    /// The arity of a class (invariant across equivalent e-nodes), or `None` if
    /// the class has no relational e-node with an acyclic derivation — e.g. a
    /// scalar e-class, or a class all of whose relational derivations are cyclic.
    ///
    /// Prefer this over [`Self::arity`] whenever the class may not be relational
    /// (mixed relational/scalar iteration). [`Self::arity`] is the asserting
    /// convenience wrapper for the common case of a known-relational class.
    pub(crate) fn try_arity(&self, id: Id) -> Option<usize> {
        self.arity_guarded(id, &mut HashSet::new())
    }

    /// The arity of a known-relational class. Panics if the class has no
    /// well-defined arity (e.g. called on a scalar class); use [`Self::try_arity`]
    /// when that is possible. Every in-tree caller passes a relational class.
    pub fn arity(&self, id: Id) -> usize {
        debug_assert!(
            !self.rel_class_nodes(id).is_empty(),
            "arity() is defined only for relational classes; class {id} has no \
             relational e-node (called on a scalar class?)",
        );
        self.try_arity(id)
            .expect("class has a well-defined arity")
    }
```

(Leave `arity_guarded` unchanged.)

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `bin/cargo-test -p mz-transform try_arity arity_panics_on_scalar_class`
Expected: PASS (all three).

- [ ] **Step 5: Full suite + clippy (no goldens should move)**

Run: `bin/cargo-test -p mz-transform 2>&1 | tail -20`
Expected: all pass, **zero golden changes**.
Run: `cargo clippy -p mz-transform --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/transform/src/eqsat/egraph/build.rs
git commit -m "SP4c: add total try_arity; arity() is the asserting wrapper (M1.1)"
```

---

### Task 3: Iterate relational classes, not all classes (M2 + M8 + M1.2)

Three behavior-neutral cleanups that stop iterating over inert scalar classes.

**Files:**
- Modify: `src/transform/src/eqsat/colored.rs` (`colored_class_members`) — M2.
- Modify: `src/transform/src/eqsat/colored_derive.rs` (`derive`) — M8.
- Modify: `src/transform/src/eqsat/egraph/build.rs` (`rel_index`, `rel_class_ids`) — M1.2.

**Interfaces:**
- Consumes: `ColoredEGraph::find`, `base.class_ids`, `EGraph::rel_class_ids`, `EGraph::nodes`, `CNode`.

- [ ] **Step 1: Write the failing M8 test**

In `src/transform/src/eqsat/colored_derive.rs` test module, add:

```rust
#[mz_ore::test]
fn derive_does_not_mark_scalar_classes_empty() {
    use crate::eqsat::egraph::CNode;
    // A plain Filter[#0 = #1](Get(2)) plus a stray interned scalar class.
    let (mut eg, _filter) = filter_eq_fixture();
    let scalar = eg.intern_scalar(&EScalar::plain(col(0)));
    eg.rebuild();
    let scalar = eg.find(scalar);
    let d = derive(&mut eg);
    // The scalar class must not be recorded as an empty relational class, nor
    // assigned a scope.
    assert!(!d.empty_classes.contains(&scalar), "scalar class is not an empty relation");
    assert!(!d.class_scope.contains_key(&scalar), "scalar class gets no scope");
    // Sanity: it really is a scalar-only class.
    assert!(
        !eg.nodes(scalar).iter().any(|n| matches!(n, CNode::Rel(_))),
        "fixture precondition: the class is scalar-only",
    );
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `bin/cargo-test -p mz-transform derive_does_not_mark_scalar_classes_empty`
Expected: FAIL — the scalar class's `bottom()`/`None` fact routes it into `empty_classes` (or it is otherwise visited).

> If this test unexpectedly PASSES before the change, the scalar fact is non-empty and M8 is purely a wasted-work cleanup; keep the test (it pins the intended invariant) and proceed to Step 3 anyway.

- [ ] **Step 3: Add the M8 relational pre-filter in `derive`**

In `src/transform/src/eqsat/colored_derive.rs`, in `derive`, at the top of the `for canon in canon_ids {` loop body (before `let fact = &facts[&canon];`), insert:

```rust
        // M8: skip scalar-only classes. They carry no Filter/Map payload to
        // reduce and are not relational empty relations, so visiting them only
        // wastes `fresh_reducer`/minimize work (and would mis-route a scalar
        // `None` fact into `empty_classes`). Relational empty classes have a
        // `CNode::Rel` node and are kept.
        if !eg.nodes(canon).iter().any(|n| matches!(n, CNode::Rel(_))) {
            continue;
        }
```

Confirm `CNode` is imported at the top of `colored_derive.rs` (it is: `use crate::eqsat::egraph::{CNode, ...}`).

- [ ] **Step 4: Run the M8 test**

Run: `bin/cargo-test -p mz-transform derive_does_not_mark_scalar_classes_empty`
Expected: PASS.

- [ ] **Step 5: Apply M2 (drop redundant filter in `colored_class_members`)**

In `src/transform/src/eqsat/colored.rs`, in `colored_class_members`, remove the redundant first filter. Change:

```rust
        let mut out: Vec<Id> = self
            .base
            .class_ids()
            .into_iter()
            .filter(|&y| self.base.find(y) == y)
            .filter(|&y| self.find(c, y) == rep)
            .collect();
```

to:

```rust
        // `class_ids()` returns only canonical class keys (the `classes` map is
        // keyed by representative after `rebuild()`), so a `base.find(y) == y`
        // filter would be a no-op. (M2)
        let mut out: Vec<Id> = self
            .base
            .class_ids()
            .into_iter()
            .filter(|&y| self.find(c, y) == rep)
            .collect();
```

- [ ] **Step 6: Apply M1.2 (`rel_index` iterates relational classes)**

In `src/transform/src/eqsat/egraph/build.rs`, in `rel_index` (around line 242), change the loop driver from `self.class_ids()` to `self.rel_class_ids()`:

```rust
    pub(crate) fn rel_index(&self) -> Index {
        let mut idx: Index = HashMap::new();
        // Iterate relational classes only; scalar classes have no relational
        // e-node and would contribute nothing to the index. (M1.2)
        for id in self.rel_class_ids() {
            for node in self.rel_class_nodes(id) {
                idx.entry(node.sym()).or_default().push((id, node.clone()));
            }
        }
        idx
    }
```

Then remove the now-stale dead-code allowance on `rel_class_ids` (it now has a real caller). In `rel_class_ids` (around line 266–280), delete the `#[allow(dead_code)]` attribute and update its doc comment, replacing the "no compiled caller — hence `#[allow(dead_code)]`" sentence with:

```rust
    /// The canonical ids of every class that holds at least one relational node.
    /// Used by [`Self::rel_index`] (so the matcher index skips scalar classes)
    /// and emitted by the build.rs DSL codegen for any rule whose left-hand side
    /// has a pure relational-variable root (`Pat::RelVar`), bounding such a root
    /// to relational classes only.
    pub(crate) fn rel_class_ids(&self) -> Vec<Id> {
```

- [ ] **Step 7: Full suite + clippy (no goldens should move)**

Run: `bin/cargo-test -p mz-transform 2>&1 | tail -20`
Expected: all pass, **zero golden changes** (behavior-neutral).
Run: `cargo clippy -p mz-transform --all-targets -- -D warnings`
Expected: clean (note `rel_class_ids` no longer needs `#[allow(dead_code)]`).

- [ ] **Step 8: Commit**

```bash
git add src/transform/src/eqsat/colored.rs src/transform/src/eqsat/colored_derive.rs src/transform/src/eqsat/egraph/build.rs
git commit -m "SP4c: iterate relational classes not all (M2/M8/M1.2)"
```

---

### Task 4: `fresh_reducer` bound + empty-unsat doc (M6 + M7)

Two doc/safety nits in `colored_derive.rs`, behavior-neutral.

**Files:**
- Modify: `src/transform/src/eqsat/colored_derive.rs` (`fresh_reducer`, and the empty-routing branch in `derive`).

**Interfaces:**
- Consumes: `EquivalenceClasses::minimize_bounded(&mut self, columns: Option<&[ReprColumnType]>, max_iters: usize)` (`pub(crate)`, in `crate::analysis::equivalences`).

- [ ] **Step 1: Bound the `fresh_reducer` fallback (M6)**

In `src/transform/src/eqsat/colored_derive.rs`, in `fresh_reducer`, replace the unbounded `clone.minimize(None)` fallback with the bounded variant the analysis merge uses (`minimize_bounded(None, 100)`), and document it:

```rust
fn fresh_reducer(ec: &EquivalenceClasses) -> BTreeMap<MirScalarExpr, MirScalarExpr> {
    if !ec.reducer().is_empty() {
        ec.reducer().clone()
    } else {
        // Defensive: `derive_facts` runs the analysis through a (bounded)
        // minimize, so a non-empty class normally already has a reducer; this
        // fallback is not expected to fire. Use the SAME bound the analysis
        // merge uses (`minimize_bounded(None, 100)`, see
        // `analysis::equivalences`) rather than an unbounded `minimize`, so the
        // fallback can never over-reduce relative to production. (M6)
        let mut clone = ec.clone();
        clone.minimize_bounded(None, 100);
        clone.reducer().clone()
    }
}
```

- [ ] **Step 2: Document the empty-but-unsat divergence (M7)**

In `derive`, on the `if fact_is_empty(fact) { ... }` branch (around lines 278–281), add a comment marking the intentional divergence from Phase-2a:

```rust
        let fact = &facts[&canon];
        // M7 (intentional divergence from Phase-2a): an empty-but-unsatisfiable
        // class is routed to `empty_classes` and its predicates are NOT rewritten.
        // The class denotes the empty relation, so extraction empty-folds it
        // regardless of predicate spelling — sound, and strictly less work than
        // Phase-2a (which would still rewrite the predicates of a doomed Filter).
        if fact_is_empty(fact) {
            empty_classes.insert(canon);
            continue;
        }
```

- [ ] **Step 3: Confirm `minimize_bounded` is importable**

`minimize_bounded` is `pub(crate)` on `EquivalenceClasses`, already in scope via the existing `use crate::analysis::equivalences::EquivalenceClasses;` in `colored_derive.rs`. No new import needed. Verify the method resolves by building.

Run: `bin/cargo-test -p mz-transform colored_derive 2>&1 | tail -20`
Expected: compiles and all `colored_derive` tests pass (no behavior change — the fallback does not fire in these fixtures).

- [ ] **Step 4: Full suite + clippy (no goldens should move)**

Run: `bin/cargo-test -p mz-transform 2>&1 | tail -20`
Expected: all pass, **zero golden changes**.
Run: `cargo clippy -p mz-transform --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add src/transform/src/eqsat/colored_derive.rs
git commit -m "SP4c: bound fresh_reducer fallback (M6); doc empty-unsat divergence (M7)"
```

---

## Final verification (after all tasks)

- [ ] Full crate suite: `bin/cargo-test -p mz-transform` — all pass.
- [ ] Clippy: `cargo clippy -p mz-transform --all-targets -- -D warnings` — clean.
- [ ] Confirm only Task 1 moved goldens; every moved golden reviewed and documented as a sound tie-break/scalar-spelling change (no arity/result changes).
- [ ] Datadriven eqsat goldens: `bin/cargo-test -p mz-transform run_tests` — clean (or regenerated + reviewed under Task 1 only).
