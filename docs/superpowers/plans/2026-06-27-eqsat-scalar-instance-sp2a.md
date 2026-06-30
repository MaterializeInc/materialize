# Eqsat SP2a — scalar as a second instance of the generic core (Implementation Plan)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the scalar equality-saturation engine on SP1's generic `core::EGraph<L>`, deleting the duplicated `ScalarEGraph` substrate, with identical observable behavior.

**Architecture:** Define a `ScalarLang: Language` (`Node = SNode`, `GraphData = ScalarGraphData{col_types, analysis}`). Add two **default-no-op** analysis hooks (`on_add`/`on_union`) to the `Language` trait; the core fires them from `add`/`union` (and thus `rebuild`). Scalar maintains `ClassAnalysis` incrementally through the hooks; the per-rebuild fixpoint recompute moves into a scalar-side `recompute_analysis` free function called from the scalar `saturate` driver. `ScalarEGraph` survives as a type alias (`= EGraph<ScalarLang>`), so `rules.rs`/`lower.rs`/`raise.rs`/`node.rs`/`analysis.rs` are otherwise untouched.

**Tech Stack:** Rust; crate `mz-transform`; module `src/transform/src/eqsat`. Tests via `bin/cargo-test` and `#[mz_ore::test]`.

**Spec:** `doc/developer/design/20260627_eqsat_scalar_instance.md`. **SP1 core:** `doc/developer/design/20260627_eqsat_generic_core.md`.

## Global Constraints

- **Behavior-neutral.** No observable change. No `REWRITE=1`, no `cargo insta accept`, no `--rewrite-results`. A refactor that needs a golden rewrite has changed behavior — stop and escalate.
- **Relational engine untouched.** `RelLang` must keep the default (no-op) hooks; the relational batch `Analysis<L>` driver and `run_analysis` are not modified. The full eqsat suite (relational 226 + scalar) must stay green after every task.
- **No new dependencies.** Workspace-deps rules in `CLAUDE.md` apply, but this plan adds none.
- **Hook fidelity.** `on_add`/`on_union` must fire at the same logical points and in the same winner/loser direction as today's `ScalarEGraph::{add, union}` (winner = `find(a)` = `ra`, loser = `find(b)` = `rb`; loser folds into winner). `recompute_analysis` must run right after congruence closure, matching the recompute tail of today's `ScalarEGraph::rebuild` (`egraph.rs:248–295`).
- **`doc/developer/generated/` is read-only.** Do not touch it.

---

## File Structure

| File | Responsibility after SP2a |
|---|---|
| `src/transform/src/eqsat/core.rs` | Generic substrate + the new `on_add`/`on_union` default hooks and `pub(crate)` accessors (`nodes`, `class_ids`, `node_count`, `data`, `data_mut`). |
| `src/transform/src/eqsat/scalar/lang.rs` *(new)* | `ScalarLang`, `ScalarSym`, `ScalarGraphData`, `impl Language for ScalarLang`. |
| `src/transform/src/eqsat/scalar/egraph.rs` | `ScalarEGraph` type alias, `Id` re-export, budget consts, scalar inherent `analysis`/`col_types`, free `saturate`/`recompute_analysis`. (Struct + substrate methods deleted.) |
| `src/transform/src/eqsat/scalar.rs` | `mod lang;`; `canonicalize` builds `EGraph<ScalarLang>` and calls the free `saturate`. |
| `src/transform/src/eqsat/scalar/{rules,lower,raise,node,analysis}.rs` | Non-test code unchanged; two `eg.saturate()` test calls in `rules.rs` become the free fn. |

---

## Task 1: Generic core — analysis hooks + accessors

**Files:**
- Modify: `src/transform/src/eqsat/core.rs`
- Test: `src/transform/src/eqsat/core.rs` (its `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: existing `Language`, `EGraph<L>`, `Id` from `core.rs`.
- Produces:
  - `Language::on_add(data: &mut Self::GraphData, id: Id, node: &Self::Node, get: &dyn Fn(Id) -> Id)` — default no-op.
  - `Language::on_union(data: &mut Self::GraphData, winner: Id, loser: Id)` — default no-op.
  - `EGraph<L>::nodes(&self, id: Id) -> Vec<L::Node>` (`pub(crate)`), `class_ids(&self) -> Vec<Id>` (`pub(crate)`), `node_count(&self) -> usize` (`pub(crate)`), `data(&self) -> &L::GraphData` (`pub(crate)`), `data_mut(&mut self) -> &mut L::GraphData` (`pub(crate)`).

- [ ] **Step 1: Write the failing test**

Add to `core.rs`'s `mod tests` a toy language whose `GraphData` is maintained by the hooks, proving `on_add` reads children via `get` and `on_union` runs in the right direction, and that `EGraph::data()`/`nodes()` expose them. Reuse the existing `Arith`/`ArithSym` types.

```rust
    // A toy analysis: each class carries the integer value of its (single) node,
    // maintained incrementally by the hooks. Exercises on_add (reading children
    // via `get`) and on_union (folding loser into winner).
    #[derive(Default)]
    struct SumData {
        vals: HashMap<Id, i64>,
    }

    struct SumLang;

    impl Language for SumLang {
        type Node = Arith;
        type Sym = ArithSym;
        type GraphData = SumData;

        fn children(node: &Arith) -> Vec<Id> {
            ArithLang::children(node)
        }
        fn map_children(node: &Arith, f: impl Fn(Id) -> Id) -> Arith {
            ArithLang::map_children(node, f)
        }
        fn symbol(node: &Arith) -> ArithSym {
            ArithLang::symbol(node)
        }
        fn on_add(data: &mut SumData, id: Id, node: &Arith, get: &dyn Fn(Id) -> Id) {
            let v = match node {
                Arith::Num(x) => *x,
                Arith::Add(a, b) => data.vals[&get(*a)] + data.vals[&get(*b)],
                Arith::Mul(a, b) => data.vals[&get(*a)] * data.vals[&get(*b)],
            };
            data.vals.insert(id, v);
        }
        fn on_union(data: &mut SumData, winner: Id, loser: Id) {
            // Loser folds into winner: drop the loser's entry, keep the winner's.
            let lo = data.vals.remove(&loser);
            if data.vals.get(&winner).is_none() {
                if let Some(lo) = lo {
                    data.vals.insert(winner, lo);
                }
            }
        }
    }

    #[mz_ore::test]
    fn on_add_reads_children_through_get() {
        let mut eg = EGraph::<SumLang>::new();
        let a = eg.add(Arith::Num(2));
        let b = eg.add(Arith::Num(3));
        let s = eg.add(Arith::Add(a, b));
        assert_eq!(eg.data().vals[&s], 5, "on_add must combine child analyses");
        assert_eq!(eg.nodes(s).len(), 1, "nodes() exposes the class contents");
    }

    #[mz_ore::test]
    fn on_union_folds_loser_into_winner() {
        let mut eg = EGraph::<SumLang>::new();
        let a = eg.add(Arith::Num(7));
        let b = eg.add(Arith::Num(9));
        // union(a, b): winner = find(a), loser = find(b). Loser's entry is dropped.
        eg.union(a, b);
        let w = eg.find(a);
        assert!(eg.data().vals.contains_key(&w), "winner keeps an entry");
        assert_eq!(eg.data().vals.len(), 1, "loser entry was removed on union");
    }

    #[mz_ore::test]
    fn default_hooks_are_noop_for_unit_graphdata() {
        // ArithLang has GraphData = () and does not override the hooks; add/union
        // must still work (defaults are no-ops).
        let mut eg = EGraph::<ArithLang>::new();
        let a = eg.add(Arith::Num(1));
        let b = eg.add(Arith::Num(2));
        eg.union(a, b);
        eg.rebuild();
        assert_eq!(eg.find(a), eg.find(b));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bin/cargo-test -p mz-transform eqsat::core 2>&1 | tail -30`
Expected: compile errors — `no method named data` / `no method named nodes` on `EGraph`, and `no method on_add/on_union in trait Language` / missing items, because the hooks and accessors don't exist yet.

- [ ] **Step 3: Add the hook methods to the `Language` trait**

In `core.rs`, add the two default methods to `trait Language` (after `symbol`):

```rust
    /// Hook fired when `add` creates a NEW e-class `id` for `node` (not on a
    /// hash-cons hit). `get` resolves a child id to its canonical root. Lets a
    /// language maintain `GraphData`-resident per-class analysis incrementally.
    /// Default: no-op (the relational engine uses the batch `Analysis` driver).
    fn on_add(
        _data: &mut Self::GraphData,
        _id: Id,
        _node: &Self::Node,
        _get: &dyn Fn(Id) -> Id,
    ) {
    }

    /// Hook fired when `union` folds `loser`'s class into `winner`'s. Lets a
    /// language merge its `GraphData`-resident analysis. Default: no-op.
    fn on_union(_data: &mut Self::GraphData, _winner: Id, _loser: Id) {}
```

- [ ] **Step 4: Add a `find_in` free helper and route `find` through it**

In `core.rs`, above `impl<L: Language> EGraph<L>`, add:

```rust
/// Follow union-find parent pointers in `uf` from `id` to its root. Non-
/// compressing: reads only `uf`, so it can run while another field of the
/// e-graph is mutably borrowed (the `on_add` hook closure).
fn find_in(uf: &[Id], mut id: Id) -> Id {
    while uf[id] != id {
        id = uf[id];
    }
    id
}
```

Replace the body of `find` to reuse it:

```rust
    /// The canonical id of `id`.
    pub fn find(&self, id: Id) -> Id {
        find_in(&self.uf, id)
    }
```

- [ ] **Step 5: Fire `on_add` from `add` and `on_union` from `union`**

Replace `add`'s tail so the hook fires for genuinely new classes, before the memo insert (so `node` is still owned):

```rust
    /// Add an e-node, returning its (canonical) e-class. Hash-conses.
    pub fn add(&mut self, node: L::Node) -> Id {
        let node = self.canon(&node);
        if let Some(&id) = self.memo.get(&node) {
            return self.find(id);
        }
        let id = self.new_class();
        self.classes.get_mut(&id).unwrap().insert(node.clone());
        // Maintain language analysis for the new class. Borrow `uf` and `data`
        // as disjoint fields so the `get` closure and `&mut data` don't alias.
        let uf = &self.uf;
        L::on_add(&mut self.data, id, &node, &|c| find_in(uf, c));
        self.memo.insert(node, id);
        id
    }
```

Replace `union`'s tail so the hook fires after the fold, with winner = `ra`, loser = `rb`:

```rust
    /// Union the classes of `a` and `b`; returns whether they were distinct.
    pub fn union(&mut self, a: Id, b: Id) -> bool {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra == rb {
            return false;
        }
        self.uf[rb] = ra;
        let nodes = self.classes.remove(&rb).unwrap_or_default();
        self.classes.entry(ra).or_default().extend(nodes);
        L::on_union(&mut self.data, ra, rb);
        true
    }
```

- [ ] **Step 6: Add the `pub(crate)` accessors**

In `impl<L: Language> EGraph<L>` (e.g. just after `index`), add:

```rust
    /// The set of e-nodes in `id`'s canonical class. Empty if `id` is unknown.
    pub(crate) fn nodes(&self, id: Id) -> Vec<L::Node> {
        let rep = self.find(id);
        self.classes
            .get(&rep)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// The canonical ids of all live e-classes.
    pub(crate) fn class_ids(&self) -> Vec<Id> {
        self.classes.keys().copied().collect()
    }

    /// Total number of e-nodes across all classes.
    pub(crate) fn node_count(&self) -> usize {
        self.classes.values().map(|ns| ns.len()).sum()
    }

    /// The language-owned auxiliary state.
    pub(crate) fn data(&self) -> &L::GraphData {
        &self.data
    }

    /// Mutable access to the language-owned auxiliary state.
    pub(crate) fn data_mut(&mut self) -> &mut L::GraphData {
        &mut self.data
    }
```

- [ ] **Step 7: Run the new tests to verify they pass**

Run: `bin/cargo-test -p mz-transform eqsat::core 2>&1 | tail -30`
Expected: PASS, including `on_add_reads_children_through_get`, `on_union_folds_loser_into_winner`, `default_hooks_are_noop_for_unit_graphdata`, and the pre-existing `add_hashconses_identical_nodes`, `congruence_collapses_after_union`, `index_buckets_by_symbol`.

- [ ] **Step 8: Run the full eqsat suite to confirm the relational engine is unperturbed**

Run: `bin/cargo-test -p mz-transform eqsat 2>&1 | tail -30`
Expected: all eqsat tests pass (relational 226 + the existing scalar tests, which still use the old `ScalarEGraph` struct at this point). The hook additions are no-ops for `RelLang` and the still-present scalar struct.

- [ ] **Step 9: Commit**

```bash
git add src/transform/src/eqsat/core.rs
git commit -m "eqsat/core: add on_add/on_union analysis hooks + class accessors

Default-no-op hooks let a Language maintain GraphData-resident per-class
analysis incrementally; the core fires them from add/union. Relational
keeps the defaults. Adds pub(crate) nodes/class_ids/node_count/data/
data_mut accessors for the upcoming scalar saturate driver.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H"
```

---

## Task 2: `ScalarLang` and `ScalarGraphData`

**Files:**
- Create: `src/transform/src/eqsat/scalar/lang.rs`
- Modify: `src/transform/src/eqsat/scalar.rs` (add `pub mod lang;`)
- Test: `src/transform/src/eqsat/scalar/lang.rs` (its `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `core::{Id, Language}`; `core::EGraph` accessors from Task 1 (`data`, `add`, `union`, `find`); `SNode` (`scalar/node.rs`); `analysis::{make, merge, ClassAnalysis}` (`scalar/analysis.rs`).
- Produces:
  - `pub struct ScalarLang;`
  - `pub enum ScalarSym { Column, Literal, Unmaterializable, Unary, Binary, Variadic, If }` (derives `Clone, PartialEq, Eq, Hash`).
  - `pub struct ScalarGraphData { pub(crate) col_types: Vec<ReprColumnType>, pub(crate) analysis: HashMap<Id, ClassAnalysis> }` (derives `Debug, Default`).
  - `impl Language for ScalarLang { type Node = SNode; type Sym = ScalarSym; type GraphData = ScalarGraphData; ... }` including `on_add`/`on_union`.

- [ ] **Step 1: Add the module declaration**

In `src/transform/src/eqsat/scalar.rs`, add to the module list (after `pub mod egraph;`):

```rust
pub mod lang;
```

- [ ] **Step 2: Write the new file with a failing test**

Create `src/transform/src/eqsat/scalar/lang.rs`:

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The scalar instantiation of the generic e-graph [`Language`].
//!
//! `ScalarLang` makes the scalar engine a second user of `core::EGraph<L>`. The
//! per-class analysis ([`ClassAnalysis`]) is maintained incrementally through the
//! `on_add`/`on_union` hooks (the fixpoint recompute lives in the scalar
//! `saturate` driver). See `doc/developer/design/20260627_eqsat_scalar_instance.md`.

use std::collections::HashMap;

use mz_repr::ReprColumnType;

use crate::eqsat::core::{Id, Language};
use crate::eqsat::scalar::analysis::{self, ClassAnalysis};
use crate::eqsat::scalar::node::SNode;

/// The operator symbol used to bucket scalar e-nodes in the matcher index.
///
/// SP2a does not run the matcher over scalar nodes (the scalar rules are
/// hand-written and matched against concrete `SNode`s), so a coarse discriminant
/// — no function identity — is sufficient to satisfy [`Language::symbol`]. A
/// future DSL matcher (SP2b) would refine this.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ScalarSym {
    Column,
    Literal,
    Unmaterializable,
    Unary,
    Binary,
    Variadic,
    If,
}

/// Language-owned state on the scalar e-graph: the relation's column types (read
/// by the typed-literal rules) and the per-class analyses (maintained by the
/// hooks). Both were fields of the old `ScalarEGraph` struct.
#[derive(Debug, Default)]
pub struct ScalarGraphData {
    /// Column types of the relation the expression is evaluated against, indexed
    /// by column position. Empty when no rule needs a column type.
    pub(crate) col_types: Vec<ReprColumnType>,
    /// Per-class analyses, keyed by canonical class id.
    pub(crate) analysis: HashMap<Id, ClassAnalysis>,
}

/// The scalar node language.
pub struct ScalarLang;

impl Language for ScalarLang {
    type Node = SNode;
    type Sym = ScalarSym;
    type GraphData = ScalarGraphData;

    fn children(node: &SNode) -> Vec<Id> {
        node.children()
    }

    fn map_children(node: &SNode, f: impl Fn(Id) -> Id) -> SNode {
        node.map_children(f)
    }

    fn symbol(node: &SNode) -> ScalarSym {
        match node {
            SNode::Column(..) => ScalarSym::Column,
            SNode::Literal(..) => ScalarSym::Literal,
            SNode::CallUnmaterializable(_) => ScalarSym::Unmaterializable,
            SNode::CallUnary { .. } => ScalarSym::Unary,
            SNode::CallBinary { .. } => ScalarSym::Binary,
            SNode::CallVariadic { .. } => ScalarSym::Variadic,
            SNode::If { .. } => ScalarSym::If,
        }
    }

    fn on_add(data: &mut ScalarGraphData, id: Id, node: &SNode, get: &dyn Fn(Id) -> Id) {
        // The new class's analysis is the node's `make` contribution, reading
        // child analyses (already present: children were added bottom-up).
        let a = analysis::make(node, &data.analysis, get);
        data.analysis.insert(id, a);
    }

    fn on_union(data: &mut ScalarGraphData, winner: Id, loser: Id) {
        // Fold loser into winner, mirroring the old ScalarEGraph::union: both
        // classes must already carry an analysis (a missing one is a bug, not a
        // recoverable state — the safe `could_error` default would be `true`,
        // and silently inventing it would corrupt a rule guard).
        let lo = data
            .analysis
            .remove(&loser)
            .expect("class must have an analysis before union");
        let wi = data
            .analysis
            .remove(&winner)
            .expect("class must have an analysis before union");
        data.analysis.insert(winner, analysis::merge(wi, lo));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::EvalError;
    use mz_ore::treat_as_equal::TreatAsEqual;
    use mz_repr::{Datum, ReprScalarType};

    use crate::eqsat::core::EGraph;

    fn err_lit() -> SNode {
        // Build a typed error literal SNode directly.
        let mir = mz_expr::MirScalarExpr::literal(
            Err(EvalError::DivisionByZero),
            ReprScalarType::Int64,
        );
        match mir {
            mz_expr::MirScalarExpr::Literal(row, typ) => SNode::Literal(row, typ),
            _ => unreachable!("literal builder returns a Literal"),
        }
    }

    fn ok_lit(v: i64) -> SNode {
        let mir = mz_expr::MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64);
        match mir {
            mz_expr::MirScalarExpr::Literal(row, typ) => SNode::Literal(row, typ),
            _ => unreachable!("literal builder returns a Literal"),
        }
    }

    #[mz_ore::test]
    fn on_add_populates_analysis() {
        let mut eg = EGraph::<ScalarLang>::new();
        let col = eg.add(SNode::Column(0, TreatAsEqual(None)));
        assert!(
            eg.data().analysis.contains_key(&col),
            "on_add must create an analysis entry for the column"
        );
        assert!(
            !eg.data().analysis[&col].could_error,
            "a column must not could_error"
        );

        let err = eg.add(err_lit());
        assert!(
            eg.data().analysis[&err].could_error,
            "an error literal must could_error"
        );
        assert!(
            eg.data().analysis[&err].literal.is_some(),
            "a literal class must carry its literal"
        );
    }

    #[mz_ore::test]
    fn on_union_merges_analysis() {
        let mut eg = EGraph::<ScalarLang>::new();
        // ok literal (could_error = false, literal = Some) ∪ column (false, None)
        let lit = eg.add(ok_lit(7));
        let col = eg.add(SNode::Column(0, TreatAsEqual(None)));
        eg.union(lit, col);
        let merged = eg.find(lit);
        let a = &eg.data().analysis[&merged];
        assert!(!a.could_error, "merge of two safe classes stays safe");
        assert!(a.literal.is_some(), "literal survives the merge");
    }
}
```

- [ ] **Step 3: Run the test to verify it passes**

Run: `bin/cargo-test -p mz-transform eqsat::scalar::lang 2>&1 | tail -30`
Expected: PASS — `on_add_populates_analysis`, `on_union_merges_analysis`. (This compiles `ScalarLang` against the Task 1 hooks and accessors.)

- [ ] **Step 4: Confirm the whole crate still builds and the eqsat suite is green**

Run: `bin/cargo-test -p mz-transform eqsat 2>&1 | tail -30`
Expected: all eqsat tests pass. `ScalarLang` is additive; the old `ScalarEGraph` struct still exists and is still what `rules`/`lower`/`raise` use.

- [ ] **Step 5: Commit**

```bash
git add src/transform/src/eqsat/scalar.rs src/transform/src/eqsat/scalar/lang.rs
git commit -m "eqsat/scalar: add ScalarLang (Language impl on the generic core)

Defines ScalarLang, ScalarSym, and ScalarGraphData{col_types, analysis}.
on_add/on_union maintain ClassAnalysis incrementally via analysis::make/
merge. Additive: the standalone ScalarEGraph still drives the engine.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H"
```

---

## Task 3: Migrate the scalar engine onto the core

This is the behavior-neutral switch-over. After it, `ScalarEGraph` is an alias, the duplicated substrate is gone, and all 88 scalar tests pass unchanged in intent.

**Files:**
- Modify (rewrite): `src/transform/src/eqsat/scalar/egraph.rs`
- Modify: `src/transform/src/eqsat/scalar.rs` (`canonicalize` + `assert_round_trip` test helper)
- Modify: `src/transform/src/eqsat/scalar/rules.rs` (two `eg.saturate()` test calls)

**Interfaces:**
- Consumes: `core::{EGraph, Id}`; Task 1 accessors (`nodes`, `class_ids`, `node_count`, `data`, `data_mut`, `find`, `add`, `union`, `rebuild`); Task 2 `ScalarLang`, `ScalarGraphData`; `analysis::{make, merge, ClassAnalysis}`; `rules::rules`; `SNode`.
- Produces:
  - `pub type ScalarEGraph = EGraph<ScalarLang>;`
  - `pub use crate::eqsat::core::Id;`
  - `pub fn saturate(eg: &mut EGraph<ScalarLang>) -> usize`
  - `pub fn recompute_analysis(eg: &mut EGraph<ScalarLang>)`
  - inherent `EGraph<ScalarLang>::analysis(&self, id: Id) -> &ClassAnalysis` and `col_types(&self) -> &[ReprColumnType]` (both `pub(crate)`).

- [ ] **Step 1: Verify the current scalar tests pass (baseline)**

Run: `bin/cargo-test -p mz-transform eqsat::scalar 2>&1 | tail -20`
Expected: PASS (88 tests). This is the baseline the migration must preserve.

- [ ] **Step 2: Rewrite `scalar/egraph.rs`**

Replace the entire contents of `src/transform/src/eqsat/scalar/egraph.rs` with:

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The scalar e-graph: an instance of the generic `core::EGraph<L>`.
//!
//! SP2a deleted the standalone scalar substrate; `ScalarEGraph` is now a type
//! alias for `EGraph<ScalarLang>`. This file keeps the scalar-specific driver
//! (`saturate`), the per-rebuild analysis fixpoint (`recompute_analysis`), the
//! growth budget, and the inherent analysis/col_types accessors. The hash-cons,
//! union-find, and congruence closure come from `crate::eqsat::core`.
//!
//! Per-class analysis is maintained two ways, exactly as before: incrementally
//! by `ScalarLang`'s `on_add`/`on_union` hooks (currency during rule
//! application) and, after each congruence closure, by `recompute_analysis` (the
//! monotone least-fixpoint that makes constant-folding's self-referential
//! classes sound).

use mz_repr::ReprColumnType;

use crate::eqsat::core::EGraph;
use crate::eqsat::scalar::analysis::{self, ClassAnalysis};
use crate::eqsat::scalar::lang::ScalarLang;
use crate::eqsat::scalar::node::SNode;
use crate::eqsat::scalar::rules;

pub use crate::eqsat::core::Id;

/// The scalar e-graph: the generic core specialized to the scalar language.
pub type ScalarEGraph = EGraph<ScalarLang>;

/// E-node budget for the saturation loop, mirroring the relational engine.
/// Saturation stops growing once the total e-node count crosses this; extraction
/// from a partially saturated graph is still sound.
const MAX_ENODES: usize = 600;

/// Per-iteration work-item cap (one (class id, e-node) snapshot per item).
/// Remaining nodes are visited next iteration, which starts from a rebuilt graph.
const MATCH_LIMIT: usize = 1_000;

/// Maximum saturation iterations. A hard stop against non-terminating rule
/// interactions; terminating rule sets reach a fixpoint well under this.
const MAX_ITERS: usize = 100;

impl EGraph<ScalarLang> {
    /// The analysis value for `id`'s canonical class.
    ///
    /// Panics if `id` has no analysis entry (it was never returned by `add`, or
    /// `recompute_analysis`/the hooks have not run since a structural change).
    pub(crate) fn analysis(&self, id: Id) -> &ClassAnalysis {
        let rep = self.find(id);
        self.data()
            .analysis
            .get(&rep)
            .expect("class must have an analysis entry; call saturate after union")
    }

    /// The column types this expression is evaluated against. Empty when the
    /// e-graph was built without types (only the type-agnostic rules rely on it).
    pub(crate) fn col_types(&self) -> &[ReprColumnType] {
        &self.data().col_types
    }
}

/// Recompute every class's analysis as a monotone least-fixpoint over the current
/// class layout. Lifted from the recompute tail of the old `ScalarEGraph::rebuild`.
///
/// Seed every class to the merge identity, then repeatedly recompute each class as
/// the merge over its nodes' `make` contributions — reading the current (possibly
/// still-seed) child values — until a pass changes nothing. Pre-seeding makes
/// self-referential classes (from constant folding) sound: `make` always finds its
/// children present, and a self-referential child reads the class's current value
/// rather than being dropped. The lattice is finite and `make`/`merge` are
/// monotone (`could_error` only rises, `literal` only goes None→Some), so this
/// converges to the conservative upper bound the analysis contract requires.
pub fn recompute_analysis(eg: &mut EGraph<ScalarLang>) {
    let all_ids = eg.class_ids();
    eg.data_mut().analysis.clear();
    for &id in &all_ids {
        eg.data_mut().analysis.insert(
            id,
            ClassAnalysis {
                could_error: false,
                literal: None,
            },
        );
    }
    loop {
        let mut changed = false;
        for &id in &all_ids {
            let nodes = eg.nodes(id);
            // A live class always holds at least one node.
            debug_assert!(!nodes.is_empty(), "recompute_analysis: class {id} has no nodes");
            let mut acc = ClassAnalysis {
                could_error: false,
                literal: None,
            };
            for node in &nodes {
                let node_a = {
                    let store = &eg.data().analysis;
                    let find = |c| eg.find(c);
                    analysis::make(node, store, &find)
                };
                acc = analysis::merge(acc, node_a);
            }
            // Read the current value into locals so the immutable borrow ends
            // before the mutable `data_mut()` insert below.
            let (cur_err, cur_has_lit) = {
                let cur = eg.data().analysis.get(&id).expect("class seeded above");
                (cur.could_error, cur.literal.is_some())
            };
            // Both fields are monotone, so an inequality is always an increase.
            // Comparing `literal` by presence suffices: equal classes carry the
            // same literal.
            if cur_err != acc.could_error || cur_has_lit != acc.literal.is_some() {
                changed = true;
                eg.data_mut().analysis.insert(id, acc);
            }
        }
        if !changed {
            break;
        }
    }
}

/// Saturate the e-graph to a fixpoint, applying every rule wherever it matches.
///
/// Each iteration restores congruence (`rebuild`), recomputes the analysis
/// fixpoint (`recompute_analysis`), then snapshots all (class, node) pairs
/// read-only (bounded by `MATCH_LIMIT`) and applies rules with mutable access so
/// rules can build nested intermediate nodes. Breaks on fixpoint (no new unions)
/// or when the e-node budget is exceeded. Returns the number of iterations run.
pub fn saturate(eg: &mut EGraph<ScalarLang>) -> usize {
    let mut iters = 0;
    for _ in 0..MAX_ITERS {
        iters += 1;
        eg.rebuild();
        recompute_analysis(eg);
        // Bound runaway growth: stop and extract from what we have (sound).
        if eg.node_count() > MAX_ENODES {
            break;
        }

        // Read-only snapshot of (class id, node), bounded by MATCH_LIMIT.
        let mut work: Vec<(Id, SNode)> = Vec::new();
        'collect: for id in eg.class_ids() {
            for node in eg.nodes(id) {
                work.push((id, node));
                if work.len() >= MATCH_LIMIT {
                    break 'collect;
                }
            }
        }
        // Apply: rules may now mutate (add nested nodes). Union each returned
        // class id into the source class.
        let mut changed = false;
        for (id, node) in work {
            for rule in rules::rules() {
                for new_id in rule(eg, &node) {
                    if eg.union(id, new_id) {
                        changed = true;
                    }
                }
            }
        }

        if !changed {
            break;
        }
    }
    iters
}
```

- [ ] **Step 3: Update `scalar.rs`'s `canonicalize` and test helper**

In `src/transform/src/eqsat/scalar.rs`, replace `canonicalize` (it must no longer call `with_col_types` or the `.saturate()` method). Rename the local to `eg` so the `egraph` module path is not shadowed:

```rust
pub fn canonicalize(expr: &MirScalarExpr, col_types: &[ReprColumnType]) -> MirScalarExpr {
    let mut eg = ScalarEGraph::new();
    eg.data_mut().col_types = col_types.to_vec();
    let root = lower::lower(&mut eg, expr);
    egraph::saturate(&mut eg);
    raise::raise(&eg, root)
}
```

In the same file's `#[cfg(test)] mod tests`, update `assert_round_trip` the same way:

```rust
    fn assert_round_trip(expr: MirScalarExpr) {
        let mut eg = ScalarEGraph::new();
        let root = lower::lower(&mut eg, &expr);
        egraph::saturate(&mut eg);
        let raised = raise::raise(&eg, root);
        assert_eq!(raised, expr, "round-trip changed the expression");
    }
```

(`egraph::saturate` resolves through `pub mod egraph;`; `super::*` brings the `egraph` module into the test scope.)

- [ ] **Step 4: Update the two `rules.rs` test `saturate` calls**

In `src/transform/src/eqsat/scalar/rules.rs`, in `mod tests`:

- In `test_fold_terminates`, replace `let iters = eg.saturate();` with:

```rust
        let iters = crate::eqsat::scalar::egraph::saturate(&mut eg);
```

- In `test_fold_cycle_could_error_is_conservative`, replace `eg.saturate();` with:

```rust
        crate::eqsat::scalar::egraph::saturate(&mut eg);
```

(Leave everything else in `rules.rs` unchanged: the `Rule` type, rule bodies, and the `eg.add`/`find`/`nodes`/`analysis`/`col_types` calls all resolve through the `ScalarEGraph` alias and the inherent accessors.)

- [ ] **Step 5: Build and run the scalar suite**

Run: `bin/cargo-test -p mz-transform eqsat::scalar 2>&1 | tail -40`
Expected: PASS — all 88 scalar tests (12 analysis + 66 rules + 10 module), unchanged in intent. In particular:
- `eqsat::scalar::analysis::tests::test_merge_could_error_survives_union` and `test_merge_preserves_literal` (direct `union`+`rebuild`, no `saturate`) pass via the `on_union` hook.
- `eqsat::scalar::rules::tests::test_fold_cycle_could_error_is_conservative` (self-cycle) passes via `recompute_analysis` inside `saturate`.

If a compile error mentions `with_col_types`, `ScalarEGraph::{find,add,union,...}` as inherent (now they come from core), or `saturate`/`Id` not found, re-check the alias and `pub use` in `egraph.rs`.

- [ ] **Step 6: Run the full eqsat suite (relational + scalar)**

Run: `bin/cargo-test -p mz-transform eqsat 2>&1 | tail -30`
Expected: all eqsat tests pass (relational 226 + scalar 88). Relational is unaffected (default hooks); scalar now runs on the core.

- [ ] **Step 7: Behavior-neutral check — broad transform tests**

Run: `bin/cargo-test -p mz-transform 2>&1 | tail -30`
Expected: PASS. No test needs a golden rewrite. (The full sqllogictest behavior-neutral sweep — transform suite with the scalar-eqsat flag in its test-on state, no `--rewrite` — is run at the final whole-branch review per the SP1 pattern; the known `case_literal` list-mode artifact is the only tolerated diff and must be confirmed to pass standalone.)

- [ ] **Step 8: Confirm no stray references to the deleted struct internals**

Run: `git grep -n "ScalarEGraph::with_col_types\|\.saturate()\|struct ScalarEGraph\|hashcons" src/transform/src/eqsat/scalar/`
Expected: no matches (the struct, `with_col_types`, `hashcons`, and the `.saturate()` method are all gone; `ScalarEGraph` now appears only as the alias and in type positions).

- [ ] **Step 9: Commit**

```bash
git add src/transform/src/eqsat/scalar/egraph.rs src/transform/src/eqsat/scalar.rs src/transform/src/eqsat/scalar/rules.rs
git commit -m "eqsat/scalar: run the scalar engine on the generic core

Delete the duplicated ScalarEGraph substrate; ScalarEGraph is now a type
alias for EGraph<ScalarLang>. Analysis is maintained incrementally by the
ScalarLang hooks and recomputed to a fixpoint by recompute_analysis,
called from the free saturate driver after each congruence closure. All
88 scalar tests pass unchanged; the relational engine is untouched.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H"
```

---

## Final verification (after all tasks)

- [ ] Full eqsat suite green: `bin/cargo-test -p mz-transform eqsat` (relational 226 + scalar 88).
- [ ] Full `mz-transform` crate tests green: `bin/cargo-test -p mz-transform`.
- [ ] Behavior-neutral sqllogictest sweep (final whole-branch review): run the transform-relevant slt with `bin/sqllogictest --optimized` and the scalar-eqsat flag in its test-on state, **no `--rewrite`**. Only the known `case_literal` list-mode artifact may differ; confirm it passes standalone (`bin/sqllogictest -- test/sqllogictest/case_literal.slt`).
- [ ] No `REWRITE`/`insta accept`/`--rewrite-results` was used anywhere.
- [ ] `ScalarEGraph` struct and its substrate are gone; `git grep "struct ScalarEGraph"` is empty.
```
