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
