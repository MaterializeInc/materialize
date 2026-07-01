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

