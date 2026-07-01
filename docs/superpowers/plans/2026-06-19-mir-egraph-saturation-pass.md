# MIR equality-saturation pass (milestone 1) implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land an in-tree `mz-transform-egraph` crate that runs the equality-saturation prototype on real `MirRelationExpr`, bailing per-subtree on unsupported constructs, exercised offline via the transform test harness (`apply=EqSat`), not yet registered in the live optimizer.

**Architecture:** A new workspace crate ports the prototype's e-graph engine (`egraph`, `cost`, `matcher`, `parser`, `dsl`, `analysis`, `cse`, `ir`) verbatim, then wraps it with three new modules: an `Interner` that owns the real `MirScalarExpr`/bailed-subtree payloads, a `lower` that translates `MirRelationExpr -> Rel` (unsupported variants become opaque leaves), and a `raise` that translates back. `optimize` calls the reused `engine::Optimizer`. Scalars are opaque; only rules that relocate payloads (never remap column indices) are active.

**Tech Stack:** Rust, Cargo workspace, `mz-expr`, `mz-repr`, `mz-transform` (Transform trait), `mz-expr-test-util` + datadriven (tests).

## Global Constraints

* The crate is `src/transform-egraph`, crate name `mz-transform-egraph`, a member of the `mz-*` workspace. Copyright header on every `.rs` file (copy from any existing `src/transform/src/*.rs`).
* No `unsafe`. No `as` conversions; use `mz_ore::cast::CastFrom`/`CastLossy` if a numeric cast is needed.
* Comments: no em-dashes or structuring semicolons. Doc comment states the contract; reasoning inline at the decision point. No vendor names in user-facing surfaces.
* Scalars stay opaque: no rule may rewrite inside a `MirScalarExpr`. Rules that remap column indices are gated off in M1.
* M1 supported variants: `Get`, `Constant`, `Project`, `Map`, `Filter`, `Join` (with `equivalences`), `Negate`, `Threshold`, `Union`, `Let`/`LocalGet`. Bail-to-leaf variants: `FlatMap`, `Reduce`, `TopK`, `ArrangeBy`, `LetRec`.
* Run `cargo fmt` after editing; `cargo check -p mz-transform-egraph` must be clean; never run `cargo fmt --check`.
* The pass is NOT added to `Optimizer::physical_optimizer` or any production pipeline in M1.

---

### Task 1: Scaffold the crate and wire it into the workspace

**Files:**
- Create: `src/transform-egraph/Cargo.toml`
- Create: `src/transform-egraph/src/lib.rs`
- Modify: `Cargo.toml:2` (`members`) and `Cargo.toml:134` (`default-members`)

**Interfaces:**
- Produces: crate `mz-transform-egraph` that builds; `pub fn optimize` stub.

- [ ] **Step 1: Create `src/transform-egraph/Cargo.toml`**

```toml
[package]
name = "mz-transform-egraph"
description = "Equality-saturation rewrites over a subset of MIR (offline, milestone 1)."
version = "0.0.0"
edition.workspace = true
rust-version.workspace = true
publish = false

[dependencies]
mz-expr = { path = "../expr" }
mz-repr = { path = "../repr" }
mz-ore = { path = "../ore" }
mz-transform = { path = "../transform" }

[dev-dependencies]
mz-expr-test-util = { path = "../expr-test-util" }

[lints]
workspace = true

[package.metadata.cargo-udeps.ignore]
normal = ["workspace-hack"]
```

(If a `workspace-hack` dependency is conventional in sibling crates, add `workspace-hack = { version = "0.0.0", path = "../workspace-hack" }` under `[dependencies]`; check `src/transform/Cargo.toml` and mirror it.)

- [ ] **Step 2: Create `src/transform-egraph/src/lib.rs`** (stub, replaced in later tasks)

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Equality-saturation rewrites over a subset of MIR relational expressions.
//!
//! Milestone 1: offline, test-only. Lowers `MirRelationExpr` to the prototype
//! `Rel`, saturates, extracts the cheapest plan, and raises back. Unsupported
//! variants become opaque leaves so the supported envelope still optimizes.

use mz_expr::MirRelationExpr;

/// Optimize `expr` by equality saturation, returning a functionally equivalent
/// plan. In milestone 1 this is a no-op placeholder.
pub fn optimize(expr: MirRelationExpr) -> MirRelationExpr {
    expr
}
```

- [ ] **Step 3: Add the member to both lists in `Cargo.toml`**

Insert `    "src/transform-egraph",` immediately after the `    "src/transform",` line in BOTH the `members = [` block (around line 121) and the `default-members = [` block (around line 250). Keep the lists sorted.

- [ ] **Step 4: Build**

Run: `cargo check -p mz-transform-egraph`
Expected: compiles clean.

- [ ] **Step 5: Commit**

```bash
git add src/transform-egraph/Cargo.toml src/transform-egraph/src/lib.rs Cargo.toml
git commit -m "transform-egraph: scaffold crate and wire into workspace"
```

---

### Task 2: Port the prototype engine modules

**Files:**
- Create (copy): `src/transform-egraph/src/{ir,egraph,cost,matcher,parser,dsl,analysis,cse}.rs`
- Create (copy): `src/transform-egraph/rules/relational.rewrite`
- Modify: `src/transform-egraph/src/lib.rs` (declare modules)

**Interfaces:**
- Produces: `ir::Rel`, `ir::Scalar`, `egraph::EGraph`, `cost::CostModel`, `dsl::RuleSet`, `parser::parse_ruleset`, `engine::Optimizer` (ported in next task) — all the prototype's public API, compiling inside the crate with its own unit tests passing.

- [ ] **Step 1: Copy the engine sources and rules**

```bash
cd /home/moritz/dev/repos/materialize/.claude/worktrees/mir-equality-optimizer
SRC=misc/mir-rewrite-dsl/src
DST=src/transform-egraph/src
for m in ir egraph cost matcher parser dsl analysis cse engine; do cp $SRC/$m.rs $DST/$m.rs; done
mkdir -p src/transform-egraph/rules
cp misc/mir-rewrite-dsl/rules/relational.rewrite src/transform-egraph/rules/relational.rewrite
```

Note: `scalar.rs` (structured scalar IR), `lean.rs`, and the `bin/` are intentionally NOT copied. The scalar IR is unused once scalars are opaque `MirScalarExpr`.

- [ ] **Step 2: Replace `src/transform-egraph/src/lib.rs` module declarations**

```rust
// Copyright header (as in Task 1) ...

//! Equality-saturation rewrites over a subset of MIR relational expressions.
//! See `docs/superpowers/specs/2026-06-19-mir-egraph-saturation-pass-design.md`.

// This crate ports a self-contained prototype; the repo-wide bans on std hash
// collections and `Iterator::zip` (determinism conventions elsewhere) do not
// apply to the ported engine.
#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

pub mod analysis;
pub mod cost;
pub mod cse;
pub mod dsl;
pub mod egraph;
pub mod engine;
pub mod ir;
pub mod matcher;
pub mod parser;

/// The built-in rule file, embedded at compile time.
pub const RULES_SRC: &str = include_str!("../rules/relational.rewrite");
```

- [ ] **Step 3: Build the ported engine**

Run: `cargo check -p mz-transform-egraph`
Expected: compiles. If `engine.rs` references `scalar::canonicalize_scalars` (the dropped module), comment out that call and the `let plan = canonicalize_scalars(plan);` line in `engine::Optimizer::optimize`, replacing with `// scalars are opaque MirScalarExpr in-tree; scalar folding is left to the existing FoldConstants transform.`

- [ ] **Step 4: Run the ported unit tests**

Run: `cargo test -p mz-transform-egraph`
Expected: the ported module tests pass (the same ones that pass in `misc/mir-rewrite-dsl`). If any test in a copied module references `scalar.rs`, delete that single test and note it in the commit message.

- [ ] **Step 5: Commit**

```bash
git add src/transform-egraph/src src/transform-egraph/rules
git commit -m "transform-egraph: port the prototype e-graph engine"
```

---

### Task 3: Give `Scalar` an interner identity

**Files:**
- Modify: `src/transform-egraph/src/ir.rs` (the `Scalar` struct + constructors)
- Modify: `src/transform-egraph/src/matcher.rs` (side conditions `all_columns`, `cols_of`, `any_false`, `all_true` to read structured fields)

**Interfaces:**
- Produces: `ir::ScalarId(pub u32)`; `Scalar { id: ScalarId, cols: BTreeSet<Col>, is_col: Option<Col>, lit: Option<bool>, text: String }`; `Scalar::interned(id, expr_facts...)` and the existing `Scalar::new(text, cols)` retained with a sentinel id for the engine's own tests.

- [ ] **Step 1: Write the failing test** in `src/transform-egraph/src/ir.rs` (append to the `#[cfg(test)]` module, or add one)

```rust
#[cfg(test)]
mod scalar_identity_tests {
    use super::*;

    #[test]
    fn interned_scalars_with_same_id_are_equal() {
        let a = Scalar::interned(ScalarId(7), [3], Some(3), None, "#3");
        let b = Scalar::interned(ScalarId(7), [3], Some(3), None, "#3");
        assert_eq!(a, b);
        assert_eq!(a.is_col, Some(3));
    }

    #[test]
    fn legacy_new_has_sentinel_id() {
        let s = Scalar::new("(#0 + #1)", [0, 1]);
        assert_eq!(s.id, ScalarId::SENTINEL);
        assert_eq!(s.cols.iter().copied().collect::<Vec<_>>(), vec![0, 1]);
    }
}
```

- [ ] **Step 2: Run it to verify failure**

Run: `cargo test -p mz-transform-egraph scalar_identity`
Expected: FAIL (no `ScalarId`, no `interned`).

- [ ] **Step 3: Edit the `Scalar` struct** in `ir.rs`

Replace the `Scalar` definition with:

```rust
/// An identity handle into the [`crate::interner::Interner`] for an opaque
/// scalar. Two scalars are the same iff their ids match.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScalarId(pub u32);

impl ScalarId {
    /// Used by the engine's own unit tests, which build scalars without an
    /// interner. Never produced by lowering, never resolved by raising.
    pub const SENTINEL: ScalarId = ScalarId(u32::MAX);
}

/// An opaque scalar expression.
///
/// `id` is the interner handle used to raise back to the real `MirScalarExpr`.
/// `cols`, `is_col`, and `lit` are the relational facts the rewrite rules read
/// (column support, whether the scalar is a bare column reference, and whether
/// it is a literal true/false). `text` is for display only.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Scalar {
    pub id: ScalarId,
    pub cols: BTreeSet<Col>,
    pub is_col: Option<Col>,
    pub lit: Option<bool>,
    pub text: String,
}

impl Scalar {
    /// A scalar with a real interner identity and precomputed relational facts.
    pub fn interned(
        id: ScalarId,
        cols: impl IntoIterator<Item = Col>,
        is_col: Option<Col>,
        lit: Option<bool>,
        text: impl Into<String>,
    ) -> Self {
        Scalar {
            id,
            cols: cols.into_iter().collect(),
            is_col,
            lit,
            text: text.into(),
        }
    }

    /// A sentinel-id scalar, for the engine's own unit tests only.
    pub fn new(text: impl Into<String>, cols: impl IntoIterator<Item = Col>) -> Self {
        let cols: BTreeSet<Col> = cols.into_iter().collect();
        let is_col = if cols.len() == 1 { cols.iter().copied().next() } else { None };
        Scalar {
            id: ScalarId::SENTINEL,
            cols,
            is_col,
            lit: None,
            text: text.into(),
        }
    }

    /// The largest column referenced, plus one (0 if none).
    pub fn min_arity(&self) -> usize {
        self.cols.iter().copied().max().map_or(0, |c| c + 1)
    }
}
```

(Keep the existing `impl fmt::Display for Scalar` writing `self.text`.)

- [ ] **Step 4: Adapt side conditions in `matcher.rs`** to read the structured fields

Find the implementations of `all_columns`, `cols_of`, `any_false`, `all_true` (they currently inspect `Scalar::text`/`cols`). Replace their bodies so:
* `all_columns(s)` is `s.iter().all(|x| x.is_col.is_some())`.
* `cols_of(s)` is `s.iter().map(|x| x.is_col.expect("all_columns checked")).collect()`.
* `any_false(p)` is `p.iter().any(|x| x.lit == Some(false))`.
* `all_true(p)` is `p.iter().all(|x| x.lit == Some(true))`.

If the prototype already keyed these on `cols`/`text`, this is a direct substitution. Do not change the matcher's payload-relocation logic.

- [ ] **Step 5: Run tests**

Run: `cargo test -p mz-transform-egraph`
Expected: PASS (new identity tests + ported engine tests).

- [ ] **Step 6: Commit**

```bash
git add src/transform-egraph/src/ir.rs src/transform-egraph/src/matcher.rs
git commit -m "transform-egraph: give Scalar an interner identity and structured facts"
```

---

### Task 4: The interner

**Files:**
- Create: `src/transform-egraph/src/interner.rs`
- Test: same file (`#[cfg(test)]`)
- Modify: `src/transform-egraph/src/lib.rs` (`pub mod interner;`)

**Interfaces:**
- Consumes: `ir::{Scalar, ScalarId}`.
- Produces:
  - `pub struct LeafId(pub u32);`
  - `pub struct Interner { /* ... */ }`
  - `Interner::new() -> Self`
  - `Interner::intern_scalar(&mut self, expr: &MirScalarExpr) -> Scalar`
  - `Interner::resolve_scalar(&self, id: ScalarId) -> &MirScalarExpr`
  - `Interner::intern_leaf(&mut self, subtree: MirRelationExpr) -> LeafId` (records arity)
  - `Interner::resolve_leaf(&self, id: LeafId) -> &MirRelationExpr`
  - `Interner::leaf_arity(&self, id: LeafId) -> usize`

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::MirScalarExpr;
    use mz_repr::{Datum, ScalarType};

    #[test]
    fn scalars_dedup_by_value() {
        let mut i = Interner::new();
        let c3 = MirScalarExpr::column(3);
        let a = i.intern_scalar(&c3);
        let b = i.intern_scalar(&MirScalarExpr::column(3));
        assert_eq!(a.id, b.id);
        assert_eq!(a.is_col, Some(3));
        assert_eq!(i.resolve_scalar(a.id), &c3);
    }

    #[test]
    fn literal_false_is_flagged() {
        let mut i = Interner::new();
        let f = MirScalarExpr::literal_false();
        let s = i.intern_scalar(&f);
        assert_eq!(s.lit, Some(false));
    }

    #[test]
    fn leaves_record_arity() {
        let mut i = Interner::new();
        let r = MirRelationExpr::constant(vec![], mz_repr::RelationType::new(vec![ScalarType::Int64.nullable(false)]));
        let id = i.intern_leaf(r.clone());
        assert_eq!(i.leaf_arity(id), 1);
        assert_eq!(i.resolve_leaf(id), &r);
        let _ = Datum::Null; // silence unused import if needed
    }
}
```

(Verify `MirScalarExpr::literal_false()` exists; if not, build it via `MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool)` or the crate's equivalent. Adjust the test accordingly during Step 3.)

- [ ] **Step 2: Run it to verify failure**

Run: `cargo test -p mz-transform-egraph interner`
Expected: FAIL (module does not exist).

- [ ] **Step 3: Implement `interner.rs`**

```rust
// Copyright header ...

//! Side tables that let the e-graph treat real `MirScalarExpr`s and bailed
//! `MirRelationExpr` subtrees as opaque, raisable payloads.

use std::collections::BTreeMap;

use mz_expr::{MirRelationExpr, MirScalarExpr};
use mz_expr::scalar::columns::Columns;

use crate::ir::{Scalar, ScalarId};

/// Identity handle for a bailed (unsupported) `MirRelationExpr` subtree, lowered
/// to an opaque leaf `Get`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeafId(pub u32);

/// Owns the real payloads referenced opaquely by the e-graph.
#[derive(Debug, Default)]
pub struct Interner {
    scalars: Vec<MirScalarExpr>,
    scalar_dedup: BTreeMap<MirScalarExpr, ScalarId>,
    leaves: Vec<MirRelationExpr>,
    leaf_arity: Vec<usize>,
}

impl Interner {
    pub fn new() -> Self {
        Interner::default()
    }

    /// Intern a scalar, deduplicating by value, returning an opaque [`Scalar`]
    /// carrying the relational facts the rewrite rules read.
    pub fn intern_scalar(&mut self, expr: &MirScalarExpr) -> Scalar {
        let id = if let Some(id) = self.scalar_dedup.get(expr) {
            *id
        } else {
            let id = ScalarId(u32::try_from(self.scalars.len()).expect("scalar count fits u32"));
            self.scalars.push(expr.clone());
            self.scalar_dedup.insert(expr.clone(), id);
            id
        };
        let cols = expr.support();
        let is_col = expr.as_column();
        let lit = match expr.as_literal() {
            Some(Ok(d)) if d == mz_repr::Datum::True => Some(true),
            Some(Ok(d)) if d == mz_repr::Datum::False => Some(false),
            _ => None,
        };
        Scalar::interned(id, cols, is_col, lit, expr.to_string())
    }

    pub fn resolve_scalar(&self, id: ScalarId) -> &MirScalarExpr {
        &self.scalars[usize::cast_from(id.0)]
    }

    /// Intern a bailed subtree, recording its arity for the opaque leaf.
    pub fn intern_leaf(&mut self, subtree: MirRelationExpr) -> LeafId {
        let arity = subtree.arity();
        let id = LeafId(u32::try_from(self.leaves.len()).expect("leaf count fits u32"));
        self.leaves.push(subtree);
        self.leaf_arity.push(arity);
        id
    }

    pub fn resolve_leaf(&self, id: LeafId) -> &MirRelationExpr {
        &self.leaves[usize::cast_from(id.0)]
    }

    pub fn leaf_arity(&self, id: LeafId) -> usize {
        self.leaf_arity[usize::cast_from(id.0)]
    }
}
```

Add `use mz_ore::cast::CastFrom;` for `usize::cast_from`. Verify `MirScalarExpr::as_column`, `as_literal`, and `support` (trait `Columns`) names against `src/expr/src/scalar.rs` and adjust imports/calls. `MirScalarExpr` must implement `Ord` for the `BTreeMap` key (verify; it does in this repo).

- [ ] **Step 4: Declare the module** in `lib.rs`: add `pub mod interner;`.

- [ ] **Step 5: Run tests**

Run: `cargo test -p mz-transform-egraph interner`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/transform-egraph/src/interner.rs src/transform-egraph/src/lib.rs
git commit -m "transform-egraph: interner for opaque scalars and bailed subtrees"
```

---

### Task 5: Lowering MirRelationExpr -> Rel

**Files:**
- Create: `src/transform-egraph/src/lower.rs`
- Test: same file
- Modify: `src/transform-egraph/src/lib.rs` (`mod lower;`)

**Interfaces:**
- Consumes: `interner::{Interner, LeafId}`, `ir::Rel`.
- Produces: `pub fn lower(expr: &MirRelationExpr, interner: &mut Interner) -> Rel`.

Lowering encodes opaque leaves as `Rel::Get { name: format!("leaf:{}", id.0), arity }`. `LocalId`-bound `Get`s become `Rel::LocalGet { id, arity }`; `Let` becomes `Rel::Let`. Every unsupported variant (`FlatMap`, `Reduce`, `TopK`, `ArrangeBy`, `LetRec`) interns the whole subtree as one leaf. Note: the prototype `Rel::Reduce` exists, but M1 bails on `Reduce` to avoid lowering aggregate scalar structure; this is deliberate and documented in the spec.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::{Id, LocalId, MirRelationExpr, MirScalarExpr};
    use mz_repr::{RelationType, ScalarType};
    use crate::interner::Interner;
    use crate::ir::Rel;

    fn base(arity: usize) -> MirRelationExpr {
        let typ = RelationType::new((0..arity).map(|_| ScalarType::Int64.nullable(false)).collect());
        MirRelationExpr::constant(vec![], typ)
    }

    #[test]
    fn lower_filter_of_get() {
        let mut i = Interner::new();
        let r = base(2).filter(vec![MirScalarExpr::column(0)]);
        let rel = lower(&r, &mut i);
        match rel {
            Rel::Filter { predicates, .. } => assert_eq!(predicates.len(), 1),
            other => panic!("expected Filter, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_reduce_becomes_opaque_leaf_with_arity() {
        let mut i = Interner::new();
        // group_key = [#0], one aggregate => arity 2.
        let r = base(3).reduce(vec![MirScalarExpr::column(0)], vec![/* aggregate */], None);
        let rel = lower(&r, &mut i);
        match rel {
            Rel::Get { arity, name } => {
                assert!(name.starts_with("leaf:"));
                assert_eq!(arity, r.arity());
            }
            other => panic!("expected opaque leaf Get, got {other:?}"),
        }
    }

    #[test]
    fn filter_over_reduce_keeps_filter_envelope() {
        let mut i = Interner::new();
        let red = base(3).reduce(vec![MirScalarExpr::column(0)], vec![], None);
        let r = red.filter(vec![MirScalarExpr::column(0)]);
        let rel = lower(&r, &mut i);
        // Filter envelope is supported; the Reduce under it is one opaque leaf.
        matches!(rel, Rel::Filter { .. }).then_some(()).expect("filter envelope");
    }
}
```

(Adjust `reduce(...)` / aggregate construction to the real `MirRelationExpr::reduce` signature; if building an `AggregateExpr` is noisy, use a `TopK` or `ArrangeBy` node instead for the "unsupported leaf" tests. The point is: an unsupported variant lowers to `Rel::Get` with the right arity, and a supported parent keeps its shape.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p mz-transform-egraph lower`
Expected: FAIL (no `lower`).

- [ ] **Step 3: Implement `lower.rs`**

```rust
// Copyright header ...

//! Translate a real `MirRelationExpr` into the prototype `Rel`. Supported
//! variants map structurally; every unsupported variant is interned whole as an
//! opaque leaf `Get`, so the supported envelope around it still saturates.

use mz_expr::{Id, MirRelationExpr};

use crate::interner::Interner;
use crate::ir::{Rel, Scalar};

/// Lower `expr` to a `Rel`, interning scalars and bailing unsupported subtrees
/// to opaque leaves in `interner`.
pub fn lower(expr: &MirRelationExpr, interner: &mut Interner) -> Rel {
    use MirRelationExpr::*;
    match expr {
        Constant { rows, typ } => {
            let card = match rows {
                Ok(rows) => u64::try_from(rows.len()).unwrap_or(u64::MAX),
                Err(_) => 0,
            };
            Rel::Constant { card, arity: typ.arity() }
        }
        Get { id: Id::Local(local), typ, .. } => Rel::LocalGet {
            id: usize::from(local.clone()),
            arity: typ.arity(),
        },
        Get { id: Id::Global(g), typ, .. } => Rel::Get {
            name: format!("global:{g}"),
            arity: typ.arity(),
        },
        Project { input, outputs } => Rel::Project {
            input: Box::new(lower(input, interner)),
            outputs: outputs.clone(),
        },
        Map { input, scalars } => Rel::Map {
            input: Box::new(lower(input, interner)),
            scalars: intern_all(scalars, interner),
        },
        Filter { input, predicates } => Rel::Filter {
            input: Box::new(lower(input, interner)),
            predicates: intern_all(predicates, interner),
        },
        Join { inputs, equivalences, .. } => Rel::Join {
            inputs: inputs.iter().map(|i| lower(i, interner)).collect(),
            equivalences: equivalences
                .iter()
                .map(|class| intern_all(class, interner))
                .collect(),
        },
        Negate { input } => Rel::Negate { input: Box::new(lower(input, interner)) },
        Threshold { input } => Rel::Threshold { input: Box::new(lower(input, interner)) },
        Union { base, inputs } => Rel::Union {
            base: Box::new(lower(base, interner)),
            inputs: inputs.iter().map(|i| lower(i, interner)).collect(),
        },
        Let { id, value, body } => Rel::Let {
            id: usize::from(id.clone()),
            value: Box::new(lower(value, interner)),
            body: Box::new(lower(body, interner)),
        },
        // Unsupported in M1: bail to an opaque leaf carrying the real arity.
        FlatMap { .. } | Reduce { .. } | TopK { .. } | ArrangeBy { .. } | LetRec { .. } => {
            let arity = expr.arity();
            let id = interner.intern_leaf(expr.clone());
            Rel::Get { name: format!("leaf:{}", id.0), arity }
        }
    }
}

fn intern_all(
    exprs: &[mz_expr::MirScalarExpr],
    interner: &mut Interner,
) -> Vec<Scalar> {
    exprs.iter().map(|e| interner.intern_scalar(e)).collect()
}
```

Verify `LocalId` -> `usize` conversion (`usize::from`) against `src/expr`; if `LocalId` wraps a `u64`, use `usize::cast_from(local.into_inner())` with `mz_ore::cast::CastFrom`. Confirm the `Constant.rows` shape (`Result<Vec<(Row, Diff)>, EvalError>`).

- [ ] **Step 4: Run tests**

Run: `cargo test -p mz-transform-egraph lower`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/transform-egraph/src/lower.rs src/transform-egraph/src/lib.rs
git commit -m "transform-egraph: lower MirRelationExpr to Rel with per-subtree bail"
```

---

### Task 6: Raising Rel -> MirRelationExpr

**Files:**
- Create: `src/transform-egraph/src/raise.rs`
- Test: same file
- Modify: `src/transform-egraph/src/lib.rs` (`mod raise;`)

**Interfaces:**
- Consumes: `interner::Interner`, `ir::Rel`.
- Produces: `pub fn raise(rel: &Rel, interner: &Interner) -> MirRelationExpr`.

Raising inverts lowering: a leaf `Get { name: "leaf:N" }` re-expands to `interner.resolve_leaf(LeafId(N))`; `Scalar`s resolve via `interner.resolve_scalar(id)`. The cleanest constructors are the `MirRelationExpr` builder methods (`.filter`, `.map`, `.project`, `.negate`, `.threshold`, `.union`, etc.). `Rel::WcoJoin` must never appear (rule gated off in Task 8); raising it `unreachable!`s with a clear message.

- [ ] **Step 1: Write the failing round-trip test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::{MirRelationExpr, MirScalarExpr};
    use mz_repr::{RelationType, ScalarType};
    use crate::interner::Interner;
    use crate::lower::lower;

    fn base(arity: usize) -> MirRelationExpr {
        let typ = RelationType::new((0..arity).map(|_| ScalarType::Int64.nullable(false)).collect());
        MirRelationExpr::constant(vec![], typ)
    }

    fn roundtrip(r: MirRelationExpr) {
        let mut i = Interner::new();
        let rel = lower(&r, &mut i);
        let back = raise(&rel, &i);
        assert_eq!(back, r, "round-trip changed the plan");
    }

    #[test]
    fn roundtrip_filter_map_union() {
        let a = base(2).filter(vec![MirScalarExpr::column(0)]);
        let b = base(2).map(vec![MirScalarExpr::column(1)]);
        roundtrip(a.union(b));
    }

    #[test]
    fn roundtrip_unsupported_is_identity() {
        let r = base(3).top_k(vec![0], vec![], None, 0, false, None);
        roundtrip(r); // whole plan is one opaque leaf, must come back identical
    }
}
```

(Adjust `top_k` to the real signature; any unsupported variant works.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p mz-transform-egraph raise`
Expected: FAIL (no `raise`).

- [ ] **Step 3: Implement `raise.rs`**

```rust
// Copyright header ...

//! Translate a `Rel` back into a real `MirRelationExpr`, resolving opaque
//! scalars and re-expanding bailed leaves through the interner.

use mz_expr::{Id, LocalId, MirRelationExpr, MirScalarExpr};
use mz_repr::RelationType;

use crate::interner::{Interner, LeafId};
use crate::ir::{Rel, Scalar};

/// Raise `rel` to a `MirRelationExpr`. Inverse of [`crate::lower::lower`].
pub fn raise(rel: &Rel, interner: &Interner) -> MirRelationExpr {
    match rel {
        Rel::Constant { arity, .. } => {
            // A bailed/constant leaf reconstructed from arity is only used for
            // genuine constants the lowering produced; rows are not modeled, so
            // re-emit an empty constant of the right arity.
            MirRelationExpr::constant(vec![], dummy_type(*arity))
        }
        Rel::Get { name, arity } => {
            if let Some(n) = name.strip_prefix("leaf:") {
                let id = LeafId(n.parse().expect("leaf id"));
                interner.resolve_leaf(id).clone()
            } else if let Some(g) = name.strip_prefix("global:") {
                MirRelationExpr::Get {
                    id: Id::Global(g.parse().expect("global id")),
                    typ: dummy_type(*arity),
                    access_strategy: Default::default(),
                }
            } else {
                unreachable!("unknown Get name {name}")
            }
        }
        Rel::LocalGet { id, arity } => MirRelationExpr::Get {
            id: Id::Local(LocalId::new(u64::cast_from(*id))),
            typ: dummy_type(*arity),
            access_strategy: Default::default(),
        },
        Rel::Project { input, outputs } => raise(input, interner).project(outputs.clone()),
        Rel::Map { input, scalars } => raise(input, interner).map(resolve(scalars, interner)),
        Rel::Filter { input, predicates } => {
            raise(input, interner).filter(resolve(predicates, interner))
        }
        Rel::Join { inputs, equivalences } => MirRelationExpr::join_scalars(
            inputs.iter().map(|i| raise(i, interner)).collect(),
            equivalences
                .iter()
                .map(|class| resolve(class, interner))
                .collect(),
        ),
        Rel::Negate { input } => raise(input, interner).negate(),
        Rel::Threshold { input } => raise(input, interner).threshold(),
        Rel::Union { base, inputs } => {
            let base = raise(base, interner);
            let inputs = inputs.iter().map(|i| raise(i, interner)).collect();
            MirRelationExpr::Union { base: Box::new(base), inputs }
        }
        Rel::Let { id, value, body } => MirRelationExpr::Let {
            id: LocalId::new(u64::cast_from(*id)),
            value: Box::new(raise(value, interner)),
            body: Box::new(raise(body, interner)),
        },
        Rel::Reduce { .. } | Rel::LetRec { .. } => {
            unreachable!("M1 never produces Reduce/LetRec in Rel; they lower to opaque leaves")
        }
        Rel::WcoJoin { .. } => {
            unreachable!("join_to_wcoj is disabled in M1; raise cannot encode WcoJoin")
        }
    }
}

fn resolve(scalars: &[Scalar], interner: &Interner) -> Vec<MirScalarExpr> {
    scalars.iter().map(|s| interner.resolve_scalar(s.id).clone()).collect()
}

/// A placeholder relation type of the given arity. Raised plans are re-typed by
/// the surrounding optimizer; M1 round-trip tests only compare structure that
/// does not depend on column types for the supported variants. Bailed leaves
/// carry their real type because they are re-expanded verbatim.
fn dummy_type(arity: usize) -> RelationType {
    RelationType::new(
        (0..arity)
            .map(|_| mz_repr::ScalarType::Int64.nullable(true))
            .collect(),
    )
}
```

IMPORTANT verifications during Step 3 (adjust code to the real API):
* The exact constructor for a join from `inputs` + `equivalences: Vec<Vec<MirScalarExpr>>`. There may be `MirRelationExpr::join_scalars` or you build `MirRelationExpr::Join { inputs, equivalences, implementation: JoinImplementation::Unimplemented }` directly. Use the direct struct form if no helper exists.
* `LocalId::new` / `Id::Global` parsing and `access_strategy: Default::default()` field name.
* `u64::cast_from` needs `use mz_ore::cast::CastFrom;`.
* The round-trip equality test for `Constant`/`Get`/`LocalGet` will FAIL if `dummy_type` differs from the original type. To make round-trip exact: also intern the original `RelationType` for `Constant`/`Get`/`LocalGet` in lowering (store the type in the interner keyed like leaves) and resolve it here, OR lower `Constant` and global `Get` as opaque leaves too (simplest: treat `Constant` with rows and any `Get` as a leaf, since M1 has no rule that rewrites them). Prefer the latter: in `lower.rs`, also bail `Constant`/global `Get` to opaque leaves; only `LocalGet` (which carries no type-sensitive payload a rule inspects) stays structural. Update Task 5's lowering accordingly and re-run its tests. This guarantees byte-exact round-trip.

- [ ] **Step 4: Reconcile lowering** per the note above: in `lower.rs`, move `Constant { .. }` and global `Get { .. }` into the bail arm (opaque leaf). Keep `Get { Id::Local }` -> `Rel::LocalGet`. Re-run Task 5 tests; fix the `unsupported_*` tests if they assumed `Constant` lowered structurally.

- [ ] **Step 5: Run tests**

Run: `cargo test -p mz-transform-egraph`
Expected: PASS (lower + raise round-trips).

- [ ] **Step 6: Commit**

```bash
git add src/transform-egraph/src/raise.rs src/transform-egraph/src/lower.rs src/transform-egraph/src/lib.rs
git commit -m "transform-egraph: raise Rel to MirRelationExpr with exact round-trip"
```

---

### Task 7: The `optimize` entry point

**Files:**
- Modify: `src/transform-egraph/src/lib.rs`
- Test: `src/transform-egraph/tests/roundtrip.rs`

**Interfaces:**
- Consumes: `lower::lower`, `raise::raise`, `interner::Interner`, `engine::Optimizer`, `parser::parse_ruleset`, `cost::CostModel`.
- Produces: `pub fn optimize(expr: MirRelationExpr) -> MirRelationExpr`; `pub fn default_ruleset() -> dsl::RuleSet`.

- [ ] **Step 1: Write the failing integration test** `src/transform-egraph/tests/roundtrip.rs`

```rust
use mz_expr::{MirRelationExpr, MirScalarExpr};
use mz_repr::{RelationType, ScalarType};
use mz_transform_egraph::optimize;

fn base(arity: usize) -> MirRelationExpr {
    let typ = RelationType::new((0..arity).map(|_| ScalarType::Int64.nullable(false)).collect());
    MirRelationExpr::constant(vec![], typ)
}

#[test]
fn merges_nested_filters() {
    // filter(p, filter(q, r)) should fuse to a single filter of two predicates.
    let r = base(2)
        .filter(vec![MirScalarExpr::column(1)])
        .filter(vec![MirScalarExpr::column(0)]);
    let out = optimize(r);
    let mut filters = 0;
    out.visit_pre(|e| if matches!(e, MirRelationExpr::Filter { .. }) { filters += 1 });
    assert_eq!(filters, 1, "nested filters should fuse");
}

#[test]
fn arity_is_preserved() {
    let r = base(3).map(vec![MirScalarExpr::column(0)]);
    let before = r.arity();
    assert_eq!(optimize(r).arity(), before);
}
```

(Confirm `MirRelationExpr::visit_pre` signature; if it takes `&mut dyn FnMut`, adapt.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p mz-transform-egraph --test roundtrip`
Expected: FAIL (optimize is a no-op stub; `merges_nested_filters` fails with 2 filters).

- [ ] **Step 3: Implement `optimize` in `lib.rs`**

```rust
use crate::cost::CostModel;
use crate::engine::Optimizer;
use crate::interner::Interner;

/// Parse the built-in rule set, panicking on a malformed embedded file.
pub fn default_ruleset() -> dsl::RuleSet {
    parser::parse_ruleset(RULES_SRC).expect("built-in rules must parse")
}

/// Optimize `expr` by equality saturation over the supported relational subset,
/// bailing per-subtree on unsupported variants. Functionally equivalent output.
pub fn optimize(expr: MirRelationExpr) -> MirRelationExpr {
    let mut interner = Interner::new();
    let rel = lower::lower(&expr, &mut interner);
    let optimizer = Optimizer::new(default_ruleset(), CostModel::new());
    let best = optimizer.optimize(rel).plan;
    let out = raise::raise(&best, &interner);
    debug_assert_eq!(out.arity(), expr.arity(), "egraph pass changed arity");
    out
}
```

Add the needed `mod`/`use` lines (`mod lower; mod raise;`, `use mz_expr::MirRelationExpr;`).

- [ ] **Step 4: Run tests**

Run: `cargo test -p mz-transform-egraph`
Expected: PASS (filters fuse; arity preserved).

- [ ] **Step 5: Commit**

```bash
git add src/transform-egraph/src/lib.rs src/transform-egraph/tests/roundtrip.rs
git commit -m "transform-egraph: optimize entry point (lower, saturate, raise)"
```

---

### Task 8: Gate off column-remapping and no-MIR-target rules

**Files:**
- Modify: `src/transform-egraph/rules/relational.rewrite`
- Test: `src/transform-egraph/tests/roundtrip.rs` (add a guard test)

**Interfaces:**
- Produces: a rule file whose active rules never remap column indices and never emit `WcoJoin`.

Rules to REMOVE (or comment out with a `# M1-disabled:` prefix and a one-line reason) because they remap columns, have no MIR target, or need a scalar optimizer:
* `push_filter_into_join_first`, `push_filter_into_join_second` (rebase predicate columns to a join input)
* `push_filter_past_project` (remaps predicate columns through the projection)
* `join_to_wcoj` (no `WcoJoin` variant in `MirRelationExpr`)
* any rule whose RHS uses a column-arithmetic combinator (`shift`/`remap`) introduced by the prototype's "column arithmetic" phase

Rules to KEEP (relocate/concat/restructure only): `merge_filters`, `fuse_projects`, `fuse_maps`, `push_filter_through_map`, `push_filter_through_negate`, `push_filter_through_threshold`, `distribute_filter_union`(`_nary`), `distribute_negate_union`(`_nary`), `flatten_union`(`_nary`), `flatten_join_first`, `negate_negate`, `threshold_idempotent`, `threshold_elision`, `union_cancel`, `reduce_elision`, `drop_true_filter`, `empty_false_filter`, `map_columns_to_projection`.

Audit `flatten_join_first`: confirm it preserves the global column order (inputs concatenated left to right) so equivalences stay valid without remap. If it reorders inputs, disable it too and note it.

- [ ] **Step 1: Write the guard test** in `tests/roundtrip.rs`

```rust
#[test]
fn no_disabled_rule_is_active() {
    let rs = mz_transform_egraph::default_ruleset();
    for banned in ["push_filter_into_join_first", "push_filter_into_join_second",
                   "push_filter_past_project", "join_to_wcoj"] {
        assert!(!rs.rule_names().iter().any(|n| n == banned),
            "rule {banned} must be disabled in M1");
    }
}
```

(Add a `pub fn rule_names(&self) -> Vec<&str>` to `dsl::RuleSet` if absent: returns each rule's name.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p mz-transform-egraph --test roundtrip no_disabled`
Expected: FAIL (banned rules still present).

- [ ] **Step 3: Edit the rule file** — remove/comment the banned rules listed above; add `RuleSet::rule_names` if needed.

- [ ] **Step 4: Run the whole test suite**

Run: `cargo test -p mz-transform-egraph`
Expected: PASS. The ported engine tests that relied on a now-disabled rule (e.g. a filter-into-join test) must be deleted or moved behind the prototype crate; note any removed test in the commit message.

- [ ] **Step 5: Commit**

```bash
git add src/transform-egraph/rules/relational.rewrite src/transform-egraph/src/dsl.rs src/transform-egraph/tests/roundtrip.rs
git commit -m "transform-egraph: restrict M1 rules to non-remapping rewrites"
```

---

### Task 9: `EqSatTransform` and the datadriven `apply=EqSat` arm

**Files:**
- Create: `src/transform-egraph/src/transform.rs`
- Modify: `src/transform-egraph/src/lib.rs` (`pub mod transform; pub use transform::EqSatTransform;`)
- Modify: `src/transform/Cargo.toml` (dev-dependency on `mz-transform-egraph`)
- Modify: `src/transform/tests/test_runner.rs:282` (`get_transform` match arm)
- Create: `src/transform/tests/test_transforms/eqsat.spec`

**Interfaces:**
- Consumes: `optimize`.
- Produces: `pub struct EqSatTransform; impl Transform for EqSatTransform`.

- [ ] **Step 1: Write `transform.rs`**

```rust
// Copyright header ...

//! A `Transform` wrapper over [`crate::optimize`], for the datadriven test
//! harness. Not registered in the live optimizer pipeline in milestone 1.

use mz_expr::MirRelationExpr;
use mz_transform::{Transform, TransformCtx, TransformError};

/// Runs the equality-saturation pass as a `Transform`.
#[derive(Debug)]
pub struct EqSatTransform;

impl Transform for EqSatTransform {
    fn name(&self) -> &'static str {
        "EqSatTransform"
    }

    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        _ctx: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        let taken = std::mem::replace(relation, MirRelationExpr::constant(vec![], mz_repr::RelationType::empty()));
        *relation = crate::optimize(taken);
        Ok(())
    }
}
```

(Confirm `RelationType::empty()` exists; otherwise use a 0-arity `RelationType::new(vec![])`.)

- [ ] **Step 2: Declare and re-export** in `lib.rs`: `pub mod transform; pub use transform::EqSatTransform;`.

- [ ] **Step 3: Add the dev-dependency** to `src/transform/Cargo.toml` under `[dev-dependencies]`:

```toml
mz-transform-egraph = { path = "../transform-egraph" }
```

- [ ] **Step 4: Add the match arm** in `src/transform/tests/test_runner.rs` `get_transform` (alphabetically near the others):

```rust
            "EqSat" => Ok(Box::new(mz_transform_egraph::EqSatTransform)),
```

- [ ] **Step 5: Write `src/transform/tests/test_transforms/eqsat.spec`**

Model the structure on an existing `.spec` (e.g. `fusion`). Provide: a `cat`/`build` to define a source, then `build apply=EqSat` blocks with `explain` output. Include at least:
* a nested-filter case (asserts fusion),
* a filtered-union case (asserts saturation result),
* a case with a `Reduce` or `TopK` under a `Filter` (asserts the supported envelope rewrites and the unsupported node is preserved verbatim).

Leave the expected-output regions empty and generate them with REWRITE in Step 6. Example skeleton (copy real directive syntax from `tests/test_transforms/fusion`):

```
cat
(defsource x [int64 int64])
----
ok

build apply=EqSat
(filter (filter (get x) [#0]) [#1])
----
----
EXPECTED OUTPUT FILLED BY REWRITE
----
----
```

- [ ] **Step 6: Generate expected output and run**

Run: `REWRITE=1 cargo test -p mz-transform --test test_transforms`
then: `cargo test -p mz-transform --test test_transforms`
Expected: PASS; inspect the rewritten `eqsat.spec` to confirm the fusion, the filtered-union result, and the preserved unsupported node are correct.

- [ ] **Step 7: Commit**

```bash
git add src/transform-egraph/src/transform.rs src/transform-egraph/src/lib.rs \
        src/transform/Cargo.toml src/transform/tests/test_runner.rs \
        src/transform/tests/test_transforms/eqsat.spec
git commit -m "transform-egraph: EqSatTransform and apply=EqSat datadriven tests"
```

---

### Task 10: Final verification and lint

**Files:** none (verification only).

- [ ] **Step 1: Workspace build**

Run: `cargo check -p mz-transform-egraph -p mz-transform`
Expected: clean.

- [ ] **Step 2: Format and lint**

Run: `cargo fmt` then `bin/lint`
Expected: no unexpected warnings. Fix any.

- [ ] **Step 3: Full test pass**

Run: `cargo test -p mz-transform-egraph` and `cargo test -p mz-transform --test test_transforms`
Expected: all pass.

- [ ] **Step 4: Confirm not wired into the live pipeline**

Run: `grep -rn "EqSatTransform\|transform_egraph\|transform-egraph" src/transform/src`
Expected: NO matches in `src/` (only in `tests/` and `Cargo.toml` dev-deps). This proves M1 stays offline.

- [ ] **Step 5: Commit any fmt/lint fixups**

```bash
git add -u
git commit -m "transform-egraph: fmt and lint"
```

---

## Self-review

* **Spec coverage:** crate + workspace (Task 1); engine port (2); opaque-scalar identity (3); interner for scalars + bailed leaves (4); per-subtree bail lowering (5); exact round-trip raising (6); `optimize` reusing `engine::Optimizer` + abstract cost (7); rule gating incl. `WcoJoin`/scalar-IR off (8); `Transform` impl + `apply=EqSat` datadriven tests incl. unsupported-node assertions (9); offline-only proof (10, Step 4). Statistics: spec says accept-but-ignore; M1 `optimize` takes no oracle, matching "ignored when blank" — a later milestone adds `EqSatOptions`. NOTE this simplification is intentional for M1.
* **Placeholder scan:** rule names, file paths, and code are concrete. The few "verify against real API" notes (LocalId conversion, join constructor, literal helpers) are explicit reconciliation steps, not deferred work.
* **Type consistency:** `optimize(MirRelationExpr) -> MirRelationExpr`, `lower(&MirRelationExpr, &mut Interner) -> Rel`, `raise(&Rel, &Interner) -> MirRelationExpr`, `Scalar { id, cols, is_col, lit, text }`, `ScalarId(u32)`, `LeafId(u32)` are used consistently across Tasks 3-9.
