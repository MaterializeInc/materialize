# Reuse-aware arrangement-sharing cost model implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-aim the equality-saturation MIR optimizer at minimizing the count of distinct maintained arrangements (reuse-aware), behind pluggable `Objective`/`Extractor` abstractions, absorbing the join order and key decision into saturation and adding an ILP extractor.

**Architecture:** The cost model already tracks per-plan arrangement identity (`ArrId`) and oracle credit (`input_already_arranged`); the memory axis just measures the wrong quantity (peak degree, lexicographic). This plan adds an exact reuse-aware arrangement count to `Cost`, factors the comparison into an `Objective` trait and extraction into an `Extractor` trait, makes per-input join keys explicit e-nodes plus join binarization/commutativity rules so saturation chooses the order and key whose arrangements align, reuses the production join planner as a mechanical realizer at raise, adds an ILP extractor for the non-compositional set-cardinality objective, and gates the whole change on a measured corpus invariant `eqsat_arrangement_count <= production_arrangement_count`.

**Tech Stack:** Rust, `mz_transform::eqsat` (`src/transform/src/eqsat/`), the `.rewrite` DSL compiled by `src/transform/build.rs`, sqllogictest goldens, `good_lp`/`microlp` for ILP (pending dependency confirmation in Task 9).

**Source spec:** `docs/superpowers/specs/2026-06-22-eqsat-arrangement-sharing-cost-model-design.md`.

## Global Constraints

* All changes confined to `src/transform/src/eqsat/` (plus `src/transform/src/join_implementation.rs` realizer reuse, `src/transform/build.rs` for DSL, root `Cargo.toml` + `deny.toml` + `about.toml` for the ILP dep). No edits under `doc/developer/generated/`.
* No `as` conversions: use `mz_ore::cast::{CastFrom, CastLossy}`.
* No `unsafe` without a `SAFETY:` comment.
* Comments: no em-dashes, no structuring semicolons; doc comment states the contract, reasoning lives inline at the decision point; no vendor names in user-facing surfaces; never drop existing comments when editing.
* The metric: an arrangement is `(canonical e-class id / arranging node, key)`. Plan memory cost = count of distinct arrangements minus oracle-covered (existing index or materialized view on the same collection and key). Identical arrangements counted once (sharing). Time stays the AGM secondary axis.
* Subsume/include audit (governing): `CanonicalizeMfp` subsume; `JoinImplementation` subsume the decision, include the realization; `LiteralConstraints`/IndexedFilter include (oracle seed); `Demand`+`ProjectionPushdown` include-for-now.
* Validation gate: corpus invariant `eqsat_arrangement_count <= production_arrangement_count`; SLT differential gate (eqsat on vs off) green. Perf tracked, not gated (research prototype).
* Both eqsat flags default ON. Debug correctness bugs immediately while fresh.
* Before any commit: `cargo fmt`, `bin/lint`, `cargo clippy`. Tests via the mz-test skill: `bin/sqllogictest --optimized -- PATH` (check), `bin/sqllogictest -- --rewrite-results PATH` (rewrite). sqllogictest takes a LIST of files in one invocation. `cargo test` / `bin/cargo-test` for Rust unit tests.
* Commit trailers on every commit:
  ```
  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_016CwwG1wCgqf3v8g6uhf1Nx
  ```

---

## File structure

* `src/transform/src/eqsat/cost.rs` (modify) — add `arrangements: usize` to `Cost`; compute it reuse-aware in the existing `collect_memory_into` walk; extend oracle credit to `ArrangeBy(Get)`.
* `src/transform/src/eqsat/objective.rs` (create) — `Objective` trait, `PeakDegree` (legacy) and `ArrangementCount` (new default) impls.
* `src/transform/src/eqsat/extract.rs` (create) — `Extractor` trait, `GreedyExtractor` (moved from `egraph.rs`), `IlpExtractor` (Task 9).
* `src/transform/src/eqsat/egraph.rs` (modify) — `extract`/`extract_with` delegate to an `Extractor` + `&dyn Objective`; expose the e-graph accessors the ILP encoder needs.
* `src/transform/src/eqsat/engine.rs` (modify) — `Optimizer` carries the chosen `Extractor`/`Objective`; threads them into `extract`.
* `src/transform/src/eqsat.rs` (modify) — `optimize_inner` selects the objective/extractor (default `ArrangementCount` + greedy, ILP behind a feature).
* `src/transform/src/eqsat/transform.rs` (modify) — read the objective-selection feature; pass to `optimize_inner`.
* `src/transform/src/eqsat/rules/relational.rewrite` (modify) — join binarization, commutativity, explicit-arrange-by-key rules.
* `src/transform/build.rs` + `src/transform/src/eqsat/dsl.rs` (modify) — register new DSL helper functions the join rules need.
* `src/transform/src/eqsat/raise.rs` (modify) — join realizer for the e-graph-chosen order/key; drop the `coalesce_mfp` crutch where saturation subsumes it.
* `src/transform/src/eqsat/validation.rs` (create) — arrangement-count corpus invariant test harness.
* `Cargo.toml`, `deny.toml`, `about.toml` (modify) — ILP dependency (Task 9).

---

## Task 1: `Objective` trait and the legacy `PeakDegree` objective (behavior-preserving)

**Files:**
* Create: `src/transform/src/eqsat/objective.rs`
* Modify: `src/transform/src/eqsat.rs` (add `mod objective;`)
* Modify: `src/transform/src/eqsat/egraph.rs:1295-1365` (`extract_with` takes `&dyn Objective`)
* Test: `src/transform/src/eqsat/objective.rs` (`#[cfg(test)]` module)

**Interfaces:**
* Consumes: `cost::Cost` (`{ memory: Vec<f64>, time: Vec<f64>, nodes: usize }`), `Cost::cmp_memory_first`, `Cost::cmp_time_first`.
* Produces:
  ```rust
  pub trait Objective {
      /// Order two candidate costs. `Less` means the first plan is preferred.
      fn cmp(&self, a: &Cost, b: &Cost) -> std::cmp::Ordering;
      /// Stable name for diagnostics and A/B selection.
      fn name(&self) -> &'static str;
  }
  pub struct PeakDegree;   // delegates to Cost::cmp_memory_first
  ```

- [ ] **Step 1: Write the failing test**

In a new `#[cfg(test)] mod tests` in `src/transform/src/eqsat/objective.rs`:

```rust
#[mz_ore::test]
fn peak_degree_matches_cmp_memory_first() {
    use crate::eqsat::cost::Cost;
    let a = Cost { memory: vec![2.0], time: vec![2.0, 1.5], nodes: 3 };
    let b = Cost { memory: vec![1.0, 1.0, 1.0], time: vec![1.5], nodes: 5 };
    let obj = PeakDegree;
    assert_eq!(obj.cmp(&a, &b), a.cmp_memory_first(&b));
    assert_eq!(obj.name(), "peak-degree");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bin/cargo-test -p mz-transform eqsat::objective`
Expected: FAIL with "cannot find type `PeakDegree`" / "module `objective` not found".

- [ ] **Step 3: Create the objective module**

`src/transform/src/eqsat/objective.rs`:

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Pluggable extraction objectives.
//!
//! An [`Objective`] orders two candidate plan costs so the extractor can pick
//! the cheaper one. The e-graph machinery and the [`crate::eqsat::cost`] model
//! are independent of which objective is chosen: the cost model computes every
//! axis, and the objective decides which axis dominates.

use crate::eqsat::cost::Cost;

/// Orders candidate plan costs for extraction. `Less` means preferred.
pub trait Objective {
    /// Order two candidate costs. `Less` means the first plan is preferred.
    fn cmp(&self, a: &Cost, b: &Cost) -> std::cmp::Ordering;
    /// Stable name for diagnostics and A/B selection.
    fn name(&self) -> &'static str;
}

/// The legacy objective: minimize worst-case peak arrangement degree, then
/// time, then node count. Retained for regression comparison against the new
/// arrangement-count objective.
pub struct PeakDegree;

impl Objective for PeakDegree {
    fn cmp(&self, a: &Cost, b: &Cost) -> std::cmp::Ordering {
        a.cmp_memory_first(b)
    }
    fn name(&self) -> &'static str {
        "peak-degree"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn peak_degree_matches_cmp_memory_first() {
        use crate::eqsat::cost::Cost;
        let a = Cost { memory: vec![2.0], time: vec![2.0, 1.5], nodes: 3 };
        let b = Cost { memory: vec![1.0, 1.0, 1.0], time: vec![1.5], nodes: 5 };
        let obj = PeakDegree;
        assert_eq!(obj.cmp(&a, &b), a.cmp_memory_first(&b));
        assert_eq!(obj.name(), "peak-degree");
    }
}
```

Add `mod objective;` to the eqsat module declarations in `src/transform/src/eqsat.rs` (next to the existing `mod cost;` / `mod cse;` lines).

- [ ] **Step 4: Run test to verify it passes**

Run: `bin/cargo-test -p mz-transform eqsat::objective`
Expected: PASS.

- [ ] **Step 5: Thread `&dyn Objective` into `extract_with` (replace `memory_first: bool`)**

In `egraph.rs`, change the selector parameter. Current (`egraph.rs:1295-1301`):

```rust
pub fn extract_with(&self, root: Id, model: &CostModel, memory_first: bool) -> Option<Rel> {
    let cmp: &dyn Fn(&Cost, &Cost) -> std::cmp::Ordering = if memory_first {
        &|a, b| a.cmp_memory_first(b)
    } else {
        &|a, b| a.cmp_time_first(b)
    };
    // ...
```

New:

```rust
pub fn extract_with(
    &self,
    root: Id,
    model: &CostModel,
    objective: &dyn crate::eqsat::objective::Objective,
) -> Option<Rel> {
    let cmp = |a: &Cost, b: &Cost| objective.cmp(a, b);
    // ... body unchanged; replace the two `cmp(&c, bc)` call sites, which already
    // call `cmp`, so no further edits inside the loop.
```

Update `extract` (`egraph.rs:1283-1285`) to pass `PeakDegree` so existing callers are unchanged this task:

```rust
pub fn extract(&self, root: Id, model: &CostModel) -> Option<Rel> {
    self.extract_with(root, model, &crate::eqsat::objective::PeakDegree)
}
```

Changing the signature breaks every `extract_with(.., bool)` caller, so they must be updated this task to keep the crate compiling. The engine has a time-first call site (`engine.rs:184`, old `false`), so add a `TimeFirst` objective alongside `PeakDegree`:

```rust
/// Time-first objective: minimize worst-case time, then peak degree, then
/// node count. The optimizer extracts a time-first alternate to surface a
/// faster-but-heavier recommendation.
pub struct TimeFirst;

impl Objective for TimeFirst {
    fn cmp(&self, a: &Cost, b: &Cost) -> std::cmp::Ordering {
        a.cmp_time_first(b)
    }
    fn name(&self) -> &'static str {
        "time-first"
    }
}
```

Then update the engine call sites: `extract_with(root, &self.model, true)` -> `&PeakDegree`, `extract_with(root, &self.model, false)` -> `&TimeFirst` (`engine.rs:181, 184`). The `extract(..)` call sites (224, 352, 400) are unchanged (they call the bool-free `extract`). Task 5 re-routes all of these through the configured extractor and objective.

- [ ] **Step 6: Run the eqsat unit + golden tests to verify no behavior change**

Run: `bin/cargo-test -p mz-transform eqsat`
Expected: PASS (extraction behavior identical; only the selector type changed).

- [ ] **Step 7: Commit**

```bash
git add src/transform/src/eqsat/objective.rs src/transform/src/eqsat.rs src/transform/src/eqsat/egraph.rs
git commit -m "transform: introduce pluggable eqsat extraction Objective"
```

---

## Task 2: Add reuse-aware arrangement count to `Cost`

**Files:**
* Modify: `src/transform/src/eqsat/cost.rs` (`Cost` struct ~85-102; `cost` ~289-305; `collect_memory`/`collect_memory_into` 359-454; `input_already_arranged` 469-504; all `Cost { .. }` literals)
* Test: `src/transform/src/eqsat/cost.rs` (`#[cfg(test)]`)

**Interfaces:**
* Consumes: `ArrId` (`Node(Rel)` / `JoinInput { input: Rel, key: BTreeSet<usize> }`), `global_id_from_leaf`, `join_key_cols_for_input`, `size_degree`.
* Produces: `Cost { arrangements: usize, memory: Vec<f64>, time: Vec<f64>, nodes: usize }` where `arrangements` is the count of distinct non-oracle-covered arrangements the plan maintains.

- [ ] **Step 1: Write the failing test**

```rust
#[mz_ore::test]
fn arrangement_count_dedups_and_credits_oracle() {
    use crate::eqsat::ir::{EScalar, Rel};
    use mz_expr::MirScalarExpr;
    // Two ArrangeBy nodes on the same collection+key collapse to one arrangement.
    let base = Rel::Get { name: "t".into(), arity: 2 };
    let key = vec![EScalar::plain(MirScalarExpr::column(0))];
    let arr = Rel::ArrangeBy { input: Box::new(base.clone()), key: key.clone() };
    let plan = Rel::Union { base: Box::new(arr.clone()), inputs: vec![arr.clone()] };
    let model = CostModel::new();
    assert_eq!(model.cost(&plan).arrangements, 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bin/cargo-test -p mz-transform eqsat::cost::tests::arrangement_count`
Expected: FAIL with "no field `arrangements` on type `Cost`".

- [ ] **Step 3: Extend the `Cost` struct**

In `cost.rs` (struct at ~85-102), add the field as the first member with a doc comment:

```rust
pub struct Cost {
    /// Count of distinct maintained arrangements, reuse-aware: arrangements
    /// covered by an existing index or materialized view (the oracle) and
    /// arrangements shared within the plan are excluded. This is the primary
    /// memory quantity the arrangement-count objective minimizes.
    pub arrangements: usize,
    /// Memory-term degrees (sorted descending, entries <= EPS dropped).
    // ... existing doc + field unchanged ...
    pub memory: Vec<f64>,
    pub time: Vec<f64>,
    pub nodes: usize,
}
```

- [ ] **Step 4: Compute the count in `CostModel::cost`**

`collect_memory` (cost.rs:359-368) already builds `seen: BTreeSet<ArrId>` of distinct charged arrangements. Refactor so the count is returned alongside the degree vector. Change `collect_memory` to return the seen-set size:

```rust
/// Accumulate the memory-term degrees (MEMORY axis) of every node into `out`,
/// returning the count of distinct reuse-aware arrangements charged.
fn collect_memory(&self, rel: &Rel, out: &mut Vec<f64>) -> usize {
    let mut seen: BTreeSet<ArrId> = BTreeSet::new();
    self.collect_memory_into(rel, out, &mut seen);
    seen.len()
}
```

In `cost` (cost.rs:289-305):

```rust
let mut memory = Vec::new();
let arrangements = self.collect_memory(rel, &mut memory);
memory.retain(|d| *d > EPS);
memory.sort_by(|a, b| b.partial_cmp(a).unwrap());

Cost {
    arrangements,
    memory,
    time,
    nodes: rel.node_count(),
}
```

- [ ] **Step 5: Extend oracle credit to `ArrangeBy(Get)`**

Currently only join inputs get oracle credit (`input_already_arranged`); an `ArrangeBy` always charges (`collect_memory_into` 381-385). For the count to be reuse-aware an `ArrangeBy` whose collection+key matches an available index must be free. Modify the `ArrangeBy` branch:

```rust
Rel::ArrangeBy { input, key } => {
    // An explicit arrangement whose collection and key match an available
    // index already exists, so it is not charged. `arrange_by_oracle_covered`
    // matches a bare opaque global Get under the ArrangeBy against the oracle.
    if !self.arrange_by_oracle_covered(input, key)
        && seen.insert(ArrId::Node(rel.clone()))
    {
        out.push(self.size_degree(input));
    }
}
Rel::Reduce { input, .. } | Rel::TopK { input, .. } => {
    if seen.insert(ArrId::Node(rel.clone())) {
        out.push(self.size_degree(input));
    }
}
```

Add the helper next to `input_already_arranged`:

```rust
/// Whether an `ArrangeBy(input, key)` is already covered by an available
/// index. Matches only a bare opaque global `Get` under the arrangement
/// (no intervening column-shifting wrappers), against an index whose key
/// column set equals `key`'s column set.
fn arrange_by_oracle_covered(&self, input: &Rel, key: &[EScalar]) -> bool {
    if self.available.is_empty() {
        return false;
    }
    let gid = match global_id_from_leaf(input) {
        Some(g) => g,
        None => return false,
    };
    let keys = match self.available.get(&gid) {
        Some(ks) => ks,
        None => return false,
    };
    let want: BTreeSet<usize> = key.iter().filter_map(|e| e.is_col()).collect();
    if want.len() != key.len() {
        // The key contains a non-column expression an index cannot match.
        return false;
    }
    keys.iter().any(|k| {
        let have: BTreeSet<usize> = k.iter().filter_map(|e| e.as_column()).collect();
        have == want
    })
}
```

- [ ] **Step 6: Fix all `Cost { .. }` literals**

`terms_cost` (cost.rs:652-659) and any other `Cost { memory, time, nodes }` literal must set `arrangements: 0`. The binary-join DP only compares time vectors, so 0 is correct there. Search:

Run: `grep -rn "Cost {" src/transform/src/eqsat/`
Add `arrangements: 0,` to each non-`self.cost` literal.

- [ ] **Step 7: Run tests to verify they pass**

Run: `bin/cargo-test -p mz-transform eqsat::cost`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/transform/src/eqsat/cost.rs
git commit -m "transform: add reuse-aware arrangement count to eqsat Cost"
```

---

## Task 3: `ArrangementCount` objective

**Files:**
* Modify: `src/transform/src/eqsat/objective.rs`
* Test: `src/transform/src/eqsat/objective.rs`

**Interfaces:**
* Consumes: `Cost::arrangements` (Task 2), `Cost::time`, `Cost::nodes`.
* Produces: `pub struct ArrangementCount;` implementing `Objective`.

- [ ] **Step 1: Write the failing test**

```rust
#[mz_ore::test]
fn arrangement_count_prefers_fewer_arrangements_over_lower_degree() {
    use crate::eqsat::cost::Cost;
    // `a` has a high peak degree but one arrangement; `b` has low degrees but
    // three arrangements. PeakDegree prefers `b`; ArrangementCount prefers `a`.
    let a = Cost { arrangements: 1, memory: vec![2.0], time: vec![2.0], nodes: 3 };
    let b = Cost { arrangements: 3, memory: vec![1.0, 1.0, 1.0], time: vec![1.5], nodes: 5 };
    assert_eq!(ArrangementCount.cmp(&a, &b), std::cmp::Ordering::Less);
    assert_eq!(PeakDegree.cmp(&a, &b), std::cmp::Ordering::Greater);
    assert_eq!(ArrangementCount.name(), "arrangement-count");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bin/cargo-test -p mz-transform eqsat::objective::tests::arrangement_count_prefers`
Expected: FAIL with "cannot find value `ArrangementCount`".

- [ ] **Step 3: Implement the objective**

Append to `objective.rs`:

```rust
/// The default objective: minimize the count of distinct maintained
/// arrangements (reuse-aware), then worst-case time, then node count.
/// Arrangement count drives steady-state memory in a differential-dataflow
/// system, so minimizing it directly targets the resource that matters.
pub struct ArrangementCount;

impl Objective for ArrangementCount {
    fn cmp(&self, a: &Cost, b: &Cost) -> std::cmp::Ordering {
        a.arrangements
            .cmp(&b.arrangements)
            .then_with(|| super::cost::cmp_vecs(&a.time, &b.time))
            .then_with(|| a.nodes.cmp(&b.nodes))
    }
    fn name(&self) -> &'static str {
        "arrangement-count"
    }
}
```

`cmp_vecs` is currently private (`cost.rs:153`). Make it `pub(crate)` so the objective can reuse the same lexicographic time comparison.

- [ ] **Step 4: Run test to verify it passes**

Run: `bin/cargo-test -p mz-transform eqsat::objective`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/transform/src/eqsat/objective.rs src/transform/src/eqsat/cost.rs
git commit -m "transform: add arrangement-count eqsat extraction objective"
```

---

## Task 4: `Extractor` trait and `GreedyExtractor`

**Files:**
* Create: `src/transform/src/eqsat/extract.rs`
* Modify: `src/transform/src/eqsat.rs` (`mod extract;`)
* Modify: `src/transform/src/eqsat/egraph.rs` (no logic change; `extract_with` already takes `&dyn Objective` from Task 1)
* Test: `src/transform/src/eqsat/extract.rs`

**Interfaces:**
* Consumes: `EGraph::extract_with(root, model, &dyn Objective) -> Option<Rel>` (Task 1), `objective::{Objective, ArrangementCount, PeakDegree}`.
* Produces:
  ```rust
  pub trait Extractor: std::fmt::Debug {
      fn extract(
          &self,
          egraph: &EGraph,
          root: Id,
          model: &CostModel,
          objective: &dyn Objective,
      ) -> Option<Rel>;
  }
  #[derive(Debug, Clone)] pub struct GreedyExtractor;
  ```

- [ ] **Step 1: Write the failing test**

```rust
#[mz_ore::test]
fn greedy_extractor_matches_extract_with() {
    use crate::eqsat::egraph::EGraph;
    use crate::eqsat::cost::CostModel;
    use crate::eqsat::ir::Rel;
    use crate::eqsat::objective::ArrangementCount;
    let mut eg = EGraph::new();
    let plan = Rel::Get { name: "t".into(), arity: 2 };
    let root = eg.add_rel(&plan);
    let model = CostModel::new();
    let direct = eg.extract_with(root, &model, &ArrangementCount);
    let via = GreedyExtractor.extract(&eg, root, &model, &ArrangementCount);
    assert_eq!(direct, via);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bin/cargo-test -p mz-transform eqsat::extract`
Expected: FAIL with "module `extract` not found".

- [ ] **Step 3: Create the extractor module**

`src/transform/src/eqsat/extract.rs` (standard Materialize license header, then):

```rust
//! Pluggable plan extractors.
//!
//! An [`Extractor`] reconstructs the chosen [`Rel`] from a saturated e-graph
//! under an [`Objective`]. [`GreedyExtractor`] is the default bottom-up
//! dynamic program; [`IlpExtractor`] (a 0/1 program) optimizes the
//! non-compositional set-cardinality objective the greedy form cannot.

use crate::eqsat::cost::CostModel;
use crate::eqsat::egraph::{EGraph, Id};
use crate::eqsat::ir::Rel;
use crate::eqsat::objective::Objective;

/// Reconstructs the best [`Rel`] for `root` from a saturated e-graph under an
/// [`Objective`]. Returns `None` only when no representative can be built (the
/// caller then falls back to the un-optimized fragment, always sound).
pub trait Extractor: std::fmt::Debug {
    fn extract(
        &self,
        egraph: &EGraph,
        root: Id,
        model: &CostModel,
        objective: &dyn Objective,
    ) -> Option<Rel>;
}

/// The default extractor: greedy bottom-up dynamic programming. It re-costs
/// each candidate subplan and keeps the cheapest per e-class under the
/// objective. Sound for any objective, but for a non-compositional objective
/// (set-cardinality arrangement sharing) it finds a local, not global, optimum.
#[derive(Debug, Clone)]
pub struct GreedyExtractor;

impl Extractor for GreedyExtractor {
    fn extract(
        &self,
        egraph: &EGraph,
        root: Id,
        model: &CostModel,
        objective: &dyn Objective,
    ) -> Option<Rel> {
        egraph.extract_with(root, model, objective)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // test from Step 1
}
```

Add `pub mod extract;` to `src/transform/src/eqsat.rs`. Confirm `Id` is exported from `egraph` (`pub type Id = usize;` at egraph.rs:41).

- [ ] **Step 4: Run test to verify it passes**

Run: `bin/cargo-test -p mz-transform eqsat::extract`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/transform/src/eqsat/extract.rs src/transform/src/eqsat.rs
git commit -m "transform: introduce pluggable eqsat Extractor trait"
```

---

## Task 5: Thread objective and extractor through the optimizer; default to arrangement count

**Files:**
* Modify: `src/transform/src/eqsat/engine.rs` (`Optimizer` struct 59-73; `new` 76-84; extract call sites 181, 184, 224, 352, 400)
* Modify: `src/transform/src/eqsat.rs` (`optimize_inner` ~129-190: build optimizer with objective + extractor)
* Test: `src/transform/tests/wcoj_decision.rs` (end-to-end: extraction uses arrangement count)

**Interfaces:**
* Consumes: `objective::{Objective, ArrangementCount}`, `extract::{Extractor, GreedyExtractor}`.
* Produces: `Optimizer::with_objective(Arc<dyn Objective>)`, `Optimizer::with_extractor(Arc<dyn Extractor>)`; `Optimizer::new` defaults to `ArrangementCount` + `GreedyExtractor`.

- [ ] **Step 1: Write the failing test**

In `src/transform/tests/wcoj_decision.rs`, add a test asserting the optimizer now minimizes arrangement count. Reuse the existing triangle-with-indexes helper pattern in that file (it builds an `Optimizer` and inspects the plan). Concretely, assert that with indexes available the extracted plan reuses them (delta lookup, no full scan) and that `Optimizer::default_objective_name()` reports `"arrangement-count"`:

```rust
#[mz_ore::test]
fn optimizer_defaults_to_arrangement_count() {
    let opt = make_test_optimizer(); // existing helper in this file
    assert_eq!(opt.objective_name(), "arrangement-count");
}
```

(If no `make_test_optimizer` helper exists, construct via `engine::Optimizer::new(default_ruleset(), CostModel::new())` as the other tests in this file do.)

- [ ] **Step 2: Run test to verify it fails**

Run: `bin/cargo-test -p mz-transform --test wcoj_decision optimizer_defaults`
Expected: FAIL with "no method named `objective_name`".

- [ ] **Step 3: Add objective + extractor fields to `Optimizer`**

`engine.rs` struct (59-73):

```rust
#[derive(Clone, Debug)]
pub struct Optimizer {
    rules: CompiledRuleSet,
    model: CostModel,
    max_iters: usize,
    union_let_defs: bool,
    seeds: Vec<IndexedFilterSeed>,
    /// Orders candidate costs during extraction. Defaults to arrangement count.
    objective: std::sync::Arc<dyn crate::eqsat::objective::Objective>,
    /// Reconstructs the chosen plan. Defaults to the greedy extractor.
    extractor: std::sync::Arc<dyn crate::eqsat::extract::Extractor>,
}
```

`new` (76-84):

```rust
pub fn new(rules: CompiledRuleSet, model: CostModel) -> Self {
    Optimizer {
        rules,
        model,
        max_iters: 100,
        union_let_defs: true,
        seeds: Vec::new(),
        objective: std::sync::Arc::new(crate::eqsat::objective::ArrangementCount),
        extractor: std::sync::Arc::new(crate::eqsat::extract::GreedyExtractor),
    }
}

/// Override the extraction objective.
pub fn with_objective(
    mut self,
    objective: std::sync::Arc<dyn crate::eqsat::objective::Objective>,
) -> Self {
    self.objective = objective;
    self
}

/// Override the extractor.
pub fn with_extractor(
    mut self,
    extractor: std::sync::Arc<dyn crate::eqsat::extract::Extractor>,
) -> Self {
    self.extractor = extractor;
    self
}

/// Name of the active objective, for diagnostics and tests.
pub fn objective_name(&self) -> &'static str {
    self.objective.name()
}
```

(`Arc<dyn Objective>` / `Arc<dyn Extractor>` are `Clone` and, with the `: Debug` supertrait from Tasks 1/4, `Debug`, so the `#[derive(Clone, Debug)]` on `Optimizer` still holds.)

- [ ] **Step 4: Route the 5 extract call sites through the extractor + objective**

Replace each call site:

* `optimize_node_with_alt` (181, 184): the primary plan uses the configured objective; the time alternate keeps `TimeFirst`. Replace:
  ```rust
  let Some(mem_plan) = self.extractor.extract(&eg, root, &self.model, self.objective.as_ref()) else {
      return (plan.clone(), None, iterations);
  };
  let time_plan =
      eg.extract_with(root, &self.model, &crate::eqsat::objective::TimeFirst);
  ```
  `TimeFirst` already exists (added in Task 1). The time alternate keeps `TimeFirst` regardless of the configured primary objective, so the faster-but-heavier `Recommendation` logic in `optimize` is unchanged.
* `optimize_node` (224), `optimize_body_with_let_union` (352), `optimize_around_scopes` (400): replace `eg.extract(root, &self.model)` with `self.extractor.extract(&eg, root, &self.model, self.objective.as_ref())`.

- [ ] **Step 5: Build the optimizer with defaults in `optimize_inner`**

`eqsat.rs` (146): `engine::Optimizer::new(rules, model)` already defaults to `ArrangementCount` + `GreedyExtractor` after Step 3, so no change is required unless a feature selects an alternative. Leave the construction as-is; objective/extractor selection by feature is added in Task 9 (ILP) and Task 11 (A/B validation).

- [ ] **Step 6: Run test + full eqsat suite**

Run: `bin/cargo-test -p mz-transform eqsat` and `bin/cargo-test -p mz-transform --test wcoj_decision`
Expected: PASS. Some extraction outcomes change (arrangement count now governs); update any unit assertions that encoded peak-degree outcomes, confirming each change is reuse-gained or arrangement-count-reduced, never reuse-lost.

- [ ] **Step 7: Check goldens shift in the intended direction**

Run: `bin/sqllogictest --optimized -- test/sqllogictest/explain/optimized_plan_as_text.slt test/sqllogictest/explain/default.slt test/sqllogictest/transform/projection_lifting.slt`
Expected: diffs are arrangement-count reductions (full scan -> index lookup, or fewer ArrangeBy). Rewrite with `bin/sqllogictest -- --rewrite-results <files>` only after confirming every diff is a reuse gain. Inspect, do not blind-accept.

- [ ] **Step 8: Commit**

```bash
git add src/transform/src/eqsat/engine.rs src/transform/src/eqsat/objective.rs src/transform/src/eqsat.rs src/transform/tests/wcoj_decision.rs test/sqllogictest/
git commit -m "transform: extract eqsat plans by arrangement count via the default objective"
```

---

## Task 6: DSL payload helpers for join reordering

**Background (constraint from the DSL):** template helper functions are pure and
syntactic. They see only matched `Payload` bindings and arities (`arity(relvar)`),
not the e-graph, equivalences-analysis, or column types. This is sufficient: a
join's `equivalences` are bound as a `Payload::Equivalences(Vec<Vec<EScalar>>)`
from the `Join[e](..)` bracket, and per-input keys and equivalence splits are pure
functions of that payload plus input arities.

**Files:**
* Modify: `src/transform/src/eqsat/matcher.rs` (helper impls, next to `concat_payload`/`shift_payload` ~77-213)
* Modify: `src/transform/src/eqsat/dsl.rs` (`PExpr` variants ~114-153)
* Modify: `src/transform/build/grammar.rs` (parse the new helper names)
* Modify: `src/transform/build/codegen.rs` (`pexpr_expr` dispatch ~433-457)
* Test: `src/transform/src/eqsat/matcher.rs` (`#[cfg(test)]`)

**Interfaces:**
* Consumes: `Payload::Equivalences`, `join_key_cols_for_input` (make `pub(crate)` in cost.rs).
* Produces three pure helpers:
  ```rust
  /// Equivalence classes fully contained in columns [0, boundary): the
  /// constraints a binary inner join over the first two inputs can enforce.
  pub(crate) fn equivs_inner(e: Payload, boundary: i64) -> Result<Payload, String>;
  /// Equivalence classes touching a column >= boundary: enforced at the outer
  /// join after binarization. Complement of `equivs_inner`.
  pub(crate) fn equivs_outer(e: Payload, boundary: i64) -> Result<Payload, String>;
  /// Remap equivalences for swapping the first two inputs (arities a, b):
  /// columns [0,a) -> +b, [a,a+b) -> -a, columns >= a+b unchanged.
  pub(crate) fn swap_equivs(e: Payload, arity_a: i64, arity_b: i64) -> Result<Payload, String>;
  ```

- [ ] **Step 1: Write the failing test**

```rust
#[mz_ore::test]
fn equivs_split_and_swap() {
    use crate::eqsat::ir::EScalar;
    use mz_expr::MirScalarExpr;
    let col = |c| EScalar::plain(MirScalarExpr::column(c));
    // Classes: {#0,#2} fully in [0,3); {#1,#4} crosses boundary 3.
    let e = Payload::Equivalences(vec![vec![col(0), col(2)], vec![col(1), col(4)]]);
    let inner = equivs_inner(e.clone(), 3).unwrap();
    let outer = equivs_outer(e.clone(), 3).unwrap();
    assert_eq!(inner, Payload::Equivalences(vec![vec![col(0), col(2)]]));
    assert_eq!(outer, Payload::Equivalences(vec![vec![col(1), col(4)]]));
    // Swap inputs of arities 2 and 1: #0->#1, #1->#2, #2->#0.
    let swapped = swap_equivs(Payload::Equivalences(vec![vec![col(0), col(2)]]), 2, 1).unwrap();
    assert_eq!(swapped, Payload::Equivalences(vec![vec![col(1), col(0)]]));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bin/cargo-test -p mz-transform eqsat::matcher::tests::equivs`
Expected: FAIL ("cannot find function `equivs_inner`").

- [ ] **Step 3: Implement the helpers in matcher.rs**

```rust
/// Equivalence classes fully contained in columns [0, boundary).
pub(crate) fn equivs_inner(e: Payload, boundary: i64) -> Result<Payload, String> {
    let Payload::Equivalences(classes) = e else {
        return Err("equivs_inner expects an equivalences payload".into());
    };
    let b = usize::cast_from(u64::try_from(boundary).map_err(|_| "negative boundary")?);
    let kept = classes
        .into_iter()
        .filter(|class| class.iter().all(|s| s.cols().iter().all(|&c| c < b)))
        .collect();
    Ok(Payload::Equivalences(kept))
}

/// Complement of `equivs_inner`: classes touching a column >= boundary.
pub(crate) fn equivs_outer(e: Payload, boundary: i64) -> Result<Payload, String> {
    let Payload::Equivalences(classes) = e else {
        return Err("equivs_outer expects an equivalences payload".into());
    };
    let b = usize::cast_from(u64::try_from(boundary).map_err(|_| "negative boundary")?);
    let kept = classes
        .into_iter()
        .filter(|class| class.iter().any(|s| s.cols().iter().any(|&c| c >= b)))
        .collect();
    Ok(Payload::Equivalences(kept))
}

/// Remap equivalences for swapping the first two inputs (arities a, b).
pub(crate) fn swap_equivs(e: Payload, arity_a: i64, arity_b: i64) -> Result<Payload, String> {
    let Payload::Equivalences(classes) = e else {
        return Err("swap_equivs expects an equivalences payload".into());
    };
    let a = usize::cast_from(u64::try_from(arity_a).map_err(|_| "negative arity")?);
    let b = usize::cast_from(u64::try_from(arity_b).map_err(|_| "negative arity")?);
    let remap = |c: usize| -> i64 {
        if c < a {
            i64::try_from(c + b).expect("column fits i64")
        } else if c < a + b {
            i64::try_from(c - a).expect("column fits i64")
        } else {
            i64::try_from(c).expect("column fits i64")
        }
    };
    let mut out = Vec::with_capacity(classes.len());
    for class in classes {
        let mut nc = Vec::with_capacity(class.len());
        for s in class {
            nc.push(s.permute_cols(remap)?);
        }
        out.push(nc);
    }
    Ok(Payload::Equivalences(out))
}
```

(`EScalar::permute_cols` and `EScalar::cols` exist; `usize::cast_from` from `mz_ore::cast`. No `as`.)

- [ ] **Step 4: Wire grammar + codegen + dsl**

* `dsl.rs`: add `PExpr` variants `EquivsInner(Box<PExpr>, IxExpr)`, `EquivsOuter(Box<PExpr>, IxExpr)`, `SwapEquivs(Box<PExpr>, IxExpr, IxExpr)`.
* `build/grammar.rs`: parse `equivs_inner(e, n)`, `equivs_outer(e, n)`, `swap_equivs(e, n, m)` into those variants (mirror the existing `shift`/`concat` parsing).
* `build/codegen.rs` `pexpr_expr`: dispatch the new variants to `matcher::equivs_inner` / `equivs_outer` / `swap_equivs`, evaluating the `IxExpr` args (the existing `arity(relvar)`/arithmetic machinery).

- [ ] **Step 5: Run test to verify it passes**

Run: `bin/cargo-test -p mz-transform eqsat::matcher`
Expected: PASS (the build.rs codegen runs as part of the build).

- [ ] **Step 6: Commit**

```bash
git add src/transform/src/eqsat/matcher.rs src/transform/src/eqsat/dsl.rs src/transform/build/grammar.rs src/transform/build/codegen.rs src/transform/src/eqsat/cost.rs
git commit -m "transform: add eqsat DSL helpers for join equivalence split and swap"
```

---

## Task 7: Join binarization and commutativity rules (order into saturation)

**Files:**
* Modify: `src/transform/src/eqsat/rules/relational.rewrite`
* Test: `src/transform/tests/wcoj_decision.rs` or a new `src/transform/tests/join_order.rs`

**Interfaces:**
* Consumes: `equivs_inner`/`equivs_outer`/`swap_equivs` (Task 6), `arity(..)`, the existing `Join[e](..)` template (n-ary; a nested `Join[e](Join[e](..), ..)` is the binarized form).
* Produces: e-nodes for alternative join orders, so the arrangement-count objective chooses the order whose per-input keys align and share. Bounded so saturation stays within `MAX_ENODES`.

- [ ] **Step 1: Write the failing test**

Assert that for a three-way join where one binarization order reuses an available index and another does not, extraction picks the index-reusing order (fewer arrangements):

```rust
#[mz_ore::test]
fn binarization_picks_index_aligned_order() {
    // Build Join(a, b, c) with an index on `c` by its join key. The order that
    // pairs the indexed input so its arrangement is reused must win on count.
    // (Reuse the MIR/Rel builders the other tests in this file use.)
    let plan = three_way_join_with_index_on_c();
    let model = CostModel::with_available(index_on_c());
    let opt = engine::Optimizer::new(default_ruleset(), model);
    let out = opt.optimize(plan).plan;
    assert!(reuses_index_on_c(&out), "expected the index-aligned order");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bin/cargo-test -p mz-transform --test wcoj_decision binarization_picks`
Expected: FAIL (current saturation has no reordering, so the fixed input order is kept).

- [ ] **Step 3: Add the binarization rule**

```
rule binarize_join_first {
    doc "join(a, b, rest) = join(join(a, b), rest), splitting equivalences"
    Join[e](a, b, rest...)
        => Join[equivs_outer(e, arity(a) + arity(b))](
               Join[equivs_inner(e, arity(a) + arity(b))](a, b),
               rest...)
    where has_three_or_more_inputs()
}
```

The `has_three_or_more_inputs` condition prevents binarizing an already-binary join (which would loop). Add it as a no-arg `Cond` (mirror `join_is_cyclic`): a method on `EGraph` returning whether the matched join root has >= 3 inputs. The inner join keeps a, b in order; column indices of `rest` are unchanged because the inner join output preserves the a-then-b column order.

- [ ] **Step 4: Add the commutativity rule (bounded)**

```
rule commute_binary_join {
    doc "join(a, b) = join(b, a), remapping equivalences"
    Join[e](a, b) => Join[swap_equivs(e, arity(a), arity(b))](b, a)
    where is_binary_join()
}
```

`is_binary_join` gates to exactly two inputs (so it composes with binarization to enumerate orders pairwise without n-ary permutation blowup). Commutativity is its own inverse; the e-graph dedups via hash-consing, so it adds at most one alternative per binary node, bounded by `MAX_ENODES`.

- [ ] **Step 5: Run test + saturation-bound check**

Run: `bin/cargo-test -p mz-transform --test wcoj_decision` and `bin/cargo-test -p mz-transform eqsat`
Expected: PASS. Confirm saturation still terminates within `MAX_ENODES` on the widest corpus join (add a test asserting `iterations < max_iters` or node count under cap for an 8-way join). If blowup occurs, narrow the binarization (e.g. only binarize when an input has an available index) and `log` the bound, per the no-silent-caps rule.

- [ ] **Step 6: Run goldens**

Run: `bin/sqllogictest --optimized -- test/sqllogictest/explain/optimized_plan_as_text.slt test/sqllogictest/ldbc_bi.slt test/sqllogictest/ldbc_bi_eager.slt`
Expected: order changes are arrangement-count reductions. Rewrite only after confirming each diff reuses more or arranges less.

- [ ] **Step 7: Commit**

```bash
git add src/transform/src/eqsat/rules/relational.rewrite src/transform/src/eqsat/egraph.rs src/transform/src/eqsat/dsl.rs src/transform/build/ src/transform/tests/ test/sqllogictest/
git commit -m "transform: enumerate join orders in eqsat saturation"
```

---

## Task 7b: Commutativity via a column-order-restoring Project

**Why:** Task 7's binarization re-associates the fixed input order but cannot
reorder inputs, so it is near-inert under the arrangement-count objective (it
only adds an intermediate join arrangement). The join-order value the spec wants
comes from commutativity. A naive `Join(a, b) = Join(b, a)` is unsound in an
e-graph: the two forms have different output column orders (`[a|b]` vs `[b|a]`),
so unioning them into one e-class lets the extractor return the commuted form to
a context whose Project/Map/Filter reference columns positionally, corrupting
them (confirmed by a real `Can't union scalar types` panic on the `mz_indexes`
catalog view). The fix wraps the commuted join in a projection that restores the
original column order, so both members of the e-class share one schema.

**Files:**
* Modify: `src/transform/src/eqsat/matcher.rs` (`swap_projection` helper; remove the `#[allow(dead_code)]` on `swap_equivs`)
* Modify: `src/transform/src/eqsat/dsl.rs`, `src/transform/build/grammar.rs`, `src/transform/build/codegen.rs`, `src/transform/src/eqsat/lean.rs` (wire `swap_projection` PExpr + the `is_binary_join` condition)
* Modify: `src/transform/src/eqsat/egraph.rs` (`is_binary_join` condition method)
* Modify: `src/transform/src/eqsat/rules/relational.rewrite` (`commute_binary_join` rule)
* Test: `src/transform/tests/wcoj_decision.rs`

**Interfaces:**
* Consumes: `swap_equivs` (Task 6), `Payload::Outputs`, `arity(..)`, `is_binary_join`.
* Produces:
  ```rust
  /// The projection that restores the original column order after swapping the
  /// first two join inputs (arities a, b). The commuted `Join(b, a)` outputs
  /// columns [b | a]; this maps them back to [a | b], so the projected result
  /// has the same schema as the original `Join(a, b)`.
  pub(crate) fn swap_projection(arity_a: i64, arity_b: i64) -> Result<Payload, String>;
  // returns Payload::Outputs((b..b+a).chain(0..b).collect())
  ```

- [ ] **Step 1: Write the failing unit test for `swap_projection`**

```rust
#[mz_ore::test]
fn swap_projection_restores_column_order() {
    // a-arity 2, b-arity 1: commuted [b | a] = [#0_b, #0_a, #1_a]; restore maps
    // back to [a | b] = positions [1, 2, 0].
    assert_eq!(
        swap_projection(2, 1).unwrap(),
        Payload::Outputs(vec![1, 2, 0]),
    );
}
```

Run `bin/cargo-test -p mz-transform eqsat::matcher::tests::swap_projection`, confirm FAIL, implement `swap_projection`, confirm PASS.

- [ ] **Step 2: Wire `swap_projection` (PExpr) and `is_binary_join` (Cond)**

`swap_projection(n, m)` is a two-IxExpr-arg template helper returning an `Outputs` payload, wired exactly like `swap_equivs` from Task 6 (dsl.rs PExpr variant, grammar parse, codegen dispatch, lean.rs opaque arm). `is_binary_join()` is a no-arg condition wired like `has_three_or_more_inputs` from Task 7 (it checks the matched join root's e-class for a Join node with exactly 2 inputs).

- [ ] **Step 3: Add the `commute_binary_join` rule**

```
rule commute_binary_join {
    doc "join(a, b) = project([restore], join(b, a)): reorder inputs, restore column order"
    Join[e](a, b)
        => Project[swap_projection(arity(a), arity(b))](
               Join[swap_equivs(e, arity(a), arity(b))](b, a))
    where is_binary_join()
}
```

Remove the `#[allow(dead_code)]` (and its comment) from `swap_equivs` in matcher.rs; it is now used.

- [ ] **Step 4: Verify termination**

`commute_binary_join` is its own inverse. Double-application gives `Project[P1](Project[P2](Join(a,b)))`, which `fuse_projects` collapses to `Project[compose(P2,P1)](Join(a,b)) = Project[identity](Join(a,b))`. `commute_binary_join` does not fire on a Project node and `fuse_projects` needs a nested Project, so no further nodes are generated; saturation converges (hash-consing + the `MAX_ENODES` cap). Add a test that an 8-way join (and a self-join) saturate without hitting the cap (assert iterations < max_iters or node count under cap). If the e-graph bloats, optionally add an identity-projection elimination rule (`Project[p] r => r` when `p` is the full identity projection), but this is not required for correctness.

- [ ] **Step 5: Regression: the panic must be gone**

The previous panic was on the `mz_indexes` catalog view at startup. Run the catalog explain golden, which exercises catalog views:

Run: `bin/sqllogictest --optimized -- test/sqllogictest/catalog_server_explain.slt`
Expected: PASS, no panic. If it panics or regresses, the Project-restore schema is wrong; debug the `swap_projection` column mapping before proceeding.

- [ ] **Step 6: Positive-utility test (addresses the Task 7 review gap)**

Build a join where reordering inputs aligns an available index so the commuted (Project-wrapped) form maintains fewer arrangements than the original order, and assert the optimizer extracts the reordered form. The assertion must inspect plan structure (the reordered join is chosen), not be a tautology. Use the `CostModel::with_available` + MIR/Rel builders the other `wcoj_decision.rs` tests use.

- [ ] **Step 7: Goldens + lint + commit**

Run the explain goldens (`optimized_plan_as_text.slt`, `ldbc_bi.slt`, `ldbc_bi_eager.slt`); accept only reuse-gain / arrangement-reduction diffs, listing each. `cargo fmt`, `bin/lint`, `cargo clippy -p mz-transform`. Commit:

```bash
git commit -m "transform: add schema-safe join commutativity to eqsat via a restoring projection"
```

---

## Task 8: Join realizer for the e-graph-chosen order

**Risk note:** `differential::plan` and `delta_queries::plan` run their own greedy
`Orderer` internally and do NOT accept a fixed order (confirmed in
`join_implementation.rs`). To make the production planner a mechanical realizer of
the e-graph's chosen order, either (a) add an optional fixed-order parameter the
planners honor instead of calling `optimize_orders`, or (b) write a dedicated
realizer that derives arrangement keys for the chosen order and emits the
`Differential`/`DeltaQuery` tuples. Option (a) reuses the most production code and
is preferred; it touches production `join_implementation.rs`, so it must preserve
existing behavior when no fixed order is supplied.

**Files:**
* Modify: `src/transform/src/join_implementation.rs` (`differential::plan`, `delta_queries::plan`, `optimize_orders` call sites; `plan_join_min_arrangements`)
* Modify: `src/transform/src/eqsat/raise.rs` (pass the e-graph-chosen order through the realizer)
* Modify: `src/transform/src/eqsat/ir.rs` (carry the chosen order on the `Join`/`WcoJoin` Rel, or derive it from the binarized tree shape at raise)
* Test: `src/transform/tests/wcoj_decision.rs`

**Interfaces:**
* Consumes: the binarized join tree shape from Task 7 (the order is the in-order leaf sequence of the chosen binary tree), `plan_join_min_arrangements`.
* Produces: `differential::plan`/`delta_queries::plan` gaining an optional `fixed_order: Option<&[usize]>` argument; when `Some`, they skip `optimize_orders` and realize that order; when `None`, behavior is unchanged.

- [ ] **Step 1: Write the failing test**

Assert that a binarized join whose chosen leaf order is `[c, a, b]` realizes a plan whose first-arranged input is `c` (the order the e-graph picked), not the Orderer's greedy choice:

```rust
#[mz_ore::test]
fn realizer_honors_chosen_order() {
    let join = mir_three_way_join();
    let order = vec![2, 0, 1]; // c, a, b
    let features = OptimizerFeatures::default();
    let available = vec![vec![]; 3];
    let (plan, _) = differential::plan_with_order(
        &join, &JoinInputMapper::new(inputs(&join)), &available,
        &empty_keys(3), &none_cards(3), &none_filters(3), &features, Some(&order),
    ).unwrap();
    assert_eq!(first_arranged_input(&plan), 2);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `bin/cargo-test -p mz-transform --test wcoj_decision realizer_honors_chosen_order`
Expected: FAIL ("no function `plan_with_order`").

- [ ] **Step 3: Add the fixed-order path to the planners**

Refactor `differential::plan` and `delta_queries::plan` so the body that consumes `optimize_orders` output is reachable with an externally supplied order. Add `plan_with_order(.., fixed_order: Option<&[usize]>)`; have the existing `plan` delegate with `None`. When `Some(order)`, build the per-input order tuples directly from `order` plus the existing key-derivation (`JoinInputCharacteristics`, arrangement keys), skipping the `Orderer`. Preserve every existing comment. When `None`, the code path is byte-identical to today.

- [ ] **Step 4: Derive the order at raise and thread it**

In `raise.rs`, when the chosen join is a binarized tree (nested `Rel::Join`), compute the in-order leaf sequence as the fixed order, flatten to the n-ary join for the planner, and call the realizer with `Some(order)`. For a flat (un-binarized) join, pass `None` (planner chooses, today's behavior). Update `plan_join_min_arrangements` to forward the order.

- [ ] **Step 5: Run test + parity goldens**

Run: `bin/cargo-test -p mz-transform --test wcoj_decision` and the explain goldens.
Expected: PASS. The `None` path keeps all existing non-eqsat join plans identical (verify the production join goldens are unchanged: `bin/sqllogictest --optimized -- test/sqllogictest/joins.slt`).

- [ ] **Step 6: Confirm subsume claim — JoinImplementation decision is now the e-graph's**

Verify that for eqsat-chosen orders, `JoinImplementation` downstream does not re-order (it re-plans only `Unimplemented`/`Differential`; a committed plan with the chosen order survives). Add a test asserting the final MIR after the full pipeline keeps the eqsat-chosen order.

- [ ] **Step 7: Commit**

```bash
git add src/transform/src/join_implementation.rs src/transform/src/eqsat/raise.rs src/transform/src/eqsat/ir.rs src/transform/tests/ test/sqllogictest/
git commit -m "transform: realize the eqsat-chosen join order via the production planner"
```

---


## Task 9: ILP extractor

**Files:**
* Modify: `Cargo.toml` (root `[workspace.dependencies]`), `src/transform/Cargo.toml` (`good_lp.workspace = true`)
* Modify: `deny.toml`, `about.toml` (only if MIT/Apache-2.0 are not already allowed; verify first)
* Modify: `src/transform/src/eqsat/egraph.rs` (expose reachable e-nodes + per-node child classes + arrangement requirements)
* Modify: `src/transform/src/eqsat/extract.rs` (`IlpExtractor`)
* Test: `src/transform/src/eqsat/extract.rs`

**Interfaces:**
* Consumes: `EGraph` classes, `Id`, `ENode`, `Objective` (for the AGM tie-break term), `CostModel` oracle (`available`).
* Produces: `#[derive(Debug, Clone)] pub struct IlpExtractor;` implementing `Extractor`; new e-graph accessors:
  ```rust
  /// E-nodes reachable from `root`, keyed by canonical class id.
  pub fn reachable(&self, root: Id) -> std::collections::BTreeMap<Id, Vec<ENode>>;
  /// Canonical child class ids of an e-node, in argument order.
  pub fn child_classes(&self, node: &ENode) -> Vec<Id>;
  /// The (arranged-child-class, key-columns) arrangements selecting `node`
  /// entails. ArrangeBy -> (input class, key cols); Reduce/TopK -> (input
  /// class, group/order key cols); Join/WcoJoin -> one per input (input
  /// class, join-key cols). Empty for all other operators.
  pub fn arrangements_of(&self, node: &ENode) -> Vec<(Id, Vec<usize>)>;
  ```

**Note (dependency caveat):** `microlp` is an early-stage pure-Rust MILP solver and may panic or lose precision on pathological instances. The plan-size cap (`MAX_PLAN_SIZE = 200`, `MAX_ENODES = 600`) bounds the model, and `IlpExtractor` must fall back to `GreedyExtractor` on any solver error (never panic the optimizer). A `coin_cbc` fallback behind a cfg is out of scope.

- [ ] **Step 1: Add the dependency**

Verify MIT and Apache-2.0 are in `deny.toml [licenses].allow` and `about.toml accepted` (they are, per the codebase check). Add to root `Cargo.toml [workspace.dependencies]`:

```toml
good_lp = { version = "1.15", default-features = false, features = ["microlp"] }
```

Add to `src/transform/Cargo.toml [dependencies]`:

```toml
good_lp = { workspace = true }
```

Run `cargo check -p mz-transform` to resolve the lock (do not regenerate the full lock).

- [ ] **Step 2: Write the failing test (small e-graph, known optimum)**

```rust
#[mz_ore::test]
fn ilp_extracts_min_arrangement_plan() {
    use crate::eqsat::egraph::EGraph;
    use crate::eqsat::cost::CostModel;
    use crate::eqsat::ir::Rel;
    use crate::eqsat::objective::ArrangementCount;
    // A class with two equivalent forms: one needs an extra ArrangeBy, one does
    // not. The ILP must pick the arrangement-free form.
    let mut eg = EGraph::new();
    let cheap = Rel::Get { name: "t".into(), arity: 1 };
    let pricey = Rel::ArrangeBy {
        input: Box::new(Rel::Get { name: "t".into(), arity: 1 }),
        key: vec![],
    };
    let a = eg.add_rel(&cheap);
    let b = eg.add_rel(&pricey);
    eg.union(a, b);
    eg.rebuild();
    let model = CostModel::new();
    let plan = IlpExtractor.extract(&eg, a, &model, &ArrangementCount).unwrap();
    assert_eq!(model.cost(&plan).arrangements, 0);
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `bin/cargo-test -p mz-transform eqsat::extract::tests::ilp`
Expected: FAIL with "cannot find value `IlpExtractor`".

- [ ] **Step 4: Expose e-graph accessors**

In `egraph.rs`, add the three public methods. `reachable` does a BFS over `classes` from `find(root)` following `child_classes`. `arrangements_of` mirrors `cost::collect_memory_into`'s ArrId logic but at class granularity (return `(child_class_id, key_cols)`), reusing `join_key_cols_for_input` (make it `pub(crate)` in cost.rs). For an `ENode::ArrangeBy { input_class, key }` return `[(find(input_class), key_cols)]`; for `Join`/`WcoJoin` return one entry per input keyed by its join-key columns; for `Reduce`/`TopK` the group/order key.

- [ ] **Step 5: Implement `IlpExtractor`**

In `extract.rs`, encode the 0/1 program with `good_lp` (per the design spec's encoding):

```rust
use good_lp::{constraint, variable, ProblemVariables, Solution, SolverModel};

/// Extracts by solving a 0/1 program that minimizes the count of distinct
/// (class, key) arrangements not covered by the oracle. The set-cardinality
/// objective is non-compositional, so greedy bottom-up extraction cannot
/// optimize it; this can.
#[derive(Debug, Clone)]
pub struct IlpExtractor;

impl Extractor for IlpExtractor {
    fn extract(
        &self,
        egraph: &EGraph,
        root: Id,
        model: &CostModel,
        objective: &dyn Objective,
    ) -> Option<Rel> {
        match self.solve(egraph, root, model, objective) {
            Some(rel) => Some(rel),
            // microlp is early-stage; any solver failure falls back to greedy,
            // which is always sound.
            None => GreedyExtractor.extract(egraph, root, model, objective),
        }
    }
}
```

`solve` builds:
* `node_sel[(class, node)]`: binary, one selected node per reachable class.
* `arr[(class, key)]`: binary indicator per distinct arrangement.
* Constraints: `sum over nodes in root-class of node_sel == 1`; for every selected node, each child class has `sum node_sel == 1` (root reachability via `class_used` binaries, `class_used[c] >= node_sel[(p, n)]` when `n` has child `c`, `class_used[root] == 1`); `arr[(c,k)] >= node_sel[(p,n)]` for each arrangement `(c,k)` that node `n` requires.
* Objective: `minimize sum arr[(c,k)] for (c,k) not oracle-covered`, with a small `epsilon * AGM-tiebreak` term derived from `objective`/`model` (use a tiny coefficient so arrangement count dominates).

After solving, read `node_sel`, then reconstruct the `Rel` top-down from `root` picking the selected node per class (reuse `egraph.build_rel` polarity logic via a selection-guided variant, or a direct top-down builder that, per class, picks the node with `node_sel == 1`). Return `None` on `ResolutionError` or if reconstruction fails.

Write the full `solve` and `build_selected` helpers concretely during implementation; keep the model under the size cap.

- [ ] **Step 6: Run test to verify it passes**

Run: `bin/cargo-test -p mz-transform eqsat::extract`
Expected: PASS.

- [ ] **Step 7: Select the ILP extractor behind a feature**

Add an `OptimizerFeatures` flag `enable_eqsat_ilp_extraction: bool` (default false initially, via `optimizer_feature_flags!` in `src/transform-types`/`optimize.rs`). Thread `ctx.features` into `optimize_with_availability -> optimize_inner` and, when set, `optimizer.with_extractor(Arc::new(IlpExtractor))`. Greedy stays the default until the corpus invariant (Task 11) confirms ILP never regresses.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock src/transform/Cargo.toml src/transform/src/eqsat/egraph.rs src/transform/src/eqsat/extract.rs
git commit -m "transform: add ILP extractor for the arrangement-count objective"
```

---

## Task 10: Subsume-or-include audit execution

**Files:**
* Modify: `src/transform/src/eqsat/rules/relational.rewrite` (ensure MFP fusion rules cover `CanonicalizeMfp`)
* Modify: `src/transform/src/eqsat/raise.rs` (drop the `coalesce_mfp` crutch once saturation subsumes it; ~165, 176)
* Test: existing eqsat goldens

**Interfaces:**
* Consumes: the MFP fusion rules already in `relational.rewrite` (`merge_filters`, map/project fusion).
* Produces: a saturation that canonicalizes MFP without the post-raise `coalesce_mfp` pass.

- [ ] **Step 1: Inventory the gap**

Run: `grep -n "coalesce_mfp\|fn coalesce_mfp" src/transform/src/eqsat/raise.rs` and `grep -n "rule " src/transform/src/eqsat/rules/relational.rewrite | grep -i "filter\|map\|project"`. List every MFP shape `coalesce_mfp` fixes that no rule produces.

- [ ] **Step 2: Add missing fusion rules (TDD per rule)**

For each gap, write a failing eqsat unit test asserting the canonical form is extracted, add the `.rewrite` rule, rerun. Example shape (only if a real gap exists; do not add rules saturation already covers):

```
rule fuse_map_map {
    doc "map(s2, map(s1, r)) = map(s1 ++ s2, r)"
    Map[s2] (Map[s1] r) => Map[concat(s1, s2)] r
}
```

- [ ] **Step 3: Remove the crutch**

Once the goldens pass with the rules alone, delete the `coalesce_mfp` calls in `raise.rs` (keep the function if `demand_pushdown` still needs it; remove only the redundant post-raise calls). Preserve all comments on the surrounding logic.

- [ ] **Step 4: Confirm Demand/ProjectionPushdown stay included**

These remain production passes after eqsat (include-for-now per the audit). No change; add a one-line comment in `transform.rs` recording the deliberate decision so a future reader does not assume it is an oversight.

- [ ] **Step 5: Run goldens**

Run: `bin/sqllogictest --optimized -- test/sqllogictest/explain/optimized_plan_as_text.slt test/sqllogictest/explain/default.slt test/sqllogictest/transform/projection_lifting.slt test/sqllogictest/catalog_server_explain.slt`
Expected: PASS, or only canonical-form-equivalent diffs. If a shape regresses, restore the crutch and record the unsubsumed shape in the plan as a known gap rather than shipping a regression.

- [ ] **Step 6: Commit**

```bash
git add src/transform/src/eqsat/rules/relational.rewrite src/transform/src/eqsat/raise.rs src/transform/src/eqsat/transform.rs test/sqllogictest/
git commit -m "transform: subsume MFP canonicalization into eqsat saturation"
```

---

## Task 11: Arrangement-count validation harness

**Files:**
* Create: `src/transform/src/eqsat/validation.rs`
* Modify: `src/transform/src/eqsat.rs` (`#[cfg(test)] mod validation;`)
* Test: `src/transform/src/eqsat/validation.rs`

**Interfaces:**
* Consumes: `optimize`/`optimize_with_availability`, `CostModel::cost(..).arrangements`, the production transform pipeline entry point for the same input.
* Produces: a test asserting `eqsat_arrangement_count <= production_arrangement_count` across a representative corpus, plus a tracked (non-gating) saturation/extraction microbenchmark.

- [ ] **Step 1: Write the corpus invariant test**

```rust
#[mz_ore::test]
fn eqsat_never_increases_arrangement_count() {
    for (name, mir, available) in corpus() {
        let model = CostModel::with_available(available.clone());
        let prod = production_plan(&mir, &available);     // run the prod pipeline
        let eq = optimize_with_availability(mir.clone(), available, Vec::new());
        let prod_arr = model.cost(&lower::lower(&prod)).arrangements;
        let eq_arr = model.cost(&lower::lower(&eq)).arrangements;
        assert!(
            eq_arr <= prod_arr,
            "{name}: eqsat {eq_arr} > production {prod_arr} arrangements",
        );
    }
}
```

Build `corpus()` from a handful of representative MIRs (triangle join with and without indexes, two-way join with a reusable index, a chain join, a reduce-over-join). Reuse the MIR builders the existing eqsat tests use.

- [ ] **Step 2: Run test to verify it fails or passes**

Run: `bin/cargo-test -p mz-transform eqsat::validation`
Expected: PASS if Tasks 1-8 achieved parity-or-better; a FAIL names the regressing query and is a real defect to fix before proceeding, not a test to weaken.

- [ ] **Step 3: Add the tracked microbenchmark**

A `#[ignore]`-by-default timing test (or a `cargo bench` entry) recording saturation + extraction wall time at the size cap, printed for tracking. Not gated (perf deferred per spec).

- [ ] **Step 4: Run the full SLT differential gate**

Run the eqsat-on-vs-off differential goldens (the probe pattern in `scratchpad/probe_eager.slt`) across the explain corpus to confirm correctness parity.

- [ ] **Step 5: Commit**

```bash
git add src/transform/src/eqsat/validation.rs src/transform/src/eqsat.rs
git commit -m "transform: validate eqsat never increases arrangement count"
```

---


## Sequencing, risks, and open questions

**Task order:** 1 -> 2 -> 3 -> 4 -> 5 establishes the pluggable objective + arrangement-count metric + greedy default (each independently shippable and golden-gated).
6 -> 7 -> 8 adds join-order saturation and the realizer (the riskiest arc; 8 touches production `join_implementation.rs`).
9 (ILP) depends only on 4 and 2.
10 (subsume audit) and 11 (validation) run last; 11 is the merge gate.

**Top risks:**

* **Task 8 realizer** edits production join planning. The `fixed_order = None` path must stay byte-identical to today, verified against `joins.slt` and the explain goldens. If the fixed-order refactor proves too invasive, fall back to a dedicated realizer (option b in Task 8) that does not touch the shared planner.
* **Task 7 saturation blowup** is the canonical e-graph explosion. The binarization/commutativity rules are bounded (binarize first-pair only, commute binary-only) and capped by `MAX_ENODES`. If a wide join still blows the cap, narrow binarization to index-bearing inputs and `log` the bound (no silent caps).
* **Task 9 ILP solver** (`microlp`) is early-stage and may panic on pathological models. `IlpExtractor` falls back to greedy on any error and stays behind a default-off feature until Task 11 confirms no regression.
* **Class-vs-Rel arrangement identity mismatch:** the ILP optimizes a class-level `(class, key)` count; `Cost::arrangements` (Task 2) counts at Rel granularity. They should agree on the extracted plan, but the ILP optimum under its model may differ slightly from the `Cost::arrangements` optimum. Task 11 measures the realized plan with `Cost::arrangements`, so any divergence surfaces as a corpus-invariant failure to investigate, not a silent loss.

**Open questions carried from the spec:**

* The exact subset of join orders saturation should enumerate (Task 7) is the central performance lever; the bounded first-pair-binarize + binary-commute encoding here is the starting point, to be tuned against the blowup cap.
* Whether projection placement affects arrangement-key alignment enough to justify subsuming `Demand`/`ProjectionPushdown` (currently include-for-now, Task 10) is left to measurement.

## Self-review

* **Spec coverage:** goal (Tasks 2-3), pluggable `Objective`/`Extractor` (1, 4-5), arrangement-count metric minus oracle (2-3), absorb join order/key (6-8), ILP extraction (9), subsume/include audit (10), validation `eqsat <= production` (11), time as AGM secondary (3), perf deferred (notes). No uncovered requirement.
* **Type consistency:** `Cost { arrangements, memory, time, nodes }`, `Objective::{cmp, name}` (+`Debug`), `Extractor::extract(egraph, root, model, objective)` (+`Debug`), `Optimizer::{with_objective, with_extractor, objective_name}`, DSL `equivs_inner`/`equivs_outer`/`swap_equivs`, planner `plan_with_order(.., Option<&[usize]>)` are used identically across the tasks that define and consume them.
