# SP-B1: Cost-model-native join commit + emitter — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the eqsat physical pass commit acyclic `Rel::Join` to a
cost-model-chosen `JoinImplementation::Differential` at raise time, so
`fixpoint_join_impl` no-ops on eqsat-produced joins, the join-key spelling
selector survives, and the variable-outer-join (VOJ) cross disappears.

**Architecture:** Surface the cost model's already-computed left-deep order from
`cost.rs` (`binary_join_order`); a new `eqsat/join_commit.rs` emitter turns that
order into a `Differential` MIR plan by reusing `join_implementation.rs`'s
mechanical lowering helpers (`implement_arrangements` / `permute_order` /
`install_lifted_mfp`); `raise.rs`'s `Rel::Join` arm routes through it when a new
flag is on; a flag-gated guard in `JoinImplementation::action` keeps the
committed plan final.

**Tech Stack:** Rust, `mz-transform` crate (eqsat optimizer), sqllogictest.

## Global Constraints

- **Approach B (cost-model-native ordering):** the join order comes from the
  eqsat cost model's orderers, never from JI's `optimize_orders`.
- **Acyclic → differential:** do not touch the `join_to_wcoj` cyclic gate; do
  not give acyclic joins a delta alternative. Cyclic joins keep the existing
  `WcoJoin → DeltaQuery` path unchanged.
- **No `Cost` / comparator changes:** do not modify the `Cost` struct,
  `cmp_memory_first`, `cmp_time_first`, or promote keyed-ness into `Cost`.
- **No planning policy inside JI:** the only JI edits are the flag-gated
  `Differential` skip guard and `pub(crate)` visibility on three helpers.
- **Flag-off = byte-identical to current `main`:** every changed path is gated
  by `enable_eqsat_native_join_commit`.
- **Flag default `true`** (in-branch, for A/B), mirroring
  `enable_eqsat_delta_join_cost`.
- **Do not edit `doc/developer/generated/`.** No customer names in any file.
- **Tests:** unit tests via `bin/cargo-test -p mz-transform <name>`; slt via
  `bin/sqllogictest -- <path>` (use `--rewrite-results` to populate expected
  EXPLAIN output, then verify by inspection/grep).

---

### Task 1: Surface the cost-model join order (`binary_join_order`)

**Files:**
- Modify: `src/transform/src/eqsat/cost.rs` (add `JoinOrder`, `JoinStep`,
  `CostModel::binary_join_order`, free fns `best_left_deep_sequence`,
  `frontier_key_cols`; add tests to the existing `mod tests`).

**Interfaces:**
- Consumes: existing `CostModel` internals — `size_degree` (`cost.rs:301`),
  `Hypergraph::build` (`1194`), `intern_hg` (`643`), `agm_degree_subset_memo`
  (`665`), `MAX_EXACT_JOIN_INPUTS` (`71`), `terms_cost`, `join_key_cols_for_input`
  (`877`), `Rel::arity` (`ir.rs:343`), `EScalar::cols`.
- Produces: `pub(crate) struct JoinOrder { pub steps: Vec<JoinStep> }`,
  `pub(crate) struct JoinStep { pub input: usize, pub key_cols: BTreeSet<usize> }`,
  and `pub(crate) fn CostModel::binary_join_order(&self, inputs: &[Rel],
  equivalences: &[Vec<EScalar>]) -> Option<JoinOrder>` (`steps[0]` is the start;
  each `key_cols` is local to its input). Consumed by Task 2's emitter and
  Task 3's routing.

- [ ] **Step 1: Write the failing tests**

Add to the `mod tests` block in `src/transform/src/eqsat/cost.rs`. If `get`,
`col`, `eq_expr` helpers already exist in that module, reuse them and drop the
duplicates; otherwise add these:

```rust
#[mz_ore::test]
fn binary_join_order_voj_local_is_keyed() {
    use mz_expr::{func, BinaryFunc};
    let get = |name: &str, arity: usize| Rel::Get { name: name.into(), arity };
    let col = |c: usize| EScalar::plain(MirScalarExpr::column(c));
    let eq = |a: usize, b: usize| {
        EScalar::plain(
            MirScalarExpr::column(a).call_binary(MirScalarExpr::column(b), BinaryFunc::Eq(func::Eq)),
        )
    };
    let model = CostModel::new();
    // VOJ shape: t1(2) cols 0-1, t2(3) cols 2-4, t3(3) cols 5-7.
    let inputs = vec![get("t1", 2), get("t2", 3), get("t3", 3)];
    // class {#0,#2}: t1.f0 = t2.f2 ; class {#5, eq(#2,#4)}: LOCAL spelling.
    let equivalences = vec![vec![col(0), col(2)], vec![col(5), eq(2, 4)]];

    let order = model
        .binary_join_order(&inputs, &equivalences)
        .expect("connected join has an order");

    assert_eq!(order.steps.len(), 3);
    // Every step after the start probes the frontier with a non-empty key:
    // a fully keyed left-deep order, no forced cross.
    for step in &order.steps[1..] {
        assert!(
            !step.key_cols.is_empty(),
            "step for input {} should be keyed, got empty key",
            step.input
        );
    }
}

#[mz_ore::test]
fn binary_join_order_disconnected_returns_order_with_cross() {
    let get = |name: &str, arity: usize| Rel::Get { name: name.into(), arity };
    let model = CostModel::new();
    let inputs = vec![get("a", 2), get("b", 2)];
    // No equivalences: the two inputs are unconnected -> the second step is a
    // cross (empty key). The order is still produced (does not panic / None).
    let equivalences: Vec<Vec<EScalar>> = vec![];

    let order = model
        .binary_join_order(&inputs, &equivalences)
        .expect("two inputs always yield an order");
    assert_eq!(order.steps.len(), 2);
    assert!(
        order.steps[1].key_cols.is_empty(),
        "disconnected step must be a cross (empty key)"
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bin/cargo-test -p mz-transform binary_join_order`
Expected: FAIL — `binary_join_order` / `JoinOrder` not found (does not compile).

- [ ] **Step 3: Add the `JoinOrder` / `JoinStep` types and helpers**

Add near the other free functions in `src/transform/src/eqsat/cost.rs` (after
`join_key_cols_for_input`, ~line 902). Ensure `use std::collections::BTreeSet;`
is in scope (it already is — `join_key_cols_for_input` uses it).

```rust
/// A chosen left-deep join order with per-step arrangement keys, surfaced from
/// the cost model so the eqsat emitter can build a `JoinImplementation::Differential`.
/// `steps[0]` is the starting input; `key_cols` are local to each step's input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinStep {
    /// Global input index (position in the join's `inputs`).
    pub input: usize,
    /// Column indices local to `input` forming this step's arrangement key.
    pub key_cols: BTreeSet<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinOrder {
    pub steps: Vec<JoinStep>,
}

/// Local key columns of the input at `[offset, offset+arity)` that are equated,
/// via some class in `equivalences`, to a column of an already-placed (frontier)
/// input. `placed` holds the `(offset, arity)` ranges of inputs placed so far.
fn frontier_key_cols(
    offset: usize,
    arity: usize,
    placed: &[(usize, usize)],
    equivalences: &[Vec<EScalar>],
) -> BTreeSet<usize> {
    let in_frontier = |c: usize| placed.iter().any(|&(o, a)| c >= o && c < o + a);
    let mut key_cols = BTreeSet::new();
    for class in equivalences {
        let mut local = Vec::new();
        let mut touches_frontier = false;
        for escalar in class {
            for col in escalar.cols() {
                if col >= offset && col < offset + arity {
                    local.push(col - offset);
                } else if in_frontier(col) {
                    touches_frontier = true;
                }
            }
        }
        if touches_frontier {
            key_cols.extend(local);
        }
    }
    key_cols
}

/// The input sequence (global indices) of the cheapest left-deep order under the
/// AGM-degree `terms_cost` objective — the same objective `DpSub` minimizes,
/// extended with backpointers so the order can be reconstructed. Mirrors the
/// `DpSub::work_terms` recurrence. Returns `None` only when `n == 0`.
fn best_left_deep_sequence(n: usize, agm: &dyn Fn(u32) -> f64) -> Option<Vec<usize>> {
    if n == 0 {
        return None;
    }
    let full = (1u32 << n) - 1;
    // best[S] = (cost terms, predecessor subset, last input added).
    let mut best: Vec<Option<(Vec<f64>, u32, usize)>> = vec![None; 1 << n];
    for i in 0..n {
        best[1 << i] = Some((vec![], 0, i)); // single input: no work, no predecessor
    }
    for s in 1..=full {
        if s.count_ones() < 2 {
            continue;
        }
        let agm_s = agm(s);
        let mut sub = s;
        while sub > 0 {
            let i = sub.trailing_zeros() as usize;
            let rest = s & !(1 << i);
            if rest != 0 {
                if let Some((rest_terms, _, _)) = &best[rest as usize] {
                    let mut cand = rest_terms.clone();
                    cand.push(agm_s);
                    let better = best[s as usize]
                        .as_ref()
                        .is_none_or(|(c, _, _)| terms_cost(&cand).lt(&terms_cost(c)));
                    if better {
                        best[s as usize] = Some((cand, rest, i));
                    }
                }
            }
            sub &= sub - 1;
        }
    }
    // Walk predecessors from the full set back to a singleton.
    let mut seq = Vec::with_capacity(n);
    let mut s = full;
    loop {
        let (_, pred, last) = best[s as usize].clone()?;
        seq.push(last);
        if pred == 0 {
            break;
        }
        s = pred;
    }
    seq.reverse();
    Some(seq)
}
```

- [ ] **Step 4: Add the `binary_join_order` method**

Add inside `impl CostModel` in `src/transform/src/eqsat/cost.rs` (next to
`binary_join_terms`, ~line 696):

```rust
/// Surface the cost-model-chosen left-deep join order with per-step local
/// arrangement keys, so the eqsat emitter can commit a `Differential` plan.
/// The order minimizes the same AGM-degree objective as `binary_join_terms`.
pub(crate) fn binary_join_order(
    &self,
    inputs: &[Rel],
    equivalences: &[Vec<EScalar>],
) -> Option<JoinOrder> {
    let n = inputs.len();
    if n == 0 {
        return None;
    }
    let arities: Vec<usize> = inputs.iter().map(|r| r.arity()).collect();
    let mut offsets = Vec::with_capacity(n);
    let mut acc = 0;
    for &a in &arities {
        offsets.push(acc);
        acc += a;
    }

    let seq: Vec<usize> = if n == 1 {
        vec![0]
    } else if n > MAX_EXACT_JOIN_INPUTS {
        // Wide-join fallback: a left-deep chain in input order, mirroring
        // `binary_join_terms`'s own wide-join fallback.
        (0..n).collect()
    } else {
        let degs: Vec<f64> = inputs.iter().map(|r| self.size_degree(r)).collect();
        let hg = Hypergraph::build(inputs, equivalences);
        let hg_id = self.intern_hg(&hg, &degs);
        let agm = |subset: u32| self.agm_degree_subset_memo(hg_id, &hg, &degs, subset);
        best_left_deep_sequence(n, &agm)?
    };

    let mut placed: Vec<(usize, usize)> = Vec::new();
    let mut steps = Vec::with_capacity(n);
    for (k, &i) in seq.iter().enumerate() {
        let key_cols = if k == 0 {
            // The start is arranged by its full join key (cols equated to any
            // other input); the first edge keys into it.
            join_key_cols_for_input(offsets[i], arities[i], equivalences)
        } else {
            frontier_key_cols(offsets[i], arities[i], &placed, equivalences)
        };
        steps.push(JoinStep { input: i, key_cols });
        placed.push((offsets[i], arities[i]));
    }
    Some(JoinOrder { steps })
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `bin/cargo-test -p mz-transform binary_join_order`
Expected: PASS (2 tests).

- [ ] **Step 6: Verify no existing cost tests regressed**

Run: `bin/cargo-test -p mz-transform eqsat::cost`
Expected: PASS (all cost-model tests, including the pre-existing ones).

- [ ] **Step 7: Commit**

```bash
git add src/transform/src/eqsat/cost.rs
git commit -m "eqsat: surface cost-model join order via binary_join_order"
```

---

### Task 2: Differential emitter (`eqsat/join_commit.rs`)

**Files:**
- Create: `src/transform/src/eqsat/join_commit.rs`
- Modify: `src/transform/src/eqsat.rs` (register the module)
- Modify: `src/transform/src/join_implementation.rs` (make three helpers
  `pub(crate)`: `implement_arrangements:944`, `permute_order:1099`,
  `install_lifted_mfp:1048`)

**Interfaces:**
- Consumes: `JoinOrder`/`JoinStep` from Task 1; the JI helpers
  `implement_arrangements(inputs: &mut [MirRelationExpr], available:
  &[Vec<Vec<MirScalarExpr>>], needed: impl Iterator<Item=&'a (usize,
  Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)>) -> (MapFilterProject,
  Vec<Option<Vec<usize>>>)`, `permute_order(order: &mut Vec<(usize,
  Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)>, lifted_projections:
  &[Option<Vec<usize>>])`, `install_lifted_mfp(new_join: &mut MirRelationExpr,
  mfp: MapFilterProject)`.
- Produces: `pub(crate) fn commit_differential(join: MirRelationExpr, order:
  JoinOrder, available: &[Vec<Vec<MirScalarExpr>>]) -> Option<MirRelationExpr>`.
  Consumed by Task 3's routing.

- [ ] **Step 1: Make the three JI helpers `pub(crate)`**

In `src/transform/src/join_implementation.rs`:
- Line 944: `fn implement_arrangements<'a>(` → `pub(crate) fn implement_arrangements<'a>(`
- Line 1048: `fn install_lifted_mfp(` → `pub(crate) fn install_lifted_mfp(`
- Line 1099: `fn permute_order(` → `pub(crate) fn permute_order(`

- [ ] **Step 2: Register the module**

In `src/transform/src/eqsat.rs`, next to `pub mod raise;` (line 39), add:

```rust
pub(crate) mod join_commit;
```

- [ ] **Step 3: Write the failing test**

Create `src/transform/src/eqsat/join_commit.rs` with only the test first (so it
fails to compile on the missing `commit_differential`), then add the impl in
Step 5. Test:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::{Id, JoinImplementation, LocalId, MirRelationExpr};
    use mz_repr::{ColumnType, RelationType, ScalarType};

    use crate::eqsat::cost::{JoinOrder, JoinStep};

    fn get(local: u64, arity: usize) -> MirRelationExpr {
        let typ = RelationType::new(
            (0..arity)
                .map(|_| ColumnType {
                    scalar_type: ScalarType::Int32,
                    nullable: true,
                })
                .collect(),
        );
        MirRelationExpr::Get {
            id: Id::Local(LocalId::new(local)),
            typ,
            access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
        }
    }

    fn step(input: usize, key_cols: &[usize]) -> JoinStep {
        JoinStep {
            input,
            key_cols: key_cols.iter().copied().collect(),
        }
    }

    #[mz_ore::test]
    fn commit_differential_builds_expected_shape() {
        // 3-input join, no available arrangements.
        let inputs = vec![get(0, 2), get(1, 2), get(2, 2)];
        let join = MirRelationExpr::join_scalars(
            inputs,
            vec![
                vec![mz_expr::MirScalarExpr::column(0), mz_expr::MirScalarExpr::column(2)],
                vec![mz_expr::MirScalarExpr::column(2), mz_expr::MirScalarExpr::column(4)],
            ],
        );
        // Order: start at input 1 (key on local col 0), then input 0 (key local
        // col 0), then input 2 (key local col 0).
        let order = JoinOrder {
            steps: vec![step(1, &[0]), step(0, &[0]), step(2, &[0])],
        };
        let available = vec![Vec::new(); 3];

        let out = commit_differential(join, order, &available)
            .expect("commit must succeed on a 3-input join");

        let MirRelationExpr::Join { implementation, .. } = &out else {
            panic!("expected a Join, got {out:?}");
        };
        match implementation {
            JoinImplementation::Differential((start, start_key, _), rest) => {
                assert_eq!(*start, 1, "start input");
                assert_eq!(start_key.as_ref().map(|k| k.len()), Some(1), "start key len");
                assert_eq!(rest.len(), 2, "two remaining inputs");
                assert_eq!(rest[0].0, 0);
                assert_eq!(rest[1].0, 2);
            }
            other => panic!("expected Differential, got {other:?}"),
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `bin/cargo-test -p mz-transform commit_differential`
Expected: FAIL — `commit_differential` not found (does not compile).

- [ ] **Step 5: Write the emitter**

Prepend to `src/transform/src/eqsat/join_commit.rs` (above the `#[cfg(test)]`
module):

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Cost-model-native join commit for eqsat extraction.
//!
//! Turns a left-deep [`JoinOrder`] chosen by the cost model into a committed
//! `MirRelationExpr::Join` with `JoinImplementation::Differential`, reusing
//! `JoinImplementation`'s mechanical lowering helpers
//! (`implement_arrangements` / `permute_order` / `install_lifted_mfp`). It does
//! not call JI's `optimize_orders` planner, `differential::plan`, or
//! `canonicalize_equivalences`: the order and keys come entirely from the cost
//! model, and the equivalences are left as extraction spelled them.

use mz_expr::{JoinImplementation, JoinInputCharacteristics, MirRelationExpr, MirScalarExpr};

use crate::eqsat::cost::JoinOrder;

/// Commit `join` (a bare, `Unimplemented` `Join`) to a `Differential` plan that
/// follows `order`. `available` gives each input's existing arrangement keys.
/// Returns `None` (caller keeps the bare join) if `join` is not a `Join` or the
/// order is empty.
pub(crate) fn commit_differential(
    mut join: MirRelationExpr,
    order: JoinOrder,
    available: &[Vec<Vec<MirScalarExpr>>],
) -> Option<MirRelationExpr> {
    if order.steps.is_empty() {
        return None;
    }
    // Build the (input, local_key, characteristics) order; element 0 is the
    // start. Characteristics are EXPLAIN-only and not produced cost-model-side.
    let mut order_tuples: Vec<(usize, Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)> = order
        .steps
        .iter()
        .map(|s| {
            let key = s.key_cols.iter().map(|&c| MirScalarExpr::column(c)).collect();
            (s.input, key, None)
        })
        .collect();

    let MirRelationExpr::Join {
        inputs,
        implementation,
        ..
    } = &mut join
    else {
        return None;
    };

    let (start, mut start_key, start_characteristics) = order_tuples[0].clone();

    // Mechanical lowering: wrap inputs in ArrangeBy / lift MFPs for reuse.
    let (lifted_mfp, lifted_projections) =
        crate::join_implementation::implement_arrangements(inputs, available, order_tuples.iter());

    // Compensate keys for any projections lifted by `implement_arrangements`.
    if let Some(proj) = &lifted_projections[start] {
        start_key.iter_mut().for_each(|k| k.permute(proj));
    }
    crate::join_implementation::permute_order(&mut order_tuples, &lifted_projections);

    // The start arrangement is recorded separately; drop it from the remainder.
    order_tuples.remove(0);

    *implementation = JoinImplementation::Differential(
        (start, Some(start_key), start_characteristics),
        order_tuples,
    );

    crate::join_implementation::install_lifted_mfp(&mut join, lifted_mfp);
    Some(join)
}
```

> Note: the `let ... = &mut join else` borrow ends after the last use of
> `inputs`/`implementation` (NLL), so `install_lifted_mfp(&mut join, ..)` and
> `Some(join)` compile — this is exactly how `differential::plan`
> (`join_implementation.rs:799-927`) is structured.

- [ ] **Step 6: Run the test to verify it passes**

Run: `bin/cargo-test -p mz-transform commit_differential`
Expected: PASS.

- [ ] **Step 7: Verify JI still compiles and its tests pass**

Run: `bin/cargo-test -p mz-transform join_implementation`
Expected: PASS (visibility change is behavior-neutral).

- [ ] **Step 8: Commit**

```bash
git add src/transform/src/eqsat/join_commit.rs src/transform/src/eqsat.rs src/transform/src/join_implementation.rs
git commit -m "eqsat: add cost-model-native Differential emitter (join_commit)"
```

---

### Task 3: Flag + threading + `raise` routing

**Files:**
- Modify: `src/sql/src/session/vars/definitions.rs` (system var + binding)
- Modify: `src/repr/src/optimize.rs` (`OptimizerFeatures` field + macro)
- Modify: `src/sql/src/plan/statement/dml.rs` (exhaustive override)
- Modify: `src/sql/src/plan/statement/ddl.rs` (exhaustive destructure)
- Modify: `src/transform/src/eqsat/transform.rs` (read flag, pass it)
- Modify: `src/transform/src/eqsat.rs` (`optimize_with_availability` /
  `optimize_inner` thread the flag; update the other entry points + callers)
- Modify: `src/transform/src/eqsat/raise.rs` (`raise` / `raise_inner` thread the
  flag; `Rel::Join` arm routes through the emitter)
- Modify: `src/transform/src/eqsat/validation.rs` (caller of
  `optimize_with_availability`)
- Modify: `test/sqllogictest/transform/eqsat_delta_join_cost.slt`

**Interfaces:**
- Consumes: `CostModel::binary_join_order` (Task 1), `commit_differential`
  (Task 2), existing `per_input_available` (used in `raise.rs`'s WcoJoin arm).
- Produces: the `enable_eqsat_native_join_commit` optimizer feature flag and the
  wired routing. Consumed by Task 4's JI guard.

- [ ] **Step 1: Add the system variable**

In `src/sql/src/session/vars/definitions.rs`, immediately after the
`enable_eqsat_delta_join_cost` definition block (ends ~line 2140), add:

```rust
    {
        name: enable_eqsat_native_join_commit,
        // Defaulted on (in-branch): routes acyclic Rel::Join through a
        // cost-model-native Differential commit at eqsat raise time, so
        // JoinImplementation no-ops on eqsat joins and the spelling selector
        // survives. Primary use is easy A/B testing; flag-off is byte-identical
        // to the prior behavior.
        desc: "commit eqsat acyclic joins to a cost-model-chosen Differential at raise time",
        default: true,
        enable_for_item_parsing: false,
    },
```

- [ ] **Step 2: Add the `SystemVars -> OptimizerFeatures` binding**

In `src/sql/src/session/vars/definitions.rs`, next to line 2376
(`enable_eqsat_delta_join_cost: vars.enable_eqsat_delta_join_cost(),`), add:

```rust
            enable_eqsat_native_join_commit: vars.enable_eqsat_native_join_commit(),
```

- [ ] **Step 3: Add the `OptimizerFeatures` field**

In `src/repr/src/optimize.rs`, next to the `enable_eqsat_delta_join_cost` field
(line 162), add — matching the surrounding `optimizer_feature_flags!` macro
syntax (copy the exact shape of the neighbouring entry, including any comment
and trailing comma):

```rust
    /// Bound from `SystemVars::enable_eqsat_native_join_commit`. Commit eqsat
    /// acyclic joins to a Differential at raise time so JoinImplementation
    /// no-ops on them.
    enable_eqsat_native_join_commit: bool,
```

- [ ] **Step 4: Add the exhaustive override sites**

In `src/sql/src/plan/statement/dml.rs`, next to line 646
(`enable_eqsat_delta_join_cost: Default::default(),`), add:

```rust
                enable_eqsat_native_join_commit: Default::default(),
```

In `src/sql/src/plan/statement/ddl.rs`, next to line 4961
(`enable_eqsat_delta_join_cost: _,`), add:

```rust
                enable_eqsat_native_join_commit: _,
```

- [ ] **Step 5: Verify the flag compiles end-to-end (no behavior yet)**

Run: `cargo check -p mz-repr -p mz-sql`
Expected: compiles. (If `cargo check` complains about `METADATA_BACKEND_URL`,
prefix with the env vars from the mz-test skill, or skip to Step 11's build.)

- [ ] **Step 6: Thread the flag through `eqsat.rs`**

In `src/transform/src/eqsat.rs`:

`optimize_with_availability` (line 126) — add a parameter and forward it:

```rust
pub fn optimize_with_availability(
    expr: MirRelationExpr,
    available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    seeds: Vec<egraph::IndexedFilterSeed>,
    use_ilp: bool,
    use_delta: bool,
    native_join_commit: bool,
) -> MirRelationExpr {
    let rules = default_ruleset().for_phase(dsl::Phase::Physical);
    optimize_inner(
        expr, true, available, rules, true, seeds, use_ilp, false, use_delta, native_join_commit,
    )
}
```

`optimize_inner` (line 181) — add the parameter and pass it to `raise::raise`:

```rust
fn optimize_inner(
    expr: MirRelationExpr,
    commit_wcoj: bool,
    available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    rules: rules::CompiledRuleSet,
    union_let_defs: bool,
    seeds: Vec<egraph::IndexedFilterSeed>,
    use_ilp: bool,
    enable_wmr_lift: bool,
    use_delta: bool,
    native_join_commit: bool,
) -> MirRelationExpr {
```

and change the raise call (line 225):

```rust
    let mut raised = raise::raise(&best, commit_wcoj, &available_for_raise, native_join_commit);
```

Update the other `optimize_inner` callers in this file to pass `false` (the
logical phase never commits): `optimize` (line 72 body), `optimize_logical`
(line 147), `optimize_with_wmr_lift` (line 168). Each currently ends its
argument list with `..., false)` for `use_delta`; append `, false`.

- [ ] **Step 7: Thread the flag through `raise.rs`**

In `src/transform/src/eqsat/raise.rs`:

`raise` (line 66) — add the parameter and forward it:

```rust
pub fn raise(
    rel: &Rel,
    commit_wcoj: bool,
    available: &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    native_join_commit: bool,
) -> MirRelationExpr {
    let mut scope = BTreeMap::new();
    let mut raised = raise_inner(rel, commit_wcoj, available, native_join_commit, &mut scope);
    // ... rest unchanged
```

`raise_inner` (line 85) — add the parameter; the local `raise` closure
(line 91-93) captures it automatically, so update only its body call:

```rust
fn raise_inner(
    rel: &Rel,
    commit_wcoj: bool,
    available: &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    native_join_commit: bool,
    scope: &mut BTreeMap<usize, ReprRelationType>,
) -> MirRelationExpr {
    let raise = |r: &Rel, scope: &mut BTreeMap<usize, ReprRelationType>| {
        raise_inner(r, commit_wcoj, available, native_join_commit, scope)
    };
```

- [ ] **Step 8: Route `Rel::Join` through the emitter**

Replace the `Rel::Join` arm (`raise.rs:134-146`) with:

```rust
        Rel::Join {
            inputs,
            equivalences,
        } => {
            // `join_scalars` intentionally drops constant-singleton (arity-0,
            // 1-row) inputs that act as join identities, so a round-trip is not
            // byte-identical for such inputs but is arity- and
            // semantics-preserving.
            let raised_inputs: Vec<MirRelationExpr> =
                inputs.iter().map(|r| raise(r, scope)).collect();
            let join = MirRelationExpr::join_scalars(
                raised_inputs.clone(),
                equivalences.iter().map(|class| resolve(class)).collect(),
            );
            // Physical phase only: commit acyclic joins to a cost-model-chosen
            // Differential so JoinImplementation no-ops on them and the spelling
            // selector survives. On any failure, fall back to the bare
            // Unimplemented join and let JoinImplementation handle it.
            if !commit_wcoj || !native_join_commit {
                return join;
            }
            let model = if available.is_empty() {
                crate::eqsat::cost::CostModel::new()
            } else {
                crate::eqsat::cost::CostModel::with_available(available.clone())
            };
            let Some(order) = model.binary_join_order(inputs, equivalences) else {
                return join;
            };
            let per_input = per_input_available(&raised_inputs, available);
            crate::eqsat::join_commit::commit_differential(join.clone(), order, &per_input)
                .unwrap_or(join)
        }
```

> `per_input_available` is already imported/used by the `WcoJoin` arm in this
> file; reuse it as-is. If the `Rel::Join` arm is reached via the local `raise`
> closure that returns the arm value (not via early `return`), the early
> `return`s above still return from `raise_inner` — matching the `WcoJoin`
> arm's `return join;` at line 269.

- [ ] **Step 9: Update `transform.rs` to pass the real flag**

In `src/transform/src/eqsat/transform.rs`, line 170, change the call to:

```rust
        let out = crate::eqsat::optimize_with_availability(
            relation.clone(),
            available,
            seeds,
            use_ilp,
            use_delta,
            ctx.features.enable_eqsat_native_join_commit,
        );
```

(Adjust to the exact existing binding/format at that site; the new last argument
is `ctx.features.enable_eqsat_native_join_commit`.)

- [ ] **Step 10: Update the remaining `optimize_with_availability` callers**

Add a trailing `, false` to every other call (test/util callers keep the
feature off): `src/transform/src/eqsat/transform.rs` lines 514, 537, 560, 595,
628, and any call in `src/transform/src/eqsat/validation.rs`. Find them with:

```bash
grep -rn "optimize_with_availability(" src/transform/src/eqsat/
```

- [ ] **Step 11: Build the transform crate**

Run: `bin/cargo-test -p mz-transform --no-run`
Expected: compiles (no unused-parameter or arity errors).

- [ ] **Step 12: Update the slt and capture the new plan**

In `test/sqllogictest/transform/eqsat_delta_join_cost.slt`:
1. Replace the header comment block (lines 3-20) with a short note that the
   flag-on plan now commits the VOJ as a **differential** with no cross, while
   flag-off (the prior delta cross) is unchanged.
2. Before the "flag ON" `EXPLAIN`, also set the new flag on:

```
simple conn=mz_system,user=mz_system
ALTER SYSTEM SET enable_eqsat_native_join_commit = true
----
COMPLETE 0
```

3. For the "flag OFF" section, also turn the new flag off so it asserts the
   prior cross behavior:

```
simple conn=mz_system,user=mz_system
ALTER SYSTEM SET enable_eqsat_native_join_commit = false
----
COMPLETE 0
```

4. Regenerate the expected EXPLAIN output:

Run: `bin/sqllogictest -- --rewrite-results test/sqllogictest/transform/eqsat_delta_join_cost.slt`
Expected: the file is rewritten with actual plans.

- [ ] **Step 13: Verify the cross is gone (flag on) and present (flag off)**

Run:
```bash
bin/sqllogictest -- test/sqllogictest/transform/eqsat_delta_join_cost.slt
grep -n "type=differential\|type=delta\|:t1\[×\]" test/sqllogictest/transform/eqsat_delta_join_cost.slt
```
Expected: the slt PASSES; the **flag-on** EXPLAIN shows `type=differential` and
contains **no** `:t1[×]` cross; the **flag-off** EXPLAIN still shows
`type=delta` with `%2 » %0:t1[×] » %1[#0]K`. If the flag-on plan still contains
`:t1[×]`, STOP — the commit did not take effect (check that all three flags —
`enable_eqsat_physical_optimizer`, `enable_eqsat_delta_join_cost`,
`enable_eqsat_native_join_commit` — are on for that query).

- [ ] **Step 14: Commit**

```bash
git add src/sql/src/session/vars/definitions.rs src/repr/src/optimize.rs \
  src/sql/src/plan/statement/dml.rs src/sql/src/plan/statement/ddl.rs \
  src/transform/src/eqsat.rs src/transform/src/eqsat/raise.rs \
  src/transform/src/eqsat/transform.rs src/transform/src/eqsat/validation.rs \
  test/sqllogictest/transform/eqsat_delta_join_cost.slt
git commit -m "eqsat: route Rel::Join through cost-model-native Differential commit (flag enable_eqsat_native_join_commit)"
```

---

### Task 4: Flag-gated JI skip guard

**Files:**
- Modify: `src/transform/src/join_implementation.rs` (guard in `action`)
- Modify: `test/sqllogictest/transform/eqsat_delta_join_cost.slt` (assert the
  committed differential is final)

**Interfaces:**
- Consumes: `features.enable_eqsat_physical_optimizer`,
  `features.enable_eqsat_native_join_commit` (Task 3).
- Produces: JI no-op on committed `Differential` joins when both flags are on.

- [ ] **Step 1: Locate the `action` entry and the `features` binding**

Run:
```bash
grep -n "fn action\b\|implementation @ (Unimplemented\|enable_eager_delta_joins\|features\." src/transform/src/join_implementation.rs | head
```
Identify where `action` destructures the `Join` (the
`implementation @ (Unimplemented | Differential(..))` match, ~line 143-168) and
how `features` (the `OptimizerFeatures`) is named in that scope.

- [ ] **Step 2: Add the guard**

In `JoinImplementation`'s `action`, immediately after the `Join` is matched and
`implementation`/`features` are in scope, and **before**
`canonicalize_equivalences` (line 168) and the plan search, add:

```rust
        // Eqsat commits acyclic joins to a Differential at raise time
        // (enable_eqsat_native_join_commit). Keep that plan final: do not let
        // JI re-plan or eager-delta-upgrade a committed Differential when the
        // eqsat physical optimizer produced it. DeltaQuery already survives
        // (the match below only takes Unimplemented | Differential).
        if features.enable_eqsat_physical_optimizer
            && features.enable_eqsat_native_join_commit
            && matches!(implementation, JoinImplementation::Differential(..))
        {
            return Ok(());
        }
```

Adjust the `JoinImplementation::Differential` path/import to however the enum is
referenced in that scope (it may already be imported unqualified as
`Differential`).

- [ ] **Step 3: Add the finality assertion to the slt**

The Task 3 slt already runs with `enable_eager_delta_joins = true`. Without the
guard, JI would convert the eqsat-committed differential back to a delta query;
with the guard it stays differential. Re-rewrite and verify:

Run: `bin/sqllogictest -- --rewrite-results test/sqllogictest/transform/eqsat_delta_join_cost.slt`
Then: `bin/sqllogictest -- test/sqllogictest/transform/eqsat_delta_join_cost.slt`
Expected: PASS; the flag-on plan is `type=differential` (the guard kept JI from
upgrading it to `type=delta`).

- [ ] **Step 4: Confirm the guard is load-bearing**

Temporarily comment out the guard, run
`bin/sqllogictest -- test/sqllogictest/transform/eqsat_delta_join_cost.slt`,
and confirm the flag-on plan changes (FAILS the just-written expectation, e.g.
flips to `type=delta`). Restore the guard and re-run to PASS. This proves the
guard's effect; do not commit with the guard removed.

- [ ] **Step 5: Run the JI unit tests**

Run: `bin/cargo-test -p mz-transform join_implementation`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/transform/src/join_implementation.rs test/sqllogictest/transform/eqsat_delta_join_cost.slt
git commit -m "eqsat: skip JoinImplementation re-plan for committed differentials (flag-gated)"
```

---

## Corpus A/B (run after Task 4, record in the SDD ledger — not a unit test)

The cost-model order optimizes the AGM-degree objective; JI optimizes
`JoinInputCharacteristics`. SP-B1 **expects** ordering movement on multi-input
joins. Capture the breadth:

Run (with `--optimized`, the whole transform corpus is large):
```bash
git stash   # baseline = pre-Task-1 HEAD; or compare against origin/main
bin/sqllogictest --optimized -- test/sqllogictest/transform/*.slt 2>&1 | tail -40
```
Record in `.superpowers/sdd/progress.md`: how many transform slt files moved,
and spot-check a sample of moved plans to confirm the movement is ordering
(not a correctness regression — row counts/types must be unchanged; only the
`implementation` / `ArrangeBy` structure should differ). Large or surprising
movement is a signal to investigate before considering any production rollout
(which is out of scope for SP-B1).

## Self-Review notes

- **Spec coverage:** Component 1 → Task 1; Component 2 → Task 2; Component 3 →
  Task 3 (routing) + flag plumbing; Component 4 → Task 4 (guard) + free
  canonicalize parity (the `Unimplemented`-only guard, unchanged); Component 5 →
  Task 3 Steps 1-5/9-10. Corpus discipline → the A/B section.
- **Type consistency:** `JoinOrder`/`JoinStep` (Task 1) are consumed verbatim by
  Task 2 (`commit_differential`) and Task 3 (routing). `commit_differential`
  signature is identical in Task 2 Interfaces, the impl, and Task 3 Step 8.
- **No `canonicalize_equivalences` call is added** anywhere (Global Constraint):
  the committed join is never `Unimplemented`, so JI's existing guard skips it.
