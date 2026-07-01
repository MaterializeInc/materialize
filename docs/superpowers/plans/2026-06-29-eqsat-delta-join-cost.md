# Eqsat Delta-Aware Join Cost Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the eqsat optimizer emit the local join-key spelling so a variable-outer-join no longer plans a cross product, by teaching extraction to choose the spelling with a delta-aware (keyed-ness) cost.

**Architecture:** A new cost function `delta_join_terms` scores a join's spelling by `(crosses, degrees)` (keyed-ness primary). A join-spelling selector reads alternative column spellings from the eqsat `Equivalences` analysis fact, applies column-pure remaps, and picks the lowest-cost spelling. Both are gated behind a new flag and wired into both extraction paths; `JoinImplementation` is unchanged and builds the local arrangement once it sees the local spelling.

**Tech Stack:** Rust, the `mz-transform` crate (`src/transform/src/eqsat/`), the optimizer feature-flag macro (`src/repr/src/optimize.rs`), system vars (`src/sql/src/session/vars/definitions.rs`), `bin/cargo-test` (nextest), `bin/sqllogictest`.

**Spec:** `docs/superpowers/specs/2026-06-29-eqsat-delta-join-cost-design.md`
**Investigation:** `doc/developer/design/20260624_eqsat/20260629_eqsat_join_cost_findings.md`

## Global Constraints

- **Flag name & default:** `enable_eqsat_delta_join_cost`, default **`true`** (in-branch; primary use is A/B testing). Flag **off** must be byte-identical to today.
- **Keyed-ness is primary:** join spellings compare by `(crosses, degrees)` — `crosses` (forced-cross count) ascending first, then the descending degree vector via `cmp_vecs`. Never fold `crosses` into a magic degree penalty.
- **Do not modify `Cost` or its comparators** (`cmp_memory_first`, `cmp_time_first`) — node selection is structurally cross-blind and those comparators feed `PeakDegree`/`TimeFirst`/the `engine.rs` recommendation. The keyed-ness axis lives only in `delta_join_terms`' return tuple and the selector.
- **Constant-side gate:** a forced cross is counted only when **both** the broadcast frontier and the joined input have size-degree > `EPS`.
- **Candidate source:** the structural eqsat `Equivalences` analysis (`src/transform/src/eqsat/analysis/equivalences.rs`), recomputed from structure — never the `enable_eq_classes_withholding_errors` `extract_equivalences` path (`equivalence_propagation.rs`).
- **No customer names** in any committed file (code, tests, slt, comments, commit messages). Use generic table names (`t1`, `t2`, `t3`).
- **Corpus A/B is the merge gate:** before merge, A/B the flag across optimizer plan goldens; every moved plan must be a defensible improvement or neutral.
- **Tests:** `COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257 bin/cargo-test -p mz-transform -- <filter>`; goldens with `REWRITE=1`; slt via `bin/sqllogictest --optimized`. Bash timeout ≥ 600000ms.

---

### Task 1: `delta_join_terms` scoring function + keyed-ness comparison

**Files:**
- Modify: `src/transform/src/eqsat/cost.rs` (add a `CostModel` method, a free comparison fn, and tests in `mod tests`)

**Interfaces:**
- Consumes: existing `CostModel` internals — `size_degree(&Rel) -> f64`, `Hypergraph::build(&[Rel], &[Vec<EScalar>])`, `intern_hg(&Hypergraph, &[f64]) -> u32`, `agm_degree_subset_memo(u32, &Hypergraph, &[f64], u32) -> f64`, the `EPS` const, and the `cmp_vecs(&[f64], &[f64]) -> Ordering` free fn. `EScalar::cols() -> BTreeSet<usize>`. `Rel::arity() -> usize`.
- Produces, for later tasks:
  - `CostModel::delta_join_terms(&self, inputs: &[Rel], equivalences: &[Vec<EScalar>]) -> (usize, Vec<f64>)` (pub(crate))
  - `pub(crate) fn delta_score_lt(a: &(usize, Vec<f64>), b: &(usize, Vec<f64>)) -> bool`

- [ ] **Step 1: Write the failing tests**

Add to `mod tests` in `cost.rs` (the module already has helpers `get`, `col`, and the `eq` closure pattern). Add a `constant` helper and an `eq_expr` helper, then three tests:

```rust
    fn constant(arity: usize) -> Rel {
        Rel::Constant {
            card: 1,
            arity,
            col_types: None,
        }
    }

    /// An `EScalar` for the two-column expression `#a = #b` (its `cols()` is
    /// `{a, b}`, so a class containing it touches both inputs — the LEFT-JOIN
    /// null-pad key shape that turns a foreign spelling into a 3-input class).
    fn eq_expr(a: usize, b: usize) -> EScalar {
        use mz_expr::{func, BinaryFunc};
        EScalar::plain(
            MirScalarExpr::column(a)
                .call_binary(MirScalarExpr::column(b), BinaryFunc::Eq(func::Eq)),
        )
    }

    #[mz_ore::test]
    fn delta_join_local_spelling_beats_cross() {
        let model = CostModel::new();
        // Inputs: t1(2) cols{0,1}, t2(3) cols{2,3,4}, t3(3) cols{5,6,7}.
        let inputs = vec![get("t1", 2), get("t2", 3), get("t3", 3)];
        // class1 = {#0, #2}: t1.c0 = t2.c0.
        let class1 = vec![col(0), col(2)];
        // The t3 key is the null-pad expr `#X = #4`. Foreign X=0 references t1
        // AND t2 -> class2 touches {t1,t2,t3} (3 inputs); local X=2 references
        // t2 only -> class2 touches {t2,t3} (2 inputs).
        let foreign = vec![vec![col(5), eq_expr(0, 4)], class1.clone()];
        let local = vec![vec![col(5), eq_expr(2, 4)], class1.clone()];

        let foreign_score = model.delta_join_terms(&inputs, &foreign);
        let local_score = model.delta_join_terms(&inputs, &local);

        assert_eq!(
            foreign_score.0, 1,
            "foreign spelling forces one cross, got {foreign_score:?}"
        );
        assert_eq!(
            local_score.0, 0,
            "local spelling forces no cross, got {local_score:?}"
        );
        assert!(
            delta_score_lt(&local_score, &foreign_score),
            "local {local_score:?} should beat foreign {foreign_score:?}"
        );
    }

    #[mz_ore::test]
    fn keyed_ness_is_primary_over_degree() {
        // Fewer crosses wins even when the degree vector is strictly larger.
        let keyed = (0usize, vec![2.0, 2.0, 2.0]);
        let crossy = (1usize, vec![2.0, 1.0, 1.0]);
        assert!(delta_score_lt(&keyed, &crossy));
        assert!(!delta_score_lt(&crossy, &keyed));
    }

    #[mz_ore::test]
    fn cheap_cross_does_not_dominate_blowup() {
        let model = CostModel::new();
        // A(2) cols{0,1}, B(2) cols{2,3} joined on #1=#2; K is a constant with
        // no equivalence (a disconnected input). K forces a structural cross,
        // but K is constant (size-degree 0), so the gate must not count it.
        let inputs = vec![get("A", 2), get("B", 2), constant(2)];
        let equivalences = vec![vec![col(1), col(2)]];
        let score = model.delta_join_terms(&inputs, &equivalences);
        assert_eq!(
            score.0, 0,
            "a cross over a constant side must not be counted, got {score:?}"
        );
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257 bin/cargo-test -p mz-transform -- eqsat::cost::tests::delta_join eqsat::cost::tests::keyed_ness eqsat::cost::tests::cheap_cross`
Expected: FAIL — `delta_join_terms` / `delta_score_lt` not found.

- [ ] **Step 3: Implement `delta_score_lt` (free fn, near `cmp_vecs`)**

```rust
/// Lexicographic "is `a` strictly cheaper than `b`" for delta-join scores
/// `(crosses, degrees)`: fewer forced crosses first (the keyed-ness axis), then
/// a smaller degree vector via `cmp_vecs`. Never constructs a `Cost`.
pub(crate) fn delta_score_lt(a: &(usize, Vec<f64>), b: &(usize, Vec<f64>)) -> bool {
    use std::cmp::Ordering::*;
    match a.0.cmp(&b.0) {
        Less => true,
        Greater => false,
        Equal => cmp_vecs(&a.1, &b.1) == Less,
    }
}
```

- [ ] **Step 4: Implement `delta_join_terms` (a `CostModel` method, near `binary_join_terms`)**

```rust
    /// The keyed-ness axis + intermediate degrees of a *delta* join over
    /// `inputs` under `equivalences`, summed over per-driver-rooted left-deep
    /// paths (a delta join maintains one path per input).
    ///
    /// Returns `(crosses, degrees)`:
    /// * `crosses` — number of forced cross-product steps across all driver
    ///   paths. A step extending `frontier` by input `j` is *keyed* iff some
    ///   equivalence class touches `j`, touches `frontier` (spans the boundary),
    ///   and has all its inputs within `frontier | {j}`. A non-keyed step is a
    ///   forced cross; it is counted only when neither side is constant (both the
    ///   broadcast frontier and input `j` have size-degree > `EPS`).
    /// * `degrees` — the intermediate-arrangement size-degrees across all paths,
    ///   each path's final full-join term dropped (it is streamed, identical
    ///   across spellings — mirrors `collect_memory_into`'s `terms.pop()`),
    ///   sorted descending.
    ///
    /// The keyed-ness axis (compared first by `delta_score_lt`) is what
    /// `binary_join_terms` cannot see: it costs a single best left-deep order
    /// and roots away from the cross. This is *not* wired into `cost()`/`Cost`;
    /// its only caller is the join-spelling selector.
    pub(crate) fn delta_join_terms(
        &self,
        inputs: &[Rel],
        equivalences: &[Vec<EScalar>],
    ) -> (usize, Vec<f64>) {
        let n = inputs.len();
        if n <= 1 {
            return (0, vec![]);
        }
        let degs: Vec<f64> = inputs.iter().map(|r| self.size_degree(r)).collect();
        let hg = Hypergraph::build(inputs, equivalences);
        let hg_id = self.intern_hg(&hg, &degs);

        // Per-input column offsets, and a column -> input-index lookup.
        let arities: Vec<usize> = inputs.iter().map(Rel::arity).collect();
        let mut offsets = vec![0usize; n];
        for i in 1..n {
            offsets[i] = offsets[i - 1] + arities[i - 1];
        }
        let input_of_col = |c: usize| -> usize {
            let mut i = 0;
            while i + 1 < n && c >= offsets[i + 1] {
                i += 1;
            }
            i
        };
        // Per equivalence class, the bitmask of inputs its scalars reference.
        let class_masks: Vec<u32> = equivalences
            .iter()
            .map(|class| {
                let mut m = 0u32;
                for escalar in class {
                    for c in escalar.cols() {
                        m |= 1u32 << input_of_col(c);
                    }
                }
                m
            })
            .collect();

        // Can any class key the step that extends `frontier` by input `j`?
        let keys_step = |frontier: u32, j: usize| -> bool {
            let jbit = 1u32 << j;
            let allowed = frontier | jbit;
            class_masks
                .iter()
                .any(|&m| m & jbit != 0 && m & frontier != 0 && (m & !allowed) == 0)
        };

        let mut crosses = 0usize;
        let mut degrees: Vec<f64> = Vec::new();

        for driver in 0..n {
            let mut frontier = 1u32 << driver;
            let mut remaining: Vec<usize> = (0..n).filter(|&i| i != driver).collect();
            let mut path: Vec<f64> = Vec::new();
            while !remaining.is_empty() {
                // Pick the next input: prefer a keyed extension, then the lowest
                // resulting AGM degree. Deterministic.
                let mut best: Option<(bool, f64, usize)> = None; // (is_cross, deg, pos)
                for (pos, &j) in remaining.iter().enumerate() {
                    let deg =
                        self.agm_degree_subset_memo(hg_id, &hg, &degs, frontier | (1u32 << j));
                    let cand = (!keys_step(frontier, j), deg, pos);
                    best = Some(match best {
                        None => cand,
                        Some(b) => {
                            if (cand.0, cand.1) < (b.0, b.1) {
                                cand
                            } else {
                                b
                            }
                        }
                    });
                }
                let (is_cross, deg, pos) = best.expect("remaining is non-empty");
                let j = remaining.remove(pos);
                if is_cross {
                    let frontier_deg =
                        self.agm_degree_subset_memo(hg_id, &hg, &degs, frontier);
                    if frontier_deg > EPS && degs[j] > EPS {
                        crosses += 1;
                    }
                }
                frontier |= 1u32 << j;
                path.push(deg);
            }
            path.pop(); // drop the streamed final-join term
            degrees.extend(path);
        }

        degrees.retain(|d| *d > EPS);
        degrees.sort_by(|a, b| b.partial_cmp(a).unwrap());
        (crosses, degrees)
    }
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257 bin/cargo-test -p mz-transform -- eqsat::cost::tests::delta_join eqsat::cost::tests::keyed_ness eqsat::cost::tests::cheap_cross`
Expected: PASS (3 tests).

- [ ] **Step 6: Confirm no existing cost test regressed**

Run: `COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257 bin/cargo-test -p mz-transform -- eqsat::cost`
Expected: PASS (all prior tests unchanged — `Cost` and the comparators were not touched).

- [ ] **Step 7: Commit**

```bash
git add src/transform/src/eqsat/cost.rs
git commit -m "eqsat/cost: delta-aware join cost with keyed-ness axis

delta_join_terms scores a join spelling as (crosses, degrees) over
per-driver-rooted delta paths; delta_score_lt compares keyed-ness first.
Not wired into Cost/cost(); consumed only by the spelling selector.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H"
```

---

### Task 2: `enable_eqsat_delta_join_cost` feature flag

**Files:**
- Modify: `src/sql/src/session/vars/definitions.rs` (system var + `From<&SystemVars> for OptimizerFeatures` binding)
- Modify: `src/repr/src/optimize.rs` (the `optimizer_feature_flags!` macro list)
- Modify: `src/sql/src/plan/statement/dml.rs` (exhaustive `OptimizerFeatureOverrides` literal)
- Modify: `src/sql/src/plan/statement/ddl.rs` (exhaustive `OptimizerFeatureOverrides` destructure)

**Interfaces:**
- Produces: the field `OptimizerFeatures::enable_eqsat_delta_join_cost: bool`, readable in transforms as `ctx.features.enable_eqsat_delta_join_cost` (Task 4 consumes this).

- [ ] **Step 1: Add the optimizer-feature field to the macro list**

In `src/repr/src/optimize.rs`, inside the `optimizer_feature_flags!({ ... })` invocation, add (next to `enable_eager_delta_joins`):

```rust
    // Bound from `SystemVars::enable_eqsat_delta_join_cost`. Gates the
    // delta-aware join-cost spelling selector in eqsat extraction.
    enable_eqsat_delta_join_cost: bool,
```

- [ ] **Step 2: Fix the two exhaustive `OptimizerFeatureOverrides` sites (build will not compile until both are updated)**

In `src/sql/src/plan/statement/dml.rs` (the `features: OptimizerFeatureOverrides { ... }` literal — every field named, no `..`), add:

```rust
                enable_eqsat_delta_join_cost: Default::default(),
```

In `src/sql/src/plan/statement/ddl.rs` (the `let OptimizerFeatureOverrides { ... } = optimizer_feature_overrides;` destructure — every field named), add:

```rust
                enable_eqsat_delta_join_cost: _,
```

(The other `OptimizerFeatureOverrides` literal in `ddl.rs` uses `..Default::default()` and needs no change. Confirm with `grep -rn "OptimizerFeatureOverrides {" src/` that only these two are exhaustive.)

- [ ] **Step 3: Add the system var**

In `src/sql/src/session/vars/definitions.rs`, near `enable_eqsat_physical_optimizer`, add:

```rust
    {
        name: enable_eqsat_delta_join_cost,
        // Defaulted on (in-branch): gates the delta-aware join-cost spelling
        // selector that lets eqsat avoid a variable-outer-join cross product.
        // The flag's primary use is easy A/B testing; flag-off is byte-identical
        // to the prior behavior.
        desc: "use the delta-aware join cost to pick join-key spellings in eqsat extraction",
        default: true,
        enable_for_item_parsing: false,
    },
```

- [ ] **Step 4: Bind the var into `OptimizerFeatures`**

In `src/sql/src/session/vars/definitions.rs`, in `impl From<&super::SystemVars> for OptimizerFeatures`, add a line in the `Self { ... }` literal:

```rust
            enable_eqsat_delta_join_cost: vars.enable_eqsat_delta_join_cost(),
```

- [ ] **Step 5: Verify it compiles across all crates that touch the macro**

Run: `cargo check -p mz-repr -p mz-sql -p mz-transform`
Expected: compiles with no errors (the macro-generated accessor `vars.enable_eqsat_delta_join_cost()` exists; both exhaustive sites updated).

- [ ] **Step 6: Commit**

```bash
git add src/repr/src/optimize.rs src/sql/src/session/vars/definitions.rs src/sql/src/plan/statement/dml.rs src/sql/src/plan/statement/ddl.rs
git commit -m "optimizer: add enable_eqsat_delta_join_cost flag (default on)

System var + optimizer feature, bound through OptimizerFeatures and both
exhaustive OptimizerFeatureOverrides sites. Read as
ctx.features.enable_eqsat_delta_join_cost. No behavior change yet.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H"
```

---

### Task 3: Join-spelling selector + unit tests

**Files:**
- Create: `src/transform/src/eqsat/join_spelling.rs`
- Modify: `src/transform/src/eqsat/mod.rs` (add `mod join_spelling;`)

**Interfaces:**
- Consumes: `CostModel::delta_join_terms`, `delta_score_lt` (Task 1); `EScalar::permute_cols`, `MirScalarExpr::as_column`, `Rel`.
- Produces: `pub(crate) fn select_join_spelling(model: &CostModel, inputs: &[Rel], base: &[Vec<EScalar>], analysis_classes: &[Vec<MirScalarExpr>]) -> Vec<Vec<EScalar>>` (Task 4 calls this from both extraction paths).

- [ ] **Step 1: Register the module**

In `src/transform/src/eqsat/mod.rs`, add alongside the other `mod` lines:

```rust
mod join_spelling;
```

- [ ] **Step 2: Write the failing test**

Create `src/transform/src/eqsat/join_spelling.rs` with only the test module first (so it fails to compile/find the fn):

```rust
//! Cost-driven join-key spelling selection for eqsat extraction.
//!
//! Given a join's resolved equivalences and the alternative column spellings
//! the eqsat `Equivalences` analysis proves equal, choose the spelling that
//! minimizes the delta-aware cost `(crosses, degrees)` — so a variable-outer
//! join emits a local key spelling and avoids a cross product. Column-pure
//! remaps only (`EScalar::permute_cols`), which preserve the `lit` fact and
//! sidestep the analysis reducer's canonical-direction bias.

use std::collections::BTreeMap;

use mz_expr::MirScalarExpr;

use crate::eqsat::cost::{delta_score_lt, CostModel};
use crate::eqsat::ir::{EScalar, Rel};

#[cfg(test)]
mod tests {
    use super::*;

    fn get(name: &str, arity: usize) -> Rel {
        Rel::Get {
            name: name.into(),
            arity,
        }
    }
    fn col(c: usize) -> EScalar {
        EScalar::plain(MirScalarExpr::column(c))
    }
    fn eq_expr(a: usize, b: usize) -> EScalar {
        use mz_expr::{func, BinaryFunc};
        EScalar::plain(
            MirScalarExpr::column(a)
                .call_binary(MirScalarExpr::column(b), BinaryFunc::Eq(func::Eq)),
        )
    }

    #[mz_ore::test]
    fn selector_picks_local_spelling_to_avoid_cross() {
        let model = CostModel::new();
        let inputs = vec![get("t1", 2), get("t2", 3), get("t3", 3)];
        // Base (foreign) spelling: the t3 null-pad key references #0 (t1),
        // making class {#5, #0=#4} a 3-input class -> a forced cross.
        let base = vec![vec![col(5), eq_expr(0, 4)], vec![col(0), col(2)]];
        // The analysis proves {#0, #2} equal, offering #0 -> #2 as a spelling.
        let analysis_classes = vec![vec![
            MirScalarExpr::column(0),
            MirScalarExpr::column(2),
        ]];

        let chosen = select_join_spelling(&model, &inputs, &base, &analysis_classes);

        // The chosen spelling has no forced cross.
        assert_eq!(model.delta_join_terms(&inputs, &chosen).0, 0);
        // And it strictly beats the base (which had a cross).
        assert!(delta_score_lt(
            &model.delta_join_terms(&inputs, &chosen),
            &model.delta_join_terms(&inputs, &base),
        ));
    }

    #[mz_ore::test]
    fn selector_keeps_base_when_no_cross() {
        let model = CostModel::new();
        let inputs = vec![get("t1", 2), get("t2", 3), get("t3", 3)];
        // Already-local base: no cross. The selector must return it unchanged
        // (byte-identical), regardless of available spellings.
        let base = vec![vec![col(5), eq_expr(2, 4)], vec![col(0), col(2)]];
        let analysis_classes = vec![vec![
            MirScalarExpr::column(0),
            MirScalarExpr::column(2),
        ]];

        let chosen = select_join_spelling(&model, &inputs, &base, &analysis_classes);
        assert_eq!(chosen, base);
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257 bin/cargo-test -p mz-transform -- eqsat::join_spelling`
Expected: FAIL — `select_join_spelling` not found.

- [ ] **Step 4: Implement the selector (above the `#[cfg(test)] mod tests`)**

```rust
/// Maximum number of multi-spelling classes to enumerate jointly, and the
/// maximum size of the joint candidate space. Above either bound the selector
/// keeps the canonical spelling (logged) so a wide join with many aliased
/// classes cannot blow up extraction time.
const CAP_CLASSES: usize = 4;
const CAP_PRODUCT: usize = 64;

/// Choose the join-key spelling minimizing the delta-aware cost.
///
/// `base` is the join's resolved equivalences today (the canonical spelling).
/// `analysis_classes` are the column-equivalence classes the eqsat
/// `Equivalences` analysis proves for this join's e-class. The selector tries,
/// per analysis class, remapping every column member to one chosen
/// representative (column-pure, via `EScalar::permute_cols`), scores each joint
/// assignment with `delta_join_terms`, and returns the cheapest by
/// `delta_score_lt`. Returns `base` unchanged when it already has no forced
/// cross, or when the candidate space exceeds the caps.
pub(crate) fn select_join_spelling(
    model: &CostModel,
    inputs: &[Rel],
    base: &[Vec<EScalar>],
    analysis_classes: &[Vec<MirScalarExpr>],
) -> Vec<Vec<EScalar>> {
    let base_vec = base.to_vec();
    let base_score = model.delta_join_terms(inputs, base);
    if base_score.0 == 0 {
        // No forced cross to fix: keep today's spelling (byte-identical).
        return base_vec;
    }

    // Column members per analysis class; only classes with >= 2 distinct columns
    // offer a spelling choice.
    let choice_classes: Vec<Vec<usize>> = analysis_classes
        .iter()
        .filter_map(|class| {
            let distinct: std::collections::BTreeSet<usize> =
                class.iter().filter_map(|e| e.as_column()).collect();
            (distinct.len() >= 2).then(|| distinct.into_iter().collect())
        })
        .collect();

    let product: usize = choice_classes.iter().map(|c| c.len()).product();
    if choice_classes.len() > CAP_CLASSES || product > CAP_PRODUCT {
        tracing::debug!(
            classes = choice_classes.len(),
            product,
            "select_join_spelling: candidate space over cap; keeping canonical spelling"
        );
        return base_vec;
    }

    // Enumerate one target column per choice class (cartesian product) via an
    // odometer over `idx`.
    let mut best = (base_score, base_vec);
    let mut idx = vec![0usize; choice_classes.len()];
    loop {
        // Build the column-pure remap for this assignment: every column in a
        // choice class maps to that class's chosen target.
        let mut colmap: BTreeMap<usize, usize> = BTreeMap::new();
        for (ci, class) in choice_classes.iter().enumerate() {
            let target = class[idx[ci]];
            for &c in class {
                if c != target {
                    colmap.insert(c, target);
                }
            }
        }
        let candidate: Vec<Vec<EScalar>> = base
            .iter()
            .map(|cls| {
                cls.iter()
                    .map(|e| {
                        e.permute_cols(|c| *colmap.get(&c).unwrap_or(&c) as i64)
                            .expect("column remap stays non-negative")
                    })
                    .collect()
            })
            .collect();
        let score = model.delta_join_terms(inputs, &candidate);
        if delta_score_lt(&score, &best.0) {
            best = (score, candidate);
        }
        if !advance(&mut idx, &choice_classes) {
            break;
        }
    }
    best.1
}

/// Odometer increment over per-class choice indices; returns false when it wraps
/// past the last assignment.
fn advance(idx: &mut [usize], classes: &[Vec<usize>]) -> bool {
    for i in 0..idx.len() {
        idx[i] += 1;
        if idx[i] < classes[i].len() {
            return true;
        }
        idx[i] = 0;
    }
    false
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257 bin/cargo-test -p mz-transform -- eqsat::join_spelling`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add src/transform/src/eqsat/join_spelling.rs src/transform/src/eqsat/mod.rs
git commit -m "eqsat: join-key spelling selector scored by delta-aware cost

select_join_spelling tries column-pure remaps from the Equivalences analysis
classes and picks the spelling with the fewest forced crosses (then lowest
degree). Caps the joint search; returns base unchanged when there is no cross.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H"
```

---

### Task 4a: Thread analysis facts + model through extraction (plumbing only)

This task is **mechanical plumbing**: extend the extraction call chain to carry an optional spelling-fact map and the `CostModel`, pre-run the `Equivalences` analysis when the flag is set, and pass it down — **without** using it at the Join arms yet. After this task the build compiles and behavior is byte-identical (the facts are threaded but ignored).

**Files:**
- Modify: `src/transform/src/transform.rs` (read the flag, pass `use_delta`)
- Modify: `src/transform/src/eqsat.rs` (`optimize_with_availability`, `optimize_inner` gain `use_delta: bool`)
- Modify: `src/transform/src/eqsat/engine.rs` (`Optimizer` field + builder; pre-run analysis; pass facts to `extract`)
- Modify: `src/transform/src/eqsat/extract.rs` (`Extractor::extract` signature; `GreedyExtractor`, `IlpExtractor`, `solve`, `build_selected`, `build_selected_inner`)
- Modify: `src/transform/src/eqsat/egraph/build.rs` (`extract_with`, `build_rel` signatures)

**Interfaces:**
- Produces a new last parameter on the extraction chain:
  `spellings: Option<&HashMap<Id, EquivalenceClasses>>` (the per-e-class `Equivalences` facts), and `model: &CostModel` reaching both Join arms. Type import: `use crate::analysis::equivalences::EquivalenceClasses;`

- [ ] **Step 1: Read the flag in `transform.rs` and thread `use_delta`**

In `src/transform/src/transform.rs`, next to `let use_ilp = ctx.features.enable_eqsat_ilp_extraction;`:

```rust
        let use_delta = ctx.features.enable_eqsat_delta_join_cost;
        let optimized = crate::eqsat::optimize_with_availability(
            relation.clone(),
            available,
            seeds,
            use_ilp,
            use_delta,
        );
```

- [ ] **Step 2: Add `use_delta` to `optimize_with_availability` / `optimize_inner`**

In `src/transform/src/eqsat.rs`, add `use_delta: bool` as the last param of both `optimize_with_availability` and `optimize_inner`, forward it through, and set it on the optimizer:

```rust
pub fn optimize_with_availability(
    expr: MirRelationExpr,
    available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    seeds: Vec<egraph::IndexedFilterSeed>,
    use_ilp: bool,
    use_delta: bool,
) -> MirRelationExpr {
    let rules = default_ruleset().for_phase(dsl::Phase::Physical);
    optimize_inner(expr, true, available, rules, true, seeds, use_ilp, false, use_delta)
}
```

In `optimize_inner`, add `use_delta: bool` as the last param and, after the `use_ilp` block:

```rust
    let optimizer = optimizer.with_delta_join_cost(use_delta);
```

Update the other internal callers of `optimize_inner` (the logical pass) to pass `false` for `use_delta` (grep `optimize_inner(` to find them; the logical entrypoint must pass `false`).

- [ ] **Step 3: Add the `Optimizer` field + builder in `engine.rs`**

Add a `delta_join_cost: bool` field to `Optimizer` (default `false` in its constructor) and a builder:

```rust
    /// Enable the delta-aware join-cost spelling selector at extraction.
    pub fn with_delta_join_cost(mut self, enabled: bool) -> Self {
        self.delta_join_cost = enabled;
        self
    }
```

- [ ] **Step 4: Pre-run the `Equivalences` analysis and pass facts into `extract`**

In `engine.rs` `optimize_node_with_alt`, after `eg.seed_indexed_filters(&self.seeds);` and before `colored_scopes`, build the facts when enabled:

```rust
            let eq_facts: Option<HashMap<Id, crate::analysis::equivalences::EquivalenceClasses>> =
                if self.delta_join_cost {
                    let arity = |c: Id| eg.arity(c);
                    let arity_fn: &dyn Fn(Id) -> usize = &arity;
                    let ctx = crate::eqsat::analysis::RelCtx {
                        arity: arity_fn,
                        data: eg.data(),
                    };
                    let facts = eg.run_analysis(
                        &crate::eqsat::analysis::equivalences::Equivalences {
                            locals: std::collections::BTreeMap::new(),
                        },
                        ctx,
                    );
                    Some(
                        facts
                            .into_iter()
                            .filter_map(|(id, opt)| opt.map(|ec| (id, ec)))
                            .collect(),
                    )
                } else {
                    None
                };
```

Then pass `eq_facts.as_ref()` as the new last arg of the `self.extractor.extract(...)` call. Pass `None` as the new last arg of the time-first `eg.extract_with(...)` recommendation call (the recommendation keeps today's spelling).

- [ ] **Step 5: Extend the `Extractor` trait + both impls (signature only)**

In `extract.rs`, add `spellings: Option<&HashMap<Id, EquivalenceClasses>>` as the last param of `Extractor::extract`, `GreedyExtractor::extract`, `IlpExtractor::extract`. `GreedyExtractor` forwards it to `egraph.extract_with(root, model, objective, colored, spellings)`. `IlpExtractor::extract` forwards `spellings` to `self.solve(egraph, root, model, spellings)` and to the greedy fallback. Thread `spellings` (and `model`) through `solve` → `build_selected(egraph, class, selected, model, spellings)` → `build_selected_inner(egraph, class, selected, depth, model, spellings)`.

- [ ] **Step 6: Extend `extract_with` + `build_rel` (signature only)**

In `build.rs`, add `spellings: Option<&HashMap<Id, EquivalenceClasses>>` as the last param of `extract_with`, and pass it (plus the already-in-scope `model`) into every `build_rel(...)` call. Add `model: &CostModel` and `spellings: Option<&HashMap<Id, EquivalenceClasses>>` as params of `build_rel`. **Do not change the Join arms yet** — they still call `self.resolve_equivalences(equivalences)`.

- [ ] **Step 7: Verify it compiles and is byte-identical**

Run: `COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257 bin/cargo-test -p mz-transform -- eqsat`
Expected: PASS (all eqsat tests unchanged — the threaded facts are not yet read).

- [ ] **Step 8: Commit**

```bash
git add src/transform/src/transform.rs src/transform/src/eqsat.rs src/transform/src/eqsat/engine.rs src/transform/src/eqsat/extract.rs src/transform/src/eqsat/egraph/build.rs
git commit -m "eqsat: thread Equivalences facts + model into extraction (plumbing)

Pre-runs the structural Equivalences analysis when enable_eqsat_delta_join_cost
is set and threads the per-e-class facts + CostModel to both extraction paths'
Join arms. Facts unused yet; behavior byte-identical.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H"
```

---

### Task 4b: Wire the spelling selector into both Join arms (behavior change)

**Files:**
- Modify: `src/transform/src/eqsat/egraph/build.rs` (greedy `build_rel` Join arm)
- Modify: `src/transform/src/eqsat/extract.rs` (ILP `build_selected_inner` Join arm)
- Create: `test/sqllogictest/transform/eqsat_delta_join_cost.slt`

**Interfaces:**
- Consumes: `select_join_spelling` (Task 3), the threaded `spellings`/`model` (Task 4a).

- [ ] **Step 1: Write the failing e2e test**

Create `test/sqllogictest/transform/eqsat_delta_join_cost.slt` (adapt the structure from the scratchpad `voj_groundtruth.slt`; anonymized table names only). The load-bearing assertion: with the flag and eager delta on, the VOJ plan has **no `[×]`** cross.

```
mode cockroach

simple conn=mz_system,user=mz_system
ALTER SYSTEM SET unsafe_enable_table_keys = true
----
COMPLETE 0

simple conn=mz_system,user=mz_system
ALTER SYSTEM SET enable_eager_delta_joins = true
----
COMPLETE 0

simple conn=mz_system,user=mz_system
ALTER SYSTEM SET enable_eqsat_delta_join_cost = true
----
COMPLETE 0

statement ok
CREATE TABLE t1 (f0 int, f1 int)

statement ok
CREATE TABLE t2 (f2 int, f3 int)

statement ok
CREATE TABLE t3 (f4 int, f5 int)

# The variable-outer join must not plan a cross product (`[×]`). Capture the
# join-implementations plan and assert it is cross-free.
query T multiline
EXPLAIN OPTIMIZED PLAN WITH(join implementations) AS VERBOSE TEXT FOR
SELECT * FROM t1 LEFT JOIN t2 ON (f0 = f2) LEFT JOIN t3 ON (f2 = f4)
----
<FILL VIA --rewrite-results AFTER THE SELECTOR IS WIRED; THE PLAN MUST CONTAIN NO "[×]">
EOF
```

Generate the expected block with `--rewrite-results` **after** Step 3, then **manually verify the rewritten plan contains no `[×]`** before committing. (The rewrite captures whatever the optimizer emits; the human gate is confirming it is cross-free.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257 bin/sqllogictest --optimized -- test/sqllogictest/transform/eqsat_delta_join_cost.slt`
Expected: FAIL — the current plan contains a `[×]` cross (selector not wired).

- [ ] **Step 3: Wire the selector into the greedy Join arm (`build.rs`)**

Replace the greedy `build_rel` Join arm (the `ENode::Join` arm currently building `equivalences: self.resolve_equivalences(equivalences)`):

```rust
                ENode::Join {
                    inputs,
                    equivalences,
                } => {
                    let resolved_inputs: Vec<Rel> = inputs
                        .iter()
                        .map(|i| get(*i, demand))
                        .collect::<Option<_>>()?;
                    let base = self.resolve_equivalences(equivalences);
                    let equivalences = match spellings.and_then(|m| m.get(&self.find(class))) {
                        Some(ec) => crate::eqsat::join_spelling::select_join_spelling(
                            model,
                            &resolved_inputs,
                            &base,
                            &ec.classes,
                        ),
                        None => base,
                    };
                    Rel::Join {
                        inputs: resolved_inputs,
                        equivalences,
                    }
                }
```

(Leave the `ENode::WcoJoin` arm unchanged — WcoJoin is out of scope per the spec.)

- [ ] **Step 4: Wire the selector into the ILP Join arm (`extract.rs`)**

Replace the `ENode::Join` arm in `build_selected_inner`:

```rust
        ENode::Join {
            inputs,
            equivalences,
        } => {
            let resolved_inputs: Vec<Rel> =
                inputs.iter().map(|&i| child(i)).collect::<Option<_>>()?;
            let base = egraph.resolve_equivalences(equivalences);
            let equivalences = match spellings.and_then(|m| m.get(&egraph.find(class))) {
                Some(ec) => crate::eqsat::join_spelling::select_join_spelling(
                    model,
                    &resolved_inputs,
                    &base,
                    &ec.classes,
                ),
                None => base,
            };
            Rel::Join {
                inputs: resolved_inputs,
                equivalences,
            }
        }
```

(Leave the `ENode::WcoJoin` arm unchanged.)

- [ ] **Step 5: Make `select_join_spelling` visible**

Change `mod join_spelling;` to `pub(crate) mod join_spelling;` in `src/transform/src/eqsat/mod.rs` if the Join arms cannot otherwise reach it.

- [ ] **Step 6: Generate the expected plan and confirm it is cross-free**

Run: `COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257 bin/sqllogictest --optimized -- --rewrite-results test/sqllogictest/transform/eqsat_delta_join_cost.slt`
Then inspect the file: the captured plan must contain **no `[×]`**. If it still contains `[×]`, add `tracing::debug!` in `select_join_spelling` (log `base_score.0`, the chosen `crosses`, and `choice_classes`), re-run the query with `MZ_TEST_LOG_FILTER=debug` to confirm the `{#0,#2}` class reaches the selector and a `crosses=0` spelling is chosen (caveat 1 — the LEFT-JOIN-lowered e-class). Fix the wiring until the plan is cross-free, then remove the temporary logging.

- [ ] **Step 7: Run the test to verify it passes**

Run: `COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257 bin/sqllogictest --optimized -- test/sqllogictest/transform/eqsat_delta_join_cost.slt`
Expected: PASS, and the committed plan block contains no `[×]`.

- [ ] **Step 8: Commit**

```bash
git add src/transform/src/eqsat/egraph/build.rs src/transform/src/eqsat/extract.rs src/transform/src/eqsat/mod.rs test/sqllogictest/transform/eqsat_delta_join_cost.slt
git commit -m "eqsat: pick local join-key spelling to avoid VOJ cross product

Both extraction Join arms now consult select_join_spelling with the
Equivalences fact, emitting the local spelling so JoinImplementation builds a
local arrangement. The variable-outer-join slt is now cross-free.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H"
```

---

### Task 5: Flag-off parity + corpus A/B gate (merge blocker)

**Files:**
- Modify: `test/sqllogictest/transform/eqsat_delta_join_cost.slt` (add a flag-off parity case)
- (Review-only) the optimizer plan goldens that move under the flag.

- [ ] **Step 1: Add a flag-off parity case to the slt**

Append to `test/sqllogictest/transform/eqsat_delta_join_cost.slt`: set `enable_eqsat_delta_join_cost = false`, re-run the same `EXPLAIN`, and capture the plan. This documents the byte-identical baseline (it WILL contain `[×]` — that is the point).

```
simple conn=mz_system,user=mz_system
ALTER SYSTEM SET enable_eqsat_delta_join_cost = false
----
COMPLETE 0

# Flag OFF: byte-identical to today (the cross is present); this is the A/B baseline.
query T multiline
EXPLAIN OPTIMIZED PLAN WITH(join implementations) AS VERBOSE TEXT FOR
SELECT * FROM t1 LEFT JOIN t2 ON (f0 = f2) LEFT JOIN t3 ON (f2 = f4)
----
<FILL VIA --rewrite-results; THIS PLAN MUST MATCH today's output and contain "[×]">
EOF
```

Generate with `--rewrite-results`, confirm the flag-off plan still contains `[×]` (proving the gate works), and re-run without rewrite to confirm PASS.

- [ ] **Step 2: Run the full transform/eqsat golden suites**

Run: `COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257 bin/cargo-test -p mz-transform`
Expected: PASS. Any datadriven golden changes are produced with `REWRITE=1` and reviewed in Step 3.

- [ ] **Step 3: A/B the optimizer corpus and review every moved plan (merge gate)**

Run the optimizer plan slt suites with the flag default on:
`COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257 bin/sqllogictest --optimized -- test/sqllogictest/*.slt test/sqllogictest/transform/*.slt`
For every plan that differs from `main`, produce the diff (`git diff` of any `--rewrite-results` output) and classify each moved plan as **improvement** (a cross removed / fewer/cheaper arrangements) or **neutral** (an equivalent re-spelling). Record the classification in the PR description. **Do not merge** if any moved plan is a regression (a cross introduced, more/larger arrangements) — investigate the selector (likely the constant-side gate or the cap) before proceeding.

- [ ] **Step 4: Commit any reviewed golden updates**

```bash
git add -A
git commit -m "eqsat: update optimizer goldens for delta-join-cost spelling (reviewed A/B)

Every moved plan reviewed as improvement or neutral; see PR body for the
classification. Flag-off parity confirmed byte-identical.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H"
```

---

## Notes for the executor

- **Task ordering:** 1 → 2 → 3 → 4a → 4b → 5. Task 3 imports the flag-free selector but does not need Task 2; Task 4a needs Tasks 1–3; Task 4b needs 4a. Tasks 1–3 are pure unit-test cycles; 4a is mechanical plumbing; 4b/5 are the behavior change + gate.
- **The one empirical unknown** (caveat 1: which LEFT-JOIN-lowered e-class carries `#0 ≡ #2`) is converted in Task 4b Step 6 via tracing, not assumed. The mechanism (3-input-class forced cross) is already locked by Task 1's unit test.
- **WcoJoin and node-selection (ILP objective) are intentionally untouched** (spec §"Out of scope"). Do not add a keyed-ness tier to the ILP objective.
- **Do not modify `Cost` or its comparators.** If a task seems to require it, stop — the design forbids it (it would change `PeakDegree`/`TimeFirst`/the recommendation).
