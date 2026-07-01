# Eqsat acyclic delta commit + join-input hints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make eqsat's native join commit emit `JoinInputCharacteristics` (K/UK/A markers) and commit `JoinImplementation::DeltaQuery` when delta is free, recovering JI's free-delta plans on indexed joins that SP-B1 regressed differential.

**Architecture:** The delta-vs-differential decision is a structural arrangement-reuse rule made at commit time in `raise.rs`, sourced from the cost model. The e-graph `Cost` is untouched. A new `cost::delta_join_order` surfaces per-driver left-deep delta paths (mirroring `binary_join_order`); `join_commit::commit_delta_query` lowers them to `DeltaQuery` reusing JI's mechanical helpers; a shared `join_commit::step_characteristics` populates hints for both differential and delta.

**Tech Stack:** Rust, `mz-transform` crate (eqsat module), `mz-expr` (`JoinImplementation`, `JoinInputCharacteristics`), sqllogictest goldens.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-06-30-eqsat-acyclic-delta-hints-design.md`. Read it.
- The e-graph `Cost` is NOT modified. The delta decision lives only in the commit path (`raise.rs` / `eqsat/join_commit.rs` / `eqsat/cost.rs` surfacing).
- Flag reuse: gate everything on the existing `enable_eqsat_native_join_commit`. No new flag. The eager variant reads existing `enable_eager_delta_joins`; the V1/V2 characteristic split reads existing `enable_join_prioritize_arranged`.
- Delta viability is expressed as `delta_join_order(..).is_some()` — never a separate `crosses == 0` check. "Keyed" is decided by `frontier_key_cols` being non-empty (the same predicate `binary_join_order` uses).
- `delta_join_order` is invoked ONLY from the `Rel::Join` arm (acyclic joins). Cyclic joins are `Rel::WcoJoin`, a different arm.
- `delta_new` = distinct non-`available` `(input, key)` lookup arrangements across all paths (deduplicated), matching `delta_queries::plan` (join_implementation.rs:727-739). `diff_new` mirrors `differential::plan`: `inputs.len().saturating_sub(2) + new_input_arrangements[start]` (join_implementation.rs:898). Compute `diff_new` only in the eager branch.
- Characteristics fields: `key_length = key.len()`; `arranged = available[input]` contains `key` (computed against the ORIGINAL `available`, before `implement_arrangements`); `unique_key` = some unique key in `inputs[input].typ().keys` whose columns are all present in `key` (matches join_implementation.rs:1184); `cardinality = None`; `filters = FilterCharacteristics::none()`.
- "~13 goldens flip" is an ESTIMATE. Do not assert an exact count or a byte-match against JI. Review the flipped set case by case.
- Verification discipline (the SP-B1 C1 lesson): for any plan-shape change, verify by EXECUTION with a result-row diff vs base (both added AND removed rows), not EXPLAIN-only.
- Regenerate sqllogictest goldens PER-FILE in isolation (`bin/sqllogictest -- --rewrite-results <one file>`). Batch rewrites drift `GlobalId::User(N)` and produce false moves.
- Reviewable A/B (required, final task): tag the goldens that flip differential → delta as "delta-recovery", separate from other diffs, and record confirmed 0 result-row changes on each.
- Commit message trailers (every commit):
  ```
  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H
  ```
- Do NOT edit anything under `doc/developer/generated/`.
- Testing commands (mz-test skill): unit tests `cargo nextest run -p mz-transform <filter>` (fallback `bin/cargo-test -p mz-transform <filter>` if `METADATA_BACKEND_URL` is required); sqllogictest `bin/sqllogictest -- test/sqllogictest/transform/<file>.slt` (add `--rewrite-results` to regenerate, `--optimized` for many/large files).

---

### Task 1: `step_characteristics` helper

**Files:**
- Modify: `src/transform/src/eqsat/join_commit.rs` (add the helper + a unit test)

**Interfaces:**
- Consumes: `mz_expr::{JoinInputCharacteristics, FilterCharacteristics, MirRelationExpr, MirScalarExpr}`.
- Produces: `fn step_characteristics(input: usize, key: &[MirScalarExpr], available: &[Vec<Vec<MirScalarExpr>>], inputs: &[MirRelationExpr], enable_join_prioritize_arranged: bool) -> JoinInputCharacteristics` — used by `commit_differential` (Task 2) and `commit_delta_query` (Task 4).

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/transform/src/eqsat/join_commit.rs` (the `get` helper there builds an `Int32` `Get`; extend it to set unique keys via a local helper):

```rust
#[mz_ore::test]
fn step_characteristics_reports_arranged_unique_and_len() {
    use mz_repr::{ReprRelationType, ReprScalarType};
    // input 0: arity 2, unique key {0}; input 1: arity 2, no key.
    let typ0 = ReprRelationType::new(vec![
        ReprScalarType::Int32.nullable(true),
        ReprScalarType::Int32.nullable(true),
    ])
    .with_keys(vec![vec![0]]);
    let in0 = MirRelationExpr::Get {
        id: mz_expr::Id::Local(mz_expr::LocalId::new(0)),
        typ: typ0,
        access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
    };
    let in1 = get(1, 2);
    let inputs = vec![in0, in1];
    // input 0 is arranged by [#0]; input 1 has no arrangements.
    let available = vec![vec![vec![MirScalarExpr::column(0)]], Vec::new()];

    // Lookup on input 0, key [#0]: arranged + unique + len 1.
    let c0 = step_characteristics(0, &[MirScalarExpr::column(0)], &available, &inputs, false);
    assert_eq!(c0.key_length(), 1);
    assert!(c0.arranged(), "input 0 is arranged by [#0]");
    // Lookup on input 1, key [#0]: not arranged, not unique.
    let c1 = step_characteristics(1, &[MirScalarExpr::column(0)], &available, &inputs, false);
    assert!(!c1.arranged(), "input 1 has no arrangement");
}
```

(If `JoinInputCharacteristics` lacks public `key_length()`/`arranged()` accessors, assert via `format!("{:?}", c0)` containing the expected fields instead. Check `src/expr/src/relation.rs` around line 3313 for accessors before writing the assertion.)

- [ ] **Step 2: Run the test, verify it fails**

Run: `cargo nextest run -p mz-transform step_characteristics_reports_arranged_unique_and_len`
Expected: FAIL — `step_characteristics` not found.

- [ ] **Step 3: Implement the helper**

Add to `src/transform/src/eqsat/join_commit.rs` (top-level, below the imports; extend the `use` line to add `FilterCharacteristics`):

```rust
/// Build the `JoinInputCharacteristics` for one order step (start or lookup)
/// from the structurally-known fields. `available` must be the ORIGINAL per-input
/// arrangement keys (before `implement_arrangements` wraps inputs in ArrangeBy),
/// matching `JoinImplementation`'s `arranged` computation. `cardinality` and
/// `filters` are left at their neutral values; the cardinality and selectivity
/// axes are future work.
fn step_characteristics(
    input: usize,
    key: &[MirScalarExpr],
    available: &[Vec<Vec<MirScalarExpr>>],
    inputs: &[MirRelationExpr],
    enable_join_prioritize_arranged: bool,
) -> JoinInputCharacteristics {
    let arranged = available[input].iter().any(|k| k.as_slice() == key);
    // A unique key qualifies iff every one of its columns appears in `key`
    // (mirrors join_implementation.rs:1184). Non-column key members never match a
    // unique-key column, so they simply do not contribute.
    let keys = inputs[input].typ().keys;
    let unique_key = keys.iter().any(|cols| {
        cols.iter()
            .all(|c| key.contains(&MirScalarExpr::column(*c)))
    });
    JoinInputCharacteristics::new(
        unique_key,
        key.len(),
        arranged,
        None,
        FilterCharacteristics::none(),
        input,
        enable_join_prioritize_arranged,
    )
}
```

- [ ] **Step 4: Run the test, verify it passes**

Run: `cargo nextest run -p mz-transform step_characteristics_reports_arranged_unique_and_len`
Expected: PASS.

- [ ] **Step 5: Format and check**

Run: `bin/fmt && cargo check -p mz-transform`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/transform/src/eqsat/join_commit.rs
git commit -m "$(cat <<'EOF'
eqsat: add step_characteristics helper for join-input hints

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H
EOF
)"
```

---

### Task 2: Part A wiring — emit hints from `commit_differential` (introduces `NativeJoinFlags`)

**Files:**
- Modify: `src/transform/src/eqsat/join_commit.rs` (call `step_characteristics`; widen `commit_differential` signature)
- Modify: `src/transform/src/eqsat/raise.rs` (introduce `NativeJoinFlags`; thread it; pass `inputs` + `prioritize_arranged` into `commit_differential`)
- Modify: `src/transform/src/eqsat.rs` (thread `NativeJoinFlags` through `optimize_with_availability` / `optimize_inner` / the other `optimize_*` callers)
- Modify: `src/transform/src/eqsat/transform.rs` (real call site reads the features; test call sites use `NativeJoinFlags::none()`)
- Goldens: affected files under `test/sqllogictest/transform/` (K/UK/A markers appear on committed differentials)

**Interfaces:**
- Consumes: `step_characteristics` (Task 1).
- Produces:
  - `pub(crate) struct NativeJoinFlags { pub commit: bool, pub prioritize_arranged: bool }` with `impl NativeJoinFlags { pub(crate) fn none() -> Self }` (defined in `raise.rs`). Task 5 adds an `eager_delta: bool` field.
  - `commit_differential(join, order, available, inputs: &[MirRelationExpr], prioritize_arranged: bool) -> Option<MirRelationExpr>` (two new params).

- [ ] **Step 1: Introduce `NativeJoinFlags`, replacing the `native_join_commit: bool` thread**

In `src/transform/src/eqsat/raise.rs`, add near the top:

```rust
/// Flags controlling the physical join-commit path. Replaces the former lone
/// `native_join_commit` bool so Part A (`prioritize_arranged`) and Part B
/// (`eager_delta`, added later) thread without growing the positional arg list.
#[derive(Clone, Copy, Debug)]
pub(crate) struct NativeJoinFlags {
    /// Commit acyclic joins to a cost-model Differential/DeltaQuery so
    /// `JoinImplementation` no-ops on them.
    pub commit: bool,
    /// Select the V2 `JoinInputCharacteristics` layout (`enable_join_prioritize_arranged`).
    pub prioritize_arranged: bool,
}

impl NativeJoinFlags {
    /// All-off: no native commit. Used by logical/offline entry points and tests.
    pub(crate) fn none() -> Self {
        NativeJoinFlags { commit: false, prioritize_arranged: false }
    }
}
```

Change `raise` and `raise_inner` to take `flags: NativeJoinFlags` instead of `native_join_commit: bool`, and update the inner closure:

```rust
pub fn raise(
    rel: &Rel,
    commit_wcoj: bool,
    available: &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    flags: NativeJoinFlags,
) -> MirRelationExpr {
    let mut scope = BTreeMap::new();
    let mut raised = raise_inner(rel, commit_wcoj, available, flags, &mut scope);
    split_mixed_reductions(&mut raised);
    raised
}

fn raise_inner(
    rel: &Rel,
    commit_wcoj: bool,
    available: &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    flags: NativeJoinFlags,
    scope: &mut BTreeMap<usize, ReprRelationType>,
) -> MirRelationExpr {
    let raise = |r: &Rel, scope: &mut BTreeMap<usize, ReprRelationType>| {
        raise_inner(r, commit_wcoj, available, flags, scope)
    };
    // ...
```

In the `Rel::Join` arm, change the guard `if !commit_wcoj || !native_join_commit` to `if !commit_wcoj || !flags.commit`.

- [ ] **Step 2: Thread `NativeJoinFlags` through `eqsat.rs`**

In `src/transform/src/eqsat.rs`, change the last param of `optimize_with_availability` and `optimize_inner` from `native_join_commit: bool` to `flags: crate::eqsat::raise::NativeJoinFlags`, pass it through to `raise::raise(&best, commit_wcoj, &available_for_raise, flags)`, and update the `optimize_logical` / `optimize_with_wmr_lift` callers (which currently pass `false`) to pass `raise::NativeJoinFlags::none()`.

- [ ] **Step 3: Update `transform.rs` call sites**

In `src/transform/src/eqsat/transform.rs`, the real call (around line 169) builds the flags from features:

```rust
let optimized = crate::eqsat::optimize_with_availability(
    relation.clone(),
    available,
    seeds,
    use_ilp,
    use_delta,
    crate::eqsat::raise::NativeJoinFlags {
        commit: ctx.features.enable_eqsat_native_join_commit,
        prioritize_arranged: ctx.features.enable_join_prioritize_arranged,
    },
);
```

The test call sites (lines ~520, 543, 565, 607, 641) pass `crate::eqsat::raise::NativeJoinFlags::none()` instead of the trailing `false`.

- [ ] **Step 4: Verify it compiles (no behavior change yet)**

Run: `cargo check -p mz-transform`
Expected: clean. (Behavior is unchanged: `commit` gates exactly as `native_join_commit` did; `prioritize_arranged` is not consumed yet — that is Step 5.)

- [ ] **Step 5: Wire `commit_differential` to emit characteristics**

In `src/transform/src/eqsat/join_commit.rs`, widen the signature and replace the `None` characteristics. The start's characteristics are computed AFTER the start key is aligned (join_commit.rs:85) but on the pre-`implement_arrangements` key, so `arranged` is tested against the original `available`:

```rust
pub(crate) fn commit_differential(
    mut join: MirRelationExpr,
    order: JoinOrder,
    available: &[Vec<Vec<MirScalarExpr>>],
    inputs: &[MirRelationExpr],
    prioritize_arranged: bool,
) -> Option<MirRelationExpr> {
```

Replace the initial `order_tuples` build (`(s.input, key, None)`) so each lookup carries characteristics:

```rust
let mut order_tuples: Vec<(usize, Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)> = order
    .steps
    .iter()
    .map(|s| {
        let key: Vec<MirScalarExpr> =
            s.key_cols.iter().map(|&c| MirScalarExpr::column(c)).collect();
        let chars = step_characteristics(s.input, &key, available, inputs, prioritize_arranged);
        (s.input, key, Some(chars))
    })
    .collect();
```

After the start-key alignment block sets `order_tuples[0].1 = aligned;` (join_commit.rs:85), recompute the start's characteristics on the aligned key:

```rust
    order_tuples[0].2 = Some(step_characteristics(
        start,
        &aligned,
        available,
        inputs,
        prioritize_arranged,
    ));
```

(The single-input case — `order_tuples.len() < 2` — keeps the characteristics computed in the map above; the start key there is its full join key, which is correct.)

- [ ] **Step 6: Pass `inputs` and `prioritize_arranged` from `raise.rs`**

In the `Rel::Join` commit arm of `raise.rs`, update the call:

```rust
crate::eqsat::join_commit::commit_differential(
    canon_join,
    order,
    &per_input,
    &raised_inputs,
    flags.prioritize_arranged,
)
.unwrap_or(join)
```

- [ ] **Step 7: Update the existing `commit_differential` unit tests**

The two tests in `join_commit.rs` (`commit_differential_builds_expected_shape`, `commit_differential_aligns_hub_start_key_to_first_lookup`) call `commit_differential(join, order, &available)`. Add the two new args: pass `&inputs` (the same `vec![get(0,2), get(1,2), get(2,2)]` they build — clone before `join_scalars` consumes it) and `false`. Example for the first test:

```rust
let inputs = vec![get(0, 2), get(1, 2), get(2, 2)];
let join = MirRelationExpr::join_scalars(inputs.clone(), vec![/* ... */]);
// ...
let out = commit_differential(join, order, &available, &inputs, false)
    .expect("commit must succeed on a 3-input join");
```

- [ ] **Step 8: Run unit tests**

Run: `cargo nextest run -p mz-transform eqsat::join_commit`
Expected: PASS (all tests in the module, including the updated two and Task 1's).

- [ ] **Step 9: Consumer audit (required — the None→Some(partial) hazard)**

The change flips committed-join order elements from `None` to `Some(chars)` with `cardinality=None`, `filters=none()`. Investigate, do not assume:

Run: `grep -rn "JoinInputCharacteristics" src/ | grep -v "src/transform/src/join_implementation.rs"`

For each reader OUTSIDE `optimize_orders`/`differential::plan`/`delta_queries::plan` (which only run on `Unimplemented` joins, not committed ones), confirm it does not make a plan decision that depends on `cardinality`/`filters` being populated, and does not treat the presence of `Some` as "fully specified". Record findings in the task report (file:line + verdict per reader). Expected outcome: display-only (EXPLAIN/rendering consume the committed order, not the chars). If a consumer fails the check, STOP and report — do not emit partial fields; the spec says defer them in that case.

- [ ] **Step 10: Regenerate affected goldens (per-file, isolation)**

Identify which transform goldens render committed differential joins with these hints. Run the corpus once to see failures:

Run: `bin/sqllogictest --optimized -- $(ls test/sqllogictest/transform/*.slt)`
For EACH failing file (excluding the known pre-existing `demand.slt` panic and `ldbc_bi.slt`/`tpch_select.slt` drift), regenerate it ALONE:

Run: `bin/sqllogictest -- --rewrite-results test/sqllogictest/transform/<file>.slt`

Then verify the diff is ONLY added K/UK/A/`ef` markers on existing join plans (no result-row changes, no plan-shape changes). Record the file list in the task report.

- [ ] **Step 11: Result-row gate vs base**

For each rewritten golden, confirm no `query` result rows changed:

Run: `git diff test/sqllogictest/transform/ | grep -E '^[+-]' | grep -vE '^[+-]{3} |EXPLAIN|» |\[|type=|Join|Map|Filter|Project|Get|ArrangeBy|%[0-9]'`
Expected: no lines that look like result-set rows. Inspect anything that does. Record the result in the report.

- [ ] **Step 12: Format, check, commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/src/eqsat/ test/sqllogictest/transform/
git commit -m "$(cat <<'EOF'
eqsat: emit JoinInputCharacteristics on committed differential joins

Native join commit now populates K/UK/A markers via step_characteristics
instead of None. Introduces NativeJoinFlags to thread the commit and
prioritize_arranged flags. EXPLAIN-only; result rows unchanged.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H
EOF
)"
```

---

### Task 3: `delta_join_order` in the cost model

**Files:**
- Modify: `src/transform/src/eqsat/cost.rs` (add `delta_join_order` + unit tests)

**Interfaces:**
- Consumes: existing `frontier_key_cols`, `Hypergraph`, `intern_hg`, `agm_degree_subset_memo`, `size_degree`, `JoinStep`, `JoinOrder`.
- Produces: `pub(crate) fn delta_join_order(&self, inputs: &[Rel], equivalences: &[Vec<EScalar>]) -> Option<Vec<Vec<JoinStep>>>` — one inner `Vec<JoinStep>` per driver (indexed by driver position), each the LOOKUPS after that driver (driver excluded). `None` if any path has a non-keyed step. Used by `raise.rs` (Task 5).

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `src/transform/src/eqsat/cost.rs` (reuse existing test helpers for building `Rel` inputs and equivalences; mirror the style of existing `binary_join_order` tests — find them with `grep -n "binary_join_order" src/transform/src/eqsat/cost.rs`):

```rust
#[mz_ore::test]
fn delta_join_order_chain_has_keyed_paths_per_driver() {
    // 3-input chain: in0.#0 = in1.#0 ; in1.#1 = in2.#0  (each input arity 2).
    let model = CostModel::new();
    let inputs = three_int_inputs(); // helper: vec![rel(2), rel(2), rel(2)]
    let equivalences = vec![
        vec![escol(0), escol(2)], // in0 col0 == in1 col0
        vec![escol(3), escol(4)], // in1 col1 == in2 col0
    ];
    let paths = model
        .delta_join_order(&inputs, &equivalences)
        .expect("connected chain has a delta plan");
    assert_eq!(paths.len(), 3, "one path per driver");
    // Every step in every path is keyed (non-empty key_cols).
    for path in &paths {
        assert_eq!(path.len(), 2, "two lookups after the driver");
        for step in path {
            assert!(!step.key_cols.is_empty(), "every delta lookup must be keyed");
        }
    }
}

#[mz_ore::test]
fn delta_join_order_disconnected_returns_none() {
    // 2 inputs, NO equivalence between them: a cross product, no delta plan.
    let model = CostModel::new();
    let inputs = vec![rel(2), rel(2)];
    let equivalences: Vec<Vec<EScalar>> = vec![];
    assert!(
        model.delta_join_order(&inputs, &equivalences).is_none(),
        "disconnected join has no delta plan"
    );
}
```

(Use the actual local helpers present in the cost.rs test module. If `escol`/`rel`/`three_int_inputs` do not exist, define them locally mirroring the inputs the existing `binary_join_order` tests build. Confirm `EScalar` column constructor name via `grep -n "impl EScalar" src/transform/src/eqsat/ir.rs`.)

- [ ] **Step 2: Run the tests, verify they fail**

Run: `cargo nextest run -p mz-transform delta_join_order`
Expected: FAIL — `delta_join_order` not found.

- [ ] **Step 3: Implement `delta_join_order`**

Add to `src/transform/src/eqsat/cost.rs`, modeled on `binary_join_order` (cost.rs:730) for the offsets/keying machinery and on `delta_join_terms` (cost.rs:865) for the per-driver greedy walk. "Keyed" is `frontier_key_cols` non-empty; a non-keyed forced step makes the whole call return `None`:

```rust
/// Surface the per-driver left-deep delta paths: one path per driver (indexed by
/// driver position, matching the renderer's `join_orders[source_relation]`).
/// Each returned `Vec<JoinStep>` is the LOOKUPS after the driver (the driver is
/// not a step), keyed via `frontier_key_cols` against the accumulating per-path
/// frontier. Returns `None` if any path has a non-keyed (cross) step, i.e. the
/// join is disconnected and has no delta plan. Caller must invoke this only on
/// acyclic `Rel::Join` inputs.
pub(crate) fn delta_join_order(
    &self,
    inputs: &[Rel],
    equivalences: &[Vec<EScalar>],
) -> Option<Vec<Vec<JoinStep>>> {
    let n = inputs.len();
    if n == 0 {
        return None;
    }
    if n == 1 {
        return Some(vec![vec![]]);
    }
    let arities: Vec<usize> = inputs.iter().map(|r| r.arity()).collect();
    let mut offsets = Vec::with_capacity(n);
    let mut acc = 0;
    for &a in &arities {
        offsets.push(acc);
        acc += a;
    }
    let degs: Vec<f64> = inputs.iter().map(|r| self.size_degree(r)).collect();
    let hg = Hypergraph::build(inputs, equivalences);
    let hg_id = self.intern_hg(&hg, &degs);

    let mut all_paths: Vec<Vec<JoinStep>> = Vec::with_capacity(n);
    for driver in 0..n {
        let mut placed: Vec<(usize, usize)> = vec![(offsets[driver], arities[driver])];
        let mut frontier_mask = 1u32 << driver;
        let mut remaining: Vec<usize> = (0..n).filter(|&i| i != driver).collect();
        let mut path: Vec<JoinStep> = Vec::with_capacity(n - 1);
        while !remaining.is_empty() {
            // Pick the next input: prefer a keyed extension (non-empty
            // frontier_key_cols), then the lowest resulting AGM degree.
            // Deterministic, mirroring delta_join_terms.
            let mut best: Option<(bool, f64, usize, BTreeSet<usize>)> = None;
            for (pos, &j) in remaining.iter().enumerate() {
                let key_cols = frontier_key_cols(offsets[j], arities[j], &placed, equivalences);
                let is_cross = key_cols.is_empty();
                let deg = self.agm_degree_subset_memo(hg_id, &hg, &degs, frontier_mask | (1u32 << j));
                let cand = (is_cross, deg, pos, key_cols);
                best = Some(match best {
                    None => cand,
                    Some(b) => {
                        if (cand.0, cand.1) < (b.0, b.1) { cand } else { b }
                    }
                });
            }
            let (is_cross, _deg, pos, key_cols) = best.expect("remaining is non-empty");
            if is_cross {
                // No keyed extension exists: the join is disconnected. No delta plan.
                return None;
            }
            let j = remaining.remove(pos);
            path.push(JoinStep { input: j, key_cols });
            placed.push((offsets[j], arities[j]));
            frontier_mask |= 1u32 << j;
        }
        all_paths.push(path);
    }
    Some(all_paths)
}
```

(Ensure `BTreeSet` is imported in cost.rs — it already is, used by `JoinStep`.)

- [ ] **Step 4: Run the tests, verify they pass**

Run: `cargo nextest run -p mz-transform delta_join_order`
Expected: PASS.

- [ ] **Step 5: Format, check, commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/src/eqsat/cost.rs
git commit -m "$(cat <<'EOF'
eqsat: surface per-driver delta join paths from the cost model

delta_join_order mirrors binary_join_order: one keyed left-deep path per
driver, or None when the join is disconnected (no delta plan).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H
EOF
)"
```

---

### Task 4: `commit_delta_query` + `delta_new_arrangements`

**Files:**
- Modify: `src/transform/src/eqsat/join_commit.rs` (add `commit_delta_query`, `delta_new_arrangements` + unit tests)

**Interfaces:**
- Consumes: `step_characteristics` (Task 1); `JoinStep` (cost.rs); `crate::join_implementation::{implement_arrangements, permute_order, install_lifted_mfp}` (already `pub(crate)`).
- Produces:
  - `pub(crate) fn commit_delta_query(join: MirRelationExpr, paths: Vec<Vec<JoinStep>>, available: &[Vec<Vec<MirScalarExpr>>], inputs: &[MirRelationExpr], prioritize_arranged: bool) -> Option<MirRelationExpr>`
  - `pub(crate) fn delta_new_arrangements(paths: &[Vec<JoinStep>], available: &[Vec<Vec<MirScalarExpr>>]) -> usize`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `join_commit.rs`:

```rust
#[mz_ore::test]
fn delta_new_arrangements_counts_distinct_missing() {
    use crate::eqsat::cost::JoinStep;
    // 3 inputs. Paths reference lookups on inputs 1 ([#0]) and 2 ([#0]); input 1
    // is already arranged by [#0], input 2 is not. Distinct missing = {(2,[#0])} = 1.
    let available = vec![
        Vec::new(),
        vec![vec![MirScalarExpr::column(0)]], // input 1 arranged by [#0]
        Vec::new(),
    ];
    let paths = vec![
        vec![step(1, &[0]), step(2, &[0])], // driver 0
        vec![step(0, &[0]), step(2, &[0])], // driver 1 (input 0 lookup on [#0], not avail)
        vec![step(1, &[0]), step(0, &[0])], // driver 2
    ];
    // Missing distinct (input,key): (2,[#0]) and (0,[#0]) => 2.
    assert_eq!(delta_new_arrangements(&paths, &available), 2);
}

#[mz_ore::test]
fn commit_delta_query_builds_deltaquery_shape() {
    let inputs = vec![get(0, 2), get(1, 2), get(2, 2)];
    let join = MirRelationExpr::join_scalars(
        inputs.clone(),
        vec![
            vec![MirScalarExpr::column(0), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(3), MirScalarExpr::column(4)],
        ],
    );
    // One path per driver, lookups only.
    let paths = vec![
        vec![step(1, &[0]), step(2, &[0])],
        vec![step(0, &[0]), step(2, &[0])],
        vec![step(1, &[0]), step(0, &[1])],
    ];
    let available = vec![Vec::new(); 3];
    let out = commit_delta_query(join, paths, &available, &inputs, false)
        .expect("commit must succeed");
    let MirRelationExpr::Join { implementation, .. } = &out else {
        panic!("expected a Join");
    };
    match implementation {
        JoinImplementation::DeltaQuery(orders) => {
            assert_eq!(orders.len(), 3, "one path per driver");
            for o in orders {
                assert_eq!(o.len(), 2, "two lookups per path");
                assert!(o.iter().all(|(_, _, c)| c.is_some()), "chars populated");
            }
        }
        other => panic!("expected DeltaQuery, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run the tests, verify they fail**

Run: `cargo nextest run -p mz-transform "eqsat::join_commit::tests::commit_delta_query_builds_deltaquery_shape" "eqsat::join_commit::tests::delta_new_arrangements_counts_distinct_missing"`
Expected: FAIL — functions not found.

- [ ] **Step 3: Implement `delta_new_arrangements`**

```rust
/// Count the distinct lookup arrangements a delta plan needs that are not already
/// in `available`, deduplicated across all paths. Matches
/// `delta_queries::plan` (join_implementation.rs:727-739).
pub(crate) fn delta_new_arrangements(
    paths: &[Vec<crate::eqsat::cost::JoinStep>],
    available: &[Vec<Vec<MirScalarExpr>>],
) -> usize {
    let mut missing: std::collections::BTreeSet<(usize, Vec<MirScalarExpr>)> =
        std::collections::BTreeSet::new();
    for path in paths {
        for step in path {
            let key: Vec<MirScalarExpr> =
                step.key_cols.iter().map(|&c| MirScalarExpr::column(c)).collect();
            let arranged = available[step.input].iter().any(|k| k.as_slice() == key.as_slice());
            if !arranged {
                missing.insert((step.input, key));
            }
        }
    }
    missing.len()
}
```

- [ ] **Step 4: Implement `commit_delta_query`**

Mirror `delta_queries::plan` (join_implementation.rs:698-772) but source the orders from `paths` and characteristics from `step_characteristics`. No start key, no start element:

```rust
/// Commit `join` (a bare `Unimplemented` `Join`) to a `DeltaQuery` following
/// `paths` (one lookup sequence per driver, driver excluded). `available` gives
/// each input's existing arrangement keys. Returns `None` if `join` is not a
/// `Join`. Reuses `JoinImplementation`'s mechanical lowering helpers.
pub(crate) fn commit_delta_query(
    mut join: MirRelationExpr,
    paths: Vec<Vec<crate::eqsat::cost::JoinStep>>,
    available: &[Vec<Vec<MirScalarExpr>>],
    inputs: &[MirRelationExpr],
    prioritize_arranged: bool,
) -> Option<MirRelationExpr> {
    // Build the (input, key, characteristics) lookup tuples per path.
    let mut orders: Vec<Vec<(usize, Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)>> = paths
        .iter()
        .map(|path| {
            path.iter()
                .map(|step| {
                    let key: Vec<MirScalarExpr> =
                        step.key_cols.iter().map(|&c| MirScalarExpr::column(c)).collect();
                    let chars =
                        step_characteristics(step.input, &key, available, inputs, prioritize_arranged);
                    (step.input, key, Some(chars))
                })
                .collect()
        })
        .collect();

    let MirRelationExpr::Join { inputs: join_inputs, implementation, .. } = &mut join else {
        return None;
    };

    // Mechanical lowering: wrap inputs in ArrangeBy / lift MFPs for reuse.
    let (lifted_mfp, lifted_projections) = crate::join_implementation::implement_arrangements(
        join_inputs,
        available,
        orders.iter().flatten(),
    );
    // Compensate keys for any projections lifted by implement_arrangements.
    orders
        .iter_mut()
        .for_each(|order| crate::join_implementation::permute_order(order, &lifted_projections));

    *implementation = JoinImplementation::DeltaQuery(orders);
    crate::join_implementation::install_lifted_mfp(&mut join, lifted_mfp);
    Some(join)
}
```

(Confirm `permute_order`'s parameter type matches `&mut Vec<(usize, Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)>` — it is the same tuple shape `delta_queries::plan` passes. Check the signature with `grep -n "fn permute_order" src/transform/src/join_implementation.rs`.)

- [ ] **Step 5: Run the tests, verify they pass**

Run: `cargo nextest run -p mz-transform eqsat::join_commit`
Expected: PASS (whole module).

- [ ] **Step 6: Format, check, commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/src/eqsat/join_commit.rs
git commit -m "$(cat <<'EOF'
eqsat: add commit_delta_query and delta_new_arrangements

commit_delta_query lowers per-driver paths to DeltaQuery (lookups only, no
stored start key), reusing JI's implement_arrangements/permute_order/
install_lifted_mfp. delta_new_arrangements counts distinct missing
arrangements like delta_queries::plan.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H
EOF
)"
```

---

### Task 5: raise.rs delta decision + execution gate + golden A/B

**Files:**
- Modify: `src/transform/src/eqsat/raise.rs` (add `eager_delta` to `NativeJoinFlags`; the delta decision; `differential_new_arrangements` helper)
- Modify: `src/transform/src/eqsat.rs` (no signature change — `NativeJoinFlags` already threaded; the `optimize_logical`/`optimize_with_wmr_lift` `none()` constructors gain the new field automatically)
- Modify: `src/transform/src/eqsat/transform.rs` (real call site sets `eager_delta` from features)
- Goldens: `test/sqllogictest/transform/` (delta flips), plus the execution gate in `test/sqllogictest/transform/join_index.slt`

**Interfaces:**
- Consumes: `cost::CostModel::delta_join_order` (Task 3); `join_commit::{commit_delta_query, delta_new_arrangements}` (Task 4); `cost::JoinOrder` (the differential order already computed at raise.rs:203).
- Produces: extended `NativeJoinFlags` with `pub eager_delta: bool`.

- [ ] **Step 1: Add `eager_delta` to `NativeJoinFlags`**

In `raise.rs`, add the field and update `none()`:

```rust
pub(crate) struct NativeJoinFlags {
    pub commit: bool,
    pub prioritize_arranged: bool,
    /// Allow delta when `delta_new <= diff_new` (not just `== 0`); reads
    /// `enable_eager_delta_joins`.
    pub eager_delta: bool,
}

impl NativeJoinFlags {
    pub(crate) fn none() -> Self {
        NativeJoinFlags { commit: false, prioritize_arranged: false, eager_delta: false }
    }
}
```

In `transform.rs`, set `eager_delta: ctx.features.enable_eager_delta_joins` in the real call site's `NativeJoinFlags { .. }`.

- [ ] **Step 2: Add the `differential_new_arrangements` helper to `raise.rs`**

Used only in the eager branch. Mirrors `differential::plan` (join_implementation.rs:898). `order` is the `JoinOrder` already computed at raise.rs:203; `available` is `per_input`. The start's new arrangement counts iff its aligned start key is not already available — but at decision time we approximate with the start key the cost model produced (the eager comparison is heuristic in JI too):

```rust
/// New arrangements a differential plan needs, mirroring
/// `differential::plan` (join_implementation.rs:898):
/// `inputs.len().saturating_sub(2) + (start arrangement is new ? 1 : 0)`.
fn differential_new_arrangements(
    order: &crate::eqsat::cost::JoinOrder,
    available: &[Vec<Vec<MirScalarExpr>>],
) -> usize {
    let n = order.steps.len();
    let start = &order.steps[0];
    let start_key: Vec<MirScalarExpr> =
        start.key_cols.iter().map(|&c| MirScalarExpr::column(c)).collect();
    let start_new = if available[start.input].iter().any(|k| k.as_slice() == start_key.as_slice()) {
        0
    } else {
        1
    };
    n.saturating_sub(2) + start_new
}
```

- [ ] **Step 3: Insert the delta decision in the `Rel::Join` arm**

In `raise.rs`, after the differential `order` is computed and validated (after the empty-key abort at raise.rs:213, before building `canon_join` and calling `commit_differential`), add the delta branch. Compute paths over the SAME `canon_escalars` and `per_input`:

```rust
// Try a delta commit first: it is preferred when it needs no new arrangements
// (strict) or no more than differential (eager). Viability is delta_join_order
// returning Some (every step keyed); a disconnected join has no delta plan.
let per_input = per_input_available(&raised_inputs, available);
if let Some(paths) = model.delta_join_order(inputs, &canon_escalars) {
    let delta_new = crate::eqsat::join_commit::delta_new_arrangements(&paths, &per_input);
    let commit_delta = if flags.eager_delta {
        delta_new <= differential_new_arrangements(&order, &per_input)
    } else {
        delta_new == 0
    };
    if commit_delta {
        let canon_join =
            MirRelationExpr::join_scalars(raised_inputs.clone(), canon_equivs.clone());
        if let Some(j) = crate::eqsat::join_commit::commit_delta_query(
            canon_join,
            paths,
            &per_input,
            &raised_inputs,
            flags.prioritize_arranged,
        ) {
            return j;
        }
    }
}
// Fall through to the SP-B1 differential commit (unchanged).
let canon_join = MirRelationExpr::join_scalars(raised_inputs.clone(), canon_equivs);
crate::eqsat::join_commit::commit_differential(
    canon_join,
    order,
    &per_input,
    &raised_inputs,
    flags.prioritize_arranged,
)
.unwrap_or(join)
```

(Note: the existing arm already computes `per_input` at raise.rs:220; reuse that single binding rather than computing it twice — move it above the delta branch and drop the later duplicate.)

- [ ] **Step 4: Compile and run all eqsat unit tests**

Run: `bin/fmt && cargo check -p mz-transform && cargo nextest run -p mz-transform eqsat::`
Expected: PASS. (Default in-branch flags: `enable_eqsat_native_join_commit` on, `enable_eager_delta_joins` off → strict `delta_new == 0`.)

- [ ] **Step 5: Add the execution gate to `join_index.slt`**

In `test/sqllogictest/transform/join_index.slt`, find a representative indexed multi-way join (a star or chain where every probe arrangement is indexed). Add (a) an `EXPLAIN OPTIMIZED PLAN` assertion showing `type=delta`, and (b) a `query` returning its rows. Generate the expected output:

Run: `bin/sqllogictest -- --rewrite-results test/sqllogictest/transform/join_index.slt`

Then MANUALLY confirm in the diff: the EXPLAIN shows `type=delta` for the chosen join, and the `query` rows are non-empty and correct (cross-check against the same query with the flag off — see Step 6).

- [ ] **Step 6: Execution gate — result rows identical to flag-off**

Temporarily disable native commit and capture rows, then re-enable and compare. Run the chosen query under both settings (use a scratch `.slt` or the session). With `enable_eqsat_native_join_commit` OFF the join renders via JI; with it ON it renders as the eqsat delta plan. Assert the multiset of rows is identical.

Run (flag off): set `enable_eqsat_native_join_commit = false` at the top of a scratch copy and run the query; record rows.
Run (flag on, default): run the committed `join_index.slt` query; record rows.
Expected: identical row multisets. Record both in the task report. ALSO confirm the flagship VOJ stays differential:

Run: `bin/sqllogictest -- test/sqllogictest/transform/eqsat_delta_join_cost.slt`
Expected: PASS, flagship still `type=differential` (its delta path crosses `t1`, so `delta_join_order` returns `None`).

- [ ] **Step 7: Regenerate the delta-flip goldens (per-file isolation)**

Run the corpus and regenerate each failing file ALONE:

Run: `bin/sqllogictest --optimized -- $(ls test/sqllogictest/transform/*.slt)`
For each failing file (excluding `demand.slt`, `ldbc_bi.slt`, `tpch_select.slt`):

Run: `bin/sqllogictest -- --rewrite-results test/sqllogictest/transform/<file>.slt`

- [ ] **Step 8: Result-row gate vs base (the C1 lesson)**

For the full golden diff, confirm NO result rows changed (only EXPLAIN `type=differential`→`type=delta` and the path/marker lines):

Run: `git diff test/sqllogictest/transform/ | grep -E '^[+-]' | grep -vE '^[+-]{3} |EXPLAIN|» |\[|type=|Join|Map|Filter|Project|Get|ArrangeBy|Threshold|Union|%[0-9]|Reduce|Constant'`
Expected: empty (no result-set rows changed). Investigate ANY surviving line. This is the gate SP-B1's C1 bug evaded; do not skip it.

- [ ] **Step 9: Reviewable A/B tagging (required)**

In the task report, list EVERY golden that flipped `type=differential` → `type=delta`, with: file name, the join (query), and "0 result-row changes confirmed". Note the count is an estimate (~13), not exact, and that the flipped set need not match JI's. This list is what the final review and PR use to audit the entanglement the reused flag does not isolate.

- [ ] **Step 10: Format, check, commit**

```bash
bin/fmt && cargo check -p mz-transform
git add src/transform/src/eqsat/ test/sqllogictest/transform/
git commit -m "$(cat <<'EOF'
eqsat: commit acyclic joins as DeltaQuery when delta is free

raise.rs now tries a delta commit before differential: commit DeltaQuery
iff delta_join_order yields a fully-keyed plan AND it needs no new
arrangements (strict) or no more than differential (eager, when
enable_eager_delta_joins). Recovers JI's free-delta plans on indexed joins
that SP-B1 committed as differential. Result rows unchanged; VOJ stays
differential.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H
EOF
)"
```

---

## Self-Review

**Spec coverage:**
- Part A `step_characteristics` (fields, original-`available` `arranged`) → Task 1. ✓
- Part A `commit_differential` wiring + flag threading + consumer audit → Task 2. ✓
- Part B `delta_join_order` (viability = `is_some`, `frontier_key_cols` predicate, acyclic-only) → Task 3. ✓
- Part B `commit_delta_query` (lookups only, no start key, reuse helpers) + `delta_new` → Task 4. ✓
- Part B raise.rs decision (strict/eager, `diff_new`) + flag reuse → Task 5. ✓
- Execution gate (rows vs flag-off, VOJ stays differential) → Task 5 Steps 5-6. ✓
- Reviewable golden A/B tagging → Task 5 Step 9. ✓
- Parity-is-estimate framing → Global Constraints + Task 5 Step 9. ✓

**Placeholder scan:** No TBD/TODO. Each code step shows the code; each verification step shows the command + expected result. Where a local test helper name is uncertain (cost.rs test module), the step says to confirm via `grep` and mirror existing tests — that is an instruction with a concrete method, not a placeholder.

**Type consistency:** `NativeJoinFlags` grows `commit`/`prioritize_arranged` (Task 2) then `eager_delta` (Task 5); `none()` updated both times. `commit_differential` gains `inputs`/`prioritize_arranged` (Task 2) and is called with them in Task 2 Step 6 and Task 5 Step 3. `delta_join_order -> Option<Vec<Vec<JoinStep>>>` (Task 3) is consumed in Task 5 Step 3. `commit_delta_query`/`delta_new_arrangements` signatures (Task 4) match their Task 5 call sites. `step_characteristics` signature (Task 1) matches all three callers.
