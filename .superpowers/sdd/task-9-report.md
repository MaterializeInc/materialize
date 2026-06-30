# Task 9 Report: ILP Extractor

## Status: DONE

**Commit:** `642c7383bf` — "transform: add ILP extractor for the arrangement-count objective"

---

## Dependency add

Added to `[workspace.dependencies]` in root `Cargo.toml`:
```toml
good_lp = { version = "1.15", default-features = false, features = ["microlp"] }
```
Added `good_lp = { workspace = true }` to `src/transform/Cargo.toml [dependencies]`.

Verified MIT and Apache-2.0 already in `deny.toml [licenses].allow` and `about.toml accepted`. No license-list edits needed.

`microlp` pulls in the banned `log` crate. Added `"microlp"` to the `wrappers` list for `[[bans.deny]] name = "log"` in `deny.toml`. `cargo deny check` passes.

Ran `cargo check -p mz-transform` to update lock entries only (65 lines in `Cargo.lock`).

---

## Egraph accessors (`egraph.rs`)

Three new `pub` methods on `EGraph`:

### `reachable(root) -> BTreeMap<Id, Vec<ENode>>`
BFS from `find(root)` following `child_classes`. Returns every reachable class with its e-nodes in stable BTreeMap order.

### `child_classes(node) -> Vec<Id>`
Calls `node.children()` and canonicalizes each via `find`.

### `arrangements_of(node) -> Vec<(Id, Vec<usize>)>`
Returns `(arranged_child_class, key_cols)` for each arrangement the node entails:
- `ArrangeBy { input, key }`: key cols from `EScalar::is_col`.
- `Reduce { input, group_key }`: group key cols (group_key is `Vec<EScalar>`, filtered via `is_col`).
- `TopK { input, shape }`: `shape.group_key` (Vec<Col> = Vec<usize>) union `shape.order_key[*].column`.
- `Join` and `WcoJoin`: one entry per input, key cols from `join_key_cols_for_input`.
- All other operators: empty.

Made `join_key_cols_for_input` in `cost.rs` `pub(crate)`. Added `pub(crate) arrange_by_oracle_covered_pub` wrapper for use in ILP oracle-coverage check.

---

## ILP encoding (`extract.rs`)

### Variables
- `node_sel[i]`: binary, one per `(class, node)` in the reachable subgraph.
- `arr_sel[j]`: binary, one per distinct `(class, key_cols)` arrangement required by any node.
- `class_used[k]`: binary, one per reachable class.

### Constraints
1. Root class has exactly one selected node: `sum node_sel[root class] == 1`.
2. Root class is used: `class_used[root] == 1`.
3. Per class: `sum node_sel[class] == class_used[class]`.
4. Per node, per child: `class_used[child] >= node_sel[(parent, node)]`.
5. Per node, per arrangement it entails: `arr_sel[(arr_class, key)] >= node_sel[(parent, node)]`.

### Objective
`minimize sum(arr_sel[j] for j not oracle-covered) + epsilon * sum(node_sel[i])`

Epsilon = 1e-4 (structural tie-break, dominated by arrangement count). Oracle coverage uses `CostModel::arrange_by_oracle_covered_pub`.

### Size cap
Returns `None` (falls back to greedy) when `total_nodes > 600`.

### Reconstruction (`build_selected`)
Top-down recursive builder. Mirrors `build_rel` without polarity-demand machinery. Depth guard of 500 to handle any pathological cycles.

---

## Fallback

Any `ResolutionError` from microlp or reconstruction failure triggers `GreedyExtractor.extract`. Mandatory and always sound.

---

## Feature flag plumbing

Added `enable_eqsat_ilp_extraction: bool` to `optimizer_feature_flags!` macro in `src/repr/src/optimize.rs`.

Default in `From<&SystemVars>` for `OptimizerFeatures`: **`true`** (ILP on by default, as instructed).

`optimize_with_availability` and `optimize_inner` gain `use_ilp: bool` parameter. `PhysicalEqSatTransform` reads `ctx.features.enable_eqsat_ilp_extraction`.

---

## TDD: RED then GREEN

- Wrote `ilp_extracts_min_arrangement_plan` first, confirmed FAIL.
- Implemented `IlpExtractor`.
- Both tests pass; full eqsat suite: **93 tests, all pass**.

---

## Files changed

- `Cargo.toml` — `good_lp` workspace dep
- `Cargo.lock` — updated by `cargo check`
- `deny.toml` — `microlp` wrapper for banned `log`
- `src/repr/src/optimize.rs` — `enable_eqsat_ilp_extraction` flag
- `src/sql/src/plan/statement/ddl.rs` — exhaust match on new field
- `src/sql/src/plan/statement/dml.rs` — struct init with `Default::default()`
- `src/sql/src/session/vars/definitions.rs` — `From<&SystemVars>` with `enable_eqsat_ilp_extraction: true`
- `src/transform/Cargo.toml` — `good_lp.workspace = true`
- `src/transform/src/eqsat.rs` — `use_ilp` param on `optimize_with_availability`/`optimize_inner`
- `src/transform/src/eqsat/cost.rs` — `pub(crate)` on `join_key_cols_for_input`, `arrange_by_oracle_covered_pub` wrapper
- `src/transform/src/eqsat/egraph.rs` — `reachable`, `child_classes`, `arrangements_of` accessors
- `src/transform/src/eqsat/extract.rs` — `IlpExtractor` impl, `is_oracle_covered` helper, test
- `src/transform/src/eqsat/transform.rs` — read `ctx.features.enable_eqsat_ilp_extraction`

---

## Self-review

- No `unsafe`, no `as` conversions.
- No em-dashes, no structuring semicolons in comments.
- `[IlpExtractor]` intra-doc link in module doc now resolves (was forward reference, now the type exists).
- `cargo fmt`, `bin/lint` (rustfmt + check-cargo pass; trufflehog/buf failures are pre-existing env issues), `cargo clippy` all clean.

## Concerns

1. **Polarity safety:** `build_selected` does not enforce the `Nonneg` demand that `build_rel` enforces for Reduce/TopK inputs. No current rule creates a Negate-rooted form in a class that feeds a non-linear reduce, so this is not a live bug. A future improvement can add polarity constraints to the ILP or post-validate and fall back on violation.

2. **microlp stability:** microlp 0.4.0 is early-stage. The `match lp_model.solve() { Ok(s) => ..., Err(_) => return None }` fallback is mandatory and implemented.

3. **ILP on by default:** `enable_eqsat_ilp_extraction` defaults to `true` as instructed. Corpus validation (Task 11) is the gate for confirming no regression.

---

## Review Fix Report (2026-06-22)

Commit: see `transform: fix ILP extractor polarity soundness, panic fallback, and kill-switch`

### Finding 1 — CRITICAL: polarity soundness

**Fix:** Added `is_nonneg_safe(rel)` and `validate_polarity(rel)` free functions in `extract.rs`.
`is_nonneg_safe` mirrors `egraph.rs::build_rel`'s polarity logic: `Negate` is unsafe; `Reduce`/`TopK`/`Threshold` are barriers (safe); leaves are safe; all other operators propagate from children.
`validate_polarity` walks the plan and returns `false` if any non-linear reduce (non-empty aggregates) or TopK has a sign-bearing input.
In `IlpExtractor::solve`, after `build_selected` reconstructs the plan, `validate_polarity` is called; a `false` result returns `None` so the caller falls back to `GreedyExtractor`.

**Test (RED before fix, GREEN after):** `ilp_polarity_soundness_reduce_negate_input` builds `Reduce(MAX, Filter(p, Negate(Get a)))` with the filter input class containing both the negate-rooted form and a nonneg-safe sibling `Filter(p, Get a)`.
The test asserts that neither the ILP nor greedy returns a polarity-unsound plan.
Result: PASS.

### Finding 2 — IMPORTANT: microlp panic fallback

**Fix:** Replaced the bare `lp_model.solve()` + read-back block with `std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| { ... }))`.
The closure contains both `lp_model.solve().ok()?` and the solution read-back loop.
`Ok(Some(sel))` proceeds; `Ok(None)` (solver error) and `Err(_)` (panic) both return `None` to the caller.
`AssertUnwindSafe` is required because `good_lp` types are not `UnwindSafe`; it is sound here because the closure result is discarded on panic (no corrupted state is observed).

### Finding 3 — IMPORTANT: objective fidelity + non-discriminating test

**Fix:** Added `arr_from_arrange_by: BTreeMap<(Id, Vec<usize>), bool>` tracking whether each collected arrangement originates from an `ArrangeBy` node.
In the objective loop, `is_oracle_covered` is only called when `arr_from_arrange_by` marks the arrangement as `true` (from ArrangeBy).
This matches the cost model (`cost.rs::collect_memory_into`), which only oracle-suppresses `ArrangeBy`, not `Reduce`/`TopK` group-key arrangements.

**Test:** `ilp_arrangement_count_not_worse_than_greedy` builds a class with two equivalent forms of `Reduce(distinct, group=[col0])` over `Get t`: one routing via an explicit `ArrangeBy` and one without.
Asserts `model.cost(ilp_plan).arrangements <= model.cost(greedy_plan).arrangements`.
Result: PASS.

### Finding 4 — MINOR: kill-switch system var

**Fix:** Added `enable_eqsat_ilp_extraction` to the `optimizer_feature_flags!` macro block in `definitions.rs` (after `enable_eqsat_physical_optimizer`), with `default: true` and `enable_for_item_parsing: false`.
Changed `From<&SystemVars> for OptimizerFeatures` to use `vars.enable_eqsat_ilp_extraction()`.
Updated the `optimizer_features_no_enable_for_item_parsing` test to destructure and `set_var!` the new field.

### Test results

```
bin/cargo-test -p mz-transform eqsat::extract
  4 tests: PASS (greedy_extractor_matches_extract_with, ilp_arrangement_count_not_worse_than_greedy,
                  ilp_extracts_min_arrangement_plan, ilp_polarity_soundness_reduce_negate_input)

bin/cargo-test -p mz-transform eqsat
  95 tests: all PASS

bin/cargo-test -p mz-sql session::vars::definitions::tests
  1 test: PASS (optimizer_features_no_enable_for_item_parsing)

cargo fmt, bin/lint (rustfmt + check-cargo pass; buf/trufflehog pre-existing), cargo clippy: clean
```
