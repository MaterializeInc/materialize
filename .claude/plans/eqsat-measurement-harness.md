# Eqsat marginal-value measurement harness implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Quantify the equality-saturation optimizer's marginal value against the surrounding greedy transform passes, by measuring per query: final arrangement count eqsat-OFF, final eqsat-ON, eqsat's INTRINSIC count (snapshot right after the eqsat pass, before downstream destructive passes), and whether the final plan changed — then attribute each query to eqsat-wins / redundant / clobbered / neutral.

**Architecture:** Extend the existing harness `src/transform/tests/eqsat_arrangement_benchmark.rs`, which already runs the full transform list with eqsat Off/Greedy/Ilp (`optimize_full`) and counts maintained arrangements (`count_arrangements`). Add an in-context intrinsic snapshot (record `count_arrangements` immediately after the eqsat transform fires inside the manual transform loop), a per-query attribution classification, a printed summary, and new fixtures in the regime where eqsat is expected to diverge from greedy (global arrangement sharing across joins, phase-ordering-sensitive sharing). All measurement is on the reuse-aware `count_arrangements` MIR metric so the three numbers are directly comparable.

**Tech Stack:** Rust, `mz-transform` crate, `#[mz_ore::test]` integration tests, `bin/cargo-test`.

## Global Constraints

* Single arrangement metric across all three numbers: the existing `count_arrangements(&MirRelationExpr)` in `eqsat_arrangement_benchmark.rs:127-151` (dedups `ArrangeBy`/`Reduce`/`TopK` by collection+key). Do NOT mix it with the `Rel`-level cost-model count, which is not directly comparable.
* The harness is a MEASUREMENT tool, not a correctness gate. It prints a table and a summary. Its only assertion is the soundness invariant `final_on <= final_off` per fixture (eqsat must never net-increase arrangements). A violation is a real finding, not a flaky test.
* Additive only: do not change the behavior of the existing `compare()`/fixtures in the file. Add new functions and fixtures alongside.
* No `as` conversions (use `mz_ore::cast`); comments have no em-dashes or clause-joining semicolons; doc comments state contracts.
* After editing: `cargo fmt`; before commit: `bin/lint`, `cargo clippy -p mz-transform`.

## File structure

* `src/transform/tests/eqsat_arrangement_benchmark.rs` — extend with the intrinsic snapshot, attribution, summary, and new fixtures. Reuses its own `count_arrangements`, `optimize_full`, and the `OptimizerFeatures`/`TransformCtx` setup already present.

---

## Task 1: Intrinsic snapshot + attribution + summary

**Files:**
- Modify: `src/transform/tests/eqsat_arrangement_benchmark.rs`

**Interfaces:**
- Consumes: existing `count_arrangements(&MirRelationExpr) -> usize`, the existing transform-list construction (`Optimizer::logical_optimizer` + `logical_cleanup_pass` + `physical_optimizer`), `OptimizerFeatures`, `TransformCtx::global`.
- Produces: `fn measure_marginal(name: &str, plan: &MirRelationExpr) -> Measurement` where `struct Measurement { final_off: usize, final_on: usize, intrinsic: usize, plan_changed: bool, verdict: Verdict }` and `enum Verdict { EqsatWins, Redundant, Clobbered, Neutral }`; a `#[mz_ore::test] fn eqsat_marginal_value()` that runs every fixture through `measure_marginal`, prints a table + summary, and asserts `final_on <= final_off`.

- [ ] **Step 1: Read the existing harness.** Read `src/transform/tests/eqsat_arrangement_benchmark.rs` in full. Identify (a) how `optimize_full` builds and runs the transform list, (b) how it toggles eqsat via `OptimizerFeatures`, (c) the `count_arrangements` signature, (d) the existing fixtures (`src`, `f1_*`, `w1_*`, etc.). The new code reuses these.

- [ ] **Step 2: Write the intrinsic snapshot.** Add a function that runs the FULL transform list with eqsat ON (logical + physical), but records `count_arrangements(&plan)` at the moment immediately AFTER the eqsat transform(s) have run and BEFORE the remaining passes. Implement by iterating the transform list manually (as `optimize_full` already does) and, after applying the transform whose type is `EqSatTransform` or `PhysicalEqSatTransform` (match by the transform's `debug_name()`/type name), snapshot the count. If both eqsat passes are present, snapshot after the LAST one. Return both the snapshot (intrinsic) and the fully-optimized plan.

```rust
// Snapshot arrangement count right after the last eqsat transform fires.
fn run_with_intrinsic_snapshot(
    plan: &MirRelationExpr,
    ctx: &mut TransformCtx,
    transforms: &[Box<dyn Transform>],
) -> (usize /* intrinsic */, MirRelationExpr /* final */) {
    let mut p = plan.clone();
    let mut intrinsic = count_arrangements(&p);
    for t in transforms {
        t.transform(&mut p, ctx).expect("transform");
        let name = t.debug_name(); // or std::any::type_name via a helper
        if name.contains("EqSat") {
            intrinsic = count_arrangements(&p);
        }
    }
    (intrinsic, p)
}
```

Verify the exact way to identify the eqsat transforms (the `Transform` trait's name accessor in this repo — check `EqSatTransform`/`PhysicalEqSatTransform` for a `debug_name`/`name` method; if none, downcast or match `std::any::type_name`). Do not guess: confirm against the trait definition.

- [ ] **Step 3: Write `measure_marginal`.** Compute `final_off` = `count_arrangements(optimize_full(plan, Off))`, and `(intrinsic, final_on_plan)` via Step 2 with eqsat ON, `final_on = count_arrangements(&final_on_plan)`. `plan_changed` = the eqsat-ON final plan differs structurally from the eqsat-OFF final plan (`!= ` on the `MirRelationExpr`). Classify:
  * `EqsatWins` if `final_on < final_off`.
  * `Clobbered` if `intrinsic < final_on` (eqsat reduced arrangements but downstream passes added them back).
  * `Redundant` if `final_on == final_off` and `intrinsic < final_off` (eqsat improved its own output but the net is unchanged because greedy reaches the same final count).
  * `Neutral` otherwise (`final_on == final_off` and `intrinsic == final_off`: eqsat changed nothing).

- [ ] **Step 4: Write the test + summary.** A `#[mz_ore::test] fn eqsat_marginal_value()` that runs all fixtures, `eprintln!`s a fixed-width table (`name  off  on  intrinsic  changed  verdict`), then a summary line (counts per verdict, total `sum(final_off) - sum(final_on)` = net arrangements saved). Assert `m.final_on <= m.final_off` for every fixture with a message naming the fixture. Run with `--nocapture`.

- [ ] **Step 5: Run, fmt, lint.**

Run: `bin/cargo-test -p mz-transform --test eqsat_arrangement_benchmark -- eqsat_marginal_value --nocapture`
Expected: PASS, table + summary printed. Capture the printed numbers verbatim into the commit message body or the report.

Then `cargo fmt`, `bin/lint`, `cargo clippy -p mz-transform` (no warnings).

- [ ] **Step 6: Commit.**

```bash
git add src/transform/tests/eqsat_arrangement_benchmark.rs
git commit -m "eqsat: marginal-value harness (final off/on, intrinsic snapshot, attribution)"
```

---

## Task 2: Divergence-regime fixtures

**Files:**
- Modify: `src/transform/tests/eqsat_arrangement_benchmark.rs`

**Interfaces:**
- Consumes: the fixture-construction style already in the file (functions returning `MirRelationExpr`), `measure_marginal` from Task 1.
- Produces: new fixture functions registered in the `eqsat_marginal_value` fixture list, each with a doc comment stating WHY eqsat is expected to diverge from greedy on it.

The existing fixtures (`f1_*`, `w1_*`) are where production already wins (local optimizations) — they will mostly show `Redundant`/`Neutral`, which is itself a result. The point of this task is to add fixtures in the regime where eqsat's global, phase-order-free search SHOULD beat greedy, so the harness can show whether it actually does.

- [ ] **Step 1: Add a global-sharing fixture.** Build a fixture where two sub-plans can share a single maintained arrangement only if a join/projection decision is made one specific way, and greedy phase-ordering would pick the other way (so greedy maintains two arrangements, eqsat one). Doc-comment the exact sharing opportunity and why greedy misses it. Example shape: two joins over the same base relation where a shared arrangement key exists only under a non-default join order.

```rust
/// Two joins over `base` that can share one arrangement on key `#0` iff both
/// are arranged by `#0`. The greedy `JoinImplementation` orders each join
/// independently and arranges the second on a different key, so it maintains
/// two arrangements. Eqsat can choose the globally-shared key. Expect
/// EqsatWins (final_on < final_off) if the lift's objective realizes it,
/// Redundant/Neutral if production already shares.
fn g1_global_shared_arrangement() -> MirRelationExpr { /* build it */ }
```

- [ ] **Step 2: Add a phase-ordering fixture.** Build a fixture where applying rule A before rule B yields fewer arrangements than B-before-A, and the greedy pipeline's fixed order picks the worse one. Doc-comment which two passes' ordering matters. Eqsat (order-free saturation) should find the better one.

- [ ] **Step 3: Register and run.** Add both to the fixture list in `eqsat_marginal_value`, run the harness with `--nocapture`, and record the verdicts. If a fixture intended to show `EqsatWins` instead shows `Redundant`/`Neutral`, that is a valid (and important) measured result — keep it, and note in its doc comment that production already captures the opportunity. Do NOT contrive a fixture that only "wins" by disabling a production pass the deployed pipeline always runs.

Run: `bin/cargo-test -p mz-transform --test eqsat_arrangement_benchmark -- eqsat_marginal_value --nocapture`
Expected: PASS, with the new fixtures in the table.

- [ ] **Step 4: fmt, lint, commit.**

`cargo fmt`, `bin/lint`, `cargo clippy -p mz-transform`, then:

```bash
git add src/transform/tests/eqsat_arrangement_benchmark.rs
git commit -m "eqsat: divergence-regime fixtures for the marginal-value harness"
```

---

## Self-review notes

* The headline metric `final_off vs final_on` is exact and faithful (full pipeline both times). The intrinsic snapshot is the in-context eqsat output, comparable because it uses the same `count_arrangements`. The attribution verdicts are derived purely from the three numbers.
* Honesty guard (Task 2 Step 3): the harness must measure the DEPLOYED pipeline. A fixture that only shows a win by turning off a production pass would be measuring a fiction. Such fixtures are rejected; a `Redundant`/`Neutral` result on a genuinely-deployed pipeline is the real answer.
* Limitation to document in the test's module comment: `count_arrangements` counts `ArrangeBy`/`Reduce`/`TopK` collections, not join-input arrangements (which are implicit in MIR until LIR). So the metric undercounts join arrangements uniformly across off/on; differences in join-arrangement sharing may be invisible at this layer. This is a known floor on the harness's resolution and must be stated, not hidden.
