# Workstream D: physical eqsat placement + index-aware cost implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Run eqsat a second time in the physical optimizer so its `WcoJoin`-to-`DeltaQuery` decision ships live, and make the cost model index-aware so the decision is sound, all behind a default-off flag.

**Architecture:** A logical eqsat pass already runs (joins left `Unimplemented`). This workstream adds a *physical* eqsat placement positioned **after `fixpoint_physical_01` and before `LiteralConstraints`/`JoinImplementation`** (so joins are still `Unimplemented` and not yet `ArrangeBy`-wrapped). The physical pass calls `optimize` (commit_wcoj=true), committing `WcoJoin` to `DeltaQuery`; downstream `JoinImplementation` only replans `Unimplemented`/`Differential`, so the `DeltaQuery` survives. Task 1 is a feasibility spike: register the placement behind a default-off flag and verify the pipeline tolerates a committed `DeltaQuery` at that point. Task 2 feeds real index/arrangement availability into the cost model so `WcoJoin` is chosen soundly, not blindly.

**Tech Stack:** Rust, `mz_transform::eqsat`, `mz_transform::Optimizer::physical_optimizer`, `IndexOracle`.

## Global Constraints

* No `as`/as_conversions; use `mz_ore::cast`. No `unsafe` without `SAFETY`.
* No em-dashes or structuring semicolons in comments. No vendor names in user-facing surfaces. Never drop comments.
* `cargo fmt`; `bin/lint`; `cargo clippy -p mz-transform` before commit (ignore buf/trufflehog env failures).
* `doc/developer/generated/` is read-only.
* The new flag `enable_eqsat_physical_optimizer` defaults **OFF** (unlike the logical `enable_eqsat_optimizer` which is on). So default CI is unaffected; verification turns it on explicitly.
* Process hygiene for environmentd/SLT: one run at a time, `killall clusterd sqllogictest` (exact names, never `pkill -f`).
* Commit footer:
  ```
  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_016CwwG1wCgqf3v8g6uhf1Nx
  ```

## Reference (from exploration)

* `Optimizer::physical_optimizer` in `src/transform/src/lib.rs` (~832): order is Typecheck, `fixpoint_physical_01` (EquivalencePropagation, fold_constants, CoalesceCase, Demand, ProjectionPushdown, LiteralLifting), `LiteralConstraints` (~881), `fixpoint_join_impl: JoinImplementation` (~882), CanonicalizeMfp, RelationCSE, ProjectionPushdown::skip_joins, CanonicalizeMfp, CaseLiteralTransform, fold_constants, Typecheck.
* The physical placement goes **between `fixpoint_physical_01` and `LiteralConstraints`** (joins Unimplemented; the only ProjectionPushdown that panics on filled joins runs inside fixpoint_physical_01, BEFORE this point; ProjectionPushdown::skip_joins later is safe).
* Logical registration to mirror: `src/transform/src/lib.rs` ~806 (`if ctx.features.enable_eqsat_optimizer { transforms.push(Box::new(eqsat::EqSatTransform)); }`).
* `EqSatTransform`: `src/transform/src/eqsat/transform.rs` (~29); it calls `optimize_logical`. The physical variant calls `optimize` (commit_wcoj=true): `src/transform/src/eqsat.rs` ~57 (`optimize` vs `optimize_logical`).
* `JoinImplementation` skips `DeltaQuery` (only replans Unimplemented/Differential): `src/transform/src/join_implementation.rs` ~588.
* Cost model is a parameter-free unit struct: `src/transform/src/eqsat/cost.rs` ~147. `IndexOracle` trait: `lib.rs` ~364; `TransformCtx.indexes`: `lib.rs` ~123. `EqSatTransform::actually_perform_transform` currently ignores `_ctx`.
* New flag goes in `OptimizerFeatures` (`src/repr/src/optimize.rs` ~96) + a `SystemVar` in `src/sql/src/session/vars/definitions.rs` (mirror `enable_eqsat_optimizer`).

---

### Task 1: Physical eqsat placement behind a default-off flag (feasibility spike)

**Files:**
- Modify: `src/repr/src/optimize.rs` (add `enable_eqsat_physical_optimizer: bool` to `OptimizerFeatures`)
- Modify: `src/sql/src/session/vars/definitions.rs` (add the `enable_eqsat_physical_optimizer` system var, default false; wire into the features struct like `enable_eqsat_optimizer`)
- Modify: `src/transform/src/eqsat/transform.rs` (add `PhysicalEqSatTransform` calling `optimize` / commit_wcoj=true; reuse the arity guard)
- Modify: `src/transform/src/lib.rs` (register `PhysicalEqSatTransform` in `physical_optimizer` between `fixpoint_physical_01` and `LiteralConstraints`, gated on the new flag)

**Interfaces:**
- Produces: `eqsat::PhysicalEqSatTransform` (mirrors `EqSatTransform` but commits WcoJoin). New feature flag `enable_eqsat_physical_optimizer` (default false).

- [ ] **Step 1: Add the flag**

Add `enable_eqsat_physical_optimizer: bool` to `OptimizerFeatures` (optimize.rs), default false. Add the system var in definitions.rs mirroring `enable_eqsat_optimizer` but `default: false` (no TODO-on note). Wire it into wherever `OptimizerFeatures` is populated from vars (grep for `enable_eqsat_optimizer` to find all sites and mirror).

- [ ] **Step 2: Add `PhysicalEqSatTransform`**

In transform.rs, add a `PhysicalEqSatTransform` struct implementing `Transform`. It mirrors `EqSatTransform::actually_perform_transform` but calls `crate::eqsat::optimize(clone)` (commit_wcoj=true) instead of `optimize_logical`. Keep the arity guard (optimize a clone, adopt only if arity matches, else `soft_panic_or_log!` no-op). Add a doc comment: this runs in the physical phase where filled join implementations are allowed downstream, so it commits the WcoJoin-to-DeltaQuery decision.

- [ ] **Step 3: Register it in `physical_optimizer`**

In lib.rs `physical_optimizer`, after the `fixpoint_physical_01` fixpoint push and before the `LiteralConstraints` push, add:
```rust
if ctx.features.enable_eqsat_physical_optimizer {
    transforms.push(Box::new(eqsat::PhysicalEqSatTransform));
}
```
Add a comment explaining the placement (joins still Unimplemented here; commits WcoJoin->DeltaQuery; JoinImplementation downstream skips DeltaQuery).

- [ ] **Step 4: Compile + unit tests**

`cargo check -p mz-transform -p mz-repr -p mz-sql` clean. `cargo test -p mz-transform --lib eqsat` and `--test wcoj_decision` pass.

- [ ] **Step 5: FEASIBILITY GATE (the crux) - does the pipeline tolerate a committed DeltaQuery at this placement?**

This is the key risk. With the flag ON, a committed `DeltaQuery` (ArrangeBy-wrapped inputs + lifted MFP) flows into `LiteralConstraints` then `JoinImplementation`. Verify nothing panics or miscompiles:
- `killall clusterd sqllogictest`, then run an environmentd-based check with the physical flag on. Easiest: a sqllogictest run with the flag set. Set it via the test harness or a `SET enable_eqsat_physical_optimizer = true` session and run a query with a multi-way (triangle) join, `EXPLAIN PHYSICAL PLAN` it, and confirm a `DeltaQuery` appears and the query executes without panic.
- ALSO run `bin/sqllogictest --optimized -- test/sqllogictest/arithmetic.slt` with the physical flag forced on (e.g. via the harness default-vars mechanism, or temporarily flip the default to true for this one verification then flip back) - confirm it completes, exit 0, zero "Non-positive multiplicity", no "can't deal with filled in join implementations" panic.
- If `LiteralConstraints` or anything between the placement and JoinImplementation panics on the committed DeltaQuery, STOP and report: the placement is not viable as-is, and Task 1 must either move the placement or leave joins Unimplemented (losing the WCOJ win). Do not force it.

- [ ] **Step 6: Add a focused test that the physical pass commits DeltaQuery**

Add a test (in `wcoj_decision.rs` or a new physical test) that runs the triangle plan through the physical placement (or directly through `optimize` with commit_wcoj=true, which already exists) and asserts a `DeltaQuery` is produced and survives a subsequent `JoinImplementation` run. Much of this may already exist in `wcoj_decision.rs` (the offline triangle test); extend it to assert the physical-placement path.

- [ ] **Step 7: Commit**

```bash
cargo fmt
bin/lint
cargo clippy -p mz-transform
git add -A src/repr/src/optimize.rs src/sql/src/session/vars/definitions.rs src/transform/src/eqsat/transform.rs src/transform/src/lib.rs <test files>
git commit -m "eqsat: physical placement committing WcoJoin to DeltaQuery (flag off)

Add enable_eqsat_physical_optimizer (default off) and PhysicalEqSatTransform,
registered before JoinImplementation so the WcoJoin-to-DeltaQuery decision
ships live. JoinImplementation skips DeltaQuery, so the decision survives."
```

(If the feasibility gate fails, instead commit a report of the failure and escalate for re-scoping.)

---

### Task 2: Index-aware cost model

**Files:**
- Modify: `src/transform/src/eqsat/cost.rs` (CostModel consumes arrangement/index availability)
- Modify: `src/transform/src/eqsat/transform.rs` (`PhysicalEqSatTransform` passes `ctx.indexes` into the cost model)
- Modify: `src/transform/src/eqsat.rs` / engine plumbing as needed to thread index availability into extraction.

**Interfaces:**
- Consumes: Task 1's `PhysicalEqSatTransform` and `ctx.indexes` (an `&dyn IndexOracle`).

- [ ] **Step 1: Write the failing test**

A test where a join input already has an available arrangement/index on the join key: the cost model should NOT charge the arrangement-build memory term for it, so a plan reusing the index is cheaper. Conversely, with no index, the triangle still prefers `WcoJoin`. Assert the cost model reflects available arrangements. (Construct a `CostModel` with a stub index oracle reporting one arrangement; assert the memory term for that input is reduced versus no oracle.)

- [ ] **Step 2: Run to verify it fails**

`cargo test -p mz-transform --lib eqsat::cost` - FAIL (CostModel is parameter-free, ignores indexes).

- [ ] **Step 3: Thread index availability into the cost model**

Give `CostModel` an index-availability input (e.g. a set of `(input-identity, key)` arrangements, derived from `ctx.indexes` plus arrangements installed in the plan). In `collect_memory`/the join-cost path, zero or reduce the arrangement memory term for a join input whose key is already available. Keep the existing behavior when no index is available (the parameter-free path becomes "empty availability"). `PhysicalEqSatTransform` builds the availability from `ctx.indexes` and passes it in; the logical `EqSatTransform` passes empty (unchanged behavior).

- [ ] **Step 4: Run to verify it passes**

`cargo test -p mz-transform --lib eqsat` all pass, including the new cost test. Confirm the triangle-without-index still picks `WcoJoin` (`wcoj_decision` tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt
bin/lint
cargo clippy -p mz-transform
git add src/transform/src/eqsat/cost.rs src/transform/src/eqsat/transform.rs src/transform/src/eqsat.rs
git commit -m "eqsat: index-aware cost for the physical placement

Feed available arrangements/indexes (from ctx.indexes) into the cost model
so a join input that is already arranged is not charged the arrangement
build, making the WcoJoin-vs-binary decision sound rather than index-blind."
```

---

### Task 3: Docs, coverage, measure

**Files:**
- Modify: `docs/superpowers/specs/2026-06-19-mir-egraph-status.md`

- [ ] **Step 1: Update coverage + findings**

Move `JoinImplementation` from Partial to Covered-with-caveat (the physical placement commits the WcoJoin-to-DeltaQuery decision live, behind the default-off flag, with index-aware cost). State honestly that it is flag-off by default pending broader SLT validation, and that the cost model is now index-aware. Note deletion phase 3 (delete `JoinImplementation`) is gated on flag-on SLT parity. Keep claims evidence-based.

- [ ] **Step 2: Measure**

Run `cargo test -p mz-transform --test compare_real -- --nocapture`; record counts. Record the truth.

- [ ] **Step 3: Commit**

```bash
cargo fmt
bin/lint
git add docs/superpowers/specs/2026-06-19-mir-egraph-status.md
git commit -m "eqsat: document physical placement + index-aware cost coverage"
```
