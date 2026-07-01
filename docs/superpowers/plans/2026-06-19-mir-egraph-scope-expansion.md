# MIR equality-saturation scope expansion implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Broaden the `mz-transform-egraph` pass so it covers more of MIR and rewrites inside scalars, closing the parity gap the differential harness exposed (losses caused by missing scalar folding) and reaching real aggregate/top-k plans.

**Architecture:** Three independent threads. M3a lowers more relational operators structurally (mechanical, mirrors M1). M2a teaches rule instantiation to remap real scalar column indices via `MirScalarExpr::permute_map`, re-enabling the gated pushdown rules. M2b adds a scalar constant-folding layer so semantic folds (`IS NULL` on a `NOT NULL` column is false, all-true/all-false predicates) fire, which is where the harness losses originate.

**Tech Stack:** Rust, `mz-expr` (`MirScalarExpr::permute_map`, `support`, `as_column`, literal helpers), the ported e-graph engine.

## Global Constraints

* Crate `src/transform-egraph`. No `unsafe`. No new `as` (use `mz_ore::cast`). `#[mz_ore::test]` not `#[test]`. Copyright headers. `cargo fmt` (never `--check`).
* Scalars rewritten only through `MirScalarExpr` APIs (`permute_map`, literal evaluation); never hand-parse the rendered text.
* Every newly supported operator must round-trip exactly (`raise(lower(x)) == x`) with rules off, and bail behavior for still-unsupported variants must be preserved.
* The pass stays offline (test-only); no live-pipeline registration in this plan.

---

### Task 1: Lower `Reduce` structurally (M3a)

**Files:** Modify `src/transform-egraph/src/lower.rs`, `src/transform-egraph/src/raise.rs`; tests in both.

**Interfaces:**
- Produces: `lower` maps `MirRelationExpr::Reduce { input, group_key, aggregates, .. }` to `Rel::Reduce { input, group_key, aggregates }` (interning the `group_key` scalars and rendering each `AggregateExpr` as an opaque interned scalar payload), and `raise` inverts it.

- [ ] **Step 1: Failing round-trip test** — build `src(0,3).reduce(vec![col(0)], vec![<one AggregateExpr>], None)`, assert `raise(lower(r)) == r`. (Confirm the real `MirRelationExpr::reduce` signature and `AggregateExpr` construction in `src/expr/src/relation.rs`.)
- [ ] **Step 2: Run, watch fail** (`Reduce` currently bails to a leaf, so the lowered shape is `Rel::Get`, not `Rel::Reduce`).
- [ ] **Step 3: Implement.** In `lower`, add a `Reduce` arm: intern `group_key` predicates as `Vec<Scalar>`; for `aggregates`, intern each `AggregateExpr` (store the whole `AggregateExpr` in a new interner table keyed like scalars, since the prototype `Rel::Reduce` carries opaque aggregate scalars). In `raise`, reconstruct `MirRelationExpr::reduce` from the resolved group key and aggregates. The interner needs an aggregate table parallel to the scalar table (`intern_aggregate`/`resolve_aggregate`).
- [ ] **Step 4: Run, watch pass.**
- [ ] **Step 5: Commit** `transform-egraph: lower Reduce structurally`.

(Note: the prototype `Rel::Reduce` exists with `group_key`/`aggregates` as `Vec<Scalar>`; aggregates are opaque, so `reduce_elision` — which needs only the group key and unique-key analysis — can fire. Confirm `reduce_elision`'s side conditions read only the group key.)

### Task 2: Lower `TopK`, `FlatMap`, `ArrangeBy` structurally (M3a)

**Files:** Modify `lower.rs`, `raise.rs`; tests.

The prototype `Rel` has no `TopK`/`FlatMap`/`ArrangeBy` variants, so this requires either adding them to `ir::Rel` (and `arity`/`children`/`with_children`) or continuing to bail them. Decision: add `Rel::TopK`, `Rel::FlatMap`, `Rel::ArrangeBy` as opaque-payload passthrough variants (no rules rewrite them yet; they exist so the operators round-trip structurally and the engine can rewrite the envelope around them without an opaque-leaf boundary).

- [ ] **Step 1: Failing round-trip tests** for each of the three operators.
- [ ] **Step 2: Run, watch fail.**
- [ ] **Step 3: Implement.** Add the three variants to `ir::Rel` with their payloads (interned scalars/keys + the structural fields like `limit`/`offset`/`monotonic` carried opaquely in the interner where they are not relational). Extend `arity`, `children`, `with_children`, and the e-graph `ENode` if rules must see them (they need not; a structural passthrough that the matcher never matches is enough for round-trip). Add `lower`/`raise` arms.
- [ ] **Step 4: Run, watch pass; full suite green.**
- [ ] **Step 5: Commit** `transform-egraph: lower TopK/FlatMap/ArrangeBy structurally`.

(If adding `ENode` variants is disproportionate, keep these three as opaque leaves and document that only the envelope around them optimizes — the same trade-off as M1. Reduce, Task 1, is the high-value one.)

### Task 3: Column-index remapping in rule instantiation (M2a)

**Files:** Modify `src/transform-egraph/src/egraph.rs` (`instantiate`), `src/transform-egraph/src/matcher.rs` (the `shift`/`remap` combinators), thread the interner into instantiation; `rules/relational.rewrite`; tests.

**Interfaces:**
- Produces: when a rule RHS uses `shift`/`remap` on a payload, instantiation materializes a REAL remapped `MirScalarExpr` (clone the interned scalar, `permute_map` its columns, re-intern) rather than only adjusting the opaque `Scalar`'s `cols`/`text`.

- [ ] **Step 1: Failing test** — a `push_filter_past_project` case: `Filter[p](Project[o] r)` optimizes to `Project[o](Filter[p'] r)` where `p'` has columns remapped through `o`; assert via `optimize` that the predicate's real column references were permuted correctly (resolve the raised filter's `MirScalarExpr` and check its `support()`).
- [ ] **Step 2: Run, watch fail** (rule disabled / remap is inert on opaque scalars).
- [ ] **Step 3: Implement.** Thread `&mut Interner` (or a scalar resolver/re-interner) into `instantiate`/`eval_pexpr`. For `Shift(k)` and `Remap(outs)`, after computing the new column mapping, fetch each scalar's real `MirScalarExpr` from the interner, apply `permute_map` with the mapping, re-intern, and emit the new `Scalar`. Re-enable `push_filter_past_project`, `push_filter_into_join_first`, `push_filter_into_join_second` in the rule file. Update the `no_disabled_rule_is_active` guard.
- [ ] **Step 4: Run, watch pass; full suite green.**
- [ ] **Step 5: Commit** `transform-egraph: remap real scalar columns in instantiation; re-enable pushdown rules`.

### Task 4: Scalar constant-folding layer (M2b, first slice)

**Files:** New `src/transform-egraph/src/scalar_fold.rs`; wire into `optimize`; `rules/relational.rewrite` (re-enable the fold-dependent relational rules); tests.

**Interfaces:**
- Produces: before/within saturation, scalars are normalized so the relational conditions `any_false`/`all_true` reflect semantic facts the input did not literalize, in particular `IS NULL(c)` on a column known `NOT NULL` folds to `false`. This is the source of the harness losses.

- [ ] **Step 1: Failing test** — a `Filter[IS NULL(#0)] r` where `#0` is `NOT NULL` should optimize to an empty collection (via `empty_false_filter` once the predicate folds to false). Assert `optimize` yields a 0-row constant.
- [ ] **Step 2: Run, watch fail** (the predicate stays opaque; `any_false` is false).
- [ ] **Step 3: Implement a bounded scalar fold.** At lower time (or a pre-pass), evaluate each interned `MirScalarExpr` against the column nullability available from the input's type (`ReprRelationType` column `nullable` flags), folding `IS NULL`/`IS NOT NULL` and literal-boolean combinations to a literal where provable; set the `Scalar`'s `lit` flag accordingly. Reuse `mz-expr`'s existing scalar reduction (`MirScalarExpr::reduce`) against a column-type context if available, rather than reimplementing folding. Re-enable `drop_true_filter`/`empty_false_filter` reliance accordingly (they are already active; this makes them fire on folded predicates).
- [ ] **Step 4: Run, watch pass.** Re-run the differential harness `compare_real.rs`; confirm the previously-lost cases now tie (record the new win/loss/tie counts in the commit message).
- [ ] **Step 5: Commit** `transform-egraph: scalar nullability/constant folding closes harness losses`.

(M2b full scalar-rewriting e-graph — scalars as e-classes with their own rules and cost — is a larger follow-on; this task does the bounded fold that closes the observed parity gap. Scope the full scalar e-graph separately once this lands.)

## Dependency notes

Tasks 1 and 2 (operator coverage) are independent of 3 and 4 (scalars) and of each other.
Task 3 (remapping) is independent of Task 4 (folding).
Recommended order: Task 1 (Reduce, highest value), then Task 4 (closes the real parity gap), then Task 3 (re-enables pushdown), then Task 2 (remaining operators).
Re-run `compare_real.rs` after Task 4 and after Task 3 to quantify the parity gap closing.
