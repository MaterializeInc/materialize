# Polarity-aware extractor implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make e-graph extraction never place a multiplicity-signed (Negate-rooted) representative directly as the input of a non-linear operator, so the unsound-but-valuable negate-join rewrite rules can be re-enabled.

**Architecture:** Replace the single-best-per-class extraction fixpoint with a demand-parameterized one over a two-element polarity lattice `{Any, Nonneg}`. Each class gets two best entries (`best_any`, `best_nonneg`). A node's children are extracted under a per-position demand: non-linear `Reduce`/`TopK` inputs always demand `Nonneg` (the soundness rule); `Negate` cannot satisfy a `Nonneg` demand; `Threshold`/`Reduce`/`TopK` are `Nonneg` barriers (their output is non-negative regardless of input); sign-preserving ops (`Map`/`Filter`/`Project`/`Union`/`Join`/`WcoJoin`) propagate the parent's demand. Then re-enable `distribute_negate_join` + `factor_negate_join`.

**Tech Stack:** Rust, `mz-transform` crate, `src/transform/src/eqsat/`.

## Global constraints

* it's all about eqsat; NO HIR changes.
* No `as` conversions; use `mz_ore::cast::{CastFrom, CastLossy}`.
* No unsafe.
* Comments: no em-dash, no structuring semicolons; doc comment states the contract, reasoning inline at the decision point; no vendor names. Never drop existing comments.
* Prefer correctness over performance; prefer reuse over writing.
* Test commands per the `mz-test` skill: `cargo nextest run -p mz-transform` (or `bin/cargo-test -p mz-transform` if METADATA_BACKEND_URL complains); datadriven rewrite with `REWRITE=1`. Use the mz-transform crate-level harness for logical-plan checks (rebuild ~40s, no environmentd/cockroach); reserve `bin/sqllogictest --optimized` for final e2e.
* Commit between tasks on branch `claude/mir-equality-optimizer-sodbej` with the repo Co-Authored-By / Claude-Session trailers.

## Background (why this is sound)

`relational.rewrite:111-137` omits `distribute_negate_join` / `factor_negate_join` because e-graph merging is bidirectional: once `join(negate(a),rest)` and `negate(join(a,rest))` share a class, the extractor may pick a `Negate`-rooted representative as the input of a `Reduce` with a non-linear aggregate (MIN/MAX/ANY/ALL), producing `Reduce_MAX(negate(...))`, which is wrong because `reduce(r) != negate(reduce(negate(r)))` for non-linear aggregates. The documented fix is "a polarity-aware extractor (forbid a Negate-rooted representative directly under a non-linear reduce), not a rule guard" — this plan builds exactly that.

Current extractor: `EGraph::extract_with` (egraph.rs:1427) runs a fixpoint filling `best: HashMap<Id, (Cost, Rel)>` with the cheapest representative per class; `build_rel` (egraph.rs:1483) rebuilds a `Rel` from an `ENode`, pulling each child from `best`. Cost is a pure compositional function of the built `Rel`, memoized by `Rel`.

## Polarity rules (the core of Task 1)

Two demands: `Any` (no constraint) and `Nonneg` (the representative's output multiplicities are non-negative). For a node being built to satisfy demand `D`:

* **Can the node satisfy `Nonneg`?** Every node EXCEPT `Negate` can (the recursion below enforces the rest). `Negate` output is signed, so a `Negate` node is never a candidate for `best_nonneg`.
* **Per-child demand** (what each child must satisfy), by node kind:
  * `Reduce` input, `TopK` input: ALWAYS `Nonneg`. This is the soundness rule and is independent of `D`. (v1 is conservative: every `Reduce` with at least one aggregate and every `TopK` demands `Nonneg`. A `Reduce` with no aggregates, i.e. a Distinct, demands `Any` since dedup is polarity-insensitive. Refinement to allow `Any` for all-linear-aggregate reduces, via `aggregate_is_input` from `crate::analysis::equivalences`, is future work noted in a comment.)
  * `Negate` input: `Any`.
  * `Threshold` input: `Any` (Threshold output is non-negative regardless of input; it is a barrier).
  * `Map`/`Filter`/`Project` input: propagate `D` (these preserve multiplicity sign, so the output is `Nonneg` iff the input is).
  * `Union`/`Join`/`WcoJoin` inputs: propagate `D` to every input (output `Nonneg` iff all inputs are).
  * leaves (`Constant`/`Get`/`LocalGet`/`Opaque`): no children; satisfy any demand (base relations are non-negative; a `LocalGet` references a binding whose sign, if negated, is carried by an explicit `Negate`, not the `LocalGet`).

Note the barriers: `Reduce`/`TopK`/`Threshold` satisfy a `Nonneg` parent demand by themselves, so their children's demands are set by the rules above (not by propagating the parent `Nonneg`). This is what lets a reduce over a legitimately-signed-then-thresholded input still extract.

---

### Task 1: Demand-parameterized extraction

**Files:**
- Modify: `src/transform/src/eqsat/egraph.rs` — `extract_with` (1427-1478) and `build_rel` (1483-1555).
- Test: inline `#[mz_ore::test]` cases in the `egraph.rs` tests module (find it: `grep -n "mod tests" src/transform/src/eqsat/egraph.rs`), or the eqsat test module where extraction is exercised. Reuse existing e-graph construction helpers used by current extraction tests (`grep -n "extract(" src/transform/src/eqsat/`).

**Interfaces:**
- Produces: `EGraph::extract_with(root, model, memory_first)` keeps its public signature and behavior for negate-free graphs, but now never returns a plan with a `Negate`-rooted (or otherwise signed) subtree directly under a non-linear `Reduce`/`TopK`.
- Consumes: existing `CostModel::cost`, `Cost::cmp_memory_first`/`cmp_time_first`, `build_rel`'s per-variant `Rel` construction.

- [ ] **Step 1: Add a polarity-demand type and the demand rules.** Introduce
  ```rust
  #[derive(Clone, Copy, PartialEq, Eq)]
  enum Demand { Any, Nonneg }
  ```
  near the extraction code. Add a helper that returns, for an `&ENode` and the demand `D` the node must satisfy, the per-child demands (a small `Vec<Demand>` aligned to `node.children()` order, or computed inline in `build_rel`). Encode the Polarity rules above. Add a helper `fn reduce_demands_nonneg_input(node: &ENode) -> bool` returning true for `TopK` and for `Reduce` with at least one aggregate (Distinct = no aggregates returns false). Document the conservative v1 choice in a comment and reference `aggregate_is_input` for the future refinement.

- [ ] **Step 2: Write the failing test.** A test that builds an e-graph where one class `c` has two representatives: a cheap `Negate(...)`-rooted form and a (possibly more expensive) `Nonneg` form, and `c` is the input of a `Reduce` with a non-linear aggregate (e.g. MAX). Assert the extracted plan's reduce input is the `Nonneg` form (no `Negate` at the reduce input). Construct the MAX aggregate and the e-graph with the same helpers existing eqsat tests use. Run it; expect FAIL (current extractor picks the cheap Negate form). If building a non-linear `Reduce` e-node by hand is heavy, drive it through the existing lower path from a hand-built `MirRelationExpr`/`Rel` and assert on the extracted `Rel`.

- [ ] **Step 3: Implement the two-map fixpoint.** In `extract_with`, replace the single `best` map with `best_any: HashMap<Id,(Cost,Rel)>` and `best_nonneg: HashMap<Id,(Cost,Rel)>`. In the per-class/per-node loop, attempt to build the node twice: once for `Demand::Any` (candidate for `best_any`) and once for `Demand::Nonneg` (candidate for `best_nonneg`, skipped when the node is `Negate`). Keep the same cost memoization (`cost_cache` keyed by built `Rel`), the same memory/time comparator, and the same deterministic tie-break (`c == *bc && rel < *br`). Continue the outer fixpoint until neither map changes. Extract the root from `best_any` (the root has no polarity demand from a parent). Keep the existing `expect("root class could not be extracted")` behavior.

- [ ] **Step 4: Implement demand-aware `build_rel`.** Change `build_rel` to `build_rel(&self, node, demand: Demand, best_any, best_nonneg) -> Option<Rel>`. Return `None` immediately if `demand == Nonneg && matches!(node, ENode::Negate { .. })`. For each child, compute its demand via the Step-1 rules and pull from `best_nonneg` when the child demand is `Nonneg`, else from `best_any`; return `None` if the chosen map lacks the child yet (preserves the partial-progress fixpoint semantics). Build the same per-variant `Rel` as today. Keep every existing per-variant comment.

- [ ] **Step 5: Run the test to verify it passes.** `cargo nextest run -p mz-transform <test_name>`. Expect PASS (reduce input is now the Nonneg form).

- [ ] **Step 6: Add guard tests.**
  * A negate-free graph extracts identically to before (pick a current extraction test; assert unchanged output) — proves no regression on the common path.
  * A `Reduce` whose input class has ONLY a `Nonneg` representative extracts that input unchanged.
  * A `Distinct` (Reduce with no aggregates) over a class that has a cheaper signed-but-equal form may still pick `Any` (documents the Distinct carve-out). If this is hard to construct meaningfully, skip and note why in the test module.

- [ ] **Step 7: Full crate green + lint.** `cargo nextest run -p mz-transform`; `cargo fmt`; `cargo clippy -p mz-transform` (no new warnings). The eqsat `.spec` goldens should be UNCHANGED (no negate rules yet); if any changed, investigate before rewriting.

- [ ] **Step 8: Commit.** `transform/eqsat: polarity-aware extraction (demand-parameterized best-of-class)`.

---

### Task 2: Re-enable the negate-join rules + soundness/win tests

**Files:**
- Modify: `src/transform/src/eqsat/rules/relational.rewrite` — replace the omission comment block (111-137) with the two rules plus a short note that the polarity-aware extractor (Task 1) makes them sound, and that the soundness rests on the extractor, not a rule guard.
- Test: a soundness test + a win test in the eqsat test module (find where rules are loaded and a full saturate+extract is run: `grep -rn "saturate\|RuleSet\|optimize_logical\|relational.rewrite" src/transform/src/eqsat/ src/transform/tests/`).

**Interfaces:**
- Consumes: Task 1's polarity-aware `extract_with`.
- Produces: the rule set now contains `distribute_negate_join` and `factor_negate_join`.

- [ ] **Step 1: Write the soundness test (failing without Task 1, must pass now).** Build the lines-114-118 scenario: a `Reduce` with a non-linear aggregate (MAX) over `Join(a, rest)`, in an e-graph/plan where `factor_negate_join` can fire (a `Join(Negate a, rest)` somewhere, or a union that introduces the negate-join merge). Saturate with the full rule set, extract, and assert the extracted plan contains NO `Reduce`/`TopK` whose immediate input is `Negate`-rooted (walk the extracted `Rel`). This is the regression gate for the previously-omitted rules.

- [ ] **Step 2: Add the rules.** Replace the 111-137 omission block:
  ```
  # Negate distributes through a join on its FIRST input. -(join(a, rest)) = join(-a, rest).
  # Sound to merge ONLY because the polarity-aware extractor forbids a Negate-rooted
  # representative directly under a non-linear Reduce/TopK; the soundness lives in the
  # extractor, not a rule guard (see eqsat::egraph polarity-aware extraction).
  rule distribute_negate_join {
      doc "negate(join(a, rest)) = join(negate(a), rest)"
      Negate (Join[e](a, rest...)) => Join[e](Negate a, rest...)
  }
  rule factor_negate_join {
      doc "join(negate(a), rest) = negate(join(a, rest))"
      Join[e](Negate a, rest...) => Negate (Join[e](a, rest...))
  }
  ```
  Keep any still-relevant prose from the old comment about the first-input-only restriction.

- [ ] **Step 3: Run the soundness test — expect PASS.** `cargo nextest run -p mz-transform <soundness_test>`.

- [ ] **Step 4: Write the win test.** A query/plan where pulling the negate out of a join unlocks `union_cancel` (the benefit named at rewrite:126-128: `Union(join(negate(a),rest), join(a,rest))` cancels). Saturate+extract and assert the join/negate/union collapses (e.g. to `Empty` or a simpler form). Confirm it does NOT collapse with the rules removed (sanity).

- [ ] **Step 5: Run the win test — expect PASS.**

- [ ] **Step 6: Full crate green + datadriven goldens.** `cargo nextest run -p mz-transform`; review eqsat `.spec` diffs (now some may change as negate-join fires — confirm benign/equivalent, then `REWRITE=1` and re-review). `cargo fmt`; `cargo clippy -p mz-transform`.

- [ ] **Step 7: Commit.** `transform/eqsat: re-enable negate-join rules (sound under polarity-aware extractor)`.

---

### Task 3: SLT differential + corpus check

**Files:**
- No source changes expected; this task is verification. If a golden genuinely improves (negate-join win) or is unaffected, record it.

- [ ] **Step 1: Run representative logical-plan SLT files one at a time** (shared CockroachDB; `killall sqllogictest` between, exact name): `joins.slt`, `subquery.slt`, `transform/reduction_pushdown.slt` via `bin/sqllogictest --optimized -- PATH`. Diff against committed goldens.

- [ ] **Step 2: Classify diffs.** For each changed file, confirm the change is a benign equivalent or a genuine win (negate-join unlocking a cancellation). Capture the list in the ledger. Do NOT bulk-rewrite goldens; that is the separate pending corpus decision.

- [ ] **Step 3: Confirm ck480 status.** Re-check `column_knowledge.slt:480`. Task 1+2 alone are NOT expected to fix it (ck480 additionally needs const-fact seeding across the Let fragment + a const-col reconstruction/cancel rule and/or set-semantics Union matching). Record whether the polarity extractor changed its plan at all.

- [ ] **Step 4: Record results in the ledger and the project memory.** Note wins, regressions (expected none), and ck480's residual status.

## Self-review checklist (run after writing, before executing)

* Spec coverage: the soundness requirement (no Negate under non-linear reduce) is enforced by Task 1 Step 4 and gated by Task 2 Step 1. The negate-join re-enable is Task 2 Step 2. ✓
* Type consistency: `Demand` used identically in `extract_with` and `build_rel`; `best_any`/`best_nonneg` named consistently. ✓
* Conservatism: v1 treats all aggregate-bearing Reduce + all TopK as nonneg-demanding; Distinct carve-out documented; linear-aggregate refinement deferred with a comment. ✓
* No placeholder steps; each code step shows the construct. ✓

## Out of scope (follow-ups)

* Linear-aggregate refinement (allow `Any` input for all-linear reduces).
* Sharper polarity lattice crediting deep-negate-under-Threshold beyond the barrier rule.
* ck480 full fix (const-fact seeding via `RecAnalysis<ConstCols>` at `letrec_local_facts`, const-col reconstruction rule, set-semantics/order-insensitive Union-arm matching).
