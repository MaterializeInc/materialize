# Eqsat projection pull-up rule implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an eqsat DSL rule that pulls a projection out of a binary-join input, so the join input becomes a bare index-backed `Get` that `JoinImplementation` can turn into a free delta join, restoring the two LDBC BI Q20 delta joins eqsat currently demotes to differential.

**Architecture:** A new `pull_project_out_of_join_first` rewrite rule in `relational.rewrite` rewrites `Join[e](Project[o] a, b)` to `Project[m](Join[remap(e,m)](a, b))` with `m = concat(o, shift(iota(arity(b)), arity(a)))`.
The equivalence remap reuses the existing `Remap` primitive (which already lifts a column map over an equivalences payload via `map_payload_cols`).
Two new side conditions guard firing: `non_identity_projection` (avoid no-ops) and `reads_indexed_global` (only fire where exposing the bare `Get` unlocks a maintained index), the latter requiring the index-availability map to be threaded onto the `EGraph` so conditions can read it.

**Tech Stack:** Rust, the in-tree eqsat e-graph optimizer (`src/transform/src/eqsat`), the chumsky-based rule DSL parser + per-rule codegen (`src/transform/build`), sqllogictest goldens.

## Global Constraints

* The base branch is `upstream/main`. Work stays on branch `claude/mir-equality-optimizer-sodbej` in the worktree `/home/moritz/dev/repos/materialize/.claude/worktrees/mir-equality-optimizer`.
* Keep the eqsat flag ON; debug correctness regressions immediately while fresh.
* Eqsat's join policy is eager delta joins (`enable_eager_delta_joins`); do not change it.
* Never use `as` conversions in Rust; use `mz_ore::cast::CastFrom`/`CastLossy` (mirror the existing `usize::cast_from(u64::try_from(..)?)` idiom already in `matcher.rs`).
* No em-dashes or structuring semicolons in comments. Doc comment states the contract; reasoning lives inline at the decision point. No vendor names in user-facing surfaces.
* Never drop existing comments when editing.
* Run `cargo fmt`, `bin/lint`, and `cargo clippy` before any commit; a change is clean only when no unexpected warnings remain.
* Tests: unit tests run with `cargo test -p mz-transform <name>` (fall back to `bin/cargo-test -p mz-transform <name>` if it complains about `METADATA_BACKEND_URL`). Sqllogictest goldens run with `bin/sqllogictest --optimized -- <path>`; rewrite with `bin/sqllogictest -- --rewrite-results <path>`.
* **The rule is necessary but may not be sufficient.** Extraction must *prefer* the pulled-up variant, not merely generate it. Investigation (this plan, Task 4) shows the cost model already rewards the bare index-backed `Get` via oracle suppression (`cost.rs::input_already_arranged`, which matches only a bare `Get`, charges the pushed-down `Project(Get)` form but zeroes the pulled-up bare-`Get` form). Task 4 measures whether that suffices. Task 5 is a contingency that only runs if measurement shows extraction does not pick the variant; it must use the principled oracle/availability lever, never an ad-hoc width-penalty term.

---

## File structure

* `src/transform/src/eqsat/egraph.rs` — add an `available` field to `EGraph`, a setter, and the `cond_reads_indexed_global` e-class walk.
* `src/transform/src/eqsat/cost.rs` — add a `pub(crate)` getter exposing `CostModel`'s existing `available` map.
* `src/transform/src/eqsat/engine.rs` — set `eg.available` from `self.model` at the four saturate sites.
* `src/transform/src/eqsat/dsl.rs` — add `Cond::NonIdentityProjection` and `Cond::ReadsIndexedGlobal` variants.
* `src/transform/build/grammar.rs` — parse `non_identity_projection(payload, rel)` and `reads_indexed_global(rel)`.
* `src/transform/build/codegen.rs` — emit guard expressions for the two new conditions.
* `src/transform/src/eqsat/rules/relational.rewrite` — add the `pull_project_out_of_join_first` rule.
* `test/sqllogictest/ldbc_bi_physical.slt` — golden, rewritten and verified in Task 4.

---

### Task 1: Thread index availability onto the EGraph and add `cond_reads_indexed_global`

**Files:**
- Modify: `src/transform/src/eqsat/cost.rs` (add getter near `with_available`, around line 266)
- Modify: `src/transform/src/eqsat/egraph.rs` (struct at line 362; new method near other `cond_*` methods around line 1100)
- Modify: `src/transform/src/eqsat/engine.rs` (four `let mut eg = EGraph::new();` sites)
- Test: `src/transform/src/eqsat/egraph.rs` (`#[cfg(test)]` module)

**Interfaces:**
- Produces: `EGraph::set_available(&mut self, available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>)`; `EGraph::cond_reads_indexed_global(&self, id: Id) -> bool`; `CostModel::available(&self) -> &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>`.
- Consumes: existing `EGraph.classes: HashMap<Id, HashSet<ENode>>`, `EGraph::find`, `ENode::{Opaque, Project, Filter, Map, ArrangeBy, ArrangeByMany}` (unboxed `Opaque(MirRelationExpr)`).

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)]` module in `egraph.rs` (use the crate's `#[mz_ore::test]` attribute, as other tests there do). Check imports: `mz_expr::{Id, MirRelationExpr}`, `mz_repr::GlobalId`, `std::collections::BTreeMap` are needed; reuse the helpers other egraph tests already use to build `Rel`/`ENode` and consult an existing test in this module for the exact constructor calls.

```rust
#[mz_ore::test]
fn reads_indexed_global_sees_through_project_only_when_indexed() {
    use mz_expr::{Id, MirRelationExpr};
    use mz_repr::{GlobalId, RelationType};

    // An Opaque global Get on gid 7.
    let get = MirRelationExpr::Get {
        id: Id::Global(GlobalId::User(7)),
        typ: RelationType::empty(),
        access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
    };

    // Build: Project over the Opaque global Get.
    let mut eg = EGraph::new();
    let get_id = eg.add(ENode::Opaque(get.clone()));
    let proj_id = eg.add(ENode::Project {
        input: get_id,
        outputs: vec![0],
    });

    // No availability: false.
    assert!(!eg.cond_reads_indexed_global(proj_id));

    // gid 7 indexed: true, seen through the Project wrapper.
    let mut available: BTreeMap<GlobalId, Vec<Vec<mz_expr::MirScalarExpr>>> = BTreeMap::new();
    available.insert(GlobalId::User(7), vec![vec![mz_expr::MirScalarExpr::column(0)]]);
    eg.set_available(available);
    assert!(eg.cond_reads_indexed_global(proj_id));
    assert!(eg.cond_reads_indexed_global(get_id));
}
```

Note: the exact field names of `MirRelationExpr::Get` and `ENode::Project` must match the in-tree definitions. Before writing, open `egraph.rs` lines 109-188 for `ENode` field names and grep `MirRelationExpr::Get {` in the crate for the current `Get` fields (the `access_strategy`/`typ` field set has changed over time). Adjust the constructor to compile.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p mz-transform reads_indexed_global_sees_through_project_only_when_indexed`
Expected: FAIL to compile with "no method named `set_available`" / "no method named `cond_reads_indexed_global`".

- [ ] **Step 3: Add the `available` field, setter, and getter**

In `egraph.rs`, extend the struct (line 362) and imports. Add to the top-of-file imports if absent: `use std::collections::BTreeMap; use mz_expr::MirScalarExpr; use mz_repr::GlobalId;` (grep first; several are likely already imported).

```rust
pub struct EGraph {
    uf: Vec<Id>,
    pub(crate) classes: HashMap<Id, HashSet<ENode>>,
    memo: HashMap<ENode, Id>,
    /// Index availability for `reads_indexed_global`: each global id maps to its
    /// maintained index keys. Empty in the logical pass; the physical pass sets
    /// it from the oracle so the pull-up rule fires only where exposing the bare
    /// `Get` unlocks an existing index.
    available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
}
```

`EGraph` derives `Default`; confirm the derive still holds (`BTreeMap` is `Default`). Add the setter in the `impl EGraph` block near `new`:

```rust
    /// Set the index-availability map read by `cond_reads_indexed_global`.
    pub fn set_available(&mut self, available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>) {
        self.available = available;
    }
```

In `cost.rs`, add next to `with_available` (after line 272):

```rust
    /// The index-availability map, so callers building a saturation e-graph can
    /// mirror it onto the e-graph for `reads_indexed_global`.
    pub(crate) fn available(&self) -> &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>> {
        &self.available
    }
```

- [ ] **Step 4: Add `cond_reads_indexed_global`**

In `egraph.rs`, add to the `impl EGraph` block that holds the other `cond_*` methods (near line 1100), mirroring `cse::reads_index_backed_get` but over e-classes:

```rust
    /// `reads_indexed_global`: the relation, after stripping column- and
    /// row-preserving wrappers (`Project`/`Filter`/`Map`/`ArrangeBy`/
    /// `ArrangeByMany`), reaches an `Opaque` global `Get` covered by a
    /// maintained index. Mirror of `cse::reads_index_backed_get`, but it walks
    /// e-classes (a class may hold several nodes) with a visited set to bound
    /// cyclic congruence. Gates the pull-up rule so it fires only where exposing
    /// the bare `Get` unlocks an existing index.
    pub(crate) fn cond_reads_indexed_global(&self, id: Id) -> bool {
        let mut stack = vec![self.find(id)];
        let mut seen = HashSet::new();
        while let Some(cls) = stack.pop() {
            if !seen.insert(cls) {
                continue;
            }
            let Some(nodes) = self.classes.get(&cls) else {
                continue;
            };
            for n in nodes {
                match n {
                    ENode::Opaque(m) => {
                        if let MirRelationExpr::Get {
                            id: mz_expr::Id::Global(gid),
                            ..
                        } = m
                        {
                            if self.available.get(gid).is_some_and(|keys| !keys.is_empty()) {
                                return true;
                            }
                        }
                    }
                    ENode::Project { input, .. }
                    | ENode::Filter { input, .. }
                    | ENode::Map { input, .. }
                    | ENode::ArrangeBy { input, .. }
                    | ENode::ArrangeByMany { input, .. } => stack.push(self.find(*input)),
                    _ => {}
                }
            }
        }
        false
    }
```

Confirm `HashSet` and `MirRelationExpr` are in scope in `egraph.rs` (both are used elsewhere in the file; add `use` only if a build error says otherwise).

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p mz-transform reads_indexed_global_sees_through_project_only_when_indexed`
Expected: PASS.

- [ ] **Step 6: Wire `available` onto the e-graph at the saturate sites**

In `engine.rs`, after each `let mut eg = EGraph::new();` immediately preceding a `saturate` call, set availability from the model. Locate them with `grep -n "EGraph::new()" src/transform/src/eqsat/engine.rs` (expected four: in `optimize_node_with_alt`, `optimize_node`, `optimize_body_with_let_union`, `optimize_around_scopes`). After each, insert:

```rust
        eg.set_available(self.model.available().clone());
```

Place it after `EGraph::new()` and before `add_rel`/`saturate`. The availability map is small (a handful of global ids), so cloning per fragment is negligible.

- [ ] **Step 7: Verify the crate builds and the eqsat unit tests pass**

Run: `cargo test -p mz-transform eqsat::`
Expected: PASS (no regressions; the new test passes).

- [ ] **Step 8: Format, lint, commit**

```bash
cd /home/moritz/dev/repos/materialize/.claude/worktrees/mir-equality-optimizer
cargo fmt
bin/lint
git add src/transform/src/eqsat/egraph.rs src/transform/src/eqsat/cost.rs src/transform/src/eqsat/engine.rs
git commit -m "eqsat: thread index availability onto EGraph for reads_indexed_global"
```

---

### Task 2: Add `non_identity_projection` and `reads_indexed_global` DSL conditions

**Files:**
- Modify: `src/transform/src/eqsat/dsl.rs` (`Cond` enum, around line 322)
- Modify: `src/transform/build/grammar.rs` (cond parser, around lines 526-571)
- Modify: `src/transform/build/codegen.rs` (`cond_expr`, around lines 299-347)
- Test: `src/transform/src/eqsat/rules.rs` (the `compiled_and_ast_agree` validation already there) plus a focused parse assertion if the DSL has a parser unit-test seam.

**Interfaces:**
- Consumes: existing free fn `cond_identity_projection(p: &Payload, rel_arity: usize) -> bool` (egraph.rs:994); `EGraph::cond_reads_indexed_global` from Task 1; the `two_idents`/`one_ident` grammar helpers (grammar.rs:515-525); `Matcher::{pl_local, rel_local}` and `eg.arity(..)` in codegen.
- Produces: `Cond::NonIdentityProjection { payload: String, rel: String }`, `Cond::ReadsIndexedGlobal { rel: String }`, both parseable and codegen-able.

- [ ] **Step 1: Add the `Cond` variants**

In `dsl.rs`, add inside `enum Cond` (after `IdentityProjection`, line 322), keeping the doc-comment-states-contract style of the surrounding variants:

```rust
    /// `non_identity_projection(payload, rel)`: the projection payload is *not*
    /// exactly `[0, 1, ..., arity(rel)-1]`. The negation of `identity_projection`.
    /// Guards `pull_project_out_of_join_first` so it does not fire on a no-op
    /// projection (which would add nothing and risk a churn loop with
    /// `drop_identity_project`).
    NonIdentityProjection { payload: String, rel: String },
    /// `reads_indexed_global(rel)`: the bound relation, after stripping
    /// column- and row-preserving wrappers, reaches an opaque global `Get`
    /// covered by a maintained index. Restricts `pull_project_out_of_join_first`
    /// to join inputs where pulling the projection up exposes a reusable index,
    /// the only case with a payoff; bounds e-graph blowup elsewhere.
    ReadsIndexedGlobal { rel: String },
```

- [ ] **Step 2: Parse the new conditions**

In `grammar.rs`, add two entries to the `cond` `choice((...))` tuple (after the `has_inner_equiv` entry, line 570). `choice` over a tuple supports well beyond the current count, so appending is fine; if the build complains about tuple arity, split into a nested `choice((choice((..)), choice((..))))`.

```rust
        two_idents("non_identity_projection")
            .map(|(payload, rel)| Cond::NonIdentityProjection { payload, rel }),
        one_ident("reads_indexed_global").map(|rel| Cond::ReadsIndexedGlobal { rel }),
```

- [ ] **Step 3: Emit guard expressions in codegen**

In `codegen.rs`, add two arms to `cond_expr` (after the `IdentityProjection` arm, line 345). `non_identity_projection` is the boolean negation of the existing free function; `reads_indexed_global` calls the Task 1 method on `eg`:

```rust
        Cond::NonIdentityProjection { payload, rel } => format!(
            "!cond_identity_projection(&{}, eg.arity({}))",
            m.pl_local(payload),
            m.rel_local(rel)
        ),
        Cond::ReadsIndexedGlobal { rel } => {
            format!("eg.cond_reads_indexed_global({})", m.rel_local(rel))
        }
```

- [ ] **Step 4: Run the rule-compilation validation**

Run: `cargo test -p mz-transform compiled_and_ast_agree`
Expected: PASS. This rebuilds the generated matchers (the `build.rs` reparses `relational.rewrite` and regenerates code) and confirms compiled and AST backends agree. A grammar or codegen mistake surfaces as a build error here.

If no rule uses the new conds yet, this only proves they compile in isolation; the firing behavior is covered in Task 3.

- [ ] **Step 5: Format, lint, commit**

```bash
cargo fmt
bin/lint
git add src/transform/src/eqsat/dsl.rs src/transform/build/grammar.rs src/transform/build/codegen.rs
git commit -m "eqsat: add non_identity_projection and reads_indexed_global DSL conditions"
```

---

### Task 3: Add the `pull_project_out_of_join_first` rule

**Files:**
- Modify: `src/transform/src/eqsat/rules/relational.rewrite` (add the rule near the other join rules, e.g. after `commute_binary_join`, line 373)
- Test: `src/transform/src/eqsat/` — add a focused saturation test. Find the module that already builds an `EGraph`, saturates with the compiled ruleset, and inspects the root class (grep `saturate(` under `src/eqsat` tests, e.g. in `engine.rs` or a dedicated rules test module) and follow its construction pattern.

**Interfaces:**
- Consumes: existing PExpr primitives `Concat`, `Shift`, `Iota`, `Remap`, `IxExpr::Arity`; existing cond `is_binary_join`; new conds `non_identity_projection`, `reads_indexed_global` from Task 2. `Remap(payload, outs)` evaluates as `remap_payload(payload, &outs.into_outputs())`, and `remap_payload` (matcher.rs:184) calls `map_payload_cols`, which already maps columns across an `Equivalences` payload (matcher.rs:164-170). So `remap(e, m)` applies the column map `m` to every scalar in every equivalence class. No new primitive is needed (the spec's `remap_equivs` is redundant with `Remap`).
- Produces: the e-graph union of `Join[e](Project[o] a, b)` with `Project[m](Join[remap(e,m)](a, b))`, both extractable.

- [ ] **Step 1: Write the failing saturation test**

Add a test that builds `Join(Project[narrow](Opaque global Get, indexed), b)`, saturates, and asserts the pulled-up form appears in the join's e-class. Model it on the nearest existing saturate-and-inspect test. Skeleton (adapt constructors to the in-tree `Rel`/builders):

```rust
#[mz_ore::test]
fn pull_project_out_of_join_first_fires() {
    // a = Project[#1, #2](Opaque global Get gid=7, arity 3), indexed on #1.
    // b = some arity-2 relation. Join on a.#1 = b.#0.
    // After saturation the root join's class must contain a Project over a Join
    // whose first input is the bare Opaque Get (arity 3), not the narrowed one.
    // Build the Rel, add_rel into an EGraph, set_available({7: [[col(1)]]}),
    // saturate with the compiled ruleset, then assert the root class holds a
    // Project node whose child class holds a Join whose first input class holds
    // the bare Opaque Get.
    // (Fill in with the exact builders used by the neighboring saturate test.)
}
```

Because the precise builder API is local to the crate, the implementer should first read one existing saturate test in full and copy its setup verbatim, changing only the plan shape. The assertion walks `eg.classes` for the root join and checks for the `Project -> Join(bare Opaque Get, _)` shape.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p mz-transform pull_project_out_of_join_first_fires`
Expected: FAIL (the assertion finds no pulled-up form, because the rule does not exist yet).

- [ ] **Step 3: Add the rule**

In `relational.rewrite`, after `commute_binary_join`:

```
rule pull_project_out_of_join_first {
    doc "join(project(o, a), b) = project(m, join(a, b)),  m = o ++ shift(iota(|b|), |a|)"
    Join[e](Project[o] a, b)
        => Project[concat(o, shift(iota(arity(b)), arity(a)))](
               Join[remap(e, concat(o, shift(iota(arity(b)), arity(a))))](a, b))
    where is_binary_join()
    where non_identity_projection(o, a)
    where reads_indexed_global(a)
}
```

Notes for the implementer:
* `m = concat(o, shift(iota(arity(b)), arity(a)))` is the shared old-to-new column map. `iota(arity(b))` is `[0..arity(b))`; `shift(_, arity(a))` makes it `[arity(a)..arity(a)+arity(b))` (the new positions of `b`'s columns); `concat(o, _)` prepends `o` (the underlying columns of `a`'s projected positions). Confirm `shift` applies to a projection payload by checking `shift_payload` (matcher.rs:178, it maps every column index `c -> c + k`); `iota` produces an `Outputs` payload, so `shift(iota(..), ..)` is `[arity(a)..]`. This matches `swap_projection`'s construction style.
* As `Project[m]` it recovers the original output: position `i` reads new join column `m[i]`. As `remap(e, m)` it renumbers every equivalence column `c` to its new position `m[c]`. `m` has length `|o| + arity(b)` = old join output arity, so every equivalence column (in old output space) indexes into `m` in range; no out-of-range `-1` is produced.
* `is_binary_join()` reads the root e-class, so the rule's LHS root must be the `Join`. It is.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p mz-transform pull_project_out_of_join_first_fires`
Expected: PASS.

- [ ] **Step 5: Run the full eqsat unit suite for regressions**

Run: `cargo test -p mz-transform eqsat::`
Expected: PASS. Watch in particular for any `validation`/roundtrip test that asserts equivalence of pre/post plans, and for saturation-time-sensitive tests.

- [ ] **Step 6: Format, lint, commit**

```bash
cargo fmt
bin/lint
git add src/transform/src/eqsat/rules/relational.rewrite src/transform/src/eqsat/
git commit -m "eqsat: add pull_project_out_of_join_first rule"
```

---

### Task 4: Measure the Q20 goldens and decide on cost-model work

**Files:**
- Modify (rewrite): `test/sqllogictest/ldbc_bi_physical.slt`
- Reference: the physical-plan EXPLAIN blocks for the two Q20 joins (production had them as `Join::Delta`; committed eqsat demoted them to differential, giving 35 delta / 125 linear vs production 37 / 123).

**Interfaces:**
- Consumes: the rule from Task 3, active in the physical pass.
- Produces: a verdict — either the two joins are back to `Join::Delta` (the rule is sufficient and Task 5 is skipped), or they are not (Task 5 runs).

- [ ] **Step 1: Capture the pre-change golden state**

Run: `git -C /home/moritz/dev/repos/materialize/.claude/worktrees/mir-equality-optimizer stash list` to confirm a clean tree, then run the golden read-only first:

Run: `bin/sqllogictest --optimized -- test/sqllogictest/ldbc_bi_physical.slt`
Expected: the file is currently committed at eqsat=35/125, so this should PASS against the committed golden. Record the current delta/linear counts by grepping the EXPLAIN output for `Join::Delta` and `Join::Linear` in the two Q20 query blocks (the myForums query and the srcs/dsts query identified in `project_egraph_delta_to_differential_regression`).

- [ ] **Step 2: Rewrite the golden with the rule active**

Run: `bin/sqllogictest -- --rewrite-results test/sqllogictest/ldbc_bi_physical.slt`

- [ ] **Step 3: Inspect the diff for the two Q20 joins**

```bash
git -C /home/moritz/dev/repos/materialize/.claude/worktrees/mir-equality-optimizer diff test/sqllogictest/ldbc_bi_physical.slt
```

Expected, if the rule is sufficient: the two Q20 join nodes change from `Join::Differential` to `Join::Delta`, and the join inputs read the bare index-backed `Get` (`PassArrangements`) rather than `Project(#1,#2)(...)`. Total delta count rises toward production's 37 (it may differ under the eager-delta policy; that is acceptable per the spec as long as arrangement count does not grow).

- [ ] **Step 4: Record the verdict and branch**

* **If both joins are now `Join::Delta`:** the rule is sufficient. Keep the rewritten golden, verify no *other* query in the file regressed (scan the full diff; unrelated plan churn must be explained or reverted), then proceed to Step 5. Skip Task 5.
* **If the joins are still differential:** revert the golden rewrite is NOT needed (it still must reflect reality), but the goal is unmet. Commit the measurement state, then run Task 5 to diagnose why extraction does not pick the pulled-up variant.

- [ ] **Step 5: Run the blast-radius goldens**

Run, and confirm pass/fail status is unchanged from before this branch's work:
```bash
bin/sqllogictest --optimized -- test/sqllogictest/tpch_select.slt
bin/sqllogictest --optimized -- test/sqllogictest/ldbc_bi.slt
```
For `catalog_server_explain.slt`, if it drifts, regenerate it fully rather than hand-patching row counts (per repo convention).

- [ ] **Step 6: Commit the golden update**

```bash
cargo fmt
bin/lint
git add test/sqllogictest/ldbc_bi_physical.slt
git commit -m "eqsat: restore LDBC BI Q20 delta joins via projection pull-up (golden)"
```

---

### Task 5 (contingent — only if Task 4 shows the joins stay differential): targeted cost-model fix

**Run this task only if Task 4 Step 4 took the "still differential" branch.** If Task 4 confirmed delta restoration, do not run Task 5; instead record in the progress ledger that the cost model already preferred the variant and no cost change was needed.

**Files:**
- Modify: `src/transform/src/eqsat/cost.rs` and/or `src/transform/src/eqsat/extract.rs`, depending on diagnosis.
- Test: a focused cost unit test asserting the pulled-up variant scores cheaper than the pushed-down variant for an indexed input.

**Interfaces:**
- Consumes: `CostModel::cost`, `input_already_arranged` (cost.rs:545), the extractor/objective.

- [ ] **Step 1: Diagnose why extraction does not prefer the pulled-up form**

Write a unit test in `cost.rs`'s test module that constructs both forms over an indexed global `Get` and prints `model.cost(&pulled_up)` vs `model.cost(&pushed_down)` using a model built with `CostModel::with_available(...)`. Determine which axis ties or inverts. Likely causes, in order of probability:
  1. The pulled-up input's join key (derived from `join_key_cols_for_input`) does not exactly equal an available index key (e.g. the binary join keys on a single column but the only index is composite), so `input_already_arranged` does not suppress the term. Fix: confirm the Q20 indexes; if a single-column index exists, suppression should fire, so re-check `join_key_cols_for_input` on the remapped equivalences.
  2. Memory ties and the node-count tie-break favors the smaller pushed-down form. Fix: only relevant if memory genuinely ties, which contradicts the suppression asymmetry; if so, the suppression is not firing (case 1).

- [ ] **Step 2: Apply the principled fix**

The lever is the existing oracle/availability suppression, not a new heuristic penalty. Do NOT add a width-penalty time term (an earlier peak-degree-style heuristic was a logged bug; see `project_egraph_cost_model`). Make the minimal change that lets `input_already_arranged` recognize the pulled-up bare-`Get` input's key as index-covered. Write the failing cost test first, then the fix, then confirm.

- [ ] **Step 3: Re-run Task 4's measurement**

Re-run `bin/sqllogictest -- --rewrite-results test/sqllogictest/ldbc_bi_physical.slt` and confirm the two Q20 joins are now `Join::Delta`. Re-run the blast-radius goldens (Task 4 Step 5).

- [ ] **Step 4: Format, lint, commit**

```bash
cargo fmt
bin/lint
cargo clippy -p mz-transform
git add src/transform/src/eqsat/cost.rs src/transform/src/eqsat/extract.rs test/sqllogictest/ldbc_bi_physical.slt
git commit -m "eqsat: value index-covered pulled-up join input so extraction prefers free delta"
```

---

## Self-review

**Spec coverage:**
* Pull-up rule (binary case) — Task 3. ✓
* Shared map `m = concat(o, shift(iota(arity(b)), arity(a)))` for both projection and equivalence remap — Task 3. ✓
* `remap_equivs` primitive — **deliberately dropped**; `Remap` already lifts over equivalences via `map_payload_cols`. Documented in Task 3 interfaces. ✓
* `reads_indexed_global` condition + threading `available` — Tasks 1, 2. ✓
* `non_identity_projection` condition — Task 2. ✓
* `Shift` over a projection payload verification — Task 3 Step 3 note (verified against `shift_payload`). ✓
* Blowup control (the three guards) — Task 3 rule has all three; `reads_indexed_global` from Task 1/2. ✓
* Cost-model dependency — Tasks 4 (measure) and 5 (contingent fix). The plan corrects the spec's premise: the cost model already rewards the bare index-backed `Get`, so the cost change is gated on measurement, not assumed. ✓
* Verification (goldens, blast radius, saturation time, unit tests) — Tasks 3 Step 5, 4. ✓
* Bidirectional `<=>` framing, n-ary/Map/Filter generalizations — explicitly out of scope (spec marks them follow-up); not planned. ✓

**Placeholder scan:** Task 1 and Task 3 tests contain "adapt constructors to the in-tree builders" guidance rather than fully compiling test bodies, because the local `Rel`/`ENode` builder API is not fully captured here; each names the exact neighboring test to copy. This is a deliberate, bounded instruction, not a TBD — the implementer reads one existing test and mirrors it. No "implement later" or vague error-handling placeholders remain.

**Type consistency:** `set_available`/`available()`/`cond_reads_indexed_global` signatures match across Tasks 1-3. `Cond::NonIdentityProjection { payload, rel }` and `Cond::ReadsIndexedGlobal { rel }` field names match between dsl.rs (Task 2 Step 1), grammar (Step 2), and codegen (Step 3). The rule (Task 3) uses `non_identity_projection(o, a)` and `reads_indexed_global(a)` matching the parser's `two_idents`/`one_ident` shapes.
