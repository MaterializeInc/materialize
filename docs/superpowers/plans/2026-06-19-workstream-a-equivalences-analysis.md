# Workstream A: equivalences as an e-class analysis implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add an `Equivalences` e-class analysis to the eqsat engine that reuses Materialize's `EquivalenceClasses`, then consume it to canonicalize scalar payloads, collapse unsatisfiable relations to empty, and drop redundant equality predicates.

**Architecture:** The eqsat engine (`src/transform/src/eqsat/`) runs monotone e-class analyses to a fixpoint each saturation round (`run_analysis` in `egraph.rs`). This workstream adds a fourth analysis whose per-e-class lattice element is `Option<EquivalenceClasses>` (Materialize's existing type from `analysis/equivalences.rs`), built by mirroring that module's `derive` per operator. Three consumers read it: a Rust-side canonicalization step that rewrites scalar payloads via `EquivalenceClasses::reducer`, and two DSL-rule conditions (`unsatisfiable`, `equiv`).

**Tech Stack:** Rust, `mz_transform::eqsat`, `mz_transform::analysis::equivalences::EquivalenceClasses`.

## Global Constraints

* No `as`/as_conversions; use `mz_ore::cast::{CastFrom, CastLossy}`.
* No `unsafe` without a `SAFETY` comment.
* No em-dashes or structuring semicolons in comments; doc comment states the contract, reasoning inline.
* No vendor names in user-facing surfaces.
* Never drop existing comments when editing.
* Reuse `EquivalenceClasses` from `src/transform/src/analysis/equivalences.rs` (same crate, crate-private access is fine). Do NOT reimplement equivalence math.
* `cargo fmt` after editing; run `bin/lint` and `cargo clippy -p mz-transform` before any commit.
* `doc/developer/generated/` is read-only.
* The `enable_eqsat_optimizer` flag is defaulted ON, so new rules change plans across the SLT corpus. Do NOT mass-rewrite SLT expected output; verify correctness on targeted files and let flag-on CI surface churn.
* Commit message footer lines:
  ```
  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_016CwwG1wCgqf3v8g6uhf1Nx
  ```

## Soundness invariant (applies to every task)

The analysis is recomputed from scratch each saturation round, so it always reflects current structure (no stale column remapping). Every `make` arm must mirror the corresponding arm of `Equivalences::derive` in `analysis/equivalences.rs` exactly, because that derivation is the proven-correct reference. The e-class `merge` combines facts from two e-nodes that denote the SAME relation, so it is sound to assert all equivalences of both (concat classes + `minimize`); `None` (empty relation) is absorbing. The canonicalization consumer (Task 2) substitutes an expression with a `reducer()`-canonical representative that is equal on every row of the relation the e-class denotes, so it preserves the multiplicity denotation. Each consumer that changes results is a Lean obligation (Task 5).

## Reference: the derivation to mirror

`src/transform/src/analysis/equivalences.rs`, `Equivalences::derive` (lines ~48-267) is the per-operator spec. The eqsat `make` mirrors it, reading payloads off `ENode` and children's `Option<EquivalenceClasses>` via `get`:

* `Constant` / `Opaque` / global `Get`: `Some(EquivalenceClasses::default())` (the eqsat engine bails these to opaque leaves; do not trawl rows).
* `LocalGet { id }`: `self.locals.get(id)` (seeded like the other analyses), else `Some(default)`.
* `Project { input, outputs }`: `get(input)` then `.project(outputs.iter().cloned())`.
* `Map { input, scalars }`: `get(input)`, then for each `(pos, e)` push class `[column(arity(input)+pos), e.expr.clone()]`.
* `Filter { input, predicates }`: `get(input)`, then push one class = `predicates.map(|p| p.expr.clone())` plus `literal_ok(Datum::True, Bool)`.
* `Join { inputs, equivalences }`: start `Some(default)`; for each input at running column offset, `permute` its classes by `offset..offset+arity(input)` and extend; then extend with the join `equivalences` (each `EScalar`'s `.expr`). If any input is `None`, the result is `None`.
* `Reduce { input, group_key, aggregates }`: mirror lines 204-252 (add group-key column classes, `minimize(None)`, `project` to key columns, handle input-passthrough aggregates via `aggregate_is_input`).
* `Negate` / `Threshold` / `TopK`: passthrough `get(input)`.
* `Union { base, inputs }`: `union_many` over child results (intersection of facts).

`EquivalenceClasses` API (all public, line numbers in `equivalences.rs`): `classes: Vec<Vec<MirScalarExpr>>` (pub field), `default()`, `union_many` (959), `project` (1015), `permute` (search `fn permute`), `minimize` (649), `reducer` (1114), `unsatisfiable` (1097).

## File structure

* `src/transform/src/eqsat/analysis.rs` — add the `Equivalences` analysis struct + `LocalFacts` field (Task 1).
* `src/transform/src/eqsat/egraph.rs` — `Analyses` field, `saturate` wiring, the canonicalization step, `check_conds` arms (Tasks 1-4).
* `src/transform/src/eqsat/dsl.rs` — `Cond::Unsatisfiable`, `Cond::Equiv` (Tasks 3-4).
* `src/transform/src/eqsat/parser.rs` — parser branches (Tasks 3-4).
* `src/transform/src/eqsat/rules/relational.rewrite` — new rules (Tasks 3-4).
* `docs/superpowers/specs/2026-06-19-mir-egraph-status.md` — coverage matrix (Task 5).

---

### Task 1: Add the `Equivalences` e-class analysis (inert)

**Files:**
- Modify: `src/transform/src/eqsat/analysis.rs` (new `Equivalences` struct implementing `Analysis`; extend `LocalFacts`)
- Modify: `src/transform/src/eqsat/egraph.rs` (extend `Analyses` struct ~line 877, instantiate in `saturate` ~line 919)
- Test: `src/transform/src/eqsat/analysis.rs` (`#[cfg(test)]` module)

**Interfaces:**
- Produces: `pub struct Equivalences { pub locals: BTreeMap<usize, Option<EquivalenceClasses>> }` implementing `Analysis<Domain = Option<EquivalenceClasses>>`. Tasks 2-4 read `an.eq: HashMap<Id, Option<EquivalenceClasses>>` from `Analyses`.

- [ ] **Step 1: Write the failing tests**

Add a test module to `analysis.rs` that builds small `ENode`s and checks `make`/`merge`. Mirror the existing analysis tests' construction style (read the existing `#[cfg(test)]` block in `analysis.rs` for how `ENode`s and the `get`/`arity` closures are built). Cover:
  * A `Filter[#0 = #1]` over a bottom input yields classes containing `{#0, #1, true}` (the equality predicate becomes an equivalence class).
  * A `Join` of two arity-1 inputs with equivalence `[#0, #1]` yields a class equating the two input columns at the joined offsets.
  * `merge(Some(default), x) == x` and `merge(None, x) == None`.
  * `merge` of `{#0=#1}` and `{#1=#2}` yields a single class `{#0,#1,#2}` after minimize.

(The implementer writes the concrete test bodies against the actual `ENode` constructors and the `Analysis` trait; the four assertions above are the binding acceptance criteria.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p mz-transform --lib eqsat::analysis`
Expected: FAIL to compile (no `Equivalences` type yet).

- [ ] **Step 3: Implement the `Equivalences` analysis**

In `analysis.rs`, add `use crate::analysis::equivalences::EquivalenceClasses;` and `use mz_expr::MirScalarExpr;` (and `Datum`, `ReprScalarType` for the `true` literal). Implement:

```rust
/// Equivalence-class analysis: per e-class, the scalar-expression equivalences
/// known to hold over the relation the class denotes. Reuses Materialize's
/// `EquivalenceClasses` (the same type the production `Equivalences` analysis
/// produces), built here by mirroring that analysis's per-operator derivation.
///
/// `None` means the relation is empty (vacuously all expressions equivalent),
/// the top of the lattice. `Some(default)` means no equivalences are known, the
/// bottom. `merge` combines the facts of two e-nodes that denote the same
/// relation, so it asserts all equivalences of both and re-minimizes.
pub struct Equivalences {
    pub locals: BTreeMap<usize, Option<EquivalenceClasses>>,
}

impl Analysis for Equivalences {
    type Domain = Option<EquivalenceClasses>;

    fn bottom(&self) -> Self::Domain {
        Some(EquivalenceClasses::default())
    }

    fn make(
        &self,
        node: &ENode,
        get: &dyn Fn(Id) -> Self::Domain,
        arity: &dyn Fn(Id) -> usize,
    ) -> Self::Domain {
        // Mirror `Equivalences::derive` in analysis/equivalences.rs, reading
        // scalar payloads off the ENode. See the per-operator reference in the
        // workstream plan. (Implementer fills each arm.)
        // ... arms per the reference list ...
        todo!("per-operator arms mirroring derive")
    }

    fn merge(&self, a: Self::Domain, b: Self::Domain) -> Self::Domain {
        match (a, b) {
            // None = empty relation = absorbing top.
            (None, _) | (_, None) => None,
            (Some(mut a), Some(b)) => {
                a.classes.extend(b.classes);
                a.minimize(None);
                Some(a)
            }
        }
    }
}
```

Implement every `make` arm per the reference list in this plan, mirroring `derive`. Extend `LocalFacts` (analysis.rs ~line 351) with `pub equivalences: BTreeMap<usize, Option<EquivalenceClasses>>` and default it empty in its constructor (recursion-aware seeding via `letrec_local_facts` can stay `Some(default)` for a first cut; note this as a known conservative gap in a comment).

- [ ] **Step 4: Wire it into the engine**

In `egraph.rs`, extend `Analyses` (~line 877) with `eq: HashMap<Id, Option<EquivalenceClasses>>`, and in `saturate` (~line 919) add `eq: self.run_analysis(&Equivalences { locals: locals.equivalences.clone() })` to the `Analyses { .. }` block. Add the necessary `use` for `Equivalences` and `EquivalenceClasses`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p mz-transform --lib eqsat::analysis`
Expected: PASS, including the four new assertions. Also run `cargo test -p mz-transform --lib eqsat` to confirm no existing analysis/saturation test regressed.

- [ ] **Step 6: Commit**

```bash
cargo fmt
bin/lint
cargo clippy -p mz-transform
git add src/transform/src/eqsat/analysis.rs src/transform/src/eqsat/egraph.rs
git commit -m "eqsat: add Equivalences e-class analysis (reuses EquivalenceClasses)

Per-e-class lattice of Option<EquivalenceClasses>, built by mirroring the
production Equivalences::derive per operator. Wired into the saturation
fixpoint; no consumer yet."
```

---

### Task 2: Canonicalize scalar payloads via the equivalences `reducer`

**Files:**
- Modify: `src/transform/src/eqsat/egraph.rs` (a new analysis-driven canonicalization step inside `saturate`)
- Test: `src/transform/src/eqsat/egraph.rs` or the datadriven `eqsat.spec`

**Interfaces:**
- Consumes: `an.eq` from Task 1.

This is the headline consumer and the one that does NOT fit the pattern-to-pattern DSL rule machinery: it rewrites scalar payloads using the per-e-class `reducer()` map. It is a Rust-side saturation step.

- [ ] **Step 1: Read the rule-application loop in `saturate`**

Read `saturate` in `egraph.rs` end to end and find where DSL rules produce new e-nodes and `union` them into the graph each round. The canonicalization step runs in the same loop, after analyses are computed.

- [ ] **Step 2: Write the failing test**

A `Filter[#0 = 5]` over an input that establishes `#0 = #1` (e.g. a join equivalence) should let `#1` be canonicalized: after saturation, the e-class for an expression mentioning `#1` should also contain the form with `#1` replaced by its canonical representative. Concretely, assert end-to-end (via the `optimize`/`optimize_logical` entry or the datadriven harness) that a plan with a redundant non-canonical scalar form is simplified. The implementer picks the tightest expressible assertion; the binding criterion is: a scalar equal to a canonical representative under the e-class equivalences is rewritten to that representative.

- [ ] **Step 3: Implement the canonicalization step**

For each e-class with `Some(ec)` analysis, compute `let r = ec.reducer();`. For each e-node in the class whose scalar payloads (`Filter` predicates, `Map` scalars, `Join` equivalences) contain a subexpression in `r`'s domain, build a new e-node with each payload expression rewritten by applying `r` (use the existing `MirScalarExpr` visit/substitution against the `reducer` map, mirroring how the production `EquivalencePropagation` applies `reducer()`). Add the rewritten e-node to the class via the same `union`/insert path the DSL rules use. Guard with a size/iteration cap consistent with the existing saturation guards so canonicalization cannot loop (a reduced form is structurally bounded by `r`).

If, after reading the engine, this requires more than localized new code (for example a new rewrite-registration mechanism), STOP and report with the specific obstacle so the controller can re-scope (this is the task flagged as highest design-risk).

- [ ] **Step 4: Run tests**

Run: `cargo test -p mz-transform --lib eqsat` and `cargo test -p mz-transform --test test_transforms`
Expected: PASS; the new canonicalization test passes and no existing case regresses (rewrite the `eqsat.spec` expected output with `REWRITE=1` only where the new canonical form is correct).

- [ ] **Step 5: Commit**

```bash
cargo fmt
bin/lint
cargo clippy -p mz-transform
git add -p src/transform/src/eqsat/egraph.rs src/transform/tests/test_transforms/eqsat.spec
git commit -m "eqsat: canonicalize scalar payloads via equivalence reducer

A saturation-time step that rewrites scalar payloads to their
equivalence-canonical representatives, the e-graph form of
EquivalencePropagation."
```

---

### Task 3: `unsatisfiable` condition, collapse to empty

**Files:**
- Modify: `src/transform/src/eqsat/dsl.rs` (`Cond::Unsatisfiable { rel: String }`)
- Modify: `src/transform/src/eqsat/parser.rs` (parse `unsatisfiable(rel)`)
- Modify: `src/transform/src/eqsat/egraph.rs` (`check_conds` arm reading `an.eq`)
- Modify: `src/transform/src/eqsat/rules/relational.rewrite` (rule `r => Empty(r) where unsatisfiable(r)`)
- Test: `src/transform/tests/test_transforms/eqsat.spec`

**Interfaces:**
- Consumes: `an.eq` from Task 1. Mirrors the existing `IsRelEmpty`/empty-propagation rule shape.

- [ ] **Step 1: Write the failing datadriven test**

Add an `eqsat.spec` case: a plan whose equivalences are contradictory (a column forced to two distinct literals, e.g. a join of `Filter[#0 = 1]` and `Filter[#0 = 2]` on the same column) collapses to `Constant <empty>`. Use the existing case (d) (`Filter (false) => Constant <empty>`) as the output-format reference.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p mz-transform --test test_transforms`
Expected: FAIL (the contradiction is not detected; the plan is returned unsimplified).

- [ ] **Step 3: Add the `Cond` variant, parser branch, and evaluation**

In `dsl.rs` add `Unsatisfiable { rel: String }` to `Cond`. In `parser.rs` `parse_cond`, add a branch for `"unsatisfiable"` that parses `unsatisfiable(rel)` (one ident in parens), mirroring the `non_negative`/`monotonic` single-rel branches. In `egraph.rs` `check_conds`, add:

```rust
Cond::Unsatisfiable { rel } => {
    let Some(&id) = b.rels.get(rel) else {
        return false;
    };
    matches!(an.eq.get(&self.find(id)), Some(Some(ec)) if ec.unsatisfiable())
}
```

(`None` analysis already means empty, so it does not need the rule; only `Some(ec)` with `ec.unsatisfiable()` is the new fact.)

- [ ] **Step 4: Add the rule**

In `relational.rewrite`, add a rule in the Phase 5 scalar-structure section that rewrites any relation to an empty constant of the same arity when `unsatisfiable`, mirroring the existing empty-propagation rules (find how `Empty(r)`/arity-carrying empty is written in the existing `empty_false_filter` family and reuse that exact template).

- [ ] **Step 5: Generate expected output and verify**

Run: `REWRITE=1 cargo test -p mz-transform --test test_transforms` then `cargo test -p mz-transform --test test_transforms`
Expected: the new case collapses to `Constant <empty>`; no unrelated case regresses.

- [ ] **Step 6: Commit**

```bash
cargo fmt
bin/lint
cargo clippy -p mz-transform
git add src/transform/src/eqsat/dsl.rs src/transform/src/eqsat/parser.rs src/transform/src/eqsat/egraph.rs src/transform/src/eqsat/rules/relational.rewrite src/transform/tests/test_transforms/eqsat.spec
git commit -m "eqsat: collapse unsatisfiable relations to empty

Add an unsatisfiable(rel) condition backed by the equivalences analysis
and a rule that rewrites a relation with contradictory equivalences to an
empty constant."
```

---

### Task 4: `equiv` condition, drop redundant equality predicate

**Files:**
- Modify: `src/transform/src/eqsat/dsl.rs` (`Cond::Equiv { a: String, b: String, rel: String }`)
- Modify: `src/transform/src/eqsat/parser.rs` (parse `equiv(a, b, rel)`)
- Modify: `src/transform/src/eqsat/egraph.rs` (`check_conds` arm)
- Modify: `src/transform/src/eqsat/rules/relational.rewrite` (redundant-equality rule)
- Test: `src/transform/tests/test_transforms/eqsat.spec`

**Interfaces:**
- Consumes: `an.eq` from Task 1.

- [ ] **Step 1: Write the failing datadriven test**

A `Filter[#a = #b]` whose input already proves `#a = #b` (for example the filter sits above a join whose equivalences force it) is redundant and the predicate is dropped. Add an `eqsat.spec` case asserting the predicate is removed.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p mz-transform --test test_transforms`
Expected: FAIL (predicate retained).

- [ ] **Step 3: Add the `Cond` variant, parser branch, evaluation**

`Cond::Equiv { a, b, rel }` reads two payload metavariables bound to bare column references and a relation metavariable. In `check_conds`:

```rust
Cond::Equiv { a, b, rel } => {
    let (Some(pa), Some(pb), Some(&id)) =
        (b_binds.payloads.get(a), b_binds.payloads.get(b), b_binds.rels.get(rel))
    else {
        return false;
    };
    // Both payloads must be bare columns; compare their canonical reps under
    // the relation's equivalences.
    let (Some(ca), Some(cb)) = (pa.as_single_column(), pb.as_single_column()) else {
        return false;
    };
    match an.eq.get(&self.find(id)) {
        Some(Some(ec)) => {
            let r = ec.reducer();
            let ea = MirScalarExpr::column(ca);
            let eb = MirScalarExpr::column(cb);
            r.get(&ea).unwrap_or(&ea) == r.get(&eb).unwrap_or(&eb)
        }
        Some(None) => true, // empty relation: vacuously equivalent
        None => false,
    }
}
```

(The implementer adapts `b_binds`/payload accessor names to the real binding API used by the other arms, for example the `Payload::columns()` helper used by `IsUniqueKey`.)

In `parser.rs`, add an `"equiv"` branch parsing `equiv(a, b, rel)` (two payload idents and a rel ident), mirroring `is_unique_key` which already parses a payload-and-rel pair.

- [ ] **Step 4: Add the rule**

In `relational.rewrite`, add a rule that drops a single-equality filter when the input proves the equivalence. Express it against the input relation (the equivalence must hold on the filter's input for the drop to be valid). Mirror the structure of the existing `push_filter_*` and `drop_true_filter` rules.

- [ ] **Step 5: Generate expected output and verify**

Run: `REWRITE=1 cargo test -p mz-transform --test test_transforms` then `cargo test -p mz-transform --test test_transforms`
Expected: the redundant predicate is dropped; no unrelated case regresses.

- [ ] **Step 6: Commit**

```bash
cargo fmt
bin/lint
cargo clippy -p mz-transform
git add src/transform/src/eqsat/dsl.rs src/transform/src/eqsat/parser.rs src/transform/src/eqsat/egraph.rs src/transform/src/eqsat/rules/relational.rewrite src/transform/tests/test_transforms/eqsat.spec
git commit -m "eqsat: drop redundant equality predicates via equivalences

Add an equiv(a, b, rel) condition and a rule that removes a filter
equality predicate already implied by the input's equivalences."
```

---

### Task 5: Docs, coverage matrix, Lean obligations, measure

**Files:**
- Modify: `docs/superpowers/specs/2026-06-19-mir-egraph-status.md`

**Interfaces:**
- Consumes: Tasks 1-4. Documentation + measurement only.

- [ ] **Step 1: Update the coverage matrix**

In the STATUS doc: move `EquivalencePropagation` from Partial/Missing to Covered (mechanism: the `Equivalences` e-class analysis plus the canonicalization step and the `unsatisfiable`/`equiv` rules). Note that `RedundantJoin`, `NonNullRequirements`, and demand-driven passes remain Missing (this workstream does equivalences only). Keep claims evidence-based.

- [ ] **Step 2: Record the Lean obligations**

Add bullets to "What is left": (a) the canonicalization consumer preserves the multiplicity denotation because the substituted representative is row-equal under the e-class equivalences; (b) `unsatisfiable => Empty` is sound because contradictory equivalences imply no row satisfies the relation. Reference the existing Lean spec location.

- [ ] **Step 3: Measure**

Run: `cargo test -p mz-transform --test compare_real -- --nocapture` and record the actual win/loss/tie counts in the STATUS doc. Record the truth whatever it is (the 4 known losses are empty-propagation gaps; equivalence-driven `unsatisfiable` may close some). If counts do not move, say so and do not massage them.

- [ ] **Step 4: Commit**

```bash
cargo fmt
bin/lint
git add docs/superpowers/specs/2026-06-19-mir-egraph-status.md
git commit -m "eqsat: document equivalences analysis coverage and Lean obligations"
```
