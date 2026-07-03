# SP2b Slice 6e: `absorb_and_or` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Port `absorb_and_or` (inner-set subsumption absorption) into the CombinedLang DSL, behavior-neutral vs the standalone `ScalarEGraph` oracle, via a generic cross-element rest-filter host on the 6c scaffold.

**Architecture:** A new `RestFilter::AbsorbSubsumed { inner }` and `Cond::AbsorbApplies { list, inner }` host a Rust cross-element helper (`rest_filters::absorb_drop_index` / `rest_absorb`) that ports the old engine's `inner_sets` + subsumption search + dropped-extras `could_error` gate. Two rules (`absorb_and`/`absorb_or`) in `scalar.rewrite`. Lean proves the two-valued absorption law (algebraic core).

**Tech Stack:** Rust (`mz-transform`), chumsky (`build/grammar.rs`), codegen (`build/codegen.rs`), Lean 4.

## Global Constraints

- Behavior-neutral vs the `ScalarEGraph` oracle; differential parity `new_combined == old_scalar` is the gate. No `--rewrite` on sqllogictest.
- **The guard is the correctness core:** fire only when every id in the dropped extras `inner-set(Q) \ inner-set(P)` has `could_error == false`. Retained `P` is NOT gated. Reuses the slice-3 `could_error` analysis field, no new analysis or lattice.
- No CI backstop for Lean (image does not build in CI, owner-confirmed — do NOT propose wiring). Aggregate `lake build` (clean rebuild) + two-sided sorry-taxonomy grep are MANDATORY hand-run gates. Permanent sorry count stays **5**; a bump to 6 (Lean fallback) is surfaced to the overseer before any sorry is added, never silent.
- No production-default flag flips. No `ScalarEGraph` deletion (slice 7).
- Scalar rules `colored: false` (build-enforced at codegen.rs:82).
- Any new `MatchGraph` trait method → ALL-IMPLS sweep incl `ColoredView` (slice-3 E0046). `cargo check --tests`, not bare (slice-3 E0502).
- chumsky 26-tuple ceiling: new conds in the existing nested scalar sub-`choice`; sibling nested group if it fills, not a 3rd level.
- Error operands in tests are Bool-typed (`(1/0)=(1/0)`), NEVER bare `Int64` (6c `analysis.rs::merge` ill-typed-collapse trap).
- `bin/fmt` + `cargo check` before every commit. Stage only your files. `doc/developer/generated/` read-only. Regenerate Lean: `cargo run -p mz-transform --example gen-lean`.

---

### Task 1: DSL AST + grammar for `AbsorbSubsumed`, `AbsorbApplies`

**Files:** Modify `src/transform/src/eqsat/dsl.rs`, `src/transform/build/grammar.rs`

**Interfaces produced:** `RestFilter::AbsorbSubsumed { inner: String }`, `Cond::AbsorbApplies { list: String, inner: String }`; grammar `absorb(xs, and|or)` (template list) and `absorb_applies(xs, and|or)` (side condition).

- [ ] **Step 1: AST in `dsl.rs`.** In `enum RestFilter` (added in 6c) add:
```rust
    /// Drop an outer operand whose inner-set (under the dual connective `inner`)
    /// is a proper superset of another operand's, when the dropped extras cannot
    /// error. Inner-set subsumption absorption. Backed by `rest_filters::rest_absorb`.
    AbsorbSubsumed { inner: String },
```
In `enum Cond` add:
```rust
    /// `absorb_applies(list, inner)`: some outer operand in `list` is
    /// subsumption-droppable under dual connective `inner` with error-free dropped
    /// extras. The fire-guard for `absorb_and_or`.
    AbsorbApplies { list: String, inner: String },
```

- [ ] **Step 2: Grammar for `absorb(xs, inner)` in `listtmpl`.** In the `listtmpl` block (grammar.rs ~454, alongside the 6c `dedup`/`drop_scalar_lit` arms), add before the `item` arm:
```rust
            let absorb = kw("absorb")
                .ignore_then(just(Token::LParen))
                .ignore_then(ident())
                .then_ignore(just(Token::Comma))
                .then(variadic_func_kw.clone())
                .then_ignore(just(Token::RParen))
                .map(|(list, inner)| TElem::FilterSplice {
                    list,
                    filter: RestFilter::AbsorbSubsumed { inner },
                });
```
and add `absorb` to the combinator `choice((...))` before `item`. `variadic_func_kw` is a parser accepting the `and`/`or` keyword as a `String` (`"and"`/`"or"`). Check whether one exists (the `Variadic[<kw>]` pattern parses a func keyword — reuse that sub-parser if it is factored out; otherwise add a small `fn variadic_func_kw() -> ...` returning `choice((kw("and").to("and".to_string()), kw("or").to("or".to_string())))`, matching the file's top-level-fn convention for token parsers).

- [ ] **Step 3: Grammar for `absorb_applies(xs, inner)` cond.** In the nested scalar-cond sub-`choice` (grammar.rs ~702-708):
```rust
            kw("absorb_applies")
                .ignore_then(just(Token::LParen))
                .ignore_then(ident())
                .then_ignore(just(Token::Comma))
                .then(variadic_func_kw.clone())
                .then_ignore(just(Token::RParen))
                .map(|(list, inner)| Cond::AbsorbApplies { list, inner }),
```
Confirm the sub-`choice` stays within the 26-tuple ceiling (it had ~19 slots headroom after 6c). No third nesting level.

- [ ] **Step 4: Parser round-trip tests** (mirror the 6c `parses_dedup_splice_with_has_duplicate_id_cond` test). Assert:
`rule r { Variadic[and](xs...) => Variadic[and](absorb(xs, or)) where absorb_applies(xs, or) }`
parses to a `FilterSplice { list: "xs", filter: AbsorbSubsumed { inner: "or" } }` and `Cond::AbsorbApplies { list: "xs", inner: "or" }`.

- [ ] **Step 5: Run parser tests** (the standalone-harness technique the 6c grammar tasks used; grammar tests live under `build.rs`). Crate will NOT fully compile (expected `E0004` non-exhaustive matches in codegen.rs / lean.rs for the new variants — Task 2/4). Confirm the parser tests pass and the variants construct.

- [ ] **Step 6: `bin/fmt`, commit** only `dsl.rs`, `grammar.rs`:
```bash
git commit -m "eqsat dsl: AbsorbSubsumed filter + AbsorbApplies cond + grammar (SP2b Slice 6e, Task 1)"
```

---

### Task 2: `rest_filters` absorb helper + cond backing + codegen emit

**Files:** Modify `src/transform/src/eqsat/rest_filters.rs`, `src/transform/src/eqsat/egraph/view.rs`, `src/transform/src/eqsat/colored/view.rs`, `src/transform/build/codegen.rs`

**Interfaces produced:** `rest_filters::{absorb_drop_index, rest_absorb}`, `MatchGraph::cond_absorb_applies`.

- [ ] **Step 1: Port `inner_sets` + subsumption core into `rest_filters.rs`.** Read the old engine `inner_sets` (`rules.rs:414`) and `absorb_and_or` (`rules.rs:601`) first and mirror them exactly.
```rust
use mz_expr::VariadicFunc;
use crate::eqsat::scalar::node::SNode;

/// One outer operand's inner-set under `inner`: the canonical, sorted, unique ids
/// of the operand's `inner` variadic node, or `{find(operand)}` if it holds none.
/// Ports `scalar::rules::inner_sets` (per operand).
fn inner_set(g: &EGraph, operand: Id, inner: &VariadicFunc) -> Vec<Id> {
    let canon = g.find(operand);
    for node in g.nodes(canon) {
        if let CNode::Scalar(SNode::CallVariadic { func, exprs }) = node {
            if &func == inner {
                let mut ids: Vec<Id> = exprs.iter().map(|&e| g.find(e)).collect();
                ids.sort();
                ids.dedup();
                return ids;
            }
        }
    }
    vec![canon]
}

/// The deterministic drop index for inner-set subsumption absorption, or `None`.
/// Mirrors `scalar::rules::absorb_and_or`'s search: the first operand `Q` (by
/// index) proper-subsumed by some distinct `P` (`inner-set(P) ⊊ inner-set(Q)`)
/// whose dropped extras `inner-set(Q) \ inner-set(P)` are all `could_error == false`.
pub(crate) fn absorb_drop_index(g: &EGraph, ids: &[Id], inner: &VariadicFunc) -> Option<usize> {
    if ids.len() < 2 {
        return None;
    }
    let sets: Vec<Vec<Id>> = ids.iter().map(|&o| inner_set(g, o, inner)).collect();
    for q in 0..sets.len() {
        for p in 0..sets.len() {
            if p == q || sets[p].len() >= sets[q].len() {
                continue;
            }
            if !sets[p].iter().all(|id| sets[q].contains(id)) {
                continue;
            }
            let extras_can_error = sets[q]
                .iter()
                .filter(|id| !sets[p].contains(id))
                .any(|&id| scalar_could_error(g, id));
            if !extras_can_error {
                return Some(q);
            }
        }
    }
    None
}

/// The kept operands after absorption, sorted by id (matching the old engine's
/// `kept.sort()` so extraction order agrees with the oracle). A single remaining
/// operand is left to `and_single`/`or_single` downstream.
pub(crate) fn rest_absorb(g: &EGraph, ids: &[Id], inner: &VariadicFunc) -> Vec<Id> {
    match absorb_drop_index(g, ids, inner) {
        Some(q) => {
            let mut kept: Vec<Id> = ids.iter().copied().enumerate()
                .filter(|(i, _)| *i != q).map(|(_, id)| id).collect();
            kept.sort();
            kept
        }
        None => ids.to_vec(),
    }
}

/// Whether scalar class `id` may error, per the base scalar `could_error` analysis.
fn scalar_could_error(g: &EGraph, id: Id) -> bool {
    g.data().scalar.analysis.get(&g.find(id)).map_or(false, |a| a.could_error)
}
```
Verify the exact paths (`CNode`, `SNode::CallVariadic`, `g.nodes`, `g.find`, `g.data().scalar.analysis`, `could_error`) against `rules.rs:414`, `BaseView::scalar_could_error` (view.rs:138), and `egraph/combined.rs`. Match them. `&func == inner` compares `VariadicFunc` by `PartialEq` (confirm `VariadicFunc: PartialEq`; the old engine uses `&func == inner_func`).

- [ ] **Step 2: `MatchGraph::cond_absorb_applies` + BaseView impl** (view.rs, near `cond_has_duplicate_id`):
```rust
    /// Whether some operand in `ids` is subsumption-droppable under dual connective
    /// `inner` with error-free dropped extras. Backs `Cond::AbsorbApplies`.
    fn cond_absorb_applies(&self, ids: &[Id], inner: &mz_expr::VariadicFunc) -> bool;
```
BaseView:
```rust
    fn cond_absorb_applies(&self, ids: &[Id], inner: &mz_expr::VariadicFunc) -> bool {
        crate::eqsat::rest_filters::absorb_drop_index(self.eg, ids, inner).is_some()
    }
```

- [ ] **Step 3: ColoredView inert impl** (colored/view.rs, near its `cond_has_duplicate_id`):
```rust
    fn cond_absorb_applies(&self, _ids: &[Id], _inner: &mz_expr::VariadicFunc) -> bool {
        false
    }
```

- [ ] **Step 4: Codegen** (build/codegen.rs):
- `cond_is_color_exact`: add `Cond::AbsorbApplies { .. } => false,`.
- Cond emit (near the `HasDuplicateId` arm):
```rust
  Cond::AbsorbApplies { list, inner } => format!(
      "g.cond_absorb_applies(&{}, &{})", m.rest_local(list), variadic_func_value(inner)
  ),
```
- Cond serialization: `Cond::AbsorbApplies { list, inner } => format!("{P}::Cond::AbsorbApplies {{ list: {}, inner: {} }}", s(list), s(inner))`.
- `FilterSplice` emit in `listtmpl_stmts` — extend the 6c `match filter`:
```rust
  RestFilter::AbsorbSubsumed { inner } => format!(
      "crate::eqsat::rest_filters::rest_absorb(g, b.rests.get({list:?}).ok_or_else(|| \"unbound rest metavariable {list}\".to_string())?, &{})",
      variadic_func_value(inner)
  ),
```
- `telem` serialization — extend the 6c `RestFilter` match: `RestFilter::AbsorbSubsumed { inner } => format!("{P}::RestFilter::AbsorbSubsumed {{ inner: {} }}", s(inner))`.

- [ ] **Step 5: `cargo check -p mz-transform --tests` clean + view tests.** (Read the `mz-test` skill first.) No rule uses the new machinery yet, so `COMPILED_RULES`/`SCALAR_COMPILED_RULES` are unchanged. If `lean.rs` `tmpl_list_expr` hits a non-exhaustive match on the new `RestFilter` variant, add the MINIMAL compile-only arm (render as the unfiltered list, matching how 6c's placeholder was handled) with a loud `TODO(Task 4)` — do NOT do the real Lean render here; flag it for Task 4.

- [ ] **Step 6: `bin/fmt`, commit** the five files:
```bash
git commit -m "eqsat: absorb rest-helper (inner-set subsumption + could_error gate) + cond backing (SP2b Slice 6e, Task 2)"
```

---

### Task 3: Port `absorb_and` / `absorb_or` to `scalar.rewrite`

**Files:** Modify `src/transform/src/eqsat/rules/scalar.rewrite`

- [ ] **Step 1: Add the two rules** (after the 6c `and_dedup`/`or_dedup`), matching neighbor formatting:
```
rule absorb_and { doc "And(.., a, .., Or(.., a, ..)) = And(.., a, ..), dropped extras error-free"
    Variadic[and](xs...) => Variadic[and](absorb(xs, or))  where absorb_applies(xs, or) }
rule absorb_or  { doc "Or(.., a, .., And(.., a, ..)) = Or(.., a, ..), dropped extras error-free"
    Variadic[or](xs...)  => Variadic[or](absorb(xs, and)) where absorb_applies(xs, and) }
```

- [ ] **Step 2: `cargo check --tests` + inspect generated rules.** Confirm in the generated `eqsat_rules.rs`:
- `absorb_and` guards on `g.cond_absorb_applies(&<rest>, &VariadicFunc::Or(..))` and its apply calls `rest_filters::rest_absorb(g, .., &VariadicFunc::Or(..))` then builds `CallVariadic { func: And.. }`.
- `absorb_or` uses `And` inner / `Or` outer.
- `SCALAR_COMPILED_RULES` = 24 (was 22); `COMPILED_RULES` = 37 (unchanged, no relational leak). Both `Pat::SVariadic`-rooted → `is_scalar_rule` routes scalar-only.

- [ ] **Step 3: Confirm direction + guard vs old engine.** Re-read `rules.rs:601` and confirm: outer And → inner Or (`switch_and_or`); the guard gates dropped extras' `could_error`, not the retained operand. The DSL `absorb(xs, or)` inner keyword must be the dual of the outer func.

- [ ] **Step 4: Scalar rule unit tests** (`bin/cargo-test -p mz-transform --lib eqsat::scalar`). Green. (The known pre-existing `test_runner.rs` / `eqsat_scalar_corpus` datadriven failure is out of scope.)

- [ ] **Step 5: `bin/fmt`, commit** `scalar.rewrite`:
```bash
git commit -m "eqsat: port absorb_and_or to scalar.rewrite (SP2b Slice 6e, Task 3)"
```

---

### Task 4: Lean — render `AbsorbSubsumed` + prove the absorption law

**Files:** Modify `src/transform/lean/MirRewrite/Semantics.lean`, `src/transform/src/eqsat/lean.rs`, regenerate `Generated.lean`.

- [ ] **Step 1: Replace the Task-2 placeholder** in `lean.rs` `tmpl_list_expr` for `RestFilter::AbsorbSubsumed` with a real render whose fold-equality you can prove (see Step 2). The placeholder (unfiltered render) MUST be gone before the `choose_proof` arm is trusted — otherwise the theorem proves a statement about the wrong list.

- [ ] **Step 2: Prove the two-valued absorption law in `Semantics.lean`.** The rule embodies `P ∧ Q = P` when `inner-set(P) ⊆ inner-set(Q)` (for outer And / inner Or: `Q ⟹ P`... confirm direction: for outer And, dropping `Q = Or(S_Q)` when a `P = Or(S_P)` with `S_P ⊆ S_Q` exists, because `Or(S_P) ⟹ Or(S_Q)` so `Or(S_P) ∧ Or(S_Q) = Or(S_P)`). Model this to whatever syntactic Lean form the render (Step 1) emits, and prove it OUTRIGHT by induction / the `denoteSFold_and_eq_all` + membership machinery from 6c where reusable. Study 6c's `denoteSFold_and_eq_all`/`_or_eq_any`, `dedupById`/`mem_dedupById` (Semantics.lean) as the model.
  - Add a fidelity NOTE (same stance as 6c dedup / isnull_fold): Lean proves the two-valued absorption ALGEBRA; the Rust rule does general inner-set subsumption with a `could_error` gate; the 3VL/error soundness of the guard rests on the Rust analysis + the differential oracle, not this Lean layer.
  - **Permanent sorry count stays 5.** If rendering the general subsumption to a provable Lean form is genuinely intractable, STOP and report DONE_WITH_CONCERNS / BLOCKED describing exactly where it stalls. Do NOT add a `sorry` (which would bump the count to 6) without escalating — that is an overseer decision.

- [ ] **Step 3: `choose_proof` arm** in `lean.rs` for the absorb rules, keyed on the RHS shape (the absorb render), placed so it does not clobber the short-circuit / drop_unit / dedup / de Morgan arms. The `AbsorbApplies` cond emits no hypothesis (the theorem is over the algebra, not the guard); an unused-hypothesis pattern is acceptable.

- [ ] **Step 4: Regenerate + parity.** `cargo run -p mz-transform --example gen-lean`; confirm `Generated.lean` changed only by the two new theorems (`git diff --exit-code` after regen clean = deterministic emitter).

- [ ] **Step 5: Aggregate `lake build` + sorry taxonomy (hand-run).** Clean rebuild green; two-sided grep permanent == 5 (or the flagged 6); `#print axioms` on the two new theorems show no `sorryAx`. Read `ci/test/lean-mir-rewrite.sh` for the checks. If you cannot run Docker/lake, say so explicitly and report what the reviewer must kernel-verify.

- [ ] **Step 6: `bin/fmt`, commit** the three files:
```bash
git commit -m "eqsat lean: prove absorption law + render AbsorbSubsumed (SP2b Slice 6e, Task 4)"
```

---

### Task 5: Differential corpus + parity + guard mutation-test + slice gate

**Files:** Modify the differential parity module (`scalar_saturate.rs`) + the shared corpus, as slice 6c did.

- [ ] **Step 1: Extend the corpus** (mirror `corpus_covers_slice6c`), for both And and Or directions:
- `a` inside the inner call at leading/middle/trailing; both `And(a, Or(a,b))` and `Or(a, And(a,b))`; extra operands (`And([a, Or(a,b), c]) → And([a, c])`); general subset (`AND(a,b) ∨ AND(a,b,c)`).
- Interactions: post-absorb `And([a]) → a`; nested absorption; absorb feeding short-circuit / drop_unit.
- `corpus_covers_slice6e` asserting the corpus actually triggers each new rule.

- [ ] **Step 2: The guard case + mutation test.** Build `And(a, Or(a, err))` with `a` null-typed and `err` a Bool-typed erroring extra (`(1/0)=(1/0)`, NOT bare Int64). Assert it does NOT fire (parity: combined keeps the erroring extra, matching the oracle). Then mutation-test the guard: temporarily disable the `could_error` gate in `absorb_drop_index` (or via a test-only path), confirm parity FAILS exactly on this term, restore. Document the mutation evidence in the report (do not commit the disabled guard).

- [ ] **Step 3: `scalar_parity_slice6e`** — per term, saturate combined + `ScalarEGraph` oracle, extract, assert equal `MirScalarExpr`.

- [ ] **Step 4: `absorb_terminates`** — a term with absorption converges in few iterations (absorb strictly shrinks; guard prevents no-op). Assert iterations << 100.

- [ ] **Step 5: Run the gate.** eqsat lib parity/corpus/termination tests + all prior slice parity green. Then the full relational golden gate, NO `--rewrite`: the slice-6a/6c glob (`success == total == 1187`, 37 files). Expect zero diffs. Aggregate `lake build` re-confirm green + permanent == 5 (or flagged 6).

- [ ] **Step 6: `bin/fmt`, commit** the test/corpus files:
```bash
git commit -m "eqsat: differential parity + guard mutation-test + slice gate for absorb (SP2b Slice 6e, Task 5)"
```

---

## Self-Review Notes (controller)

- **Guard (correctness core):** `could_error` on dropped extras `inner-set(Q) \ inner-set(P)`, not the retained `P`, not `a-not-null`. Confirmed `rules.rs:639-646`.
- **Machinery / surface #2:** cross-element subsumption is a Rust helper on the 6c scaffold, NOT a declarative pair-search. The host generalizes to union-cancel; the per-rule search logic does not (subsumption ≠ inverse-cancel). Report this as the readiness answer.
- **Lean:** two-valued absorption law, guard-vacuous; prove outright, flag before any permanent-sorry bump.
- **Behavior-neutral:** full subsumption (matches oracle), enabling slice-7 retirement of `absorb_and_or`.
