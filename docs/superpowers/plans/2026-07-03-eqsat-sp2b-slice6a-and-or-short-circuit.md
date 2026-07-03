# SP2b Slice 6a — and_or_short_circuit (list-quantified cond)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Port `and_or_short_circuit` (`And([…,false,…]) => false`, `Or([…,true,…]) => true`) into the declarative CombinedLang DSL, behavior-neutral vs the standalone `EGraph<ScalarLang>` engine (still the differential oracle; deletion is slice 7). Production optimizer untouched.

**Architecture:** One new machinery piece — a **list-quantified cond** over a variadic operator's rest-captured operand list (`Vec<Id>`), with a pluggable per-element predicate. 6a's predicate is scalar-literal-bool. Designed **grammar-general over CombinedLang** (the rest-quantification works on any variadic's `Vec<Id>`, incl relational `Union`), so slice-6c (drop_unit/dedup, rest-splice) and 6e (absorb, cross-element) extend it, and a post-slice-6 relational union-cancel re-test can reuse it. RHS is `Tmpl::SBool` (exists since slice 4). No rest-splice, no cross-element in 6a.

**Tech Stack:** Rust (build-time codegen + runtime), chumsky grammar, Lean 4 (`MirRewrite`).

## Global Constraints

- **Behavior-neutral, additive-only.** Production path untouched. Standalone `crate::eqsat::scalar` engine intact as A/B oracle. Shared-surface changes additive (one new `Cond` variant, one grammar production pair, one codegen arm, one default view method, one Lean cond-arm + helper lemma).
- **Differential parity is the gate.** `canonicalize_combined(e, ct) == crate::eqsat::scalar::canonicalize(e, ct)` over 6a + all prior, old engine oracle. Relational goldens `bin/sqllogictest --optimized`, **never `--rewrite`**.
- **No CI backstop for Lean.** Permanent-sorry trip-wire (`ci/test/lean-mir-rewrite.sh`) and aggregate `lake build` are **hand-run, MANDATORY**. Permanent sorry stays **5** (no new builtin). Two-sided trip-wire unchanged; only move it if the count actually moves (it must not).
- **UNCONDITIONAL fold, NO could_error guard.** Verified from `src/expr/src/scalar/func/variadic.rs:80-97` (And) / `:1147-1169` (Or): a literal-`false` operand makes And return `false` via an unconditional early return (line 85) that discards any accumulated error, and short-circuits before later operands are evaluated. So `And([1/0, false])` and `And([false, 1/0])` both eval to `false`. Or is symmetric (true dominates). The old-engine `and_or_short_circuit` (`scalar/rules.rs:245-260`) is therefore unguarded; match it exactly. This is the E-err envelope: the rule folds unconditionally and it is correct.
- **No lattice addition.** The cond reuses the existing scalar `literal` analysis via `scalar_lit_bool_or_null`. Slice-3 lattice freeze holds.
- **Grammar-general, not scalar-hardcoded.** The rest-quantified cond binds a `Vec<Id>` rest metavar and applies a per-element predicate. Keep the rest-quantification general (it will be reused for relational variadics in 6c/6e); only the scalar-lit-bool predicate is 6a-specific. If grammar-generality proves materially harder than scalar-only, STOP and flag the overseer (do not silently shortcut to scalar-only — that would re-block the union-cancel research thread).
- **All 6a rules `colored: false`.** The slice-4 `emit()` color-assert (`!(colored && is_scalar_rule)`) must still hold and cover the new cond shape. New `Cond::AnyScalarLit` → `is_color_exact` returns `false`.
- **Process:** any view trait method added → prefer a DEFAULT method (ColoredView inherits, no all-impls sweep); if a non-default method is unavoidable, ALL-IMPLS sweep incl ColoredView (slice-3 E0046). Run `cargo check --tests`, not bare `check` (slice-3 E0502). `bin/fmt`. Stage only your files. `git status` clean before commit; never broad-clean unstaged SDD scratch.
- **Out of scope:** no 6b–6f rules (null_prop_variadic, err_prop_variadic, drop_unit, dedup, flatten_assoc, absorb_and_or, factor_and_or). No production reroute / ScalarEGraph deletion (slice 7). Do not touch `doc/developer/generated/`.

**Worktree:** `/home/moritz/dev/repos/materialize/.claude/worktrees/mir-equality-optimizer`, branch `claude/mir-equality-optimizer-sodbej`. BASE (pre-6a): `fe04413e31`.

**Reference (read, match, don't reinvent):**
- Old rule: `scalar/rules.rs::and_or_short_circuit` (245-260) + `lit_bool` (73-81). Direction: `And` zero=`false`, `Or` zero=`true`; fire if `any` operand is that literal; RHS = the zero literal. Unguarded.
- Eval semantics: `src/expr/src/scalar/func/variadic.rs` And `:80-97`, Or `:1147-1169`.
- Existing conds: `dsl.rs::Cond` (`AnyFalse`/`AllTrue` are payload-quantified — different binding kind), `scalar_lit_true`/`scalar_no_error` (single-scalar). codegen cond emit `codegen.rs:491-495` (`g.cond_*`), serialization `~1583`, `is_color_exact` `~34`. Rest binding: `codegen.rs::variadic` (365-385) binds `xs...` to `restN: Vec<Id>`; `b.rests`; accessors `pl_local`/`rel_local` (449/441) — add `rest_local`.
- View: `egraph/view.rs::scalar_lit_bool_or_null` (trait 48, BaseView 140, ColoredView inert stub). `Tmpl::SBool` (dsl.rs, codegen, lean.rs) from slice 4.
- Lean: `Semantics.lean` `denoteS`/`denoteSFold` (andE=`foldr && true`, orE=`foldr || false`), the `negate_unionAll` helper-lemma pattern; `lean.rs` `choose_proof` scalar arms (the `andE`/`orE` de Morgan arm + `first | … | sorry` fallback), cond→hypothesis modelling in `emit_rule`.

---

### Task 1: DSL AST + grammar for the list-quantified cond

**Files:** Modify `src/transform/src/eqsat/dsl.rs`, `src/transform/build/grammar.rs`; test inline in `grammar.rs`.

**Interface produced:**
```rust
// dsl.rs — Cond
/// `scalar_any_lit_false(xs)` / `scalar_any_lit_true(xs)`: some operand in the
/// rest-captured list `xs` (a variadic operator's operands, `Vec<Id>`) is a scalar
/// literal equal to `value`. List-quantified: the per-element check is the scalar
/// `literal` analysis. Grammar-general over CombinedLang (the rest is `Vec<Id>` for
/// any variadic, scalar or relational); only the per-element predicate is scalar.
/// Gates `and_or_short_circuit`.
Cond::AnyScalarLit { list: String, value: bool }
```

- [ ] **Step 1:** Add `Cond::AnyScalarLit { list, value }` to `dsl.rs` near `Cond::ScalarNoError`, with the doc above.
- [ ] **Step 2:** Grammar — add to the cond `choice((...))`:
```rust
one_ident("scalar_any_lit_false").map(|list| Cond::AnyScalarLit { list, value: false }),
one_ident("scalar_any_lit_true").map(|list| Cond::AnyScalarLit { list, value: true }),
```
- [ ] **Step 3:** Parse test: `rule r { Variadic[and](xs...) => false where scalar_any_lit_false(xs) }` round-trips to `Cond::AnyScalarLit{ list:"xs", value:false }`; same for `_true`. Confirm the cond names a rest metavar cleanly (the `xs` here is the same ident bound by `Variadic[and](xs...)`).
- [ ] **Step 4:** `cargo check -p mz-transform` — expect RED with the expected `E0004` non-exhaustive-match for `Cond::AnyScalarLit` in codegen.rs (+ possibly lean.rs). Confirm those are the ONLY errors.
- [ ] **Step 5:** `bin/fmt`; stage `dsl.rs` + `grammar.rs` only; commit.

---

### Task 2: cond machinery — rest_local accessor + default view method + codegen

**Files:** Modify `src/transform/build/codegen.rs`, `src/transform/src/eqsat/egraph/view.rs`; test inline.

**Interfaces produced:**
```rust
// view trait (DEFAULT method — ColoredView inherits, no all-impls sweep):
fn cond_any_scalar_lit(&self, ids: &[Id], value: bool) -> bool {
    ids.iter().any(|&id| self.scalar_lit_bool_or_null(id) == Some(Some(value)))
}
```

- [ ] **Step 1:** Add the DEFAULT `cond_any_scalar_lit` method to the view trait (same trait as `scalar_lit_bool_or_null`, view.rs:48). Default body iterates + reuses `scalar_lit_bool_or_null`. Confirm ColoredView inherits it (its `scalar_lit_bool_or_null` inert-stub returns `None` → cond always false → never fires through ColoredView; correct since 6a is `colored:false`).
- [ ] **Step 2:** Add a `rest_local(&self, name: &str) -> String` accessor to the matcher builder in codegen.rs (mirror `pl_local`/`rel_local`, looking up `self.rests`). Panic/expect on unbound (codegen guarantees the rest is bound by the pattern).
- [ ] **Step 3:** codegen cond emit (near codegen.rs:491):
```rust
Cond::AnyScalarLit { list, value } => format!("g.cond_any_scalar_lit(&{}, {})", m.rest_local(list), value),
```
- [ ] **Step 4:** `is_color_exact` (codegen.rs:34): `Cond::AnyScalarLit { .. } => false`. Serialization arm (~1583): `Cond::AnyScalarLit { list, value } => format!("{P}::Cond::AnyScalarLit {{ list: {}, value: {} }}", s(list), value)`.
- [ ] **Step 5:** Any other exhaustive `Cond` match the compiler flags (e.g. in `lean.rs` — leave a `todo!()` there ONLY to compile-check, revert byte-clean; the real lean.rs arm is Task 4). `cargo check --tests -p mz-transform` GREEN with the temp lean stub, then `git restore` lean.rs.
- [ ] **Step 6:** Unit test: `cond_any_scalar_lit` on a tiny graph — a list with a literal-false id returns true for `value:false`, false for `value:true`; a list of columns returns false. `bin/fmt`; stage codegen.rs + view.rs only; commit.

---

### Task 3: port `and_or_short_circuit` to `scalar.rewrite`

**Files:** Modify `src/transform/src/eqsat/rules/scalar.rewrite`.

- [ ] **Step 1:** Add (SCALAR_COMPILED_RULES 16 → 18), matching existing comment/house style:
```
# A variadic AND with a literal-false operand is false, regardless of the other
# operands or whether they could error. MZ's And short-circuits on false (an
# unconditional early return that discards any accumulated error, see
# func/variadic.rs), so this is sound with no could_error gate. Mirrors
# scalar/rules.rs::and_or_short_circuit. Or is dual (true dominates).
rule and_short_circuit {
    doc "And([.., false, ..]) = false"
    Variadic[and](xs...) => false where scalar_any_lit_false(xs)
}

rule or_short_circuit {
    doc "Or([.., true, ..]) = true"
    Variadic[or](xs...) => true where scalar_any_lit_true(xs)
}
```
- [ ] **Step 2:** `cargo check --tests -p mz-transform` — build script compiles the rules through grammar+codegen. GREEN (temp-stub lean.rs's new cond arm if `cargo check` compiles lean.rs and it's non-exhaustive; revert byte-clean — real arm is Task 4). Confirm the 2 rules appear in `SCALAR_COMPILED_RULES` (16→18), none leak into relational `COMPILED_RULES`.
- [ ] **Step 3:** Inspect generated `find_and_short_circuit`: matches `Variadic[and]`, binds `xs` = rest `Vec<Id>`, cond `g.cond_any_scalar_lit(&restN, false)`, RHS `literal_false()`. Quote it in the report.
- [ ] **Step 4:** `bin/fmt`; stage scalar.rewrite only; commit.

---

### Task 4: Lean — model the existence cond, prove the fold

**Files:** Modify `src/transform/lean/MirRewrite/Semantics.lean`, `src/transform/src/eqsat/lean.rs`; regenerate `Generated.lean`; check `ci/test/lean-mir-rewrite.sh`.

The theorem shape: `∀ env (xs : List ScalarExpr), (hyp : ∃ x ∈ xs, denoteS env x = false) → denoteS env (andE xs) = denoteS env (litB false)`, i.e. `denoteSFold env xs true (·&&·) = false`. The cond is the PRECONDITION (not a soundness guard), so it MUST become a hypothesis, else the theorem is false. Provable by list induction.

- [ ] **Step 1:** `Semantics.lean` — add a helper lemma (the `negate_unionAll` pattern), proved outright:
```lean
theorem denoteSFold_and_false (env : Nat → Bool) (xs : List ScalarExpr)
    (h : ∃ x ∈ xs, denoteS env x = false) :
    denoteSFold env xs true (· && ·) = false := by
  induction xs with
  | nil => simp at h
  | cons a as ih =>
    obtain ⟨x, hx_mem, hx⟩ := h
    -- case on whether the false element is the head or in the tail
    sorry  -- implementer: discharge via List.mem_cons + Bool.and_eq_false, or `by_cases`
```
and the dual `denoteSFold_or_true` (`foldr || false = true` when some element is true). If the induction resists a clean tactic within a reasonable time-box, leave the lemma as a **provable-later `sorry`** (NOT `-- PERMANENT SORRY`; it is genuinely provable, differential is the real oracle) — but attempt the proof first.
- [ ] **Step 2:** `lean.rs` — `emit_rule`: add a cond→hypothesis arm for `Cond::AnyScalarLit { list, value }` → hypothesis `∃ x ∈ {list}, denoteS env x = {value}` (a `List ScalarExpr` binder `xs` already exists from the variadic rest). `choose_proof`: for the `andE`/`orE` short-circuit shape with this hypothesis, emit `by intro …; exact denoteSFold_and_false … h` (or the `_or_true` dual), wrapped `first | (…) | sorry` for safety.
- [ ] **Step 3:** Regenerate: `cargo run -p mz-transform --example gen-lean`. Confirm 2 new theorems (`and_short_circuit`, `or_short_circuit`) that are NOT permanent sorries. `grep -rho "PERMANENT SORRY" src/transform/lean/MirRewrite | wc -l` == **5** (unchanged).
- [ ] **Step 4:** Aggregate `lake build` (MANDATORY clean rebuild): `cd src/transform/lean && lake build` GREEN. `#print axioms rule_and_short_circuit` — if the helper lemma is proved, expect no `sorryAx` (or a single provable-later `sorryAx` if time-boxed; note which). CI `expected_permanent=5` unchanged; run `ci/test/lean-mir-rewrite.sh` (Docker) GREEN, count==5.
- [ ] **Step 5:** `bin/fmt` the Rust; stage Semantics.lean + lean.rs + Generated.lean (+ CI script only if touched, which it should not be) only; commit.

---

### Task 5: differential corpus + parity + slice gate

**Files:** Modify `src/transform/src/eqsat/scalar_saturate.rs` (add `scalar_parity_slice6a` + `corpus_covers_slice6a`), `src/transform/tests/testdata/eqsat_scalar_corpus`.

**Corpus-shaping:** after 6a the still-unported rules are 6b–6f (null_prop_variadic, err_prop_variadic, drop_unit, dedup, flatten_assoc, absorb_and_or, factor_and_or). Inputs must not trigger those in the old engine, or a divergence is a corpus artifact. Keep to And/Or over distinct columns + the literal false/true operand + at most the interacting empty/single cases.

- [ ] **Step 1:** `scalar_parity_slice6a` (extend the slice-5 harness, old engine oracle):
  - **Positions:** `And([false, #0, #1])`, `And([#0, false, #1])`, `And([#0, #1, false])` — leading/middle/trailing, all ⇒ false. Dual for `Or([true, …])`.
  - **Nulls (3VL):** `And([#0, false, null])` ⇒ false (false dominates null). `Or([#0, true, null])` ⇒ true. Confirm parity.
  - **E-err envelope (both orders — the crux):** `And([false, 1/0])` AND `And([1/0, false])` both ⇒ false; `Or([true, 1/0])` AND `Or([1/0, true])` both ⇒ true. Assert combined==old on all four. (No guard to mutation-test; instead confirm the fold fires and matches the old engine on the error inputs — that IS the envelope check. Note in the report that the rule is unguarded-by-design and why.)
  - **Interactions:** `And([false])` (single) — and_single ⇒ #operand=false AND short_circuit ⇒ false, must agree, no fight. `And([])` (empty) — and_empty ⇒ true, short_circuit does NOT fire (no false operand). `And([false, false])`. Confirm parity + no non-termination.
- [ ] **Step 2:** Termination: lower `And([false, #0])` directly (bypass `canonicalize_combined`, slice-4 pattern), `saturate`, assert `iters` well under `MAX_ITERS` (100). `false` is not re-foldable; no ping-pong with and_single/and_empty.
- [ ] **Step 3:** `corpus_covers_slice6a`: non-vacuity — corpus contains an `and(` with a `false` operand and an `or(` with a `true` operand, and an error-operand short-circuit case.
- [ ] **Step 4:** Add corpus lines.
- [ ] **Step 5:** SLICE GATE (all MANDATORY, hand-run, record each):
  - eqsat unit tests pass (`--lib`); new parity GREEN.
  - Differential parity: 6a + all prior, no `--rewrite`.
  - Relational goldens `bin/sqllogictest --optimized -- test/sqllogictest/transform/` — zero diffs, no `--rewrite`, success==total (55 files).
  - Aggregate `lake build` clean-rebuild GREEN; `PERMANENT SORRY == 5`.
  - `ci/test/lean-mir-rewrite.sh` GREEN.
  - Termination converges.
  - `git status` clean except this slice's files.
- [ ] **Step 6:** `bin/fmt`; stage scalar_saturate.rs + corpus only; commit.

---

## Self-review notes
- **Error crux resolved:** UNCONDITIONAL fold, no guard (verified from And/Or eval early-return, not the doc comment alone). Envelope tested both operand orders. Surfaced to overseer before landing.
- **Grammar-general:** the cond quantifies over a `Vec<Id>` rest — general over CombinedLang; only the scalar-lit predicate is 6a-specific. Not scalar-hardcoded. Ledger must flag: post-slice-6 relational union-cancel re-test queued (this rest-quantified cond is the reusable foundation 6c/6e/union-cancel extend).
- **Permanent sorry stays 5** (declarative SBool RHS, no builtin). Two-sided trip-wire unchanged.
- **No lattice add** (reuses `scalar_lit_bool_or_null`). **No all-impls sweep** (default view method).
- **Ledger flag for the driver:** queue the post-slice-6 relational union-cancel re-test that reuses this rest-quantified cond machinery (carries the batch-1 Stage-2a finding).
