# SP2b Slice 3: analysis-gated scalar rules Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the analysis-gate capability (scalar `Cond` variants reading the existing `could_error`/`literal` analysis) plus the `SIf` scalar pattern/template, and port the `If` rules `if_true`, `if_false_or_null`, `if_same_branches` through them, behavior-neutral. The combined path must equal the old standalone `EGraph<ScalarLang>` engine on a corpus that exercises the could_error and literal-bool/null gate axes.

**Architecture:** Extend the one rewrite-DSL grammar and its codegen with an `SIf` scalar node and three per-id scalar side-conditions that read `CombinedData.scalar`'s already-maintained `ClassAnalysis`. No new analysis, no new state: the lattice stays `could_error + literal`. The determinism-parity extractor already reconstructs `SNode::If` and the saturate driver already recomputes the scalar analysis each round, so both are reused unchanged. `if_same_branches` uses a non-linear pattern (`If(c, x, x)`), which the codegen `guard()` already turns into a same-e-class equality check. The old engine stays the A/B oracle (delete-last is slice 7).

**Tech Stack:** Rust, the `mz-transform` crate, a `chumsky`-based build-time grammar, datadriven tests, Lean 4 (theorem emission, aggregate `lake build`-verified).

## Global Constraints

- **Behavior-neutral, strict gate.** No `--rewrite`, no `cargo insta accept`. The combined path reproduces the old engine's output exactly on the corpus.
- **`ScalarLang` the type stays.** Deletes nothing (delete-last is slice 7). `crate::eqsat::scalar::canonicalize` remains the differential oracle.
- **Production untouched.** The three production callers stay on the old engine. This slice only grows the NEW path used by the differential test.
- **Relational grammar byte-unchanged.** `relational.rewrite` is not edited. Codegen changes are additive scalar arms; relational `find`/`apply`/`COMPILED_RULES` emission unchanged.
- **Analysis lattice frozen.** Do NOT extend `ClassAnalysis` beyond `could_error + literal`. The view methods only READ it. No new `on_add`/`on_union` logic.
- **Do not touch:** the extractor (`scalar_extract.rs`, already handles `SNode::If`), the escalar policy, the saturate driver's analysis recompute.
- **Only the If rules this slice.** `and_or_short_circuit` and `and_or_drop_unit` are DEFERRED (they need a list-quantified cond and a rest-filter template respectively, which are variadic-list-structural machinery, not per-id gates; grouped with the deferred `flatten_assoc` for the variadic-set slice). Do NOT port them.
- **Lean green = aggregate `lake build`.** The slice-2 lesson: per-slice "Lean green" is not real until the aggregate builds. `lake build` (via `ci/test/lean-mir-rewrite.sh` or `cd src/transform/lean && lake build`) is a gate item. `PERMANENT SORRY` count stays 0 (no builtin-appliers until slice 4).
- **Cheap checks before every commit:** `bin/fmt` and `cargo check -p mz-transform`.
- **Test commands (mz-test skill):** unit via `bin/cargo-test -p mz-transform <filter>`; slt via `bin/sqllogictest --optimized -- test/sqllogictest/transform/`; Lean via `cd src/transform/lean && lake build`.

---

## Verified slice-3 rule set (vs `src/transform/src/eqsat/scalar/rules.rs`)

The source has 20 rules. Slices 1-2 ported `not_not`, `and_or_single`, `not_demorgan`. Slice 3 ports the `If` subset, each needing exactly the SIf + per-id analysis-gate machinery:

| Concrete rule | reduce-parity source | Shape | Gate |
|---|---|---|---|
| `if_true` | `if_true` | `If(c, t, e) => t` | `c` literal `true` |
| `if_false_or_null` | `if_false_or_null` | `If(c, t, e) => e` | `c` literal `false` or `null` |
| `if_same_branches` | `if_same_branches` | `If(c, x, x) => x` | `c` cannot error (non-linear `x` gives `then == els`) |

`if_true`/`if_false_or_null` exercise the **literal-bool/null** gate axis; `if_same_branches` is the **first could_error-gated rule** (the could_error axis). `and_or_short_circuit`/`and_or_drop_unit` from the spec's slice-3 row are deferred (see Global Constraints).

---

## File Structure

**Modified files:**
- `src/transform/src/eqsat/dsl.rs`: `Pat::SIf { cond, then, els }`, `Tmpl::SIf { cond, then, els }`; `Cond::ScalarLitTrue { scalar }`, `Cond::ScalarLitFalseOrNull { scalar }`, `Cond::ScalarNoError { scalar }`.
- `src/transform/build/grammar.rs`: parse `If(c, t, e)` (pattern + template) and the three `scalar_*` side conditions.
- `src/transform/src/eqsat/egraph/view.rs`: `MatchGraph` methods `scalar_could_error(id) -> bool` and `scalar_lit_bool_or_null(id) -> Option<Option<bool>>`; `BaseView` impls reading `self.eg.data().scalar.analysis`.
- `src/transform/build/codegen.rs`: `Matcher::node`/`tmpl_stmts`/AST-echo arms for `SIf`; widen `Matcher::child` scalar detection to `SUnary | SVariadic | SIf`; `cond_expr` + `cond_is_color_exact` arms for the three scalar conds; `is_scalar_rule` unaffected (roots are still SUnary/SVariadic/SIf — add SIf).
- `src/transform/src/eqsat/rules/scalar.rewrite`: add `if_true`, `if_false_or_null`, `if_same_branches`.
- `src/transform/src/eqsat/lean.rs`: `translate_pat`/`translate_tmpl`/`collect_binders` arms for `SIf`; scalar-cond-to-hypothesis arms; `choose_proof` for the If rules.
- `src/transform/lean/MirRewrite/Semantics.lean`: extend `ScalarExpr`/`denoteS` with `ifE`.
- `src/transform/lean/MirRewrite/Generated.lean`: regenerated (three If theorems).
- `src/transform/src/eqsat/scalar_saturate.rs`: grow the in-crate differential harness with If cases (could_error + literal-bool/null).
- `src/transform/tests/testdata/eqsat_scalar_corpus`: grow the fixture.

**No new files.**

---

## Interfaces (cross-task contract)

- `dsl::Pat::SIf { cond: Box<Pat>, then: Box<Pat>, els: Box<Pat> }`; `dsl::Tmpl::SIf { cond: Box<Tmpl>, then: Box<Tmpl>, els: Box<Tmpl> }`. Mirrors the ternary shape of `SNode::If { cond, then, els }`.
- `dsl::Cond::ScalarLitTrue { scalar: String }` (the named scalar metavar's class is a literal `true`), `Cond::ScalarLitFalseOrNull { scalar: String }` (literal `false` or `null`), `Cond::ScalarNoError { scalar: String }` (class has `could_error == false`).
- `MatchGraph::scalar_could_error(&self, id: Id) -> bool` — `self.eg.data().scalar.analysis.get(&id).map_or(false, |a| a.could_error)`.
- `MatchGraph::scalar_lit_bool_or_null(&self, id: Id) -> Option<Option<bool>>` — reads `analysis.literal`, returns `Some(Some(true/false))`, `Some(None)` for null, `None` otherwise (mirrors `rules.rs::lit_bool_or_null`).
- `cond_expr` emits: `ScalarLitTrue` → `g.scalar_lit_bool_or_null({id}) == Some(Some(true))`; `ScalarLitFalseOrNull` → `matches!(g.scalar_lit_bool_or_null({id}), Some(Some(false)) | Some(None))`; `ScalarNoError` → `!g.scalar_could_error({id})`, where `{id}` is `m.rel_local(scalar)` (scalar metavars bind as rel-locals).
- A scalar rule is still classified by a scalar-sort root: `is_scalar_rule` becomes `matches!(r.lhs, Pat::SUnary{..} | Pat::SVariadic{..} | Pat::SIf{..})`.

---

### Task 1: SIf + scalar Cond AST variants + grammar

**Files:**
- Modify: `src/transform/src/eqsat/dsl.rs`
- Modify: `src/transform/build/grammar.rs`
- Test: `src/transform/build/grammar.rs` (`#[cfg(test)]`, mirroring `parses_scalar_variadic_single`)

**Interfaces:** Produces `Pat::SIf`, `Tmpl::SIf`, and `Cond::{ScalarLitTrue, ScalarLitFalseOrNull, ScalarNoError}`.

- [ ] **Step 1: Add the `Pat::SIf` and `Tmpl::SIf` variants** in `dsl.rs`:

```rust
// in enum Pat, after SVariadic:
    /// A scalar `If(cond, then, els)`. Non-linear use (the same metavar in
    /// `then` and `els`) is enforced as a same-e-class equality by codegen's
    /// `guard()`, which is how `if_same_branches` matches `If(c, x, x)`.
    SIf { cond: Box<Pat>, then: Box<Pat>, els: Box<Pat> },
// in enum Tmpl, after SVariadic:
    /// Build a scalar `If(cond, then, els)`.
    SIf { cond: Box<Tmpl>, then: Box<Tmpl>, els: Box<Tmpl> },
```

- [ ] **Step 2: Add the three scalar `Cond` variants** in `dsl.rs`'s `enum Cond`:

```rust
    /// `scalar_lit_true(s)`: the class bound to scalar metavar `s` is a literal
    /// `true` (via the scalar `literal` analysis). Gates `if_true`.
    ScalarLitTrue { scalar: String },
    /// `scalar_lit_false_or_null(s)`: the class bound to `s` is a literal
    /// `false` or a literal `null`. Gates `if_false_or_null`.
    ScalarLitFalseOrNull { scalar: String },
    /// `scalar_no_error(s)`: the class bound to `s` has `could_error == false`
    /// (scalar `could_error` analysis). Gates `if_same_branches`.
    ScalarNoError { scalar: String },
```

- [ ] **Step 3: Parse `If(...)` in the `pat` grammar** (grammar.rs, alongside `svariadic`):

```rust
        let sif = kw("If")
            .ignore_then(just(Token::LParen))
            .ignore_then(pat.clone())
            .then_ignore(just(Token::Comma))
            .then(pat.clone())
            .then_ignore(just(Token::Comma))
            .then(pat.clone())
            .then_ignore(just(Token::RParen))
            .map(|((cond, then), els)| Pat::SIf {
                cond: Box::new(cond),
                then: Box::new(then),
                els: Box::new(els),
            });
```

Add `sif` to the `pat` `choice((...))` before `svariadic`.

- [ ] **Step 4: Parse `If(...)` in the `tmpl` grammar** (mirror, using `tmpl.clone()` and `Tmpl::SIf`); add to the `tmpl` `choice((...))` before `tsvariadic`.

- [ ] **Step 5: Parse the three side conditions** in the `cond` grammar. They follow the existing one-ident cond shape (`kw(name)` then `(ident)`). Reuse the `one_ident` helper if present:

```rust
        let scalar_lit_true = one_ident("scalar_lit_true").map(|s| Cond::ScalarLitTrue { scalar: s });
        let scalar_lit_false_or_null =
            one_ident("scalar_lit_false_or_null").map(|s| Cond::ScalarLitFalseOrNull { scalar: s });
        let scalar_no_error = one_ident("scalar_no_error").map(|s| Cond::ScalarNoError { scalar: s });
```

Add all three to the `cond` `choice((...))`.

- [ ] **Step 6: Parse tests** (grammar.rs `#[cfg(test)]`):

```rust
    #[test]
    fn parses_scalar_if_with_cond() {
        let src = "rule if_true { If(c, t, e) => t where scalar_lit_true(c) }";
        let rules = crate::grammar::parse(src).expect("parses");
        assert!(matches!(rules[0].lhs, crate::dsl::Pat::SIf { .. }));
        assert!(matches!(rules[0].conds[0], crate::dsl::Cond::ScalarLitTrue { .. }));
    }

    #[test]
    fn parses_scalar_if_nonlinear() {
        let src = "rule if_same { If(c, x, x) => x where scalar_no_error(c) }";
        let rules = crate::grammar::parse(src).expect("parses");
        match &rules[0].lhs {
            crate::dsl::Pat::SIf { then, els, .. } => {
                assert!(matches!(**then, crate::dsl::Pat::RelVar(ref n) if n == "x"));
                assert!(matches!(**els, crate::dsl::Pat::RelVar(ref n) if n == "x"));
            }
            _ => panic!("expected SIf"),
        }
    }
```

(Confirm the DSL's side-condition keyword is `where` by checking an existing conded rule in `relational.rewrite`; if the grammar uses a different separator, match it.)

- [ ] **Step 7: Run tests / build.** `bin/cargo-test -p mz-transform parses_scalar_if_with_cond parses_scalar_if_nonlinear` (or `cargo check` if build-script tests are unreachable, per the slice-1/2 note). Expect the AST to parse. `cargo check -p mz-transform` is EXPECTED RED after this task (new `Pat`/`Tmpl`/`Cond` variants make `codegen.rs` and `lean.rs` matches non-exhaustive; fixed in Tasks 3 and 5). State which test path applied.

- [ ] **Step 8: Commit** (`bin/fmt` first):
```bash
git add src/transform/src/eqsat/dsl.rs src/transform/build/grammar.rs
git commit -m "eqsat dsl: scalar If pattern/template + analysis-gate conds"
```

---

### Task 2: Scalar analysis view methods

**Files:**
- Modify: `src/transform/src/eqsat/egraph/view.rs`
- Test: inline `#[cfg(test)]` in `view.rs` (or exercised by Task 6's differential).

**Interfaces:** Produces `MatchGraph::scalar_could_error`, `MatchGraph::scalar_lit_bool_or_null`; `BaseView` impls.

- [ ] **Step 1: Add the two methods to the `MatchGraph` trait** (view.rs, near `scalar_class_nodes`):

```rust
    /// Whether the scalar class `id` may produce a runtime error, per the scalar
    /// `could_error` analysis. `false` if the class carries no scalar analysis.
    fn scalar_could_error(&self, id: Id) -> bool;
    /// The boolean-or-null literal of scalar class `id`: `Some(Some(true/false))`
    /// for a bool literal, `Some(None)` for a null literal, `None` otherwise.
    fn scalar_lit_bool_or_null(&self, id: Id) -> Option<Option<bool>>;
```

- [ ] **Step 2: Implement them on `BaseView`** (view.rs `impl MatchGraph for BaseView`), reading the already-populated scalar analysis:

```rust
    fn scalar_could_error(&self, id: Id) -> bool {
        self.eg
            .data()
            .scalar
            .analysis
            .get(&self.eg.find(id))
            .map_or(false, |a| a.could_error)
    }

    fn scalar_lit_bool_or_null(&self, id: Id) -> Option<Option<bool>> {
        let (row, _ty) = self
            .eg
            .data()
            .scalar
            .analysis
            .get(&self.eg.find(id))?
            .literal
            .as_ref()?;
        let row = row.as_ref().ok()?;
        match row.unpack_first() {
            mz_repr::Datum::True => Some(Some(true)),
            mz_repr::Datum::False => Some(Some(false)),
            mz_repr::Datum::Null => Some(None),
            _ => None,
        }
    }
```

(Confirm the analysis is keyed by canonical id; `recompute_analysis` inserts by the class id from `class_ids()`. Use `self.eg.find(id)` to canonicalize the lookup, matching how the extractor/rules canonicalize. Confirm `Datum`/`Row::unpack_first` import path.)

- [ ] **Step 3: Failing test** (view.rs `#[cfg(test)]`): build a combined e-graph with a literal-true scalar class and assert `scalar_lit_bool_or_null` returns `Some(Some(true))` after a scalar-analysis recompute; assert `scalar_could_error` is `false` for a bare column and `true` for a division that can error. (Reuse `scalar_saturate::recompute_analysis` via a small helper, or lower a literal and call the analysis recompute directly. Keep it minimal; the real exercise is Task 6.)

- [ ] **Step 4: Run test.** `bin/cargo-test -p mz-transform scalar_lit` — this task is additive so it does not by itself fix the Task-1 RED build; `cargo check` still fails on the codegen/lean exhaustiveness. Verify the new methods compile by confirming no NEW errors reference `view.rs`.

- [ ] **Step 5: Commit:**
```bash
git add src/transform/src/eqsat/egraph/view.rs
git commit -m "eqsat: scalar could_error + lit_bool_or_null view methods"
```

---

### Task 3: Codegen SIf arms + scalar cond emission + child-widen

**Files:**
- Modify: `src/transform/build/codegen.rs`
- Test: exercised by Task 4's build; behavior by Task 6.

**Interfaces:** Produces `Matcher::node`/`tmpl_stmts`/AST-echo arms for `SIf`; `cond_expr`/`cond_is_color_exact` arms for the three scalar conds; `Matcher::child` scalar detection widened; `is_scalar_rule` widened.

- [ ] **Step 1: `Matcher::node` arm for `Pat::SIf`** (mirror the SVariadic arm; `SNode::If { cond, then, els }`):

```rust
            Pat::SIf { cond, then, els } => {
                self.stmts.push(format!(
                    "let crate::eqsat::scalar::node::SNode::If {{ cond: ec{c}, then: et{c}, els: ee{c} }} = {node} else {{ continue }};"
                ));
                self.child(cond, &format!("*ec{c}"));
                self.child(then, &format!("*et{c}"));
                self.child(els, &format!("*ee{c}"));
            }
```

(`SNode::If` fields are `Id` (Copy); deref like the SUnary `*e{c}` precedent. Confirm field names `cond`/`then`/`els` against `scalar/node.rs`.)

- [ ] **Step 2: Scalar-If root branch in `find_stmts`** (mirror the `Pat::SUnary`/`Pat::SVariadic` branches, symbol `ScalarSym::If`):

```rust
        Pat::SIf { .. } => {
            s.push_str(
                "for (root_id, root_node) in g.nodes_by_scalar_sym(crate::eqsat::scalar::lang::ScalarSym::If) {\n",
            );
            // ... identical structure to the SVariadic branch, root_node/root_id, m.node, body, brace unwind ...
        }
```

(Copy the exact body of the current `Pat::SVariadic` branch, changing only `ScalarSym::Variadic` → `ScalarSym::If`. `ScalarSym::If` exists at `scalar/lang.rs:74`.)

- [ ] **Step 3: `tmpl_stmts` arm for `Tmpl::SIf`** (build `CNode::Scalar(SNode::If{..})`):

```rust
        Tmpl::SIf { cond, then, els } => {
            let vc = tmpl_stmts(cond, hole, out, fresh);
            let vt = tmpl_stmts(then, hole, out, fresh);
            let ve = tmpl_stmts(els, hole, out, fresh);
            let c = fresh.id();
            let v = format!("id{c}");
            out.push_str(&format!(
                "let {v} = g.add(CNode::Scalar(crate::eqsat::scalar::node::SNode::If {{ cond: {vc}, then: {vt}, els: {ve} }}));\n"
            ));
            v
        }
```

- [ ] **Step 4: Widen `Matcher::child` scalar detection** to include SIf:

```rust
                let scalar = matches!(pat, Pat::SUnary { .. } | Pat::SVariadic { .. } | Pat::SIf { .. });
```

- [ ] **Step 5: Widen `is_scalar_rule`:**

```rust
fn is_scalar_rule(r: &Rule) -> bool {
    matches!(r.lhs, Pat::SUnary { .. } | Pat::SVariadic { .. } | Pat::SIf { .. })
}
```

- [ ] **Step 6: AST-echo `pat`/`tmpl` arms for `SIf`** (mirror the `SVariadic` echo arms, boxing the three children with `pat(...)`/`tmpl(...)`).

- [ ] **Step 7: `cond_expr` arms for the three scalar conds:**

```rust
        Cond::ScalarLitTrue { scalar } => format!(
            "g.scalar_lit_bool_or_null({}) == Some(Some(true))",
            m.rel_local(scalar)
        ),
        Cond::ScalarLitFalseOrNull { scalar } => format!(
            "matches!(g.scalar_lit_bool_or_null({}), Some(Some(false)) | Some(None))",
            m.rel_local(scalar)
        ),
        Cond::ScalarNoError { scalar } => {
            format!("!g.scalar_could_error({})", m.rel_local(scalar))
        }
```

- [ ] **Step 8: `cond_is_color_exact` arms for the three scalar conds.** Scalar rules run only in the scalar saturate pass, never the relational colored pass, so color-exactness does not affect them, but the match must be exhaustive. Return `true` (they read the deterministic scalar analysis, not a color-approximated relational analysis) and add a one-line comment noting scalar conds do not participate in colored relational saturation.

- [ ] **Step 9: Build check.** `cargo check -p mz-transform` — after this task the codegen matches are exhaustive again; the ONLY remaining errors must be the `lean.rs` exhaustiveness gaps for `Pat::SIf`/`Tmpl::SIf`/the three `Cond`s (Task 5's scope). Capture the `grep E0004` output showing only lean.rs errors remain.

- [ ] **Step 10: Commit:**
```bash
git add src/transform/build/codegen.rs
git commit -m "eqsat codegen: scalar If match/build arms + scalar-cond emission + child-widen"
```

---

### Task 4: Port the three If rules to `scalar.rewrite`

**Files:**
- Modify: `src/transform/src/eqsat/rules/scalar.rewrite`
- Test: build compiles the three `find_*`/`apply_*`; behavior in Task 6.

- [ ] **Step 1: Add the rules** (after the slice-2 rules). Confirm the side-condition separator keyword against an existing conded rule in `relational.rewrite` (shown here as `where`; match whatever the grammar actually uses):

```
# A constant-true condition selects the then-branch.
rule if_true {
    doc "If(true, t, e) = t"
    If(c, t, e) => t where scalar_lit_true(c)
}

# A constant-false or null condition selects the else-branch.
rule if_false_or_null {
    doc "If(false|null, t, e) = e"
    If(c, t, e) => e where scalar_lit_false_or_null(c)
}

# Identical branches collapse when the condition cannot error. The repeated `x`
# binds then and els to the same e-class (enforced by codegen's guard).
rule if_same_branches {
    doc "If(c, x, x) = x  when c cannot error"
    If(c, x, x) => x where scalar_no_error(c)
}
```

- [ ] **Step 2: Build.** `cargo check -p mz-transform` — the build script parses and generates `find_if_true_base`/`apply_if_true_base` (and the other two); `SCALAR_COMPILED_RULES` grows to 8 (not_not + and_single + or_single + not_demorgan_and + not_demorgan_or + if_true + if_false_or_null + if_same_branches). The ONLY remaining errors must be lean.rs exhaustiveness (Task 5). Confirm no parse error / no `unknown scalar ... keyword` panic. Inspect `$OUT_DIR/eqsat_rules.rs` to confirm the guard for `if_same_branches` contains the auto-generated `then == els` equality (the repeated-`x` handling).

- [ ] **Step 3: Commit:**
```bash
git add src/transform/src/eqsat/rules/scalar.rewrite
git commit -m "eqsat: if_true / if_false_or_null / if_same_branches as declarative scalar rules"
```

---

### Task 5: Lean If denotation + theorems (greens the crate)

**Files:**
- Modify: `src/transform/lean/MirRewrite/Semantics.lean`
- Modify: `src/transform/src/eqsat/lean.rs`
- Modify: `src/transform/lean/MirRewrite/Generated.lean` (regenerated)
- Test: `cargo run -p mz-transform --example gen-lean`; then `cd src/transform/lean && lake build`.

**Ordering note:** this task runs BEFORE the differential (Task 6) because, until the `lean.rs` `Pat::SIf`/`Tmpl::SIf`/`Cond` arms exist, the crate does not compile (the slice-2 ordering lesson). It also emits the three theorems, so it must run AFTER Task 4 adds the rules.

- [ ] **Step 1: Extend the Lean denotation with `ifE`** (Semantics.lean, in the `ScalarExpr` inductive + `denoteS`):

```lean
  | ifE : ScalarExpr → ScalarExpr → ScalarExpr → ScalarExpr
-- in denoteS (inside the mutual block, an equation):
  | ScalarExpr.ifE c t e => if denoteS env c then denoteS env t else denoteS env e
```

Two-valued `Bool` fidelity (same as slices 1-2): the model has no `null`/error, so `if` uses the `Bool` condition directly. Document that the could_error gate on `if_same_branches` has no counterpart in the error-free model (the theorem holds unconditionally there), and that `if_false_or_null`'s null case collapses into the `false` branch of the `Bool` `if`.

- [ ] **Step 2: `lean.rs` arms.** Extend `collect_binders`/`translate_pat`/`translate_tmpl` for `Pat::SIf`/`Tmpl::SIf` (emit `ScalarExpr.ifE (...) (...) (...)`). Translate the three scalar conds into theorem hypotheses (mirror how relational conds become hypotheses; inspect an existing conded relational rule's emitted theorem in `Generated.lean` for the pattern):
  - `scalar_lit_true(c)` → hypothesis `denoteS env <c> = true`.
  - `scalar_lit_false_or_null(c)` → hypothesis `denoteS env <c> = false` (the null case folds into `false` in the two-valued model; document this).
  - `scalar_no_error(c)` → NO hypothesis (errors unmodeled); document that the gate is dropped in the Bool model.
  Extend `choose_proof` for the If rules: `intro ...; simp [denoteS]` should close all three (`if_true`/`if_false_or_null` by the hypothesis rewriting the `if`; `if_same_branches` by `if c then x else x = x`, provable via `cases denoteS env c` or `simp`). If a proof does not close, use the `first | (simp [denoteS]; done) | sorry` fallback (a provable-later, non-`PERMANENT` sorry).

- [ ] **Step 3: Regenerate.** `cargo run -p mz-transform --example gen-lean`. Confirm `rule_if_true`, `rule_if_false_or_null`, `rule_if_same_branches` appear in `Generated.lean`, and all prior theorems remain.

- [ ] **Step 4: `cargo check -p mz-transform` fully GREEN** (0 errors). Capture it.

- [ ] **Step 5: Aggregate `lake build` GREEN.** `cd src/transform/lean && lake build` (or `bash ci/test/lean-mir-rewrite.sh`). Expect "Build completed successfully". Confirm `grep -c "PERMANENT SORRY" src/transform/lean/MirRewrite/*.lean` totals 0, and note which If theorems proved vs sorry-stubbed.

- [ ] **Step 6: Commit:**
```bash
bin/fmt
git add src/transform/lean/MirRewrite/Semantics.lean src/transform/lean/MirRewrite/Generated.lean src/transform/src/eqsat/lean.rs
git commit -m "eqsat lean: If denotation + if_* theorems"
```

---

### Task 6: Corpus + differential parity (could_error + literal axes)

**Files:**
- Modify: `src/transform/tests/testdata/eqsat_scalar_corpus`
- Modify: `src/transform/src/eqsat/scalar_saturate.rs` (in-crate `#[cfg(test)]` harness)
- Test: the harness.

**Interfaces:** Consumes `canonicalize_combined` and `crate::eqsat::scalar::canonicalize`.

- [ ] **Step 1: Grow the corpus** with If cases that exercise BOTH gate axes. Every input must be shaped so the old engine's UNPORTED rules do not diverge (no nested same-func, no operand-list literal folds that only short_circuit/drop_unit would touch, no binary-under-Not). Use literals only where the If gate needs them:
  - Literal-bool fold: `if(true, #0, #1)` → `#0`; `if(false, #0, #1)` → `#1`; `if(null, #0, #1)` → `#1` (null takes else). These need typed columns; supply `col_types` in the harness case (bool columns, and the literal typed bool/null).
  - could_error gate (the FIRST could_error workout): `if_same_branches` with an error-free condition — `if(#0, #1, #1)` → `#1` (branches identical, `#0` a bool column, cannot error) MUST collapse; and a NEGATIVE control where the condition CAN error — `if(1/0 = 0, #1, #1)` (a division-bearing condition) must NOT collapse under the old engine either (both engines keep it), so parity still holds and the gate is proven to fire only when could_error is false. Confirm the old engine's `if_same_branches` is the same could_error-gated rule so parity is exact.

- [ ] **Step 2: Add `scalar_parity_if`** to the in-crate harness, differencing `canonicalize_combined` vs the old oracle over the If cases plus a regression sampling of slice-1/2 shapes. Build the `MirScalarExpr` values in Rust (constructors for `If`, literal bool/null, a divide-by-zero condition). Pass the right `col_types` for the literal-fold and could_error cases. A failure is the slice-3 finding: do NOT adjust the assertion or corpus to force a pass; report the diverging input, the combined output, and the old output.

- [ ] **Step 3: Add `corpus_covers_slice3`** asserting the fixture exercises the If literal-fold and the could_error gate (substring checks for `if(true,` and the identical-branch case).

- [ ] **Step 4: Run.** `bin/cargo-test -p mz-transform scalar_parity_if scalar_parity_variadic scalar_parity_not_not corpus_covers` — all PASS. A divergence is the go/no-go finding.

- [ ] **Step 5: Commit:**
```bash
bin/fmt
git add src/transform/tests/testdata/eqsat_scalar_corpus src/transform/src/eqsat/scalar_saturate.rs
git commit -m "eqsat: differential corpus + parity for analysis-gated If rules"
```

---

### Task 7: Slice-3 gate + regression sweep

**Files:** Test only.

- [ ] **Step 1: Full eqsat unit suite.** `bin/cargo-test -p mz-transform eqsat` — all PASS (relational + slice-1/2/3 scalar).
- [ ] **Step 2: Differential parity (the slice-3 criterion).** `bin/cargo-test -p mz-transform scalar_parity_if scalar_parity_variadic scalar_parity_not_not` — all PASS. A failure is the go/no-go trigger: STOP, report the diverging case, do not adjust output.
- [ ] **Step 3: Relational golden regression (NO rewrite).** `bin/sqllogictest --optimized -- test/sqllogictest/transform/` — 1346/1346, zero diffs. Do NOT pass `--rewrite`.
- [ ] **Step 4: Aggregate `lake build` GREEN** (the slice-2 lesson — this is a hard gate item). `cd src/transform/lean && lake build` (or `bash ci/test/lean-mir-rewrite.sh` if the container path is preferred). Expect "Build completed successfully". Confirm `PERMANENT SORRY` total is 0.
- [ ] **Step 5: Termination note.** Confirm `canonicalize_combined` converges on the If corpus (no hang; the gated rules fire only when their analysis condition holds, so no oscillation).
- [ ] **Step 6: Gate-record commit:**
```bash
git commit --allow-empty -m "eqsat: slice-3 gate PASS (analysis-gated If parity + relational goldens + lake build green)"
```

---

## Self-Review

**Spec coverage (slice-3 brief):**
- SIf pattern/template → Tasks 1, 3. Scalar Cond variants (per-id analysis gates) → Tasks 1, 3. View methods reading the existing lattice → Task 2. `Matcher::child` widen to `SUnary | SVariadic | SIf` → Task 3 Step 4. Verified rule set (if_true, if_false_or_null, if_same_branches); And/Or literal gates deferred with reason → header + Global Constraints.
- Differential parity slice-3 + prior, could_error + literal axes → Task 6. Old engine the oracle, not deleted.
- Aggregate `lake build` green as a gate item → Task 5 Step 5 + Task 7 Step 4. `PERMANENT SORRY` stays 0.
- Lean If denotation + 1:1 theorems (proved where simp closes) → Task 5.
- Production/relational goldens untouched (no `--rewrite`, relational.rewrite byte-unchanged) → Task 7 Step 3, Global Constraints.

**Non-goal adherence:** analysis lattice not extended (view methods read-only); extractor untouched (already handles `SNode::If`); escalar policy untouched; no const-eval/type-context/variadic-set machinery.

**Placeholder scan:** every code step shows code. The `where`-keyword and `SNode::If` field names are called out as confirm-against-source points (Task 1 Step 6, Task 3 Step 1). Build-script grammar tests may be unreachable from the crate test target (slice-1/2 precedent) — stated in Task 1 Step 7.

**Type consistency:** `scalar_lit_bool_or_null -> Option<Option<bool>>` and `scalar_could_error -> bool` (Task 2) are consumed verbatim by `cond_expr` (Task 3 Step 7). `Cond::{ScalarLitTrue, ScalarLitFalseOrNull, ScalarNoError}` field name `scalar` (Task 1 Step 2) matches `m.rel_local(scalar)` (Task 3). `is_scalar_rule` and the `find_stmts` scalar-root branches agree on `SUnary | SVariadic | SIf`.

**Ordering:** Task 5 (Lean) precedes Task 6 (differential) because the crate does not compile until the `lean.rs` SIf/cond arms land — the slice-2 lesson, made explicit. Build is intentionally RED between Task 1 and Task 5 (codegen exhaustiveness closes at Task 3, lean at Task 5), noted per task.

**Overseer traps addressed:** (1) the corpus exercises the analysis-gate axis for real — literal-bool/null folds (`if(true/false/null, ...)`) AND the first could_error workout (`if_same_branches` positive collapse + a division-bearing negative control), not trivial inputs (Task 6 Step 1). (2) Lean green is asserted only off a clean aggregate `lake build` (Task 5 Step 5, Task 7 Step 4), never per-slice-in-isolation.
