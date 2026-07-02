# SP2b Slice 4: Const-eval builtin-applier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the DSL's first builtin-applier (RHS computed by Rust) with `const_fold`, plus the declarative constant-literal RHS template with `and_or_empty`, over the one `CombinedLang` machinery, behavior-neutral against the standalone engine.

**Architecture:** `const_fold` becomes a builtin: a new LHS pattern `Scalar(e)` matches any scalar *call* node (unary/binary/variadic/if) and binds its class; a new RHS term `const_eval(e)` names a Rust function in a new `eqsat/scalar_builtins.rs` that ports old `const_fold`'s eval onto the combined graph and returns the folded-literal id (`Err` = no fire, silently skipped by the saturation loop, mirroring the old rule's `return vec![]`). `and_or_empty` stays declarative: it splits into `and_empty` (`Variadic[and]()` empty pattern `=> true`) and `or_empty` (`=> false`), landing a new constant-boolean-literal RHS template `Tmpl::SBool`. Lean gains `opaque constEval` and `litB : Bool → ScalarExpr`: `const_fold`'s theorem is the first permanent `sorry`; `and_empty`/`or_empty` prove outright via `simp`.

**Tech Stack:** Rust (build-time chumsky grammar + string-emitting codegen), Lean 4.12.0 (Mathlib-free), the eqsat `EGraph<CombinedLang>`.

## Global Constraints

- **Behavior-neutral, no `--rewrite`.** Relational slt goldens must stay byte-identical (`bin/sqllogictest --optimized -- test/sqllogictest/transform/`, zero diffs, NEVER `--rewrite`). The scalar path is not wired into production until slice 7.
- **Old `EGraph<ScalarLang>` stays the A/B differential oracle.** Deletion is slice 7. Every gate compares `new_combined(e) == old_scalar(e)` restricted to the rules ported so far.
- **Builtins reuse `mz_expr` verbatim.** `const_eval` calls `MirScalarExpr::eval`/`typ` exactly as old `const_fold` does (`scalar/rules.rs:102-173`). Never reimplement the evaluator.
- **Aggregate `lake build` is the Lean gate, not per-file isolation.** `cd src/transform/lean && lake build` (local 4.12.0) must be green on a clean rebuild. Slice 2 was reported green while silently broken because only isolated files were checked.
- **Sorry taxonomy, exact counts after this slice:** PERMANENT SORRY = **1** (`const_fold` only), PRE-EXISTING GAP = **1** (`filter_unionAll`, distinct marker, unchanged), provable-later = **26** (unchanged; `and_empty`/`or_empty` prove outright, adding zero sorries). The permanent marker text is exactly `-- PERMANENT SORRY: RHS is a Rust builtin`. `ci/test/lean-mir-rewrite.sh` must assert the permanent count is exactly 1 so a future accidental sorry still trips it.
- **Per commit:** `bin/fmt` and `cargo check -p mz-transform` (and `cargo check -p mz-transform --tests` where a trait method or test changes — slice-3 lesson: bare `check` hid an E0502 that only `--tests` surfaced). Adding a trait method requires an **all-impls sweep** (`BaseView` + `ColoredView`; slice-3 missed `ColoredView`, E0046).
- **Do not touch `doc/developer/generated/`** (read-only) or production optimizer code.

---

## Background: the seam as it exists

- **DSL AST** `src/transform/src/eqsat/dsl.rs`: `Pat` (LHS), `Tmpl` (RHS), `Cond` (side conditions). Scalar variants so far: `Pat::SUnary/SVariadic/SIf`, `Tmpl::SUnary/SVariadic/SIf`. No builtin term, no "any scalar node" pattern, no boolean-literal template.
- **Grammar** `src/transform/build/grammar.rs`: chumsky parsers. `pat` recursive parser (~line 287), scalar `sunary`/`svariadic`/`sif` (~382-406), the `pat` `choice((...))` at ~408. `tmpl` recursive parser with `tsunary`/`tsvariadic`/`tsif` (~513+). `listpat` (~292) already accepts empty `()` via `separated_by(Comma)` → `ListPat{items:[],rest:None}`.
- **Codegen** `src/transform/build/codegen.rs`: `find_stmts` (585) dispatches root iteration per LHS shape (`Pat::SUnary/SVariadic/SIf` each iterate `g.nodes_by_scalar_sym(ScalarSym::X)`); `body` (545) sets `b.root = root_id` and records metavars into `b.rels`; `tmpl_stmts` (779) builds RHS nodes; `apply_body`/`emit_apply` (997/1004) wrap it as `apply_NAME_base(g: &mut EGraph, b: &EBindings) -> Result<Id,String>`; `is_scalar_rule` (1017) routes a rule to `SCALAR_COMPILED_RULES`; `sym_name` (109), `cond_is_color_exact` (28), `cond_expr` (436).
- **Saturation** `src/transform/src/eqsat/scalar_saturate.rs:163` and `egraph/saturate.rs:435` both consume apply as `if let Ok(new_id) = (compiled[qi].apply)(eg, &b) { .. }` — **an `Err` from apply is silently skipped.** This is what lets `const_eval` signal no-fire.
- **View** `src/transform/src/eqsat/egraph/view.rs`: `MatchGraph` trait, impls `BaseView` and `ColoredView`. Already exposes `scalar_class_nodes(id) -> Vec<SNode>`, `nodes_by_scalar_sym(sym) -> Vec<(Id,SNode)>`, `scalar_could_error`, `scalar_lit_bool_or_null`.
- **`ScalarSym`** `src/transform/src/eqsat/scalar/lang.rs:28`: `Column, Literal, Unmaterializable, Unary, Binary, Variadic, If`. The four *call* syms are `Unary, Binary, Variadic, If`.
- **Old rules** `src/transform/src/eqsat/scalar/rules.rs`: `const_fold` (102-173, the eval port), `and_or_empty` (227-235, `And()→true`/`Or()→false` via `func.unit_of_and_or()`), `and_or_single` (214-222, fires on `len==1` only — distinct arity from empty, NOT redundant).
- **Lean** `src/transform/lean/MirRewrite/Semantics.lean`: `ScalarExpr = var|notE|andE(List)|orE(List)|ifE` (207), `denoteS` (224) with `@[simp] denoteSFold` walker. No boolean-literal constructor yet. Relational opaque combinators (`equivsInner`, `flatMapB`, …) show the `opaque` pattern. `src/transform/src/eqsat/lean.rs`: `emit_rule` (55) builds `denoteS env lhs = denoteS env rhs`; `translate_pat` (297), `translate_tmpl` (365), `choose_proof` (632), `is_scalar_rule` (189, must include the new pattern).
- **CI** `ci/test/lean-mir-rewrite.sh:52-56`: greps `PERMANENT SORRY`, currently asserts count 0 ("expected 0 before slice 4").

## Design decisions (resolved during read-first; carry verbatim into tasks)

1. **`and_or_empty` is NOT redundant with `and_single`/`or_single`.** `and_single` fires only on `len==1`; empty is `len==0`. It DOES overlap `const_fold` (both fold `And([])→true`), but both live in the old engine and produce the same literal, so unioning both is harmless and required for parity. Ship both. `and_or_empty` splits into two fixed-func rules (`and_empty`, `or_empty`) because the connective decides the literal and func-metavar binding is a slice-5 capability.

2. **The builtin hatch does not exist; this slice introduces it.** No prior `Tmpl` computes an RHS via Rust and no pattern matches "any scalar node".

3. **`const_eval` operates at the CLASS level.** `apply` receives `b.root` (a class id), not the specific matched node. `const_eval` scans `g`'s scalar nodes in that class for a foldable representative (all children carry a literal analysis, node is a call, not `Panic`, not unmaterializable), evals it, and returns the literal id. Sound by congruence: any foldable representative's eval is the value of the whole class, and the old per-node rule unions the identical literal into the same class. `Err` when no representative folds.

4. **Lean builtin theorem is honestly unprovable.** `opaque constEval : ScalarExpr → ScalarExpr` + theorem `denoteS env (constEval e) = denoteS env e`, marked `-- PERMANENT SORRY: RHS is a Rust builtin`. The opaque function makes `sorry` genuinely required (not `rfl`-closable), mirroring the relational opaque combinators.

---

## Task 1: AST variants + grammar

**Files:**
- Modify: `src/transform/src/eqsat/dsl.rs` (add `Pat::Scalar`, `Tmpl::Builtin`, `Tmpl::SBool`)
- Modify: `src/transform/build/grammar.rs` (parsers for `Scalar(e)`, `const_eval(e)`, `true`/`false`)
- Test: build-time grammar unit tests in `grammar.rs`'s `#[cfg(test)]` module (mirror the existing scalar-parser tests)

**Interfaces:**
- Produces: `Pat::Scalar { binding: String }`; `Tmpl::Builtin { name: String, args: Vec<String> }`; `Tmpl::SBool(bool)`. Consumed by codegen (Task 3) and Lean (Task 5).

- [ ] **Step 1: Add the AST variants.** In `dsl.rs`, add to `enum Pat`:

```rust
    /// Matches any scalar CALL node (unary/binary/variadic/if), binding its
    /// e-class to `binding`. Does not destructure the function or arguments,
    /// so it needs no func-metavar machinery. Roots the `const_fold` builtin,
    /// whose RHS evaluates the class. Leaves (Column/Literal/Unmaterializable)
    /// are never matched: only call syms are iterated.
    Scalar { binding: String },
```

Add to `enum Tmpl`:

```rust
    /// A builtin applier: the RHS is computed by the named Rust function in
    /// `crate::eqsat::scalar_builtins`, called with the graph and the class ids
    /// bound to `args`. Used where the result is an evaluation product that
    /// cannot be a declarative template. The Lean theorem is a permanent `sorry`.
    Builtin { name: String, args: Vec<String> },
    /// A constant boolean literal (`true`/`false`) as a scalar node. The
    /// declarative RHS of `and_empty`/`or_empty`.
    SBool(bool),
```

- [ ] **Step 2: Add grammar parsers.** In `grammar.rs`, inside the `pat` recursive parser, add before the final `choice`:

```rust
        let scalar_any = kw("Scalar")
            .ignore_then(
                relvar_ident().delimited_by(just(Token::LParen), just(Token::RParen)),
            )
            .map(|binding| Pat::Scalar { binding });
```

Add `scalar_any` to the `pat` `choice((...))` tuple (before `relvar` so the `Scalar` keyword wins over a bare ident). In the `tmpl` recursive parser, add:

```rust
        let builtin = ident()
            .then(
                relvar_ident()
                    .separated_by(just(Token::Comma))
                    .collect::<Vec<_>>()
                    .delimited_by(just(Token::LParen), just(Token::RParen)),
            )
            .map(|(name, args)| Tmpl::Builtin { name, args });
        let sbool = choice((
            kw("true").to(Tmpl::SBool(true)),
            kw("false").to(Tmpl::SBool(false)),
        ));
```

Add `sbool` and `builtin` to the `tmpl` `choice((...))`. Order them so `sbool` (fixed keywords) and the operator templates precede `builtin` (which is `ident "(" args ")"` and would otherwise shadow keyword-led forms); place `builtin` LAST in the choice so every named-node template (`Filter`, `Map`, `Unary`, …) is tried first. Confirm `kw` matches whole keywords (not a prefix of an ident) so `true`/`false` do not swallow identifiers.

- [ ] **Step 3: Grammar unit tests.** Add tests mirroring the existing scalar-parser tests: parse `rule const_fold { Scalar(e) => const_eval(e) }` and assert `lhs == Pat::Scalar{binding:"e"}`, `rhs == Tmpl::Builtin{name:"const_eval", args:vec!["e"]}`; parse `rule and_empty { Variadic[and]() => true }` and assert `lhs == Pat::SVariadic{func:"and", inputs: ListPat{items:vec![],rest:None}}`, `rhs == Tmpl::SBool(true)`.

- [ ] **Step 4: Run the grammar tests.**

Run: `cargo test -p mz-transform --test <grammar test target> 2>&1 | tail -20` (or the crate's build-script test harness; match how existing grammar tests run). Expected: the new parse tests PASS. The full crate build will be **RED** (codegen `tmpl_stmts`/`find_stmts`/`is_scalar_rule`/`sym_name` and lean arms are non-exhaustive over the new variants) — that is expected and caught in Tasks 3/5.

- [ ] **Step 5: Commit.** `bin/fmt`, then:

```bash
git add src/transform/src/eqsat/dsl.rs src/transform/build/grammar.rs
git commit -m "eqsat dsl: Scalar(any-call) pattern, Builtin + SBool templates (slice-4 AST)"
```

---

## Task 2: `scalar_builtins.rs` with `const_eval`

**Files:**
- Create: `src/transform/src/eqsat/scalar_builtins.rs`
- Modify: `src/transform/src/eqsat/mod.rs` (or wherever eqsat submodules are declared) to add `pub mod scalar_builtins;`
- Test: `#[cfg(test)]` in `scalar_builtins.rs`

**Interfaces:**
- Produces: `pub fn const_eval(g: &mut EGraph<CombinedLang>, class: Id) -> Result<Id, String>`. Consumed by codegen's `Tmpl::Builtin` emission (Task 3).

- [ ] **Step 1: Write the failing test.** In `scalar_builtins.rs`'s test module, build a combined graph, add `1 + 2` as scalar literals (`CNode::Scalar(SNode::CallBinary{ AddInt64, lit1, lit2 })`), call `const_eval(&mut g, class_of_the_call)`, and assert the returned id's class carries the literal `3`. Add a negative test: a call with a `Column` child returns `Err`. Mirror the eval/type helpers used in `view.rs`'s scalar test module for constructing literals.

- [ ] **Step 2: Run it, expect FAIL** (module/function does not exist).

- [ ] **Step 3: Implement `const_eval`.** Port `scalar/rules.rs::const_fold` (lines 102-173) onto the combined graph. The port: scan the class's scalar nodes, take the first foldable call node, eval it, add the literal. Reuse `MirScalarExpr::eval`/`typ` verbatim.

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Builtin appliers: rule right-hand sides computed by Rust (`mz_expr`
//! evaluation or type inference), not expressible as a declarative template.
//! Each is named from a `.rewrite` rule's `=> name(args)` RHS and called by the
//! generated `apply_NAME_base`. Their Lean theorems are permanent `sorry`s.

use mz_expr::{Eval, MirScalarExpr, UnaryFunc};
use mz_repr::RowArena;

use crate::eqsat::egraph::combined::{CNode, CombinedLang};
use crate::eqsat::egraph::EGraph; // adjust path to the concrete combined EGraph alias used by apply
use crate::eqsat::scalar::node::SNode;
use crate::eqsat::Id;

/// Constant-fold a scalar class: if any scalar call node in `class` has all
/// literal-analysis children (and is not `mz_panic` and not a leaf), evaluate it
/// and return the folded-literal id. `Err` when no representative folds, which
/// the saturation loop treats as "rule did not fire".
///
/// Class-level, unlike the old per-node `const_fold`: `apply` receives the class
/// id, not the matched node. Sound by congruence: a foldable representative's
/// value is the value of the whole class, and unioning that literal into the
/// class is exactly what the old rule did per node. Panic exclusion and the
/// eval itself mirror `scalar/rules.rs::const_fold` verbatim.
pub fn const_eval(g: &mut EGraph<CombinedLang>, class: Id) -> Result<Id, String> {
    // Find a foldable representative. Collect the needed owned data before any
    // mutation so no borrow of `g` is held across `g.add`.
    let folded = {
        let mut chosen: Option<MirScalarExpr> = None;
        'nodes: for node in g.scalar_class_nodes(class) {
            // Leaves have nothing to fold.
            match node {
                SNode::Column(..) | SNode::Literal(..) | SNode::CallUnmaterializable(..) => {
                    continue;
                }
                // Never evaluate `mz_panic` at optimization time: folding it
                // would abort the optimizer instead of surfacing at runtime.
                // `reduce`/unary.rs makes the same exclusion.
                SNode::CallUnary {
                    func: UnaryFunc::Panic(_),
                    ..
                } => continue,
                _ => {}
            }
            // Every child class must carry a literal analysis.
            let children = node.children();
            let mut child_exprs: Vec<MirScalarExpr> = Vec::with_capacity(children.len());
            for &c in &children {
                let Some((row, col_type)) = g.scalar_literal(c) else {
                    continue 'nodes;
                };
                child_exprs.push(MirScalarExpr::Literal(row, col_type));
            }
            // Reassemble the call with literal children (no Column refs, so
            // empty datums/col_types are sound), matching old const_fold.
            let call = match &node {
                SNode::CallUnary { func, .. } => MirScalarExpr::CallUnary {
                    func: func.clone(),
                    expr: Box::new(child_exprs[0].clone()),
                },
                SNode::CallBinary { func, .. } => MirScalarExpr::CallBinary {
                    func: func.clone(),
                    expr1: Box::new(child_exprs[0].clone()),
                    expr2: Box::new(child_exprs[1].clone()),
                },
                SNode::CallVariadic { func, .. } => MirScalarExpr::CallVariadic {
                    func: func.clone(),
                    exprs: child_exprs,
                },
                SNode::If { .. } => MirScalarExpr::If {
                    cond: Box::new(child_exprs[0].clone()),
                    then: Box::new(child_exprs[1].clone()),
                    els: Box::new(child_exprs[2].clone()),
                },
                SNode::Column(..) | SNode::Literal(..) | SNode::CallUnmaterializable(..) => {
                    unreachable!("leaves filtered above")
                }
            };
            let temp = RowArena::new();
            chosen = Some(MirScalarExpr::literal(
                call.eval(&[], &temp),
                call.typ(&[]).scalar_type,
            ));
            break;
        }
        chosen
    };
    let Some(MirScalarExpr::Literal(row, col_type)) = folded else {
        return Err("const_eval: no foldable representative".to_string());
    };
    Ok(g.add(CNode::Scalar(SNode::Literal(row, col_type))))
}
```

> **Implementer notes:** (a) Resolve the real paths/aliases — `EGraph<CombinedLang>`, `Id`, `g.add`, `g.scalar_class_nodes`, and a per-id literal reader. The view exposes `scalar_class_nodes` and the literal via `data().scalar.analysis.get(find(id)).literal`; add a small `scalar_literal(id) -> Option<(Result<Row,EvalError>, ReprColumnType)>` accessor on the concrete `EGraph`/its data if one does not already exist on the mutable graph (the *view* has the analyses, but `apply` holds `&mut EGraph`; use the graph's own data accessor, `g.data().scalar...`). (b) `node.children()` and `SNode` are the same shared type the old rule uses. (c) Keep the `Panic` and leaf exclusions exactly. (d) A literal child whose analysis is `Err(e)` (error literal) is still `Some((Err(e), ty))`: reconstructing `MirScalarExpr::Literal(Err(e), ty)` and re-evaluating reproduces the error-as-data, which is old const_fold's exact behavior — do not drop or special-case it.

- [ ] **Step 4: Run the tests, expect PASS.** `cargo test -p mz-transform const_eval 2>&1 | tail -20`. (The full crate is still RED until Task 3 wires codegen; run the test with `--no-run`-tolerant scoping or land it green after Task 3 if the crate cannot compile in isolation — in that case, move Step 4's run to the end of Task 3 and note it here.)

- [ ] **Step 5: Commit.** `bin/fmt`, `cargo check -p mz-transform --tests` (expected still RED on codegen exhaustiveness, GREEN specifically for this module's contents):

```bash
git add src/transform/src/eqsat/scalar_builtins.rs src/transform/src/eqsat/mod.rs
git commit -m "eqsat: scalar_builtins::const_eval (class-level port of const_fold onto CombinedLang)"
```

---

## Task 3: codegen — root iteration, builtin/SBool emission, routing

**Files:**
- Modify: `src/transform/build/codegen.rs`

**Interfaces:**
- Consumes: `Pat::Scalar`, `Tmpl::Builtin`, `Tmpl::SBool` (Task 1); `scalar_builtins::const_eval` (Task 2).
- Produces: `find_NAME_base`/`apply_NAME_base` for a `Scalar`-rooted rule; routing into `SCALAR_COMPILED_RULES`.

- [ ] **Step 1: Root iteration for `Pat::Scalar`.** In `find_stmts` (585), add a match arm for `Pat::Scalar { binding }` that iterates all four call syms and binds the class. It records the metavar into `b.rels` (via the same `body`/`Matcher` path the other scalar roots use) so `Tmpl::Builtin`'s args resolve:

```rust
        Pat::Scalar { binding } => {
            // Iterate every scalar CALL sym; leaves are never matched.
            for sym in [
                "Unary", "Binary", "Variadic", "If",
            ] {
                s.push_str(&format!(
                    "for (root_id, _root_node) in g.nodes_by_scalar_sym(crate::eqsat::scalar::lang::ScalarSym::{sym}) {{\n"
                ));
                s.push_str("let root_id = root_id;\n");
                // Bind the class to the metavariable so `const_eval(e)` resolves.
                s.push_str(&format!("let r_scalar = root_id;\n"));
                // Record via a Matcher so `body` emits `b.rels.insert(binding, ..)`.
                // Reuse `m.rel(binding)` to register the metavar → local mapping.
                // (Implementer: register `binding` with local `r_scalar` on `m`
                //  before calling `body`, matching how RelVar roots record.)
                s.push_str(&body(rule, &m_with_binding, mode));
                s.push_str("}\n");
            }
        }
```

> **Implementer:** the cleanest wiring is to register the binding on the `Matcher` (`let local = m.rel(binding);` then emit `let {local} = root_id;`) exactly as the `Pat::RelVar` arm does at 589-596, but loop over the four syms. Verify `body` emits `b.rels.insert(binding, local)`. Keep `open_braces` balanced.

- [ ] **Step 2: `Tmpl::Builtin` and `Tmpl::SBool` emission.** In `tmpl_stmts` (779), add:

```rust
        Tmpl::Builtin { name, args } => {
            let c = fresh.id();
            let v = format!("id{c}");
            // Resolve each arg metavar to its bound class Id.
            let mut arg_locals = Vec::new();
            for a in args {
                let al = format!("ba{}", fresh.id());
                out.push_str(&format!(
                    "let {al} = b.rels.get({a:?}).copied().ok_or_else(|| \"unbound scalar metavariable {a}\".to_string())?;\n"
                ));
                arg_locals.push(al);
            }
            let call_args = std::iter::once("g".to_string())
                .chain(arg_locals)
                .collect::<Vec<_>>()
                .join(", ");
            out.push_str(&format!(
                "let {v} = crate::eqsat::scalar_builtins::{name}({call_args})?;\n"
            ));
            v
        }
        Tmpl::SBool(b) => {
            let c = fresh.id();
            let v = format!("id{c}");
            let ctor = if *b { "literal_true" } else { "literal_false" };
            out.push_str(&format!(
                "let {v} = {{ let lit = mz_expr::MirScalarExpr::{ctor}(); \
                 let mz_expr::MirScalarExpr::Literal(row, ct) = lit else {{ unreachable!() }}; \
                 g.add(CNode::Scalar(crate::eqsat::scalar::node::SNode::Literal(row, ct))) }};\n"
            ));
            v
        }
```

> **Implementer:** confirm the generated `apply_NAME_base` has `g: &mut EGraph` in scope (it does) and that `CNode`/`SNode`/`mz_expr` paths resolve in the generated module (match the fully-qualified style already used for `Tmpl::SUnary`). `const_eval`'s single non-`g` arg means `const_eval(g, e_id)`.

- [ ] **Step 3: Route to `SCALAR_COMPILED_RULES`.** In `is_scalar_rule` (1017), add `Pat::Scalar { .. }` to the matched set so a `Scalar`-rooted rule compiles into the scalar table, not the relational one.

- [ ] **Step 4: Exhaustiveness.** Update `sym_name` (109) if it must be total over `Pat` (add a `Pat::Scalar` arm or confirm it is only called on non-scalar roots and the `find_stmts` dispatch never reaches it — if `sym_name` is `unreachable!` for scalar roots, keep that and add `Pat::Scalar` to the unreachable group with a comment). No `Cond` changes this slice, so `cond_expr`/`cond_is_color_exact` are untouched. Any other total `match` over `Pat`/`Tmpl` in codegen must gain arms.

- [ ] **Step 5: Build green (except Lean).** Run `cargo check -p mz-transform 2>&1 | tail -30`. Expected: codegen exhaustiveness errors gone; only `lean.rs` non-exhaustive-match errors on `Pat::Scalar`/`Tmpl::Builtin`/`Tmpl::SBool` remain (Task 5). If the build-script short-circuits before `lean.rs` compiles (slice-2 lesson), note that lean errors surface only after Task 5's arms — that is acceptable; do not chase phantom greenness. Run the Task-2 `const_eval` unit test now if it could not run in isolation earlier: `cargo test -p mz-transform const_eval 2>&1 | tail -20`, expect PASS.

- [ ] **Step 6: Commit.** `bin/fmt`:

```bash
git add src/transform/build/codegen.rs
git commit -m "eqsat codegen: Scalar-root iteration over call syms, Builtin/SBool emission, scalar routing"
```

---

## Task 4: port rules to `scalar.rewrite`

**Files:**
- Modify: `src/transform/src/eqsat/rules/scalar.rewrite`

**Interfaces:**
- Consumes all Task-1/2/3 machinery. Produces three new rules; `SCALAR_COMPILED_RULES` count goes 8 → 11.

- [ ] **Step 1: Add the rules.** Append to `scalar.rewrite`:

```
# All-literal scalar calls fold to their evaluated literal. RHS is the Rust
# builtin `const_eval` (reconstruct + mz_expr eval); the Lean theorem is a
# permanent sorry. Matches any scalar call node; the builtin declines (no-op)
# on non-literal args, on `mz_panic`, and on unmaterializable calls.
rule const_fold {
    doc "all-literal scalar call = its evaluated literal"
    Scalar(e) => const_eval(e)
}

# An empty AND is the unit `true`; an empty OR is the unit `false`. Declarative
# constant-literal RHS (not a builtin). Distinct from `and_single`/`or_single`,
# which fire only at arity one.
rule and_empty {
    doc "And() = true"
    Variadic[and]() => true
}

rule or_empty {
    doc "Or() = false"
    Variadic[or]() => false
}
```

- [ ] **Step 2: Build and inspect generated code.** `cargo check -p mz-transform 2>&1 | tail -20` (still RED on lean until Task 5). Inspect the generated `find_const_fold_base`/`apply_const_fold_base` and the two `*_empty` rules in the build output (`target/.../out/` or the codegen debug dump if one exists) to confirm: `const_fold` iterates all four call syms and calls `scalar_builtins::const_eval(g, ..)?`; `and_empty` matches `insN.len() == 0` and emits `literal_true`. Confirm `SCALAR_COMPILED_RULES` now lists 11 rules and `const_fold`/`and_empty`/`or_empty` are NOT in the relational `COMPILED_RULES`.

- [ ] **Step 3: Commit.** `bin/fmt`:

```bash
git add src/transform/src/eqsat/rules/scalar.rewrite
git commit -m "eqsat rules: const_fold (builtin), and_empty/or_empty (declarative literal)"
```

---

## Task 5: Lean — opaque `constEval`, `litB`, emission arms, permanent-sorry marker, CI count

**Files:**
- Modify: `src/transform/lean/MirRewrite/Semantics.lean` (add `opaque constEval`, `litB` constructor + `denoteS` equation)
- Modify: `src/transform/src/eqsat/lean.rs` (emit arms for `Pat::Scalar`, `Tmpl::Builtin`, `Tmpl::SBool`; `is_scalar_rule`; permanent-sorry marker)
- Modify: `ci/test/lean-mir-rewrite.sh` (expected permanent count 0 → 1)

**Interfaces:**
- Consumes `Pat::Scalar`, `Tmpl::Builtin`, `Tmpl::SBool`. Greens the crate and the AGGREGATE `lake build`.

- [ ] **Step 1: Semantics additions.** In `Semantics.lean`, extend `ScalarExpr` with a boolean-literal constructor and its denotation, and declare the opaque builtin:

```lean
  | litB : Bool → ScalarExpr
```

Add to `denoteS`:

```lean
  | ScalarExpr.litB b => b
```

After `denoteS`/`denoteSFold`, add:

```lean
/-- The const-eval builtin, opaque: its result is computed by Rust `mz_expr`
    evaluation, not modeled in Lean. Rules whose RHS is `constEval` carry a
    permanent `sorry`; the opaque declaration is what makes that `sorry`
    genuinely required rather than `rfl`-closable. -/
opaque constEval : ScalarExpr → ScalarExpr
```

> **Implementer:** if `denoteSFold` or any `@[simp]` lemma pattern-matches `ScalarExpr` exhaustively, add the `litB` arm there too. Verify `cd src/transform/lean && lake build` compiles `Semantics.lean` alone before touching the emitter.

- [ ] **Step 2: Emitter arms.** In `lean.rs`:
  - `is_scalar_rule` (189): add `Pat::Scalar { .. }` so a `Scalar`-rooted rule denotes through `denoteS` and binds an `env`.
  - `translate_pat` (297): `Pat::Scalar { binding } => binding.clone()` — the bound var is a plain `ScalarExpr` binder.
  - `collect_binders`: a `Pat::Scalar { binding }` contributes `binding` as a single `ScalarExpr` leaf var (like a scalar `RelVar`).
  - `translate_tmpl` (365): `Tmpl::SBool(b) => format!("ScalarExpr.litB {}", if *b {"true"} else {"false"})`; `Tmpl::Builtin { name, args }` for `name == "const_eval"` → `format!("constEval {}", args[0])` (general builtins map name→Lean opaque fn; only `const_eval`/`constEval` this slice).
  - `choose_proof` (632): when the RHS is a `Tmpl::Builtin`, emit the permanent sorry. The emitted theorem body must be exactly:

```
by
    -- PERMANENT SORRY: RHS is a Rust builtin
    sorry
```

  For `and_empty`/`or_empty` (RHS `Tmpl::SBool`, LHS empty `andE`/`orE`), the existing scalar `simp [denoteS]` path must close the goal `denoteS env (andE []) = denoteS env (litB true)` (`denoteSFold` over `[]` reduces to the init `true`; `denoteS (litB true) = true`). Confirm it proves; if `simp [denoteS]` alone does not reduce the empty fold, add `denoteSFold` to the lemma set (it is already `@[simp]`, so `simp [denoteS]` should suffice).

- [ ] **Step 3: Build the crate and the aggregate lake.** `bin/fmt`; `cargo check -p mz-transform 2>&1 | tail -20` (expect GREEN now); `cargo run -p mz-transform --example gen-lean` (regenerate `Generated.lean`); then `cd src/transform/lean && lake build 2>&1 | tail -30` — expect **AGGREGATE GREEN**. Confirm `Generated.lean` contains exactly one `-- PERMANENT SORRY: RHS is a Rust builtin` (above `const_fold`'s `sorry`) and that `and_empty`/`or_empty` theorems carry no `sorry`.

- [ ] **Step 4: Update the CI sorry-count guard.** In `ci/test/lean-mir-rewrite.sh`, change the assertion from `-ne 0`/"expected 0" to expect exactly 1, keeping the trip-wire:

```bash
expected_permanent=1  # const_fold (slice 4). Grows to 6 by slice 6 (spec 2.7).
permanent=$(grep -rho "PERMANENT SORRY" "$lean_dir/MirRewrite" | wc -l || true)
if [ "$permanent" -ne "$expected_permanent" ]; then
    echo "error: found $permanent PERMANENT SORRY marker(s); expected $expected_permanent" >&2
    exit 1
fi
```

Update the surrounding comment to drop "before slice 4" and state the current expected count and why (`const_fold` is the one builtin ported so far).

- [ ] **Step 5: Commit.** 

```bash
git add src/transform/lean/MirRewrite/Semantics.lean src/transform/lean/MirRewrite/Generated.lean src/transform/src/eqsat/lean.rs ci/test/lean-mir-rewrite.sh
git commit -m "eqsat lean: opaque constEval + litB, const_fold permanent sorry, and/or_empty proved; CI count 1"
```

---

## Task 6: differential corpus + parity

**Files:**
- Modify: `src/transform/src/eqsat/scalar_saturate.rs` (extend the differential harness + corpus-coverage assertion)

**Interfaces:**
- Consumes the whole slice-4 rule set. Uses the existing `scalar_parity_*`/`corpus_covers_*` harness shape from slices 2/3.

- [ ] **Step 1: Write the failing parity test.** Add `scalar_parity_const_eval` asserting `new_combined(e) == old_scalar(e)` restricted to `{const_fold, and_empty, or_empty}` plus all prior rules, over an adversarial corpus that exercises every axis:
  - **All-literal fold, non-error:** `1 + 2` (`CallBinary AddInt64`) → `3`; `Not(true)` → `false`; `If(true, 1, 2)` → `1`.
  - **All-literal fold, error-as-data (highest risk):** `1/0` (`CallBinary DivInt64`) and an overflow (e.g. `i64::MAX + 1`) must fold to the SAME error literal in both engines — NOT panic, NOT silent drop. This is the negative control.
  - **Partial-literal, must NOT fold:** `#0 + 1` (column child) stays unfolded in both.
  - **Empty identities:** `And()` → `true`, `Or()` → `false`.
  - **Interaction with slice-2 `and_single`:** `And(#0)` collapses to `#0` (not via empty), and `And()` does not fight `and_single` or non-terminate — assert the run reaches a fixpoint and the canonical forms match.

Add `corpus_covers_const_eval` asserting each axis above is present (mirroring `corpus_covers_slice3`), so the parity test cannot pass vacuously.

- [ ] **Step 2: Run it, expect PASS** (the machinery is complete): `cargo test -p mz-transform scalar_parity_const_eval corpus_covers_const_eval 2>&1 | tail -30`.

- [ ] **Step 3: Mutation-test the error-eval control.** Temporarily break the port (e.g. make `const_eval` skip the `Err` literal branch, or drop the overflow case) and confirm the error-eval parity assertion FAILS; then revert. Record in the commit body that the control has real detection power (slice-3 pattern). Do not leave the mutation in.

- [ ] **Step 4: Full scalar unit suite green.** `cargo test -p mz-transform eqsat 2>&1 | tail -20` — expect all scalar/eqsat unit tests PASS (target ≥ the 340 from slice 3, now higher with the new tests).

- [ ] **Step 5: Commit.** `bin/fmt`, `cargo check -p mz-transform --tests`:

```bash
git add src/transform/src/eqsat/scalar_saturate.rs
git commit -m "eqsat: differential corpus + parity for const_fold/and_empty/or_empty (error-eval control mutation-tested)"
```

---

## Task 7: slice-4 gate

**Files:** none (verification + gate-record commit).

- [ ] **Step 1: eqsat unit tests.** `cargo test -p mz-transform eqsat 2>&1 | tail -20` — PASS.
- [ ] **Step 2: differential parity.** `cargo test -p mz-transform scalar_parity 2>&1 | tail -20` — all slices' parity PASS.
- [ ] **Step 3: relational goldens, NO `--rewrite`.** `bin/sqllogictest --optimized -- test/sqllogictest/transform/` — zero diffs across all files. Report file count and pass/fail.
- [ ] **Step 4: aggregate lake build, clean rebuild.** `cd src/transform/lean && rm -rf .lake/build && lake build 2>&1 | tail -30` — GREEN. Then `grep -rho "PERMANENT SORRY" MirRewrite | wc -l` — exactly **1**. Optionally run `bash ci/test/lean-mir-rewrite.sh` if the container is available.
- [ ] **Step 5: termination.** Confirm the parity runs reach a fixpoint (no MAX_ITERS saturation warning); a folded literal is hashconsed and not re-foldable, so no growth.
- [ ] **Step 6: gate-record commit.**

```bash
git commit --allow-empty -m "eqsat: slice-4 gate PASS (const-eval builtin parity + relational goldens + aggregate lake green, PERMANENT SORRY=1)"
```

---

## Self-review notes (author)

- **Spec coverage:** §2.5 builtin-applier term ✔ (Task 1 `Tmpl::Builtin` + Task 2 `const_eval`); `const_fold` builtin ✔; `and_or_empty` declarative literal ✔ (split into `and_empty`/`or_empty`, Task 4); §2.7 permanent-sorry set grows by exactly the one builtin ported ✔ (Task 5). Slice-4 table row (machinery + 2 rules + differential gate) ✔.
- **Flags resolved:** (1) `and_or_empty` NOT redundant with `and_single`/`or_single` (arity 0 vs 1); ships as two declarative rules — slice does not collapse. (2) Builtin hatch did not exist; introduced here.
- **Type consistency:** `Pat::Scalar{binding:String}`, `Tmpl::Builtin{name:String,args:Vec<String>}`, `Tmpl::SBool(bool)` used identically across dsl/grammar/codegen/lean. `const_eval(g: &mut EGraph<CombinedLang>, class: Id) -> Result<Id,String>` matches the `apply` call site `scalar_builtins::const_eval(g, e_id)?`.
- **Process lessons wired in:** all-impls sweep + `cargo check --tests` (Global Constraints); aggregate lake as the Lean gate (Task 5/7); mutation-test the error control (Task 6); build intentionally RED between AST and codegen/lean catch-up (Tasks 1-5), Lean before differential (Task 5 before 6).
