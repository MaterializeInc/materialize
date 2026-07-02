# SP2b Slice 5 — type-context builtins + func-metavar binding

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port five type-context scalar rewrites (`if_err_cond`, `null_prop_binary`, `err_prop_binary`, `isnull_fold`, `not_binary_negate`) into the declarative DSL over the one `CombinedLang` machinery, behavior-neutral against the standalone `EGraph<ScalarLang>` engine (still the A/B differential oracle; deletion is slice 7). Production optimizer stays unmodified.

**Architecture:** Three rules become **builtin appliers** (Rust RHS in `scalar_builtins.rs`, permanent Lean sorry), reusing the slice-4 `Scalar(e)` catch-all + `Tmpl::Builtin` surface. `isnull_fold` is **declarative** (`=> false` guarded by a new `scalar_non_nullable` cond), proved in Lean. `not_binary_negate` introduces the DSL's first **func-metavar binding**: a new `Pat::SBinaryVar` matches any binary call and binds the `BinaryFunc` symbol, and `Tmpl::SBinaryNegate` emits the negated call; the Lean theorem is a permanent sorry.

**Tech Stack:** Rust (`mz-transform` build-time codegen + runtime), chumsky grammar, Lean 4 (`MirRewrite`).

## Global Constraints

Copied verbatim from the slice-5 spec and the SP2b design (`docs/superpowers/specs/2026-07-01-eqsat-sp2b-scalar-dsl-port-design.md`). Every task's requirements implicitly include this section.

- **Behavior-neutral, additive-only.** The production optimizer path is untouched. The standalone `EGraph<ScalarLang>` engine (`crate::eqsat::scalar`) stays intact as the A/B differential oracle. The only shared-surface changes are additive (new AST variants, new grammar productions, new codegen arms, new view method, new Lean constructors).
- **Differential parity is the gate.** `canonicalize_combined(e, ct) == crate::eqsat::scalar::canonicalize(e, ct)` over these 5 rules + all prior, old engine as oracle. Run with `bin/sqllogictest --optimized` for relational goldens; **never `--rewrite`**.
- **No CI backstop for Lean.** The Lean Docker image does not build in CI, so the permanent-sorry trip-wire (`ci/test/lean-mir-rewrite.sh`) and the aggregate `lake build` are **hand-run, MANDATORY** gate items. `expected_permanent` grows `1 → 5` this slice; the guard is two-sided (fails on both 4 and 6).
- **No lattice addition.** The scalar analysis lattice `ClassAnalysis { could_error, literal }` was frozen at slice 3. Nullability for `isnull_fold`/`scalar_non_nullable` is computed on demand from `scalar_extract::raise + typ + col_types`, NOT stored in the lattice (exactly as the old `isnull_fold`). If any rule is found to need a new e-class analysis, STOP and flag the overseer.
- **Negation table is authoritative, not transcribed.** `not_binary_negate` calls `BinaryFunc::negate() -> Option<BinaryFunc>` (`src/expr/src/scalar/func/macros.rs`, `binary.rs`), the same method the old engine uses (`scalar/rules.rs:723`). Never hand-write a negation pair table.
- **Error-as-data, never panic/drop.** `if_err_cond`/`err_prop_binary` fold to the same error literal the old engine produces (`MirScalarExpr::Literal(Err(e), ty)`), reproduced by re-evaluation, never a panic or a dropped error. `If`-error semantics are PRESCRIBED (not merely within the envelope): reproduce the old engine's If-error behavior exactly.
- **Termination converges.** `Not(f) → neg(f)` must not ping-pong. The folded/negated node is hash-consed; extraction picks the cheaper form. Verify iteration count stays well under `MAX_ITERS` (100).
- **All scalar rules are `colored: false`.** The `colored` keyword is an explicit grammar opt-in; the slice-4 `emit()` assert `!(colored && is_scalar_rule)` must still hold and must cover the new scalar pattern/template shapes.
- **Process:** any new `MatchGraph`/view trait method → ALL-IMPLS sweep incl `ColoredView` (slice-3 E0046). Run `cargo check --tests`, not bare `check`. Stage only your files; never broad-clean unstaged SDD scratch. `bin/fmt` before reporting.
- **Out of scope:** no slice-6 variadic-set rules (`null_prop_variadic`, `err_prop_variadic`, `factor_and_or`, `absorb_and_or`, `and_or_dedup`, `flatten_assoc`, `and_or_short_circuit`, `and_or_drop_unit`). No production reroute or engine delete (slice 7). Do not touch `doc/developer/generated/`. If a rule turns out to need list-quantified conds / rest-splice, defer and flag.

**Worktree:** `/home/moritz/dev/repos/materialize/.claude/worktrees/mir-equality-optimizer`, branch `claude/mir-equality-optimizer-sodbej`. BASE (pre-slice-5): current `HEAD` (`bf1f8331bd`).

**Canonical reference implementations** (read, match, do NOT reinvent):
- Old rules: `src/transform/src/eqsat/scalar/rules.rs` — `if_err_cond` (944–959), `null_prop_binary` (979–1002), `err_prop_binary` (1051–1070), `isnull_fold` (1134–1153), `not_binary_negate` (708–733), helpers `call_scalar_type` (875–905), `is_literal_null` (912–917), `literal_err` (923–929).
- Slice-4 builtin surface: `src/transform/src/eqsat/scalar_builtins.rs` (`const_eval`, `scalar_class_nodes`, `scalar_literal`), `dsl.rs` (`Pat::Scalar`, `Tmpl::Builtin`, `Tmpl::SBool`), `build/grammar.rs` (`scalar_any`, `builtin`, `sbool`, cond `choice`), `build/codegen.rs` (`Pat::Scalar` find, `Tmpl::Builtin` apply, `ScalarNoError` emit at 514, `is_scalar_rule` at 60, `emit()` color-assert), `lean.rs` (`is_scalar_rule`, `translate_pat/tmpl` scalar arms, `choose_proof` `is_builtin_rhs`), `Semantics.lean` (`ScalarExpr`, `denoteS`, `constEval`).
- Combined-graph typing: `scalar_extract::raise(g, id) -> MirScalarExpr`; `g.data().scalar.col_types` (set `scalar_saturate.rs:187`); `g.data().scalar.analysis` (could_error/literal, read by `scalar_builtins::scalar_literal`); apply contract `Result<Id, String>`, `Err` = no-fire (`scalar_saturate.rs:163`).

---

### Task 1: DSL AST + grammar for the new variants

**Files:**
- Modify: `src/transform/src/eqsat/dsl.rs`
- Modify: `src/transform/build/grammar.rs`
- Test: inline `#[cfg(test)]` in `grammar.rs` (follow the slice-4 `scalar_any`/`sbool` test precedent)

**Interfaces produced (later tasks consume these exact shapes):**
```rust
// dsl.rs — Pat
/// Matches any scalar binary call, binding the `BinaryFunc` symbol to `func`
/// (a metavariable, NOT a fixed keyword) and the operands to `expr1`/`expr2`.
/// The DSL's first func-metavar binding. Roots `not_binary_negate`.
Pat::SBinaryVar { func: String, expr1: Box<Pat>, expr2: Box<Pat> }

// dsl.rs — Tmpl
/// Build a binary call whose function is `negate(func)` for the `BinaryFunc`
/// bound to metavar `func` by an `SBinaryVar` on the LHS, over `expr1`/`expr2`.
/// Declines (rule does not fire) when `BinaryFunc::negate()` is `None`.
Tmpl::SBinaryNegate { func: String, expr1: Box<Tmpl>, expr2: Box<Tmpl> }

// dsl.rs — Cond
/// `scalar_non_nullable(s)`: the class bound to scalar metavar `s` is provably
/// non-nullable, via `scalar_extract::raise(g, s).typ(col_types).nullable == false`.
/// Computed on demand (no lattice storage). Gates `isnull_fold`.
Cond::ScalarNonNullable { scalar: String }
```

- [ ] **Step 1: Add the three AST variants.** In `dsl.rs`, add `Pat::SBinaryVar`, `Tmpl::SBinaryNegate`, `Cond::ScalarNonNullable` with the doc comments above. Place `SBinaryVar` next to `Pat::SIf`, `SBinaryNegate` next to `Tmpl::SBool`, `ScalarNonNullable` next to `Cond::ScalarNoError`.

- [ ] **Step 2: Grammar — `scalar_non_nullable` cond.** In `grammar.rs`, add to the cond `choice((...))` (next to line 648):
```rust
one_ident("scalar_non_nullable").map(|scalar| Cond::ScalarNonNullable { scalar }),
```

- [ ] **Step 3: Grammar — func-metavar binary pattern.** Add a `Pat` production `Binary[<ident>](pat, pat)` where the bracket ident is the func metavar. Model on the `SVariadic` pattern parser but with a fixed two-operand shape and reusing `bracket_ident()` for the metavar name:
```rust
let sbinary_var = kw("Binary")
    .ignore_then(bracket_ident())
    .then_ignore(just(Token::LParen))
    .then(pat.clone())
    .then_ignore(just(Token::Comma))
    .then(pat.clone())
    .then_ignore(just(Token::RParen))
    .map(|((func, e1), e2)| Pat::SBinaryVar {
        func,
        expr1: Box::new(e1),
        expr2: Box::new(e2),
    });
```
Add `sbinary_var` to the pattern `choice((...))`. NOTE: `Binary` is a new leading keyword; ensure it does not shadow / is not shadowed by existing pattern productions (the slice-4 test probed this for `builtin`).

- [ ] **Step 4: Grammar — negate template.** Add a `Tmpl` production `Binary[negate(<ident>)](tmpl, tmpl)`:
```rust
let tsbinary_negate = kw("Binary")
    .ignore_then(just(Token::LBrack))
    .ignore_then(kw("negate"))
    .ignore_then(just(Token::LParen))
    .ignore_then(ident())
    .then_ignore(just(Token::RParen))
    .then_ignore(just(Token::RBrack))
    .then_ignore(just(Token::LParen))
    .then(tmpl.clone())
    .then_ignore(just(Token::Comma))
    .then(tmpl.clone())
    .then_ignore(just(Token::RParen))
    .map(|((func, e1), e2)| Tmpl::SBinaryNegate {
        func,
        expr1: Box::new(e1),
        expr2: Box::new(e2),
    });
```
Add `tsbinary_negate` to the template `choice((...))`. It is keyword-led (`Binary` then `negate`), so place it so it does not collide with any bare-ident/builtin template.

- [ ] **Step 5: Tests.** Add parse tests (slice-4 style): `Unary[not](Binary[f](a, b)) => Binary[negate(f)](a, b)` round-trips to the expected `Pat::SUnary{ input: SBinaryVar{..} }` / `Tmpl::SBinaryNegate{..}`; `Unary[isnull](x) => false where scalar_no_error(x), scalar_non_nullable(x)` parses with both conds. Probe that `Binary[negate(f)]` in a template is not mis-parsed as `Binary[f]` + stray tokens.

- [ ] **Step 6: `cargo check -p mz-transform` (build script parses `.rewrite`).** Expected: RED with `E0004` non-exhaustive-match errors in `codegen.rs`, `lean.rs` for the three new variants (Tasks 3, 4, 6 fill them). Confirm the errors are ONLY the expected non-exhaustive arms.

- [ ] **Step 7: Commit** (`git add` dsl.rs grammar.rs only).

---

### Task 2: `scalar_builtins` — the three type-context builtins

**Files:**
- Modify: `src/transform/src/eqsat/scalar_builtins.rs`
- Test: inline `#[cfg(test)]` in the same file

**Interfaces produced:**
```rust
pub fn if_err_cond(g: &mut EGraph, class: Id) -> Result<Id, String>;
pub fn null_prop_binary(g: &mut EGraph, class: Id) -> Result<Id, String>;
pub fn err_prop_binary(g: &mut EGraph, class: Id) -> Result<Id, String>;
```
Each scans `class`'s scalar nodes for its target shape; `Err(_)` = rule does not fire (silently skipped by `scalar_saturate.rs:163`). All three root at the `Scalar(e)` catch-all, so `class == b.root == the bound e`.

**Consumes (existing):** `scalar_class_nodes(g, id)`, `scalar_literal(g, id)`, `scalar_extract::raise(g, id)`, `g.data().scalar.col_types`, `g.data().scalar.analysis` (via a `could_error` reader — add a private `scalar_could_error(g, id)` helper mirroring `scalar_literal`), `g.add(CNode::Scalar(SNode::Literal(..)))`.

Shared helper to add first:
```rust
/// The result scalar type of the call node `node` in the combined graph, the
/// combined-graph analog of `scalar/rules.rs::call_scalar_type`: raise each
/// child to its cheapest `MirScalarExpr`, reassemble the call, and type it
/// against the stored `col_types` (children may carry Columns).
fn call_scalar_type(g: &EGraph, node: &SNode) -> ReprScalarType {
    let col_types = g.data().scalar.col_types.clone();
    let raised: Vec<MirScalarExpr> = node
        .children()
        .iter()
        .map(|&c| scalar_extract::raise(g, c))
        .collect();
    let assembled = /* match node -> MirScalarExpr::{CallUnary,CallBinary,CallVariadic,If}
                       with `raised[..]`, unreachable! on leaves; mirror call_scalar_type */;
    assembled.typ(&col_types).scalar_type
}

/// `could_error` of scalar class `id` (mutable-graph reader; the view's
/// `scalar_could_error` is not available to a builtin holding `&mut EGraph`).
fn scalar_could_error(g: &EGraph, id: Id) -> bool {
    g.data().scalar.analysis.get(&g.find(id)).map(|a| a.could_error).unwrap_or(false)
}

/// Whether scalar class `id` is a literal `null` (mirrors rules.rs::is_literal_null).
fn is_literal_null(g: &EGraph, id: Id) -> bool {
    matches!(scalar_literal(g, id), Some((Ok(row), _)) if row.unpack_first() == Datum::Null)
}

/// The `EvalError` when class `id` is a literal error (mirrors rules.rs::literal_err).
fn literal_err(g: &EGraph, id: Id) -> Option<EvalError> {
    match scalar_literal(g, id)? { (Err(e), _) => Some(e), (Ok(_), _) => None }
}
```

- [ ] **Step 1: `if_err_cond`.** Scan class nodes for an `SNode::If { cond, then, els }` whose `cond` class is a literal error (`literal_err(g, cond)`). Result type = `raise(g, then).typ(col_types).union(&raise(g, els).typ(col_types))`; on `Err` union (incompatible branch types) return `Err` (do NOT unwrap-panic — mirror rules.rs:953). Return `g.add(CNode::Scalar(SNode::Literal(Err(err), result_ty)))`. **No** could_error gate (If evaluates cond first). Test: `If(1/0, #0, #1)`-shaped folds to the error literal; `If(true, ..)` and `If(#col_cond, ..)` do NOT fire here.

- [ ] **Step 2: `null_prop_binary`.** Scan for `SNode::CallBinary { func, expr1, expr2 }` with `func.propagates_nulls()`, one operand `is_literal_null`, and the OTHER operand `!scalar_could_error(other)`. Result = typed null: `g.add(CNode::Scalar(SNode::Literal(Ok(null_row), call_scalar_type(g, node))))` via `MirScalarExpr::literal_null(ty)` destructured to `SNode::Literal`. Mirror rules.rs:979–1002 gate exactly (the gate blocks `err→null`). Test: `AddInt64(null, #0)` with `#0` non-erroring folds to null; `AddInt64(null, 1/0)` does NOT (other can error); a non-null-propagating func does NOT.

- [ ] **Step 3: `err_prop_binary`.** Scan for `SNode::CallBinary { expr1, expr2, .. }`; if `expr1` is a literal error and `!scalar_could_error(expr2)` → typed err literal (`literal_err` + `call_scalar_type`); else symmetric on `expr2`. No `propagates_nulls` gate (matches rules.rs:1051). Test: `AddInt64(1/0, #0)` with `#0` non-erroring folds to the `1/0` error; `AddInt64(1/0, 1/0_col)` where the other can error does NOT.

- [ ] **Step 4: Adversarial unit tests.** Error-arg control against the shape old `if_err_cond`/`err_prop_binary` produce (reproduce the exact `EvalError`, assert equality, not just "is_err"). Nested: `err_prop_binary` where the error operand is itself a folded error literal. `is_err()` on inapplicable shapes for each builtin (no target node in class).

- [ ] **Step 5: `cargo check --tests -p mz-transform`** — module compiles (Task-1 codegen E0004 may still gate the crate; if so, verify the new fns compile via `cargo check --tests` isolating this module the way slice-4 Task 2 did, then confirm clean). Run the new unit tests. `bin/fmt`.

- [ ] **Step 6: Commit** (`scalar_builtins.rs` only).

---

### Task 3: matcher + `EBindings` func-binding surface + view `scalar_nullable`

**Files:**
- Modify: `src/transform/src/eqsat/matcher.rs` (or wherever `EBindings` / scalar match lives — locate via the slice-4 `Pat::Scalar` matcher)
- Modify: `src/transform/src/eqsat/egraph/view.rs` (trait + `BaseView` + `ColoredView` impls)
- Modify: any other `MatchGraph`/view impls the sweep surfaces
- Test: inline unit tests for `scalar_nullable` and the func-binding

**Interfaces produced:**
```rust
// EBindings: a bound BinaryFunc symbol, retrievable by metavar name.
fn bind_binary_func(&mut self, name: &str, func: BinaryFunc);
fn binary_func(&self, name: &str) -> BinaryFunc;   // panics if unbound (codegen guarantees binding)

// view trait (BaseView + ColoredView)
fn scalar_nullable(&self, id: Id) -> bool;         // raise+typ+col_types nullability
```

- [ ] **Step 1: `EBindings` func-binding.** Add storage for named `BinaryFunc` bindings alongside the existing class-id bindings. This is the new func-metavar surface. Keep it minimal: a small map or a parallel vec keyed by metavar name. Do not disturb the class-id binding path.

- [ ] **Step 2: view `scalar_nullable`.** Add to the view trait and implement on `BaseView` via `scalar_extract::raise(self.eg, id).typ(&self.eg.data().scalar.col_types).nullable`. Implement on `ColoredView` too (all-impls sweep). If `ColoredView`'s scalar surface is an inert stub (as with the slice-3 scalar methods), mirror that stub convention — but confirm `scalar_nullable` is only ever called from a `colored: false` rule (all slice-5 rules are), so the `ColoredView` impl is never hit at runtime. Add the same defensive stub the other scalar view methods use.

- [ ] **Step 3: All-impls sweep.** `cargo check --tests` and fix every `E0046` (missing trait method) across all view impls. Do not add speculative impls — only what the compiler demands.

- [ ] **Step 4: Tests.** `scalar_nullable` returns true/false correctly for a nullable vs non-nullable typed column (build a tiny graph with `col_types`). Func-binding round-trips a `BinaryFunc` through `EBindings`.

- [ ] **Step 5: `bin/fmt`, commit** (matcher/view files only).

---

### Task 4: codegen — emit the new find/apply/cond + exhaustiveness

**Files:**
- Modify: `src/transform/build/codegen.rs`

**Consumes:** Task-1 AST variants, Task-2 builtin fns, Task-3 `EBindings::bind_binary_func`/`binary_func` + view `scalar_nullable`.

- [ ] **Step 1: `isnull` unary func-table.** Add to the unary func-keyword tables (codegen.rs ~149 pattern, ~167 constructor):
```rust
"isnull" => "mz_expr::UnaryFunc::IsNull(_)".to_string(),           // match arm
"isnull" => "mz_expr::UnaryFunc::IsNull(mz_expr::func::IsNull)".to_string(),  // ctor
```
(Confirm the exact `IsNull` path in `mz_expr`.)

- [ ] **Step 2: `Cond::ScalarNonNullable` emit.** Mirror `ScalarNoError` (codegen.rs:514) and its serialization arms (506, 1545):
```rust
Cond::ScalarNonNullable { scalar } => format!("!g.scalar_nullable({})", m.rel_local(scalar)),
// is_color_exact (~60): Cond::ScalarNonNullable { .. } => false,
// serialize (~1545): Cond::ScalarNonNullable { scalar } => format!("{P}::Cond::ScalarNonNullable {{ scalar: {} }}", s(scalar)),
```

- [ ] **Step 3: `Pat::SBinaryVar` find.** Emit a matcher that iterates `CNode::Scalar(SNode::CallBinary { func, expr1, expr2 })` nodes of the candidate class, binds `func` via `EBindings::bind_binary_func`, and recurses on `expr1`/`expr2` (which for `not_binary_negate` are `RelVar` leaves `a`/`b`, so they bind whole classes). Model the iteration on the `Pat::Scalar` catch-all find but restricted to `CallBinary` and with the func + two operand bindings.

- [ ] **Step 4: `Tmpl::SBinaryNegate` apply.** Emit:
```rust
let f = b.binary_func("<func>");
let Some(neg) = f.negate() else { return Err("not_binary_negate: func not negatable".into()); };
let e1 = /* build/lookup expr1 class */; let e2 = /* build/lookup expr2 class */;
Ok(g.add(CNode::Scalar(SNode::CallBinary { func: neg, expr1: e1, expr2: e2 })))
```
The `Err` path is the negate-declines case (natural no-fire). Reuse the `Tmpl::Builtin` apply plumbing for the `Result<Id, _>` return.

- [ ] **Step 5: `is_scalar_rule` + exhaustiveness.** Add `Pat::SBinaryVar` to `is_scalar_rule` (codegen.rs:60 and the `lean.rs` copy in Task 6). Add all missing match arms flagged by E0004: `sym_name`, any `Pat`/`Tmpl`/`Cond` exhaustive matches in codegen. Confirm the `emit()` color-assert still holds for `SBinaryVar`-rooted and `SBinaryNegate`-RHS rules (all `colored: false`).

- [ ] **Step 6: `cargo check --tests -p mz-transform`** — GREEN (build script compiles all rules incl the not-yet-added `.rewrite` rules? No: rules are added in Task 5. Codegen must compile with the CURRENT `.rewrite` files, so at this point the new variants have codegen arms but no rule uses them yet — that is fine and compiles). Inspect the generated `OUT_DIR` output for one representative to confirm find/apply shapes.

- [ ] **Step 7: `bin/fmt`, commit** (codegen.rs only).

---

### Task 5: port the five rules to `scalar.rewrite`

**Files:**
- Modify: `src/transform/src/eqsat/rules/scalar.rewrite`

- [ ] **Step 1: Add the rules** (SCALAR_COMPILED_RULES 11 → 16). Exact text:
```
# If with a literal-error condition folds to that error, typed as the union of
# the branch types. Builtin RHS (needs branch typing). If evaluates the
# condition first, so no could_error gate. The Lean theorem is a permanent sorry.
rule if_err_cond {
    doc "If(err, t, e) = err"
    Scalar(e) => if_err_cond(e)
}

# A binary call with a literal-null operand folds to null when the OTHER operand
# cannot error, for a null-propagating func. Builtin. Permanent sorry.
rule null_prop_binary {
    doc "f(null, b) = null when f propagates nulls and b cannot error"
    Scalar(e) => null_prop_binary(e)
}

# A binary call with a literal-error operand folds to that error when the OTHER
# operand cannot error. Builtin. Permanent sorry.
rule err_prop_binary {
    doc "f(err, b) = err when b cannot error"
    Scalar(e) => err_prop_binary(e)
}

# IsNull of a provably non-nullable, error-free operand is false. Declarative
# constant-literal RHS guarded by the no-error and non-nullable conds.
rule isnull_fold {
    doc "IsNull(x) = false when x is non-nullable and cannot error"
    Unary[isnull](x) => false where scalar_no_error(x), scalar_non_nullable(x)
}

# Not of a negatable binary comparison is the negated comparison over the same
# operands, e.g. Not(a = b) = a != b. Binds the func symbol as a metavar and
# emits negate(f). Declines when the func has no negation. Permanent sorry.
rule not_binary_negate {
    doc "Not(f(a, b)) = negate(f)(a, b) for negatable f"
    Unary[not](Binary[f](a, b)) => Binary[negate(f)](a, b)
}
```
House style: periods/commas in comments, no semicolons/em-dashes.

- [ ] **Step 2: `cargo check --tests -p mz-transform`** — build script compiles the new rules through grammar → codegen. GREEN (Lean codegen arm for the new variants still pending Task 6 → expect the `lean.rs` E0004/`unimplemented!` only if `gen-lean` is exercised; plain `cargo check` does not run `gen-lean`, so this should be GREEN. If the crate builds `lean.rs` match arms, add stubs per Task 6). Confirm the 5 new rules appear in `SCALAR_COMPILED_RULES` and none leak into relational `COMPILED_RULES`.

- [ ] **Step 3: Run eqsat unit tests** (`bin/cargo-test -p mz-transform eqsat` or the slice-4 command) — existing tests still pass; the new rules are exercised by Task 7's parity, not here.

- [ ] **Step 4: `bin/fmt`, commit** (scalar.rewrite only).

---

### Task 6: Lean — constructors, translation, proofs, CI count

**Files:**
- Modify: `src/transform/lean/MirRewrite/Semantics.lean`
- Modify: `src/transform/src/eqsat/lean.rs`
- Regenerate: `src/transform/lean/MirRewrite/Generated.lean` (`cargo run -p mz-transform --example gen-lean`)
- Modify: `ci/test/lean-mir-rewrite.sh`

- [ ] **Step 1: `Semantics.lean` — isNull + binary-func opacity.** Add to `ScalarExpr`:
```lean
  | isNullE : ScalarExpr → ScalarExpr
  | binaryE : BinFunc → ScalarExpr → ScalarExpr → ScalarExpr
```
Add before `ScalarExpr` (opaque, needs `Inhabited` for codegen):
```lean
/-- An opaque binary scalar function symbol. Its 3VL semantics and its negation
    table are Rust metadata (`BinaryFunc::negate`), not modeled here, which is
    why `not_binary_negate`'s theorem is a permanent sorry. -/
opaque BinFunc : Type
noncomputable instance : Inhabited BinFunc := ⟨Classical.choice (by ...)⟩  -- or `axiom`-free witness; match JoinSpec's pattern
opaque negateFunc : BinFunc → BinFunc
opaque denoteBin : BinFunc → Bool → Bool → Bool
```
Add `denoteS` arms:
```lean
  | ScalarExpr.isNullE _ => false          -- two-valued model has no null; IsNull is always false
  | ScalarExpr.binaryE f a b => denoteBin f (denoteS env a) (denoteS env b)
```
Document `isNullE => false` as honest at the model's established two-valued fidelity: the model has no `null`, exactly the reason `if_same_branches` holds unconditionally here (Semantics.lean's existing `ifE`/`scalar_no_error` note). It is not `Unit`-triviality exploitation — the rule only fires when the operand is provably non-null and error-free, so `false` is the true value.

- [ ] **Step 2: `lean.rs` — translation arms.**
  - `is_scalar_rule` (lean.rs:194): add `Pat::SBinaryVar { .. }`.
  - `collect_binders`: `Pat::SBinaryVar { func, expr1, expr2 }` binds `func` as `"BinFunc"` then recurses into `expr1`/`expr2`. NOTE: the `is_scalar` retype loop (lean.rs:71–78) force-retypes every binder to `ScalarExpr`/`List ScalarExpr`; add a carve-out so a `"BinFunc"` binder is NOT retyped.
  - `translate_pat`: `Pat::SBinaryVar { func, expr1, expr2 } => format!("ScalarExpr.binaryE {} {} {}", func, arg(translate_pat(expr1)), arg(translate_pat(expr2)))`. `Pat::SUnary` add `"isnull" => format!("ScalarExpr.isNullE {}", ...)`.
  - `translate_tmpl`: `Tmpl::SBinaryNegate { func, expr1, expr2 } => format!("ScalarExpr.binaryE (negateFunc {}) {} {}", func, ...)`.
  - `choose_proof`: (a) add an `isNullE` provable arm — when `lhs.contains("ScalarExpr.isNullE")` return `by\n    {intro}simp [denoteS]` (goal `denoteS env (isNullE x) = denoteS env (litB false)` → `false = false`); (b) mark `SBinaryNegate` permanent — thread an `is_opaque_scalar_rhs` flag (true when `matches!(rule.rhs, Tmpl::SBinaryNegate { .. })`) that, like `is_builtin_rhs`, emits `by\n    -- PERMANENT SORRY: negate table is Rust metadata\n    sorry`. Order the `isNullE` arm before the generic `andE/orE` and fallback arms.

- [ ] **Step 3: Regenerate.** `cargo run -p mz-transform --example gen-lean`. Confirm `Generated.lean` gains 5 theorems: `if_err_cond`, `null_prop_binary`, `err_prop_binary` carry `-- PERMANENT SORRY: RHS is a Rust builtin`; `not_binary_negate` carries `-- PERMANENT SORRY: negate table is Rust metadata`; `isnull_fold` proves (`simp [denoteS]`, no sorry). Total `PERMANENT SORRY` markers = 5.

- [ ] **Step 4: Aggregate `lake build`** (MANDATORY, clean rebuild): `cd src/transform/lean && lake build`. GREEN. Then kernel-verify: `#print axioms` on `rule_isnull_fold` shows `[propext, ...]` (proved), on the 4 permanent theorems shows `[sorryAx]`.

- [ ] **Step 5: CI trip-wire.** `ci/test/lean-mir-rewrite.sh`: `expected_permanent=1` → `expected_permanent=5`; update the inline comment (drop the stale "the one builtin ported so far"; state 4 builtins/opaque + `isnull_fold` proved, grows toward 6 by slice 6). Run `ci/test/lean-mir-rewrite.sh` locally (Docker) — GREEN, count == 5.

- [ ] **Step 6: Commit** (Semantics.lean, lean.rs, Generated.lean, CI script). `bin/fmt` the Rust.

---

### Task 7: differential corpus + parity + slice gate

**Files:**
- Modify: `src/transform/src/eqsat/scalar_saturate.rs` (add `scalar_parity_slice5` + `corpus_covers_slice5`)
- Modify: `src/transform/tests/testdata/eqsat_scalar_corpus`

**Corpus-shaping constraint:** after slice 5 the STILL-unported rules are the slice-6 variadic-set rules (`null_prop_variadic`, `err_prop_variadic`, `and_or_dedup`, `and_or_short_circuit`, `and_or_drop_unit`, `flatten_assoc`, `factor_and_or`, `absorb_and_or`). Every parity input must avoid triggering those in the old engine, or the divergence is a corpus artifact not a slice-5 bug. In practice: no variadic null/err-prop shapes, no factorable AND/OR trees.

- [ ] **Step 1: `scalar_parity_slice5`.** Extend the slice-4 harness (`canonicalize_combined(e, ct) == crate::eqsat::scalar::canonicalize(e, ct)`). Axes:
  - **if_err_cond:** `If(1/0, #0, #1)` (literal error cond) folds to the error; branch-type-union path exercised with distinct branch types.
  - **null_prop_binary:** `AddInt64(null, #0)` non-erroring other → null; `AddInt64(null, 1/0)` other-can-error → no fold (parity holds both engines).
  - **err_prop_binary:** `AddInt64(1/0, #0)` non-erroring other → err; error-in-other-operand control.
  - **isnull_fold:** `IsNull(#0)` with `#0` typed non-nullable → false; `IsNull(#0)` with `#0` nullable → no fold.
  - **not_binary_negate — CRUX 2:** every negation pair from `BinaryFunc::negate()` (enumerate the funcs that have a negation: eq/neq, lt/gte, lte/gt, and any others `negate()` returns Some for), each `× {both non-null cols, one null literal operand, both null}`. Result-diff vs old engine. Include nested `Not(Not(f(a,b)))` and `Not(f(a, <error>))`.
  - **Interaction:** `if_err_cond` with `const_fold` (slice 4) and `if_true` (slice 3) on the same If; `not_binary_negate` under a `not_not` (slice 1).

- [ ] **Step 2: MUTATION TEST (required, Crux 2).** Temporarily swap one negation pair to a wrong-but-plausible partner (e.g. force `Lt.negate()` handling to return `Gt` instead of `Gte` in a local test shim, OR assert that using the wrong partner makes parity FAIL on the null-operand rows). Confirm the parity assertion fails exactly there, then revert. This proves the differential has detection power on the negation table, not vacuous. Record the observed failure in the ledger.

- [ ] **Step 3: Termination.** Lower `Not(f(#0, #1))` directly (bypass `canonicalize_combined`), `saturate`, assert `iters` well under `MAX_ITERS` — `Not(f) → neg(f)` and back must not ping-pong.

- [ ] **Step 4: `corpus_covers_slice5`.** Non-vacuity guards: the corpus file contains representative text for each of the 5 rules (`is null`, a negation pair, an `if(1/0`, a null-prop, an err-prop shape).

- [ ] **Step 5: Add corpus lines** to `eqsat_scalar_corpus` covering the above.

- [ ] **Step 6: SLICE GATE (all MANDATORY, hand-run):**
  - eqsat unit tests pass (`--lib`), new parity tests GREEN.
  - Differential parity: the new 5 + all prior, no `--rewrite`.
  - Relational goldens: `bin/sqllogictest --optimized -- test/sqllogictest/transform/` (all 55 files), **zero diffs, no `--rewrite`**, success == total.
  - Aggregate `lake build` clean-rebuild GREEN; `PERMANENT SORRY == 5`.
  - `ci/test/lean-mir-rewrite.sh` GREEN.
  - Termination converges (no `MAX_ITERS`).
  - `git status` clean except this slice's files.
  - Record the gate in the ledger.

- [ ] **Step 7: `bin/fmt`, commit** (scalar_saturate.rs + corpus only).

---

## Self-review notes

- **Spec coverage:** all 5 rules have a task path; both cruxes covered (Crux 1 = Tasks 1/3/4 func-metavar surface; Crux 2 = Task 7 negation-pair × null + mutation test; Crux 3 = Task 2 `if_err_cond` error-as-data + Task 7 If-error control). Lattice-freeze honored (nullability on-demand, Task 3 doc). CI two-sided count (Task 6). No-CI-backstop → both Lean gates hand-run and MANDATORY (Tasks 6, 7).
- **Permanent-sorry accounting:** const_fold(1) + if_err_cond + null_prop_binary + err_prop_binary (3 builtins) + not_binary_negate (opaque negate) = 5. `isnull_fold` proved. `expected_permanent: 1 → 5`.
- **Type consistency:** builtin signature `(g: &mut EGraph, class: Id) -> Result<Id, String>` matches slice-4 `const_eval` and the apply call site. `EBindings::binary_func` / view `scalar_nullable` names used identically in Tasks 3 and 4.
- **Deferred / flag-if-hit:** if the func-metavar matcher/codegen surface proves disproportionate, the builtin fallback for `not_binary_negate` (class-scanning `Scalar(e)` builtin, same permanent sorry) is the documented escape — flag the overseer before switching. Slice-6 machinery (list-quantified conds, rest-splice) must not leak in; defer + flag.
