# SP2b Slice 6b: `null_prop_variadic` + `err_prop_variadic` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port `null_prop_variadic` (`rules.rs:1013`) and `err_prop_variadic` (`rules.rs:1085`) into the CombinedLang DSL as Rust builtins, behavior-neutral against the standalone `ScalarEGraph` oracle.

**Architecture:** Two `pub fn` in `scalar_builtins.rs` mirroring the old-engine rules exactly, wired via two `Scalar(e) => NAME(e)` DSL rules. The `Tmpl::Builtin` dispatch, grammar, and codegen are already generic (no change). Each builtin RHS carries a permanent Lean sorry, so the count moves 5 → 7. This is a direct mirror of slice-5's binary `null_prop_binary` / `err_prop_binary`.

**Tech Stack:** Rust (`mz-transform`), the eqsat DSL, Lean 4 (`lake`).

## Global Constraints

- Behavior-neutral. Never run sqllogictest with `--rewrite`. Production behavior untouched. No production-default flag flips.
- Both new rules are `colored: false`. No analysis-lattice additions (slice-3 freeze).
- No `ScalarEGraph` deletion (slice 7). No grammar / DSL-AST / codegen changes needed (builtin dispatch is generic).
- Error operands in tests: keep well-typed per `call_scalar_type`. The Bool-collapse trap is And/Or-specific and does not apply to MakeTimestamp-class variadics (result type is the variadic's own type).
- Lean image does not build in CI (owner-confirmed). The permanent-sorry trip-wire bump and aggregate `lake build` are hand-run MANDATORY gates. Do not propose CI wiring.
- `doc/developer/generated/` is read-only.
- Format with `bin/fmt`, `cargo check` before reporting. Stage only your own files; never broad-clean unstaged scratch.

---

### Task 1: The two variadic builtins + unit tests

**Files:**
- Modify: `src/transform/src/eqsat/scalar_builtins.rs` (add two `pub fn` after `err_prop_binary` at `:303`; add unit tests in the `mod tests` block after the `err_prop_binary` tests)

**Interfaces:**
- Consumes (existing module helpers, do not redefine): `scalar_class_nodes(g: &EGraph, id: Id) -> Vec<SNode>` (`:23`), `is_literal_null(g, id) -> bool` (`:182`), `literal_err(g, id) -> Option<EvalError>` (`:188`), `scalar_could_error(g, id) -> bool` (`:171`), `call_scalar_type(g, node) -> ReprScalarType` (`:131`), `scalar_literal(g, id)` (`:39`). `SNode::CallVariadic { func, exprs }`. `func.propagates_nulls()`.
- Produces: `pub fn null_prop_variadic(g: &mut EGraph, class: Id) -> Result<Id, String>`, `pub fn err_prop_variadic(g: &mut EGraph, class: Id) -> Result<Id, String>` (consumed by Task 2's DSL rules).

- [ ] **Step 1: Write `null_prop_variadic`**

Transcribe `rules.rs:1013-1034` into the `scalar_builtins.rs` idiom (find eligible node via `scalar_class_nodes` + `find_map`, build the literal via `g.add(CNode::Scalar(SNode::Literal(row, col_type)))`, exactly like `null_prop_binary` at `:235`). Full body:

```rust
/// `f(.., null, ..) -> null` for a variadic `f` that propagates nulls, GATED on
/// every non-null-literal operand being error-free. The Rust RHS of the
/// `null_prop_variadic` declarative rule.
///
/// Mirrors `scalar/rules.rs::null_prop_variadic` exactly, including its
/// deliberate deviation from `reduce`: reduce's variadic null-prop is ungated on
/// the other operands, but eval surfaces an operand's error over null
/// (`eval(makets(null, 1/0))` is `Err`, not `Null`). Rewriting `f(.., null, ..)`
/// to null when another operand can error would turn an error into null. The gate
/// blocks that. This rule never fires on `And`/`Or`, which do not propagate nulls.
pub fn null_prop_variadic(g: &mut EGraph, class: Id) -> Result<Id, String> {
    let target = scalar_class_nodes(g, class).into_iter().find_map(|node| {
        let SNode::CallVariadic { func, exprs } = &node else {
            return None;
        };
        if !func.propagates_nulls() {
            return None;
        }
        if !exprs.iter().any(|&e| is_literal_null(g, e)) {
            return None;
        }
        // Every operand that is not itself a literal null must be error-free, else
        // that operand's error would surface instead of the propagated null.
        let other_can_error = exprs
            .iter()
            .any(|&e| !is_literal_null(g, e) && scalar_could_error(g, e));
        if other_can_error {
            return None;
        }
        Some(node.clone())
    });
    let Some(node) = target else {
        return Err("null_prop_variadic: no eligible CallVariadic node".to_string());
    };
    let ty = call_scalar_type(g, &node);
    let MirScalarExpr::Literal(row, col_type) = MirScalarExpr::literal_null(ty) else {
        unreachable!("MirScalarExpr::literal_null always builds a Literal variant")
    };
    Ok(g.add(CNode::Scalar(SNode::Literal(row, col_type))))
}
```

- [ ] **Step 2: Write `err_prop_variadic`**

Transcribe `rules.rs:1085-1111`. Full body:

```rust
/// `f(.., err_lit, ..) -> err_lit` for a variadic `f` that propagates nulls,
/// GATED on every other operand being error-free. The Rust RHS of the
/// `err_prop_variadic` declarative rule.
///
/// Mirrors `scalar/rules.rs::err_prop_variadic` exactly. Takes the FIRST literal
/// error by iteration order. A second literal-error operand is excluded from the
/// "other" set (via `literal_err(..).is_none()`), so it does not block the fire.
/// That is sound: eval is left-to-right via `?`, so the first error is the one a
/// full evaluation surfaces, and `const_fold` agrees on the all-literal case. The
/// `propagates_nulls` gate mirrors reduce's variadic err-prop, so this never fires
/// on `And`/`Or`.
pub fn err_prop_variadic(g: &mut EGraph, class: Id) -> Result<Id, String> {
    let target = scalar_class_nodes(g, class).into_iter().find_map(|node| {
        let SNode::CallVariadic { func, exprs } = &node else {
            return None;
        };
        if !func.propagates_nulls() {
            return None;
        }
        let err = exprs.iter().find_map(|&e| literal_err(g, e))?;
        // Every operand that is not itself a literal error must be error-free.
        let other_can_error = exprs
            .iter()
            .any(|&e| literal_err(g, e).is_none() && scalar_could_error(g, e));
        if other_can_error {
            return None;
        }
        Some((err, node.clone()))
    });
    let Some((err, node)) = target else {
        return Err("err_prop_variadic: no eligible CallVariadic node".to_string());
    };
    let ty = call_scalar_type(g, &node);
    let MirScalarExpr::Literal(row, col_type) = MirScalarExpr::literal(Err(err), ty) else {
        unreachable!("MirScalarExpr::literal always builds a Literal variant")
    };
    Ok(g.add(CNode::Scalar(SNode::Literal(row, col_type))))
}
```

- [ ] **Step 3: Add unit tests**

Mirror the four `null_prop_binary` tests (`scalar_builtins.rs:521-604`) and the `err_prop_binary` tests (`:606+`), adapted to `CallVariadic` on a null-propagating variadic. Use `MakeTimestamp` as the null-propagating variadic (see `rules.rs:2626-2696` `test_null_prop_variadic_*` for the `make_timestamp()` + operand-vector shape; reuse or add a `make_timestamp()` test helper and a `lit_f64`/`variadic`-builder helper in the `mod tests` block, following the existing `col`/`lit_int`/`lit_null_int`/`add64`/`div64`/`array_remove_func` helper pattern already in the test module). Cover, for each builtin:
  - fires on `makets(null, safe-column, lits..)` → typed null literal (null-prop) / fires on `makets(err_lit, safe..)` → that error literal (err-prop).
  - blocked when another operand can error (`makets(null, 1/c0, ..)` → `.is_err()`), the guard proof.
  - errs on a non-null-propagating variadic (find one with `propagates_nulls() == false`, e.g. `Coalesce` — assert the property in-test like `null_prop_binary_blocked_for_non_null_propagating_func` does).
  - errs on an inapplicable shape (a bare column class → `.is_err()`).

- [ ] **Step 4: `cargo check --tests -p mz-transform` and run the new tests**

Run: `bin/cargo-test -p mz-transform eqsat::scalar_builtins`
Expected: new tests PASS; no warnings on the new code.

- [ ] **Step 5: `bin/fmt`, then commit** (stage only `scalar_builtins.rs`).

---

### Task 2: Wire the two DSL rules

**Files:**
- Modify: `src/transform/src/eqsat/rules/scalar.rewrite` (add two rules after `err_prop_binary` at `:105`)

**Interfaces:**
- Consumes: `scalar_builtins::null_prop_variadic`, `err_prop_variadic` (Task 1).
- Produces: two new entries in `SCALAR_COMPILED_RULES` (24 → 26); `COMPILED_RULES` unchanged.

- [ ] **Step 1: Add the rules**

```
# A null-propagating variadic call with a literal-null operand folds to null when
# every other operand cannot error. Builtin. Permanent sorry.
rule null_prop_variadic {
    doc "f(.., null, ..) = null when f propagates nulls and every other operand cannot error"
    Scalar(e) => null_prop_variadic(e)
}

# A null-propagating variadic call with a literal-error operand folds to that error
# when every other operand cannot error. Builtin. Permanent sorry.
rule err_prop_variadic {
    doc "f(.., err, ..) = err when f propagates nulls and every other operand cannot error"
    Scalar(e) => err_prop_variadic(e)
}
```

- [ ] **Step 2: `cargo check -p mz-transform`** (build.rs regenerates the compiled rules; a missing/misspelled builtin name fails here).
Expected: clean build.

- [ ] **Step 3: Assert the rule counts**

Confirm `SCALAR_COMPILED_RULES.len() == 26` and `COMPILED_RULES.len()` unchanged (37). If there is an existing count assertion test, run it; otherwise verify via the generated output. Run: `bin/cargo-test -p mz-transform eqsat -- --list 2>/dev/null | head` is not required — instead run the existing scalar-rule count test if present (grep the test module for `SCALAR_COMPILED_RULES`), else add a one-line assertion to the slice-6b parity test in Task 4.

- [ ] **Step 4: `bin/fmt`, then commit** (stage only `scalar.rewrite`).

---

### Task 3: Lean — regenerate obligations + bump the trip-wire

**Files:**
- Regenerate: `src/transform/lean/MirRewrite/Generated.lean` (via `cargo run -p mz-transform --example gen-lean`)
- Modify: `ci/test/lean-mir-rewrite.sh:55` (`expected_permanent` 5 → 7, update comment)

**Interfaces:**
- Consumes: the two DSL rules (Task 2). The Lean emitter keys the permanent sorry on any `Tmpl::Builtin` RHS (`lean.rs:795`), so the two new obligations appear automatically. No hand-authored proof.

- [ ] **Step 1: Regenerate `Generated.lean`**

Run: `cargo run -p mz-transform --example gen-lean`
Expected: two new obligations for `null_prop_variadic` / `err_prop_variadic`, each with `-- PERMANENT SORRY: RHS is a Rust builtin`. Confirm the diff adds exactly two `PERMANENT SORRY` markers and no other semantic change.

- [ ] **Step 2: Bump the trip-wire**

In `ci/test/lean-mir-rewrite.sh`, change `expected_permanent=5` to `expected_permanent=7` and extend the comment to list `null_prop_variadic, err_prop_variadic`.

- [ ] **Step 3: Aggregate `lake build` clean rebuild + axiom check**

Run the trip-wire script (`ci/test/lean-mir-rewrite.sh`) or the aggregate `lake build` directly on a clean build. Confirm: build GREEN, `grep -rho "PERMANENT SORRY" MirRewrite | wc -l` == 7, no `sorryAx` in the non-permanent obligations (`#print axioms` on the changed theorems shows only the permanent sorries carry it, matching the binary null/err-prop pair).

- [ ] **Step 4: Commit** (stage only `Generated.lean` and `lean-mir-rewrite.sh`).

---

### Task 4: Differential corpus + parity + guard mutation-test + slice gate

**Files:**
- Modify: `src/transform/src/eqsat/scalar_saturate.rs` (add `scalar_parity_slice6b` + a termination test, mirroring `scalar_parity_slice6c` / `_slice6e`)
- Modify (if the corpus is a datadriven file): `src/transform/tests/testdata/eqsat_scalar_corpus` — add a `corpus_covers_slice6b` case following the existing slice pattern. (Note: the datadriven `test_runner` on this file has a KNOWN pre-existing line-4 parse failure unrelated to this slice; ignore it, it predates the branch.)

**Interfaces:**
- Consumes: the combined `canonicalize_combined` path and the standalone `scalar::canonicalize` oracle (see how `scalar_parity_slice6e` calls both and `assert_eq!` per term).

- [ ] **Step 1: Add `scalar_parity_slice6b`**

Mirror `scalar_parity_slice6e`. Build each case as a `MirScalarExpr`, run `canonicalize_combined` and the `scalar::canonicalize` oracle, `assert_eq!`. Cases (all with correct column types set):
  - null-prop fires: `makets(null, c0, 1, 0, 0, 0.0)` (safe operands) → typed null. (see `rules.rs:2631` for the operand vector.)
  - null at middle/trailing positions → typed null.
  - err-prop fires: `makets(1/0, c0, 1, 0, 0, 0.0)` with a literal `1/0` (Int64, well-typed for the year operand) and safe others → the DivisionByZero error literal.
  - **null-vs-error priority**: `makets(null, 1/0, 1, 0, 0, 0.0)` → the error, NOT null. Assert the combined result equals the oracle AND is an `Err` literal (not null).
  - blocked: `makets(null, 1/c0, 1, 0, 0, 0.0)` → call left intact (both engines agree, unchanged).
  - non-null-propagating variadic with a null/error operand → unchanged (neither rule fires).
  - **And/Or negative controls**: `And([null, true])`, `Or([false, (1/0)=(1/0)])` (Bool-typed error via the `(1/0)=(1/0)` idiom for the Bool connective) → assert `combined == scalar`, confirming neither engine 6b-folds them (they route through short-circuit / drop_unit / single instead).

- [ ] **Step 2: Guard mutation-test**

Add a dedicated test `null_prop_variadic_guard_blocks_erroring_operand` that builds `makets(null, 1/0, ..)` and asserts the combined result is the ERROR literal (not null), matching the oracle. Then MANUALLY mutation-test: temporarily delete the `other_can_error` gate in `scalar_builtins::null_prop_variadic`, rebuild, and confirm THIS test (and the null-vs-error priority parity case) fail EXACTLY there (combined wrongly folds to null vs oracle error). Restore the gate byte-identical. Record the observed failure in the report; do not commit the mutation.

- [ ] **Step 3: Termination test**

Add `null_err_prop_variadic_terminates` asserting saturation converges (iters ≤ 10) on a corpus mixing the above, mirroring `absorb_terminates` / `drop_unit_dedup_terminates`.

- [ ] **Step 4: Full slice gate**

Run, and record outputs in the report:
  - `bin/cargo-test -p mz-transform eqsat` — all pass (the one KNOWN pre-existing `test_runner` datadriven line-4 failure excepted; note it).
  - Relational goldens: `bin/sqllogictest --optimized -- <the 37 slt files>` with NO `--rewrite` — success == total (expect 1187/1187).
  - Aggregate `lake build` green, permanent count == 7 (from Task 3, re-confirm).

- [ ] **Step 5: `bin/fmt`, then commit** (stage only `scalar_saturate.rs` and, if touched, the corpus testdata file).

---

## Self-review notes

- Type consistency: `null_prop_variadic` / `err_prop_variadic` signatures match the binary builtins' `(g: &mut EGraph, class: Id) -> Result<Id, String>`; the DSL `Scalar(e) => NAME(e)` form and generic `Tmpl::Builtin` dispatch require no other wiring.
- The permanent-count move (5 → 7) is the one manual, no-CI-backstop step. Task 3 isolates it and Task 4 re-confirms it.
- No grammar/codegen/DSL-AST change: the builtin call parses and emits generically. If Task 2 fails to build, the cause is a builtin name mismatch, not missing wiring.
