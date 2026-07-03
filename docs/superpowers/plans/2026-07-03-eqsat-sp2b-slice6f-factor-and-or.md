# SP2b Slice 6f: `factor_and_or` (builtin) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Port `factor_and_or` (`scalar/rules.rs:478`) into the CombinedLang DSL as ONE Rust builtin, behavior-neutral against the standalone `ScalarEGraph` oracle. Last scalar rule of SP2b.

**Architecture:** One `scalar_builtins::factor_and_or(g, class)` applier mirroring `null_prop_variadic`, wired by one rule `Scalar(e) => factor_and_or(e)`. Lean: one explicitly-named opaque `factorAndOr` with a third-category sorry comment (permanent 10 -> 11). Non-destructive fire-once; portable because both engines share extraction/cost.

**Tech Stack:** Rust (`mz-transform`), the eqsat DSL, Lean 4 (`lake`).

## Global Constraints

- Behavior-neutral. Never run sqllogictest with `--rewrite`. Production untouched. No production-default flag flips.
- ONE builtin, ONE rule, ONE opaque, ONE sorry (permanent 10 -> 11). Two builtins would be two sorries (12), contradicting the locked count and the "port exactly" directive.
- `colored: false`. No analysis-lattice additions (slice-3 freeze). Reuse the slice-3 `could_error` field for the residual gate.
- Port `rules.rs:478-565` + `inner_sets` (`rules.rs:414-433`) EXACTLY: full-intersection factoring, non-empty-residual condition, residual-error gate (common factor exempt), sorted branch ids for hashcons stability. Do not reinvent.
- Lean: factor gets its OWN named opaque `factorAndOr` + own `translate_tmpl` arm; NOT routed through 6d's `variadicOpaqueE` arm. Third-category sorry comment verbatim (see Task 3). Verify not absorbed.
- The func/op-metavar equality-guard prereq is CLOSED-MOOT; do NOT build it.
- Adding a trait method (none expected here) -> sweep ALL impls incl ColoredView; `cargo check --tests`, not bare.
- Error operands in tests: Bool-typed `(1/0)=(1/0)`, never bare Int64 (the 6c collapse trap).
- Lean image does not build in CI. Trip-wire bump (10 -> 11) and aggregate `lake build` are hand-run MANDATORY gates. Do not propose CI wiring.
- `doc/developer/generated/` read-only. Format `bin/fmt`, `cargo check` before reporting. Stage only your own files.

---

### Task 1: The `factor_and_or` builtin + unit tests

**Files:**
- Modify: `src/transform/src/eqsat/scalar_builtins.rs` (add `pub fn factor_and_or` after the variadic builtins; add unit tests in `mod tests`)

**Interfaces:**
- Consumes (existing module helpers): `scalar_class_nodes(g, id) -> Vec<SNode>`, `scalar_could_error(g, id) -> bool`. `SNode::CallVariadic { func, exprs }`, `CNode::Scalar`, `g.find`, `g.nodes`, `g.add`. `mz_expr::VariadicFunc` with `.switch_and_or()`.
- Produces: `pub fn factor_and_or(g: &mut EGraph, class: Id) -> Result<Id, String>`.

- [ ] **Step 1: Write `factor_and_or`.** Transcribe `rules.rs:478-565` + `inner_sets` (`rules.rs:414-433`) into the `scalar_builtins` idiom (`scalar_class_nodes(g, class).into_iter().find_map(...)` to find the eligible node, `g.add(CNode::Scalar(SNode::CallVariadic { .. }))` to build, `Err(..)` on no fire). Add a private `is_and_or` mirror and a local `inner_sets`. Full body:

```rust
/// `(a∧b)∨(a∧c) -> a∧(b∨c)` and the dual: undistribute a common factor out of an
/// AND/OR, residual-error gated. The Rust RHS of the `factor_and_or` builtin rule.
///
/// Mirrors `scalar/rules.rs::factor_and_or` exactly: full-intersection factoring
/// (the factor is the inner-set intersection common to EVERY branch), with the
/// non-empty-residual condition (an empty residual is the absorption case, left to
/// `absorb`) and the residual-error gate (every residual operand must be error-free;
/// the common factor MAY error and is deliberately exempt, the CLU-137 narrowing).
/// Non-destructive: builds and returns the factored form for the caller to union in,
/// so extraction picks the cheaper form. Fires only when the factored form is
/// well-formed; otherwise `Err` (no union), mirroring the old engine's `vec![]`.
pub fn factor_and_or(g: &mut EGraph, class: Id) -> Result<Id, String> {
    let target = scalar_class_nodes(g, class).into_iter().find_map(|node| {
        let SNode::CallVariadic { func, exprs } = &node else {
            return None;
        };
        if !is_and_or(func) || exprs.len() < 2 {
            return None;
        }
        Some((func.clone(), exprs.clone()))
    });
    let Some((outer_func, outer_operands)) = target else {
        return Err("factor_and_or: no eligible And/Or node".to_string());
    };
    let inner_func = outer_func.switch_and_or();

    // Each outer operand's inner-operand set under `inner_func` (canonical, sorted,
    // unique), or a singleton `{find(operand)}` if it holds no such call.
    let branch_sets: Vec<Vec<Id>> = outer_operands
        .iter()
        .map(|&operand| {
            let canon = g.find(operand);
            for sibling in g.nodes(canon) {
                if let CNode::Scalar(SNode::CallVariadic { func, exprs }) = sibling {
                    if func == inner_func {
                        let mut ids: Vec<Id> = exprs.iter().map(|&e| g.find(e)).collect();
                        ids.sort();
                        ids.dedup();
                        return ids;
                    }
                }
            }
            vec![canon]
        })
        .collect();

    // The intersection common to every branch (built from branch 0, kept sorted/unique).
    let mut intersection = branch_sets[0].clone();
    for set in &branch_sets[1..] {
        intersection.retain(|id| set.contains(id));
    }
    if intersection.is_empty() {
        return Err("factor_and_or: no common factor".to_string());
    }

    // Each branch's residual; an empty residual is the absorption case (deferred).
    let mut residuals: Vec<Vec<Id>> = Vec::with_capacity(branch_sets.len());
    for set in &branch_sets {
        let residual: Vec<Id> = set
            .iter()
            .copied()
            .filter(|id| !intersection.contains(id))
            .collect();
        if residual.is_empty() {
            return Err("factor_and_or: empty residual (absorption case)".to_string());
        }
        residuals.push(residual);
    }

    // Residual-error gate: every residual operand must be provably error-free. The
    // common factor is intentionally exempt (never moved past a masking value).
    for residual in &residuals {
        if residual.iter().any(|&id| scalar_could_error(g, id)) {
            return Err("factor_and_or: residual can error".to_string());
        }
    }

    // Build the factored form. Sort branch ids so the combination node is stable
    // across re-firing (hashcons keys on the operand vector).
    let mut branch_ids: Vec<Id> = residuals
        .into_iter()
        .map(|residual| {
            if residual.len() == 1 {
                residual[0]
            } else {
                g.add(CNode::Scalar(SNode::CallVariadic {
                    func: inner_func.clone(),
                    exprs: residual,
                }))
            }
        })
        .collect();
    branch_ids.sort();
    let residual_combination = g.add(CNode::Scalar(SNode::CallVariadic {
        func: outer_func,
        exprs: branch_ids,
    }));
    let mut factored_exprs = intersection;
    factored_exprs.push(residual_combination);
    Ok(g.add(CNode::Scalar(SNode::CallVariadic {
        func: inner_func,
        exprs: factored_exprs,
    })))
}

/// An And or Or variadic (the connectives `factor_and_or` distributes over).
fn is_and_or(func: &mz_expr::VariadicFunc) -> bool {
    matches!(
        func,
        mz_expr::VariadicFunc::And(_) | mz_expr::VariadicFunc::Or(_)
    )
}
```
(NOTE: verify `switch_and_or` is `pub` on `VariadicFunc` — `variadic.rs:1757` — and that the `CNode::Scalar(SNode::CallVariadic { func, exprs })` node-scan matches the real types used by `null_prop_variadic` in this file. Adjust the `func == inner_func` comparison to match the borrow shape `g.nodes` yields, as `inner_set` does in `rest_filters.rs`.)

- [ ] **Step 2: Unit tests** in `scalar_builtins.rs mod tests`, mirroring the `null_prop_variadic` tests. Build `EGraph` classes directly (`g.add(CNode::Scalar(SNode::CallVariadic { .. }))`):
  - fires on `Or(And(c0,c1), And(c0,c2))` -> returns an `And`-rooted node whose operands are `c0` and an `Or(c1,c2)` (assert the returned node's structure via `scalar_class_nodes`); dual `And(Or..)` case.
  - `Err` on no common factor (`Or(And(c0,c1), And(c2,c3))`).
  - `Err` on empty residual / absorption (`Or(And(c0,c1), And(c0,c1,c2))` -> intersection `{c0,c1}`, first branch residual empty).
  - `Err` on erroring residual (residual operand with `could_error`).
  - `Err` on an inapplicable shape (bare column class; single-branch `Or`).

- [ ] **Step 3: `cargo check --tests -p mz-transform` + run the new tests.** `bin/cargo-test -p mz-transform eqsat::scalar_builtins`. New tests pass, no warnings on the new code.

- [ ] **Step 4: `bin/fmt`, commit** (stage only `scalar_builtins.rs`).

---

### Task 2: Wire the rule

**Files:**
- Modify: `src/transform/src/eqsat/rules/scalar.rewrite`

**Interfaces:**
- Consumes: `scalar_builtins::factor_and_or`. Produces: one new `SCALAR_COMPILED_RULES` entry (31 -> 32); `COMPILED_RULES` unchanged (37).

- [ ] **Step 1: Add the rule** (after the flatten rules), mirroring `null_prop_variadic`:
```
# Undistribute a common factor from an And/Or: (a∧b)∨(a∧c) = a∧(b∨c) and dual.
# Full-intersection factoring, residual-error gated. Builtin. Permanent sorry.
rule factor_and_or {
    doc "(a∧b)∨(a∧c) = a∧(b∨c) and dual: undistribute a common factor, residual-error gated"
    Scalar(e) => factor_and_or(e)
}
```

- [ ] **Step 2: `cargo check -p mz-transform`** (build.rs regenerates; a missing/misspelled builtin name fails here). Clean build.

- [ ] **Step 3: Confirm counts.** `SCALAR_COMPILED_RULES` 31 -> 32, `COMPILED_RULES` unchanged (37). `bin/cargo-test -p mz-transform eqsat 2>&1 | tail -20`, no failures introduced (the known corpus `test_runner` line-4 failure predates the branch, ignore).

- [ ] **Step 4: `bin/fmt`, commit** (stage only `scalar.rewrite`).

---

### Task 3: Lean — named opaque + third-category sorry + trip-wire 10 -> 11

**Files:**
- Modify: `src/transform/lean/MirRewrite/Semantics.lean` (`opaque factorAndOr`)
- Modify: `src/transform/src/eqsat/lean.rs` (`translate_tmpl` arm + `choose_proof` factor arm)
- Regenerate: `src/transform/lean/MirRewrite/Generated.lean` (via `cargo run -p mz-transform --example gen-lean`)
- Modify: `ci/test/lean-mir-rewrite.sh` (`expected_permanent` 10 -> 11, comment)

**Interfaces:**
- Consumes: the rule (Task 2). The Lean emitter is NOT generic on builtin name (`translate_tmpl` panics on an unmapped builtin, `scalar_variadic_ctor` precedent), so the arm + opaque decl are required.

- [ ] **Step 1: Named opaque decl.** In `Semantics.lean`, add near the other opaque scalar builtins (`constEval`, `nullPropVariadic`):
```lean
/-- Distributive factoring `(a∧b)∨(a∧c) = a∧(b∨c)` (and dual). Computed by the
    Rust builtin `factor_and_or`. Distributivity IS provable in this two-valued
    model, so this opaque is a REPRESENTATION artifact of the builtin RHS, not a
    genuine gap. It is dischargeable by declarativizing the rule. Rules whose RHS
    is `factorAndOr` carry a permanent `sorry` for that representation reason. -/
opaque factorAndOr : ScalarExpr → ScalarExpr
```

- [ ] **Step 2: `translate_tmpl` arm.** In `lean.rs` (the `Tmpl::Builtin` per-name match, ~lean.rs:534), add:
```rust
            "factor_and_or" => format!("factorAndOr {}", args[0]),
```

- [ ] **Step 3: `choose_proof` third-category arm.** In `lean.rs`, BEFORE the generic `if is_builtin_rhs` arm (lean.rs:826), add a factor-specific arm keyed on the rendered RHS so factor gets the third-category comment, NOT the generic builtin text and NOT the `variadicOpaqueE` text:
```rust
        // factor_and_or: the THIRD sorry category. Distributivity IS provable in
        // the two-valued Bool model; this sorry is purely a representation artifact
        // of the builtin RHS (the fact is not expressed declaratively), and is
        // dischargeable by declarativizing the rule. It is neither opaque-computation
        // (const_fold, eval-dependent) nor outside-value-domain (6d non-Bool flatten).
        if rhs.contains("factorAndOr") {
            return "by\n    -- PERMANENT SORRY: distributivity IS provable in the Bool model; this sorry is a\n    -- representation artifact of the builtin RHS (not declaratively expressed),\n    -- dischargeable by declarativizing. NOT opaque-computation (const_fold,\n    -- eval-dependent) NOR outside-value-domain (6d non-Bool flatten).\n    sorry".to_string();
        }
```
(Confirm factor's rendered LHS is a plain var `e`, so the `variadicOpaqueE` arm at lean.rs:848 is never reached for this rule.)

- [ ] **Step 4: Regenerate + inspect.** `cargo run -p mz-transform --example gen-lean`. Confirm: exactly ONE new obligation `rule_factor_and_or`, carrying the third-category comment (the four `--` comment lines + `sorry`), NOT the generic "RHS is a Rust builtin" text and NOT the `variadicOpaqueE` "non-Bool variadic" text. Exactly one new `PERMANENT SORRY` marker.

- [ ] **Step 5: Bump trip-wire.** `ci/test/lean-mir-rewrite.sh`: `expected_permanent=10` -> `11`; extend the comment to list `factor_and_or` and note the third category (provable-in-Bool-model-but-builtin-shaped, dischargeable by declarativizing).

- [ ] **Step 6: Aggregate `lake build` + count.** `bash ci/test/lean-mir-rewrite.sh`. Confirm: build GREEN, `grep -rho "PERMANENT SORRY" src/transform/lean/MirRewrite | wc -l` == 11, script exit 0. If the host lacks the Lean toolchain, use the script's Docker path and report exactly what ran; do not claim a green build you did not observe.

- [ ] **Step 7: Commit** (stage `Semantics.lean`, `lean.rs`, `Generated.lean`, `lean-mir-rewrite.sh`).

---

### Task 4: Differential parity + termination-under-saturation + slice gate

**Files:**
- Modify: `src/transform/src/eqsat/scalar_saturate.rs` (`scalar_parity_slice6f`, `factor_terminates`)
- Modify (if used): `src/transform/tests/testdata/eqsat_scalar_corpus` (`corpus_covers_slice6f`)

**Interfaces:**
- Consumes: `canonicalize_combined` + the standalone `scalar::canonicalize` oracle (see `scalar_parity_slice6e`).

- [ ] **Step 1: `scalar_parity_slice6f`.** Mirror `scalar_parity_slice6e`: build each case, run BOTH engines, `assert_eq!` per case. Cases (types set):
  - Fires: `Or(And(c0,c1), And(c0,c2))` -> `And(c0, Or(c1,c2))`; dual `And(Or..)`; n-ary 3-branch sharing a factor; common factor at a non-leading position inside the inner nodes.
  - Does NOT fire: no common factor; partial-share (some branches lack the factor); mixed inner connectives; single branch. Assert `combined == oracle` (both unchanged).
  - Residual-error gate: erroring residual blocks (combined == oracle, outer connective intact, NOT the unsound factored form); erroring common factor fires (CLU-137). Bool-typed `(1/0)=(1/0)` errors.
  - Cascade: factor feeding / fed-by flatten, short-circuit, drop_unit, dedup, absorb, single, empty -> each `combined == oracle` through the cascade.

- [ ] **Step 2: `factor_terminates` (fork 4).** Assert saturation converges (iters <= a small bound, mirror `flatten_terminates`) over factorable nesting. Confirm fire-once (factor does not re-fire on its own output) and no ping-pong with flatten. If it does not converge, STOP and report (that is the slice-7 blocker the recon judged absent).

- [ ] **Step 3: Corpus coverage.** If prior slices added `corpus_covers_slice6X` to `tests/testdata/eqsat_scalar_corpus`, add `corpus_covers_slice6f` in the same pattern. (Known pre-existing `test_runner` line-4 failure predates the branch; ignore.)

- [ ] **Step 4: Full gate.** Record every command + output in the report:
  - `bin/cargo-test -p mz-transform eqsat` — all pass (except the known corpus `test_runner` line-4).
  - Relational goldens, NO `--rewrite`: run the eqsat slt set prior slices used (grep the ledger; prior runs 1187/1187 across 37 files). Confirm success == total.
  - `bash ci/test/lean-mir-rewrite.sh` exit 0, permanent count == 11 (re-confirm Task 3).

- [ ] **Step 5: `bin/fmt`, commit** (stage only `scalar_saturate.rs` and, if touched, the corpus fixture).

---

## Self-review notes

- ONE builtin, ONE rule, ONE opaque, ONE sorry -> permanent 10 -> 11 (the locked count). The "two builtins" phrasing in the prompt is superseded by the count constraint + the one-fn old engine.
- The third-category sorry is honest: distributivity IS provable in the Bool model; the sorry is a representation artifact of the builtin choice, dischargeable by declarativizing. The comment says exactly this.
- factor's opaque is explicitly named (`factorAndOr`) and matched in `choose_proof` BEFORE the generic builtin arm, so it is never silently absorbed by the generic or `variadicOpaqueE` arms.
- Fork 4 (termination) and fork 5 (parity) are confirmed by Task 4's tests, not assumed. If either fails, STOP and report the slice-7 blocker.
