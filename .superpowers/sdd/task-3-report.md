# Task 3 report: Lean — named opaque + third-category sorry + trip-wire 10 -> 11

Status: COMPLETE. Aggregate Docker `lake build` observed GREEN (exit 0), PERMANENT
SORRY count == 11.

## Refinement 1: own explicitly-named opaque decl + translate_tmpl arm

`Semantics.lean` (after `errPropVariadic`, ~line 547):

```lean
/-- Distributive factoring `(a∧b)∨(a∧c) = a∧(b∨c)` (and dual). Computed by the
    Rust builtin `factor_and_or`. Distributivity IS provable in this two-valued
    model, so this opaque is a REPRESENTATION artifact of the builtin RHS, not a
    genuine gap. It is dischargeable by declarativizing the rule. Rules whose RHS
    is `factorAndOr` carry a permanent `sorry` for that representation reason. -/
opaque factorAndOr : ScalarExpr → ScalarExpr
```

`lean.rs` `translate_tmpl` `Tmpl::Builtin` per-name match (after `err_prop_variadic`):

```rust
"factor_and_or" => format!("factorAndOr {}", args[0]),
```

Explicitly NAMED + MATCHED opaque. NOT routed through 6d's `variadicOpaqueE`.

## Refinement 2: third-category sorry arm in choose_proof

Added in `choose_proof` inside `if is_scalar {`, placed BEFORE the generic
`if is_builtin_rhs` arm (previously at lean.rs:827, now follows this new arm), keyed
on `rhs.contains("factorAndOr")`. Returns exactly the four `--` lines + `sorry`.
The arm carries a `//` code comment naming it the THIRD category (provable-in-Bool
but builtin-shaped, dischargeable by declarativizing; distinct from opaque-computation
and outside-value-domain).

Placement / non-absorption confirmed:
- BEFORE the generic `is_builtin_rhs` arm, so factor does NOT get the generic
  "RHS is a Rust builtin" text.
- The rule LHS is `Scalar(e)` (scalar.rewrite:210-212), which renders to a plain
  var `e`. It never contains `ScalarExpr.variadicOpaqueE`, so the `variadicOpaqueE`
  arm (lean.rs:849) is never reached for this rule. Factor is NOT absorbed by the
  "non-Bool variadic" text.

## Refinement 3: trip-wire 10 -> 11

`ci/test/lean-mir-rewrite.sh`: `expected_permanent=10` -> `11`; comment extended to
list `factor_and_or` and its third category (distributivity provable in the Bool
model but riding a builtin RHS, dischargeable by declarativizing, distinct from
opaque-computation and outside-value-domain).

## Generated.lean diff (regen via `cargo run -p mz-transform --example gen-lean`)

Exactly ONE new obligation, exactly ONE new PERMANENT SORRY marker (10 -> 11):

```lean
-- (a∧b)∨(a∧c) = a∧(b∨c) and dual: undistribute a common factor, residual-error gated
theorem rule_factor_and_or :
    ∀ (env : Nat → Bool) (e : ScalarExpr), denoteS env e = denoteS env (factorAndOr e) := by
    -- PERMANENT SORRY: distributivity IS provable in the Bool model; this sorry is a
    -- representation artifact of the builtin RHS (not declaratively expressed),
    -- dischargeable by declarativizing. NOT opaque-computation (const_fold,
    -- eval-dependent) NOR outside-value-domain (6d non-Bool flatten).
    sorry
```

LHS is the plain var `e` (RHS `factorAndOr e`). NOT the generic "RHS is a Rust
builtin" text and NOT the `variadicOpaqueE` "non-Bool variadic" text.

## Gate commands and output tails

- `cargo run -p mz-transform --example gen-lean` — wrote Generated.lean, no error.
- `grep -c "PERMANENT SORRY" Generated.lean` — 10 before, 11 after.
- `grep -rho "PERMANENT SORRY" src/transform/lean/MirRewrite | wc -l` — 11.
- `bash ci/test/lean-mir-rewrite.sh` — Docker daemon up and used (builds the pinned
  Lean-toolchain image, then `lake build` over the bind-mounted lib). Output tail:
  `✔ [4/5] Built MirRewrite` / `Build completed successfully.` / exit 0. The permanent
  count guard passed (script would exit 1 on != 11). Only `sorry`/unused-variable
  linter warnings, no errors.
- `bin/fmt` — all tasks successful (did not alter Generated.lean beyond the regen).
- `cargo check -p mz-transform` — clean.

Nothing was left un-run locally: the Docker `lake build` path executed and was
observed GREEN.

## Staged files

`Semantics.lean`, `lean.rs`, `Generated.lean`, `lean-mir-rewrite.sh` only.

## Concerns

None blocking. The permanent-sorry guard has no CI backstop (owner-confirmed,
hand-run), consistent with prior slices.
