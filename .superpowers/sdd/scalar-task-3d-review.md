# Task 3d review: IsNull-of-non-nullable fold

Reviewer: independent code review agent
Files read: `src/transform/src/eqsat/scalar/rules.rs` (impl + tests),
`src/transform/src/eqsat/scalar/analysis.rs` (analysis struct + merge),
`src/transform/src/eqsat/scalar/raise.rs` (raise::raise signature),
`src/transform/src/eqsat/scalar/egraph.rs` (col_types accessor),
`src/expr/src/scalar/reduce.rs:50-65` (reduce_pre IsNull arm).

---

## Spec compliance

- [x] **Both gates present** (`rules.rs:1133` could_error, `rules.rs:1138-1139` nullable) — checked in the correct order, both on the operand `*expr` not the IsNull node
- [x] **could_error gate reads operand** `eg.analysis(*expr).could_error` (`rules.rs:1133`); not the outer IsNull class
- [x] **Nullable gate reads operand** `raise::raise(eg, *expr).typ(eg.col_types()).nullable` (`rules.rs:1138`); same `col_types` reduce uses
- [x] **False literal** `eg.add(destructure_literal(MirScalarExpr::literal_false()))` (`rules.rs:1142`); consistent with other literal-building rules
- [x] **Borrow discipline** both reads produce owned primitives (`bool`) before `eg.add()`; the chain `raise::raise(eg, *expr).typ(eg.col_types()).nullable` returns `bool` and drops all intermediates before line 1142
- [x] **Registered once** `rules.rs:56`, last in the array, after err-prop rules
- [x] **Only IsNull matched** `rules.rs:1128` matches `UnaryFunc::IsNull(_)` exclusively
- [x] **decompose_is_null deferred** noted in doc comment `rules.rs:1122-1123`; not attempted
- [x] **Doc comment complete** covers: the rewrite, nullability source + regression-case motivation, could_error gate + why (IsNull propagates operand error), deviation from reduce, scope deferral
- [x] **No `as` casts** in the new code
- [x] **No em-dashes or clause-joining semicolons** in new comments or tests

---

## Soundness verdict

Both gates are present, correctly positioned, and read from the operand class:

1. `eg.analysis(*expr).could_error` (`rules.rs:1133`): the analysis struct for `CallUnary` is `func.could_error() || child.could_error` (`analysis.rs:70`). For `IsNull` over a column, the column class has `could_error = false` (`analysis.rs:56`); over `1/c0`, the div class has `could_error = true` (DivInt64 `func.could_error()` is true, `analysis.rs:78`). The gate is a sound over-approximation: `could_error == false` means no node in the class can error. Gate is trustworthy.

2. `raise::raise(eg, *expr).typ(eg.col_types()).nullable` (`rules.rs:1138`): `raise` reconstructs the operand `MirScalarExpr`, `.typ()` is called with `eg.col_types()` (the same slice stored at e-graph construction time, mirroring reduce's `column_types` argument). `nullable == false` is an over-approximation: the type system guarantees the value is never null when this flag is false. This matches reduce's gate `!expr.typ(column_types).nullable` exactly.

When both gates pass, `x` is never null and never errors, so `IsNull(x)` is always `false`. The fold is exact-eval sound. Folding when either gate fails would be unsound (null case: IsNull returns `true`; error case: IsNull propagates `Err`, not `false`). Both unsound cases are excluded.

---

## Test adequacy

All four tests are non-vacuous:

**test_isnull_fold_fires_on_non_nullable_col** (`rules.rs:2821`): col_types marks c0 non-nullable; `canonicalize` must produce `literal_false()`; differential over non-null int rows confirms eval agreement.

**test_isnull_fold_blocked_on_nullable_col** (`rules.rs:2841`): `int_col_types()` = `Int64.nullable(true)` (`rules.rs:2508-2509`); fold must not fire; differential includes `Datum::Null` where `IsNull(null) = true`, which would differ from `false`. This proves the fold would be observable and wrong if the gate were absent.

**test_isnull_fold_blocked_when_operand_can_error** (`rules.rs:2866`): `IsNull(1/c0)` with c0 non-nullable. `1/c0` is non-nullable-typed but `could_error == true` (DivInt64). The test asserts: (a) output is not `false`, (b) output is still an IsNull call, (c) differential at c0=0 and c0=5 agrees, (d) explicit assertion `eval_owned(&expr, &[Datum::Int64(0)]).is_err()` proves the hazard. Non-vacuous: a pure nullability check (without `could_error` gate) would fold to `false` here, which would fail the differential at c0=0.

**test_isnull_fold_composition_not_is_not_null** (`rules.rs:2905`): `NOT(IsNull(c0))` with `Bool.nullable(false)` -> `literal_true()`; differential over `[Datum::True, Datum::False]`. Exercises the `isnull_fold` + `not_not` + const-fold chain, mirroring the regression predicate `(#1) IS NOT NULL`.

Implementer reports 87 scalar tests pass (was 83), including `test_canonicalize_eval_differential` (300 cases). All four requirements from the brief are covered.

---

## Issues

None found. No Critical, Important, or Minor issues.

Strengths: the rule is short (19 lines), the gate ordering is correct (cheap analysis read before the more expensive `raise` call), the doc comment is complete and well-structured, test 3's explicit `is_err()` assertion makes the unsoundness proof concrete rather than implicit, and the borrow discipline is textbook-correct.

---

## Assessment

**Task quality: Approved.**

The implementation is sound, exact-eval correct, and strictly more conservative than `reduce`. Both gates are present and applied to the operand class. The nullability source mirrors reduce exactly. Borrow discipline is clean. All four specified tests are non-vacuous and cover the critical cases. Doc comment meets the spec. No `as` casts, no em-dashes, no clause-joining semicolons.
