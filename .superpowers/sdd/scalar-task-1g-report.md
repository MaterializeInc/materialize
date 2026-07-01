# Task 1g report: scalar If-error-condition rule

## Status

DONE

## What was done

Added the `if_err_cond` rule to
`src/transform/src/eqsat/scalar/rules.rs`. The rule matches
`SNode::If { cond, then, els }` when `literal_err(eg, *cond)` is `Some(err)`
and returns `SNode::Literal(Err(err), result_ty)` where `result_ty` is
`then_ty.union(&els_ty)`. It is registered in the `rules()` list after
`if_same_branches`.

## ReprColumnType::union return shape

`ReprColumnType::union(&self, col: &ReprColumnType) -> Result<ReprColumnType, anyhow::Error>`

It returns `Result`, not `Option`. The `else` guard in the rule uses
`let Ok(result_ty) = then_ty.union(&els_ty) else { return vec![]; }`.

## Tests

Four new tests, all green (56 total, 0 failures):

* `test_if_err_cond_fires`: `if(1/0, c0, c1)` with two nullable Int64 columns.
  After saturation, `1/0` const-folds to `Literal(Err(DivisionByZero), Int64)`,
  so `if_err_cond` fires. Result is `Literal(Err(DivisionByZero), _)`.
  Differential over three `(c0, c1)` rows confirms both input and output eval
  to `Err(DivisionByZero)`.
* `test_if_err_cond_branch_type_union`: `if(1/0, c0::nullable, 5::non-null)`.
  No panic, result is error literal with nullable column type (union of
  nullable and non-null is nullable).
* `test_if_non_err_cond_not_affected`: `if(col, c1, c1)` resolves via
  `if_same_branches`, not an error literal.
* `test_if_true_false_rules_still_fire_regression`: `if(true,...)` and
  `if(false,...)` still resolve to the correct branch (1d rules unbroken).

## Concerns

None. The rule is a straightforward port of `reduce_if`'s `Err(err)` arm.
The only deviation from reduce is the conservative fallback (`return vec![]`)
on union failure instead of `unwrap()`, which should never be reached in
well-typed input.
