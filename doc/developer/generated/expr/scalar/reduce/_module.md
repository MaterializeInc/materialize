---
source: src/expr/src/scalar/reduce.rs
revision: fc2aaf02e7
---

# mz-expr::scalar::reduce

Rewrite-rule-driven simplification of `MirScalarExpr`. This module owns the top-level fixed-point loop and the pre/post-pass dispatch; the per-variant rules live in its private submodules.

## Entry point

`reduce(expr, column_types)` drives simplification by repeatedly calling `visit_mut_pre_post` until the expression stops changing. Each iteration applies:

1. **Pre-order pass** (`reduce_pre`) — fires before children are visited. Handles `IsNull` and `Not` because these rules push themselves inward and the result at the current position must then be visited again normally:
   - `IsNull` on a non-nullable expression → `false`.
   - `IsNull` on a compound expression → attempts `decompose_is_null` to split into a disjunction of simpler `IsNull` calls.
   - `Not(Not(x))` → `x`.
   - `Not(a <op> b)` → `a negate(<op>) b` when the binary function has a negation.
   - `Not(And/Or(...))` → De Morgan rewrite via `demorgans`.

2. **Post-order pass** (`reduce_post`) — fires after children are fully reduced. Dispatches by variant to the submodule entry points:
   - `CallUnary` → `unary::reduce_call_unary`
   - `CallBinary` → `binary::reduce_call_binary`
   - `CallVariadic` → `variadic::reduce_call_variadic`
   - `If` → `if_then::reduce_if`
   - Terminal variants (`Column`, `Literal`, `CallUnmaterializable`) are left unchanged.

## Submodules

- `binary` — post-order rewrites for `CallBinary` nodes.
- `if_then` — post-order rewrites for `If` nodes.
- `unary` — post-order rewrites for `CallUnary` nodes.
- `variadic` — post-order rewrites for `CallVariadic` nodes.
