---
source: src/expr/src/scalar/reduce/if_then.rs
revision: 4fe7ed31b7
---

# mz-expr::scalar::reduce::if_then

Post-order rewrites for `If` nodes, called from `reduce::reduce_post`.

## Entry point

`reduce_if(e, column_types)` requires `e` to be an `If { cond, then, els }` node. It applies the following rewrites in order:

### Constant condition

If `cond` is a literal, the expression collapses immediately:
- `True` → replace with `then`.
- `False` or `Null` → replace with `els`.
- Literal error → replace with a typed literal error (the type is the union of `then` and `els` types).

### Identical branches

If `then == els`, the condition is irrelevant and the expression is replaced with `then`.

### Boolean branch elimination

When both branches are non-null boolean literals, the `If` is rewritten into an equivalent boolean expression that avoids branching. SQL three-valued logic (NULL conditions must not propagate to the result) is preserved via explicit `IS NULL` guards:
- `IF cond THEN true ELSE els` → `((cond IS NOT NULL) AND cond) OR els`
- `IF cond THEN false ELSE els` → `((NOT cond) OR (cond IS NULL)) AND els`
- `IF cond THEN then ELSE true` → `(NOT cond) OR (cond IS NULL) OR then`
- `IF cond THEN then ELSE false` → `(cond IS NOT NULL) AND cond AND then`

### Function hoisting

When both branches apply the same function to differing subexpressions, the `If` is pushed inside the call so the function is called only once. Type-compatibility of the branches being hoisted is checked to avoid ill-typed expressions:
- Both branches are `CallUnary(f, _)` with the same `f` and matching input types → the inner `If` selects between the two arguments and `f` is applied to the result.
- Both branches are `CallBinary(f, e1, _)` sharing the same left argument → the inner `If` selects between the two right arguments.
- Both branches are `CallBinary(f, _, e2)` sharing the same right argument → the inner `If` selects between the two left arguments.
