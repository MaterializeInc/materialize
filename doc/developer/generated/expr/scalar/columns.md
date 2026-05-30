---
source: src/expr/src/scalar/columns.rs
revision: 0b82784bb8
---

# mz-expr::scalar::columns

Defines the `Columns` trait, a generic interface for scalar expressions that reference row columns.

Implementors must provide `is_column`, `as_column`, `support_into`, and `visit_columns`. The remaining methods have default implementations built on those primitives:

- `support` — collects all referenced column indices into a `BTreeSet<usize>`. Callers can use `BTreeSet::last()` to extract the maximum referenced column.
- `support_into` — like `support`, but accumulates into a caller-supplied set to avoid allocation when building combined support across multiple expressions.
- `permute` — rewrites every column reference using a slice as a lookup table (`column[i] → permutation[i]`).
- `permute_map` — same as `permute` but accepts a `BTreeMap<usize, usize>` instead of a slice. Neither variant requires the mapping to be a strict permutation; only the columns actually referenced need entries.
- `visit_columns` — applies a mutable closure to every column index in the expression, forming the primitive used by both permute methods and any other column-rewriting pass.

`MirScalarExpr` implements `Columns` and re-exports the trait from `mz_expr::scalar`.
