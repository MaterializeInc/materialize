---
source: src/expr/src/scalar/func/impls/range.rs
revision: 703a0c27c8
---

# mz-expr::scalar::func::impls::range

Provides scalar function implementations for PostgreSQL range types: constructors, `lower`/`upper` extraction, containment (`@>`, `<@`), overlap (`&&`), union/intersection/difference, `isempty`, `lower_inc`/`upper_inc`, and casts to/from string.
