---
source: src/transform/src/normalize_lets.rs
revision: 0c487b44b0
---

# mz-transform::normalize_lets

Implements `NormalizeLets`, which reorganizes `Let` and `LetRec` nodes so that within each scope all bindings appear at the root in identifier order and use a contiguous block of IDs.
The `inline_mfp` flag, when `true`, inlines Map-Filter-Project operators wrapped around `Get` expressions, which is required immediately before `JoinImplementation`.
Also exports `renumber_bindings` for renumbering identifiers in an expression starting from a caller-supplied `IdGen`, used to prepare expressions for inlining.
