---
source: src/transform/src/fusion.rs
revision: a5355b2e89
---

# mz-transform::fusion

Groups transforms that fuse consecutive like-kinded operators into one.
The top-level `Fusion` transform applies all per-operator fusion rules in a single post-order pass by dispatching to the individual submodule actions.
Submodules cover `Filter`, `Join`, `Map`, `Negate`, `Project`, `Reduce`, `TopK`, and `Union`; each provides both a standalone `Transform` implementation and a public `action` function for use by composite transforms such as `NormalizeOps` and `CanonicalizeMfp`.
