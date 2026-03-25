---
source: src/transform/src/fusion/project.rs
revision: a5355b2e89
---

# mz-transform::fusion::project

Implements `Project` fusion: merges a `Project` with its immediate child when the child is also a `Project` or another fusible operator such as `Map`, `Filter`, `Negate`, or `Join`, thereby eliminating redundant intermediate projections.
