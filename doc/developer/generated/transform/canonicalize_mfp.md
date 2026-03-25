---
source: src/transform/src/canonicalize_mfp.rs
revision: a5355b2e89
---

# mz-transform::canonicalize_mfp

Implements `CanonicalizeMfp`, which extracts and optimizes any Map-Filter-Project sequence from a subtree and rebuilds it in canonical order: `Map` then `Filter` then `Project`.
As part of canonicalization it performs CSE on the scalar expressions within the MFP via `MapFilterProject::optimize`, and calls `fusion::filter::Filter::action` to canonicalize predicates.
Identity MFPs are dropped, and the transform recurses through `Negate` operators by pushing the MFP past them.
