---
source: src/transform/src/cse/relation_cse.rs
revision: e757b4d11b
---

# mz-transform::cse::relation_cse

Implements `RelationCSE`, which identifies common relation subexpressions and places them behind `Let` bindings by composing `ANF` and `NormalizeLets`.
The `inline_mfp` flag is forwarded to `NormalizeLets`; setting it `true` is required for the pass that runs immediately before `JoinImplementation` so that MFPs around `Get`s are inlined before join planning.
