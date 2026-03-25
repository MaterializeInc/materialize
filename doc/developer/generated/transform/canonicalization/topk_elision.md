---
source: src/transform/src/canonicalization/topk_elision.rs
revision: e757b4d11b
---

# mz-transform::canonicalization::topk_elision

Implements `TopKElision`, which removes `TopK` operators that have no effect: if both offset is zero and there is no limit (or the limit is `NULL`), the operator is replaced by its input; if the limit is the literal `0`, the subtree is replaced with an empty constant.
