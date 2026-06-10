---
source: src/transform/src/union_cancel.rs
revision: e757b4d11b
---

# mz-transform::union_cancel

Implements `UnionBranchCancellation`, which detects pairs of branches in a `Union` where one is the negation of the other and cancels them out, removing both branches.
The cancellation is safe in recursive (`LetRec`) contexts because both branches must share the same binding scope.
