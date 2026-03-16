---
source: src/transform/src/notice/index_already_exists.rs
revision: e757b4d11b
---

# mz-transform::notice::index_already_exists

Defines the `IndexAlreadyExists` optimizer notice, emitted when a newly created index is structurally identical to an existing index on the same object.
Carries the IDs of the duplicate index, the existing index, and the indexed object, and renders a human-readable message and hint via `OptimizerNoticeApi`.
