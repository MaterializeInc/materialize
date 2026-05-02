---
source: src/transform/src/notice/index_key_empty.rs
revision: e757b4d11b
---

# mz-transform::notice::index_key_empty

Defines the `IndexKeyEmpty` optimizer notice, emitted when an index is created with an empty key, which routes all data to a single worker and causes skew.
The notice recommends using `CREATE DEFAULT INDEX` instead and provides a plain-text action.
