---
source: src/transform/src/notice/equals_null.rs
revision: 65a9c707f2
---

# mz-transform::notice::equals_null

Defines the `EqualsNull` optimizer notice, emitted when a query contains a `= NULL` or `<> NULL` comparison that always evaluates to `NULL`.
Implements `OptimizerNoticeApi` with a message, a hint to use `IS NULL` instead, and no associated action.
