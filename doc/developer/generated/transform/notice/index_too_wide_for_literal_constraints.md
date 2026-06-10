---
source: src/transform/src/notice/index_too_wide_for_literal_constraints.rs
revision: e757b4d11b
---

# mz-transform::notice::index_too_wide_for_literal_constraints

Defines the `IndexTooWideForLiteralConstraints` optimizer notice, emitted when a candidate index exists but cannot be used for a literal equality lookup because its key contains more columns than the predicate requires.
Carries the index ID, the full key, the usable subset of key columns, the concrete literal values, and a recommended narrower key; renders a `CREATE INDEX` SQL statement as an actionable suggestion.
