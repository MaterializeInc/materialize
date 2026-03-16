---
source: src/transform/src/literal_constraints.rs
revision: 52af3ba2a1
---

# mz-transform::literal_constraints

Implements `LiteralConstraints`, which detects predicates of the form `<expr> = literal` on top of a `Get` and converts them into an `IndexedFilter` semi-join when a matching index exists.
The resulting plan performs a point-lookup against the index rather than a full scan, and an `IndexTooWideForLiteralConstraints` notice is emitted when a suitable but too-wide index is found.
This transform must run after `LiteralLifting` and before `JoinImplementation`.
