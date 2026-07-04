---
source: src/adapter/src/catalog/consistency.rs
revision: 699d823624
---

# adapter::catalog::consistency

Implements `CatalogState::check_consistency`, a diagnostic function that walks the entire in-memory catalog and verifies internal referential-integrity invariants (e.g. every item's schema exists, every cluster replica's cluster exists, all ID cross-references are symmetric).
Returns a `CatalogInconsistencies` struct listing all violations found; used by the `CheckConsistency` command and in tests.
Schema item consistency checks iterate over `schema.items`, `schema.types`, and `schema.functions` so that types and functions are verified for consistency alongside regular catalog items.
