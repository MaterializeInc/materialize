---
source: src/adapter/src/catalog/consistency.rs
revision: 9d0a7c3c6f
---

# adapter::catalog::consistency

Implements `CatalogState::check_consistency`, a diagnostic function that walks the entire in-memory catalog and verifies internal referential-integrity invariants (e.g. every item's schema exists, every cluster replica's cluster exists, all ID cross-references are symmetric).
Returns a `CatalogInconsistencies` struct listing all violations found; used by the `CheckConsistency` command and in tests.
