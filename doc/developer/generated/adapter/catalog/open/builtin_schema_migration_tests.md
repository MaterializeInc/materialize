---
source: src/adapter/src/catalog/open/builtin_schema_migration_tests.rs
revision: ea77b8b38b
---

# adapter::catalog::open::builtin_schema_migration_tests

Contains turmoil-based integration tests for `builtin_schema_migration`, verifying that schema migrations applied across multiple simulated versions and concurrent processes produce consistent results.
The main test generates random builtin tables and sources, evolves their schemas across versions with random migration steps (evolution or replacement), spawns multiple concurrent processes per version, and asserts that all processes converge on the same migration outcome despite random crashes.
