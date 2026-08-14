---
source: src/adapter/src/catalog/open/builtin_schema_migration_tests.rs
revision: 57107078dc
---

# adapter::catalog::open::builtin_schema_migration_tests

Contains turmoil-based integration tests for `builtin_schema_migration`, verifying that schema migrations applied across multiple simulated versions and concurrent processes produce consistent results.
The main test generates random builtin tables and sources, evolves their schemas across versions with random migration steps (evolution or replacement), spawns multiple concurrent processes per version, and asserts that all processes converge on the same migration outcome despite random crashes.
`test_migration_steps_resolve_to_builtins` verifies that every step in the `MIGRATIONS` list names a builtin that currently exists with the declared `CatalogItemType`. A step naming a builtin that no longer exists or that changed type would panic `validate_migration_steps` at catalog open, which only the upgrade nightly catches; this unit test catches the same failure cheaply at CI time.
