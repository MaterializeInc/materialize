---
source: src/adapter/src/catalog/open/builtin_schema_migration_tests.rs
revision: 3a822d8457
---

# adapter::catalog::open::builtin_schema_migration_tests

Contains turmoil-based integration tests for `builtin_schema_migration`, verifying that schema migrations applied across multiple simulated versions and concurrent processes produce consistent results.
The main test generates random builtin tables and sources, evolves their schemas across versions with random migration steps (evolution or replacement), spawns multiple concurrent processes per version, and asserts that all processes converge on the same migration outcome despite random crashes.
`test_migration_steps_resolve_to_builtins` verifies that every step in the `MIGRATIONS` list names a builtin that currently exists with the declared `CatalogItemType`. A step naming a builtin that no longer exists or that changed type would panic `validate_migration_steps` at catalog open, which only the upgrade nightly catches; this unit test catches the same failure cheaply at CI time.
`hydration_history_forced_migration_policy` verifies that `mz_object_hydration_history` participates in a forced evolution migration but not in a forced replacement migration, confirming the exemption in `participates_in_forced_migration` that prevents accidental data loss when schema changes are force-applied during dev upgrades.
