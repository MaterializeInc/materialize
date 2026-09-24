---
source: src/testdrive/src/action/consistency.rs
revision: 08ae3e9df0
---

# testdrive::action::consistency

Implements post-test consistency checks against a live Materialize environment.
`Level` controls when checks run: after each file (default), after each statement (for debugging), or disabled entirely.
`run_consistency_checks` calls three sub-checks in sequence — coordinator internal consistency via its HTTP API, in-memory vs. on-disk catalog state, and statement-logging completeness — and aggregates all failures into a single error.
`check_catalog_state` returns early without making any HTTP request when no catalog config is supplied (`state.materialize.catalog_config.is_none()`). The catalog dump (100+ MB) is only fetched when `--validate-catalog-store` was passed, so the roughly 170 of 175 test compositions that omit that flag skip the fetch entirely.
`run_check_shard_tombstone` provides a targeted check that a specific persist shard has been fully tombstoned, retrying until the timeout.
