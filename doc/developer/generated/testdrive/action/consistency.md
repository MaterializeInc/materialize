---
source: src/testdrive/src/action/consistency.rs
revision: 541f36e7d8
---

# testdrive::action::consistency

Implements post-test consistency checks against a live Materialize environment.
`Level` controls when checks run: after each file (default), after each statement (for debugging), or disabled entirely.
`run_consistency_checks` calls three sub-checks in parallel — coordinator internal consistency via its HTTP API, in-memory vs. on-disk catalog state, and statement-logging completeness — and aggregates all failures into a single error.
`run_check_shard_tombstone` provides a targeted check that a specific persist shard has been fully tombstoned, retrying until the timeout.
