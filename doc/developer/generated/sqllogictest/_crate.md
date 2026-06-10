---
source: src/sqllogictest/src/lib.rs
revision: 30d929249e
---

# sqllogictest

A driver for sqllogictest, the SQL correctness testing framework originally developed for SQLite, adapted to run against Materialize.
Provides a generic parser (`ast`, `parser`) for `.slt` files and a Materialize-specific runner (`runner`) that spins up a full `environmentd` instance and executes test records against it.
The `util` module provides minor formatting helpers; the binary (`bin/sqllogictest`) provides the CLI entry point.
Key dependencies include `mz-environmentd`, `mz-adapter-types`, `mz-pgrepr`, `mz-repr`, `tokio-postgres`, and `mz-orchestrator-process`.
