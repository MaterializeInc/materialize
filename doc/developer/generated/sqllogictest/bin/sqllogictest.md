---
source: src/sqllogictest/src/bin/sqllogictest.rs
revision: 6eeaca032b
---

# sqllogictest binary

`main()` pins the rustls crypto provider to `aws-lc-rs` via `rustls::crypto::aws_lc_rs::default_provider().install_default()` before any other setup, preventing a panic when both `aws-lc-rs` and `ring` provider features are linked.
The `sqllogictest` binary entry point; parses CLI arguments (verbosity, quiet mode, rewrite mode, JUnit report path, Postgres URL, prefix, file/directory paths, system parameter defaults, log filter, replica size and count, shard/shard-count for parallel sharding, and flags for auto-indexing tables/selects, auto-transactions, table keys, and fail-fast behavior) and drives `Runner` over the specified test files or directories.
Supports walking directory trees, collecting per-file `Outcomes`, generating JUnit XML reports, and exiting with a non-zero status on failures.
Validates and injects required system parameter defaults (e.g., `enable_logical_compaction_window`) before constructing a `RunConfig`.
Force-enables the `enable_cluster_controller` and `enable_background_alter_cluster` dyncfgs for the test suite so that the cluster controller owns the managed-cluster replica set during testing; a caller-supplied value for either name takes precedence.
An `OutputStream` wrapper optionally prefixes each output line with a UTC timestamp.
