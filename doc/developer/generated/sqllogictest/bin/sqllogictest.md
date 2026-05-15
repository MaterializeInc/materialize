---
source: src/sqllogictest/src/bin/sqllogictest.rs
revision: c18b8e405f
---

# sqllogictest binary

The `sqllogictest` binary entry point; parses CLI arguments (verbosity, quiet mode, rewrite mode, JUnit report path, Postgres URL, prefix, file/directory paths, system parameter defaults, log filter, replica size and count, shard/shard-count for parallel sharding, and flags for auto-indexing tables/selects, auto-transactions, table keys, and fail-fast behavior) and drives `Runner` over the specified test files or directories.
Supports walking directory trees, collecting per-file `Outcomes`, generating JUnit XML reports, and exiting with a non-zero status on failures.
Validates and injects required system parameter defaults (e.g., `enable_logical_compaction_window`) before constructing a `RunConfig`.
An `OutputStream` wrapper optionally prefixes each output line with a UTC timestamp.
