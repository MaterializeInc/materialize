---
source: src/sqllogictest/src/bin/sqllogictest.rs
revision: a1418992fa
---

# sqllogictest binary

The `sqllogictest` binary entry point; parses CLI arguments (verbosity, rewrite mode, JUnit report path, Postgres URL, file/directory paths, var overrides) and drives `Runner` over the specified test files or directories.
Supports walking directory trees, collecting per-file `Outcomes`, generating JUnit XML reports, and exiting with a non-zero status on failures.
