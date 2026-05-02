---
source: src/sqllogictest/src/runner.rs
revision: caa8122228
---

# sqllogictest::runner

Implements the Materialize-specific sqllogictest runner: spins up a full `mz-environmentd` instance in-process, executes each `Record` against it via `tokio-postgres`, and compares actual output against expected output.
Provides `Runner`, `RunConfig`, `Outcomes`, and `WriteFmt` as the primary API for running test files; supports result rewriting, multiple connections/users, COPY directives, and JUnit XML output.
The runner handles timestamp management needed for serialized execution semantics and translates Materialize-specific types (numerics, intervals, dates, ACL items, etc.) into the text format sqllogictest expects.
When `--auto-index-selects` is enabled, the runner executes each SELECT query normally first; only if that query succeeds does it additionally execute the query via a view-backed indexed path to verify consistency. If the view-backed execution produces a different result, the runner reports an `InconsistentViewOutcome` (or a warning if the inconsistency matches known acceptable patterns).
