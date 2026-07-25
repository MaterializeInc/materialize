---
source: src/sqllogictest/src/runner.rs
revision: fc9d219c84
---

# sqllogictest::runner

Implements the Materialize-specific sqllogictest runner: spins up a full `mz-environmentd` instance in-process, executes each `Record` against it via `tokio-postgres`, and compares actual output against expected output.
Provides `Runner`, `RunConfig`, `Outcomes`, and `WriteFmt` as the primary API for running test files; supports result rewriting, multiple connections/users, COPY directives, and JUnit XML output.
The runner handles timestamp management needed for serialized execution semantics and translates Materialize-specific types (numerics, intervals, dates, ACL items, etc.) into the text format sqllogictest expects.
When `--auto-index-selects` is enabled, the runner executes each SELECT query normally first; only if that query succeeds does it additionally execute the query via a view-backed indexed path to verify consistency. If the view-backed execution produces a different result, the runner reports an `InconsistentViewOutcome` (or a warning if the inconsistency matches known acceptable patterns).
`RunnerInner` holds an `active_user: Option<String>` field tracking the user set by the most recent `user` directive; records run on that user's cached connection instead of the default connection. The `run_user` method ensures the role exists, connects as it, and updates `active_user`. On `reset-server`, all non-default user roles are dropped (best-effort) and `active_user` is cleared.
When a column's declared type does not match the value the query returned, `format_datum` falls back to text encoding rather than panicking, so the mismatch surfaces as an output diff instead of aborting the run.
`error_matches` wraps expected-error regex matching: if the expected-error string is not a valid regex (common in CockroachDB-imported files), it returns `false` so the record fails with a mismatch outcome rather than aborting the run.
`strip_crdb_table_items` removes CockroachDB physical-layout items (`INDEX`, `UNIQUE INDEX`, `INVERTED INDEX`, `FAMILY`) from `CREATE TABLE` column lists so that CockroachDB test files can be parsed; constraints are kept unchanged.
`Runner` holds a `replacements: Vec<(Regex, String)>` field that accumulates substitutions from `Record::Replace` directives. These are applied to the actual output of each `query` before comparison or rewriting, masking non-deterministic tokens. Replacements are stored on the outer `Runner` (not on `RunnerInner`) so they survive a `reset-server` directive.
