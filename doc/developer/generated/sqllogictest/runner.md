---
source: src/sqllogictest/src/runner.rs
revision: 047cb5995c
---

# sqllogictest::runner

Implements the Materialize-specific sqllogictest runner: spins up a full `mz-environmentd` instance in-process, executes each `Record` against it via `tokio-postgres`, and compares actual output against expected output.
Provides `Runner`, `RunConfig`, `Outcomes`, and `WriteFmt` as the primary API for running test files; supports result rewriting, multiple connections/users, COPY directives, and JUnit XML output.
The runner handles timestamp management needed for serialized execution semantics and translates Materialize-specific types (numerics, intervals, dates, ACL items, etc.) into the text format sqllogictest expects.
