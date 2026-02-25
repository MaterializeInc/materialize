---
description: >
Run or add Materialize tests (slt, testdrive, pgtest, cargo test).
Trigger when the user mentions wanting to run a test, or add a new test.
argument-hint: <test-type> <file-or-pattern>
allowed-tools: [Bash, Read, Edit, Grep, Glob, Task]
---

Run or help add tests in the Materialize repo. Supports sqllogictest, testdrive, pgtest, and Rust unit tests.

## Test framework quick reference

| What you're testing | Where | Format | How to run |
|---|---|---|---|
| SQL correctness, types, functions | `test/sqllogictest/` | `.slt` | `./bin/sqllogictest -- test/sqllogictest/FILE.slt -v` |
| Sources/sinks, Kafka, catalog, pgwire | `test/testdrive/` | `.td` | `cd test/testdrive && ./mzcompose run default FILE.td` |
| Raw pgwire messages (COPY, extended protocol) | `test/pgtest/` | `.pt` | `cargo test -p environmentd pgtest` |
| Pure logic, decoding, pure functions | next to code | Rust `#[test]` | `cargo test -p CRATE_NAME` |

## Environment setup

Before running any `cargo` or `bin/` commands, always prefix with:
```bash
source "$HOME/.cargo/env" &&
```
This ensures the Rust toolchain is available in Claude Code's shell environment.

## Steps

1. Parse arguments from: $ARGUMENTS
   - First arg (optional): test type — `slt`, `td`, `pgtest`, `cargo`, or `add`.
   - Remaining args: file path, pattern, or test name.
   - If no args, ask what to test.
   - If no test type but a file path is given, infer from extension: .slt → slt, .td → td, .pt → pgtest, .rs → cargo.
   - If only a keyword is given with no type, search `test/sqllogictest/`, `test/testdrive/`, and `test/pgtest/` for matching files and present options.
2. Find the Materialize repo root — check current directory first, and traverse up the directory path until see `src`, `bin`, `ci`, `doc`.
3. Based on test type, run the tests


### Running `slt` (sqllogictest)
```bash
./bin/sqllogictest [--release] -- test/sqllogictest/FILE.slt -v
```
- Use `--rewrite-results` if I ask to update expected output.
- If a pattern is given instead of a file, search `test/sqllogictest/` for matching files.

### Running `td` (testdrive)
```bash
cd test/testdrive
./mzcompose run default FILE.td
```

### Running `pgtest`
```bash
cargo test -p environmentd pgtest
# or specific: cargo test -p environmentd test_pgtest_FILE_NAME
```

### Running `cargo` (Rust unit/integration tests)
```bash
cargo test -p CRATE_NAME [-- test_name] [--nocapture]
```

### Adding a test (`add`)
- Ask what behavior to test.
- Determine the right framework based on the table above.
- For `.slt`: use `mode cockroach`, test NULLs and edge cases.
- For `.pt`: wire new files in `src/environmentd/tests/pgwire.rs`.
- For Rust: use `#[mz_ore::test]` (or `#[mz_ore::test(tokio::test)]` for async).
- Do NOT modify files in `test/sqllogictest/sqlite` or `test/sqllogictest/cockroach` (upstream).

4. Show full output. If tests fail, show failures clearly and offer to help fix.

Note: The first run includes a cargo build step that can take several minutes. Use a bash timeout of at least 600000ms (10 min) for test commands.

If more guidance is needed on how to run the test, or set up the environment read `doc/developer/guide-testing.md` and `doc/developer/guide.md`

## Rules

- Always show the full error output before attempting fixes.
- Ask before modifying test files.
- Do NOT commit test changes — I'll decide when to commit.
