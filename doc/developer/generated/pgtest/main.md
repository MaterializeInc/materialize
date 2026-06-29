---
source: src/pgtest/src/main.rs
revision: 30d929249e
---

# mz-pgtest::main

CLI binary entry point for `mz-pgtest`.
Parses three arguments via clap — `--addr` (default `localhost:6875`), `--user` (default `materialize`), and a positional `directory` — then delegates to `mz_pgtest::walk` with a 30-second read timeout to run all `.pt` test files in the given directory.
