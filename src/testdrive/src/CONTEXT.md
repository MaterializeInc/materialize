# testdrive (src/)

Integration test driver. Parses `.td` script files, executes built-in
commands and SQL against a live Materialize instance and external systems,
and optionally rewrites expected output in-place.

## Files / modules (LOC ≈ 9,219)

| Path | LOC | What it owns |
|---|---|---|
| `action/` (subtree) | 5,617 | All built-in command implementations — see `action/CONTEXT.md` |
| `parser.rs` | 854 | `parse()` → `Vec<PosCommand>`; `Command`, `SqlCommand`, `BuiltinCommand` |
| `lib.rs` | 196 | Entry points: `run_file`, `run_stdin`, `run_string` |
| `error.rs` | 187 | `PosError` wrapping `anyhow::Error` with source-file positions |
| `util/postgres.rs` + `util/text.rs` | ~50 | Connection helpers, diff display |
| `format/avro.rs` + `format/bytes.rs` | ~50 | Avro encode/decode, hex-byte decoding |
| `bin/testdrive.rs` | 59 | CLI binary: arg parsing, drives `run_file`/`run_stdin` |

## Key concepts

- **Parse → Dispatch loop**: `parser::parse` produces `Vec<PosCommand>`;
  `lib.rs` iterates them, calling `action::create_state` once and dispatching
  each `PosCommand` via `Run::run` on the `action.rs` match arm.
- **`State`** — all runtime connections and mutable test context; single
  instance per script run.
- **`Config`** — large flat struct; every external system's address lives here
  (MZ pgwire, Kafka, S3/AWS, CSR, MySQL, DuckDB, SQL Server, Fivetran).
- **`rewrite_results` mode** — sql.rs replaces expected-output sections using
  byte offsets captured at parse time.
- **Production dependency** — `consistency.rs` directly imports `mz-adapter`
  and `mz-catalog`; the only part of testdrive that goes through internal
  catalog APIs rather than SQL.

## Bubbled findings for src/CONTEXT.md

- **Production code coupling in test binary**: testdrive Cargo.toml depends on
  `mz-adapter` and `mz-catalog` (not just the SQL client) for consistency
  checks. This locks testdrive to the production catalog schema and makes it
  sensitive to internal refactors. Mitigation: expose consistency check endpoints
  via HTTP or a thin internal SQL interface.
- **`Config` is a monolithic flat struct** (≈100 fields across six external
  systems): adding a new system currently requires touching one large struct and
  the `create_state` function. Grouping into per-system sub-structs would
  improve locality.
- **No `Action` trait abstraction**: builtin commands are dispatched through a
  large `match` on string names in `action.rs`. Adding a new command requires
  edits in three places: the match arm, the sub-module, and `action.rs`'s
  module list. A registered-handler approach could reduce coupling.

## Cross-references

- Generated docs: `doc/developer/generated/testdrive/`
- `action/CONTEXT.md` for built-in command breakdown.
