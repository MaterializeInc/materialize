# mz-testdrive

Integration test driver for Materialize. Parses and executes `.td` script
files containing SQL queries with expected results and built-in commands that
interact with Kafka, S3, schema registry, PostgreSQL, MySQL, DuckDB, SQL Server,
Fivetran, and persist shards.

## Files / modules (LOC ≈ 9,219)

| Path | LOC | What it owns |
|---|---|---|
| `src/action/` | 5,617 | All built-in command implementations — see [`src/action/CONTEXT.md`](src/action/CONTEXT.md) |
| `src/parser.rs` | 854 | `parse()` → `Vec<PosCommand>`; `Command`, `SqlCommand`, `BuiltinCommand` |
| `src/lib.rs` | 196 | Entry points: `run_file`, `run_stdin`, `run_string` |
| `src/error.rs` | 187 | `PosError` wrapping `anyhow::Error` with source-file positions |
| `src/util/postgres.rs` + `src/util/text.rs` | ~50 | Connection helpers, diff display |
| `src/format/avro.rs` + `src/format/bytes.rs` | ~50 | Avro encode/decode, hex-byte decoding |
| `src/bin/testdrive.rs` | 59 | CLI binary: arg parsing, drives `run_file`/`run_stdin` |

## Key concepts

- **Parse → Dispatch loop**: `parser::parse` produces `Vec<PosCommand>`; `lib.rs` iterates them, calling `action::create_state` once and dispatching each `PosCommand` via `Run::run` on the `action.rs` match arm.
- **`State`** — all runtime connections and mutable test context; single instance per script run.
- **`Config`** — large flat struct; every external system's address lives here (MZ pgwire, Kafka, S3/AWS, CSR, MySQL, DuckDB, SQL Server, Fivetran).
- **`rewrite_results` mode** — `sql.rs` replaces expected-output sections using byte offsets captured at parse time.
- **Production dependency** — `consistency.rs` directly imports `mz-adapter` and `mz-catalog`; the only part of testdrive that goes through internal catalog APIs rather than SQL.

## Package identity

Crate name: `mz-testdrive`. Binary: `testdrive`. Key dependencies:
`mz-adapter`, `mz-catalog` (consistency checks), `rdkafka`, `mz-ccsr`,
`mz-persist-client`, `tokio-postgres`, `mysql_async`, `duckdb`, `tiberius`.

## Key interfaces (exported, library mode)

- `run_file(config, path)` — parse + execute a `.td` file
- `run_stdin(config)` / `run_string(config, src)` — alternate entry points
- `Config` — all external-system addresses and test options
- `State` — mutable runtime context (connections, variable map, retry settings)

## Bubbled findings for src/CONTEXT.md

- **Production coupling**: `mz-adapter` + `mz-catalog` are direct dependencies
  for consistency checks; testdrive is the only integration-test binary that
  reaches into internal catalog internals. This creates a tight coupling between
  test infrastructure and production internals.
- **`Config` monolith**: ~100-field flat struct groups options for six external
  systems; poor locality for future system additions.
- **String-dispatch for built-ins**: large `match` arm in `action.rs`; three-
  site edit required per new command (module, import, match arm).

## Cross-references

- Generated docs: `doc/developer/generated/testdrive/`
- `src/action/CONTEXT.md` for built-in command breakdown.
