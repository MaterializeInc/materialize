# mz-testdrive

Integration test driver for Materialize. Parses and executes `.td` script
files containing SQL queries with expected results and built-in commands that
interact with Kafka, S3, schema registry, PostgreSQL, MySQL, DuckDB, SQL Server,
Fivetran, and persist shards.

## Structure (≈ 9,278 LOC)

| Path | LOC | What it owns |
|---|---|---|
| `src/` | 9,219 | All source — see `src/CONTEXT.md` |
| `src/action/` | 5,617 | Built-in command implementations — see `src/action/CONTEXT.md` |

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
- See `src/CONTEXT.md` and `src/action/CONTEXT.md` for detail.
