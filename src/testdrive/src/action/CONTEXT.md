# testdrive::action

All built-in command implementations and shared runtime state for testdrive.
`action.rs` is the dispatcher; sub-modules implement individual command families.

## Files (LOC ≈ 5,617 across this directory)

| File | LOC | What it owns |
|---|---|---|
| `action.rs` (parent) | 1,255 | `State`, `Config`, `Run` trait impl, `create_state`, command dispatch (`run` match) |
| `sql.rs` | 838 | `run_sql` / `run_fail_sql`: retry loop, result comparison, result rewriting |
| `s3.rs` | 719 | S3 object upload (text, CSV, Parquet) |
| `consistency.rs` | 403 | Post-file coordinator + catalog consistency checks; `Level` enum |
| `schema_registry.rs` | 223 | Confluent Schema Registry subjects and schemas |
| `set.rs` | 140 | Session variable assignment, SQL-sourced var set, retry config |
| `protobuf.rs` | 66 | `protoc` descriptor compilation |
| `persist.rs` | 64 | Persist shard compaction commands |
| `http.rs` | 55 | Arbitrary HTTP requests |
| `webhook.rs` | 72 | Materialize webhook source appends |
| `file.rs` | 133 | Local file writes with optional compression |
| `fivetran.rs` | 137 | Fivetran Destination gRPC calls |
| `kafka/` | ~300 | Topic create/delete/ingest/verify (sub-module per operation) |
| `postgres/` | ~80 | PostgreSQL connect + execute |
| `mysql/` | ~80 | MySQL connect + execute |
| `duckdb/` | ~80 | DuckDB connect + execute/query |
| `sql_server/` | ~80 | SQL Server connect + execute |
| `skip_if.rs` / `skip_end.rs` | 58 | Conditional block skipping |
| `sleep.rs` | 38 | Fixed and random sleeps |
| `version_check.rs` | 41 | Materialize version gating |
| `nop.rs` | 14 | No-op command |

## Key concepts

- **`State`** — runtime context: active connections (Postgres, Kafka producer/admin,
  CCSR, S3, MySQL, DuckDB, SQL Server, Fivetran), config, variable map, retry
  settings, rewrite positions.
- **`Config`** — static configuration captured at test start; large flat struct
  grouping MZ pgwire params, Kafka opts, S3/AWS config, Fivetran URLs, etc.
- **`Run` trait** — single async `run(&self, state) → ControlFlow`; each
  `PosCommand` is dispatched through `action.rs`'s match arm.
- **Retry loop** — `sql.rs` wraps most SELECT/DML with `mz_ore::retry::Retry`;
  DDL and EXPLAIN are not retried.
- **`rewrite_results` mode** — `sql.rs` replaces expected-output sections
  in-place using byte-offset ranges captured by the parser.
- **`consistency.rs`** — directly imports `mz_adapter::catalog` and
  `mz_catalog`; the only module that pierces the SQL-client boundary into
  production internals.

## Cross-references

- Parent: `src/testdrive/src/CONTEXT.md`
- Generated docs: `doc/developer/generated/testdrive/action/`
