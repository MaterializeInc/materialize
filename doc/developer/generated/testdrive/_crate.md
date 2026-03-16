---
source: src/testdrive/src/lib.rs
revision: d7429ca6b9
---

# testdrive

Integration test driver for Materialize that executes `.td` script files containing SQL queries, expected results, and builtin commands that interact with external systems.
The library entry points are `run_file`, `run_stdin`, and `run_string`; all three parse the script text with `parser::parse`, initialize shared state via `action::create_state`, and execute commands one by one through the `Run` trait, with optional `skip-if`/`skip-end` block skipping and per-file consistency checks.
A `rewrite_results` mode replaces expected-output sections in the source file in-place when actual results differ, enabling test authoring.
Key dependencies include `mz-adapter` and `mz-catalog` for direct catalog access (used in consistency checks), `rdkafka` for Kafka operations, `mz-ccsr` for the Confluent Schema Registry, `mz-persist-client` for persist shard operations, `tokio-postgres` for Materialize and PostgreSQL SQL execution, `mysql_async` for MySQL, `duckdb` for DuckDB, and `tiberius`/`mz-sql-server-util` for SQL Server.

## Module structure

| Module | Role |
|--------|------|
| `parser` | Parses script text into `PosCommand` values |
| `error` | Error types with source-location context |
| `action` | State, dispatch, and all builtin command implementations |
| `action::consistency` | Post-test coordinator and catalog consistency checks |
| `action::sql` | SQL and expected-failure SQL execution |
| `action::kafka` | Kafka topic and message operations |
| `action::postgres` | PostgreSQL connection and statement execution |
| `action::mysql` | MySQL connection and statement execution |
| `action::duckdb` | DuckDB connection and query execution |
| `action::sql_server` | SQL Server connection and statement execution |
| `action::schema_registry` | Confluent Schema Registry operations |
| `action::s3` | S3 object uploads (text, CSV, Parquet) |
| `action::file` | Local file writes with optional compression |
| `action::http` | Arbitrary HTTP requests |
| `action::webhook` | Materialize webhook source appends |
| `action::persist` | Persist shard compaction |
| `action::protobuf` | `protoc` descriptor compilation |
| `action::psql` | `psql` CLI execution |
| `action::fivetran` | Fivetran Destination gRPC calls |
| `action::set` | Session variable and retry configuration |
| `action::sleep` | Fixed and random sleeps |
| `action::skip_if` / `skip_end` | Conditional block skipping |
| `action::version_check` | Materialize version gating |
| `format::avro` | Avro encoding/decoding helpers |
| `format::bytes` | Hex-escape byte string decoding |
| `util::postgres` | PostgreSQL connection helpers |
| `util::text` | Text comparison and diff display |
