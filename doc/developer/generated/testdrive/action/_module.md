---
source: src/testdrive/src/action.rs
revision: 1f5cd0d026
---

# testdrive::action

Central module that defines the `Config`, `State`, `ControlFlow`, and `CatalogConfig` types, and provides the `create_state` async function that establishes all external connections needed to run a test.
`Config` holds every connection parameter (Materialize PostgreSQL endpoints, Kafka broker, Schema Registry URL, AWS config, persist URLs, etc.) and behavioral knobs (timeouts, retry parameters, result-rewrite mode).
`State` is the mutable per-run context that carries live client handles, active variable substitutions, per-session regex, and accumulated `Rewrite` records for result rewriting.
The `Run` trait with its single `run` method is the dispatch point: the `PosCommand::run` implementation resolves a `BuiltinCommand` name to the appropriate action function across all submodules, performs variable substitution, and routes `SqlCommand`/`FailSqlCommand` to `action::sql`.
Child modules cover every external system: `sql`, `consistency`, `kafka`, `postgres`, `mysql`, `duckdb`, `sql_server`, `schema_registry`, `s3`, `file`, `http`, `webhook`, `persist`, `protobuf`, `psql`, `fivetran`, `set`, `sleep`, `skip_if`, `skip_end`, `version_check`, and `nop`.
