---
source: src/testdrive/src/action.rs
revision: 84f88ca968
---

# testdrive::action

Central module that defines the `Config`, `State`, `ControlFlow`, and `CatalogConfig` types, and provides the `create_state` async function that establishes all external connections needed to run a test.
`Config` holds every connection parameter (Materialize PostgreSQL endpoints, Kafka broker, Schema Registry URL, AWS config, persist URLs, Fivetran destination URL and files path, etc.) and behavioral knobs (timeouts, retry parameters, result-rewrite mode).
`MaterializeState` is an internal struct embedded in `State` that groups all Materialize-specific connection state: the active `tokio_postgres::Client`, SQL and HTTP addresses (public, internal, password, SASL), the `EnvironmentId`, `BootstrapArgs`, catalog config, and the AWS account/external-ID/connection-role-ARN values queried from the running environment at startup (used to open a matching catalog copy for consistency checks). `State` is the mutable per-run context that carries live client handles (including `MaterializeState`), active variable substitutions, per-session regex, Fivetran destination connection details, and accumulated `Rewrite` records for result rewriting.
The `Run` trait with its single `run` method is the dispatch point: the `PosCommand::run` implementation resolves a `BuiltinCommand` name to the appropriate action function across all submodules, performs variable substitution, and routes `SqlCommand`/`FailSqlCommand` to `action::sql`.
Child modules cover every external system: `sql`, `consistency`, `kafka`, `postgres`, `mysql`, `duckdb`, `sql_server`, `schema_registry`, `s3`, `file`, `fivetran`, `glue`, `http`, `webhook`, `persist`, `protobuf`, `psql`, `set`, `sleep`, `skip_if`, `skip_end`, `version_check`, and `nop`.
The `glue` submodule exposes two commands: `glue-create-schema` and `glue-verify-compatibility`.
