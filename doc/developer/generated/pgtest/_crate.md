---
source: src/pgtest/src/lib.rs
revision: e757b4d11b
---

# mz-pgtest

Provides a datadriven test framework for the PostgreSQL wire protocol, used to send and verify wire-level messages against any Postgres-compatible server.

## Module structure

* `lib.rs` — the library containing all test framework logic and public API.
* `main.rs` — a thin CLI binary (`mz-pgtest`) that parses arguments and delegates to `mz_pgtest::walk`.

## Key types and functions

* `PgConn` (private) — a raw TCP connection to a Postgres server that speaks the wire protocol directly using `postgres-protocol`; handles the startup handshake, message framing, send/receive buffering, and a configurable read timeout.
* `PgTest` — the public harness managing a named pool of `PgConn` connections; exposes `send` and `until` methods used by the test runner.
* `walk` / `run_test` — integrate with the `datadriven` framework; `walk` recurses a directory of `.pt` files and `run_test` dispatches `send` and `until` directives.
* Frontend message types (`Query`, `Parse`, `Bind`, `Execute`, `Describe`) — JSON-deserializable structs representing client messages that `send` serialises onto the wire.
* Backend message types (`ReadyForQuery`, `RowDescription`, `DataRow`, `CommandComplete`, `ErrorResponse`, etc.) — JSON-serialisable structs used to format received server messages as human-readable test output.

## Directives

Test files use two directives:
* `send` — encodes and transmits one or more frontend messages.
* `until` — accumulates backend messages until a named message type is received; supports `conn=`, `err_field_typs=`, `no_error_fields`, and `ignore=` arguments.

## Dependencies

Depends on `postgres-protocol`, `datadriven`, `bytes`, `serde_json`, `anyhow`, and `mz-ore`.
Used by integration tests in `test/pgtest` and `test/pgtest-mz` to verify wire-protocol behaviour of Materialize against PostgreSQL.
