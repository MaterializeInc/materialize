---
name: mz-run
description: >
  This skill should be used when the user wants to "run Materialize locally",
  "start environmentd", "check compilation", "format code", "lint", "cargo
  check", "cargo fmt", "cargo clippy", or mentions compiling, running, formatting,
  linting, log filters, or jemalloc in the Materialize repository.
---

# Developing Materialize

## Compiling

Check compilation with `cargo check`.
Do not use `cargo build` or `cargo run` to build or run Materialize.

## Running locally

Start Materialize using `bin/environmentd --optimized`.
Pass `--reset` to start with a default catalog, useful when testing changes to the catalog itself.

Access Materialize using psql:
* `psql -p 6875 -h localhost -U materialize` for regular access.
* `psql -p 6877 -h localhost -U mz_system` for system access.

If it fails because CockroachDB is not running, start it:

```
docker run --name=cockroach -d -p 26257:26257 -p 26258:8080 \
  cockroachdb/cockroach:latest start-single-node --insecure --store=type=mem,size=2G
```

If the container already exists, use `docker start cockroach` instead.

## Formatting and linting

Format Rust and Python files with `bin/fmt`.
Run `bin/lint` to check for lint errors.
Run `cargo clippy --all-targets -- -D warnings` to check for Rust-specific warnings.
If Rust dependencies were added or changed, run `cargo hakari generate`.

## Log filters

Set `MZ_LOG_FILTER` to a tracing-compatible filter expression.
For example, `mz_adapter::catalog::apply=trace,warn` would enable trace logging for the `mz-adapter` crate's `catalog::apply` module, and `warn` for the rest of the system.

## jemalloc

On Linux, we link against jemalloc by default.
Compile with `--no-default-features` to use the system allocator.
