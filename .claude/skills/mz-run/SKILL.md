---
name: mz-run
description: >
  This skill should be used when the user wants to "run Materialize locally",
  "start environmentd", "check compilation", "format code", "lint", "cargo
  check", "cargo fmt", "cargo clippy", "bin/fmt", "bin/lint", or mentions
  compiling, building, running, formatting, linting, log filters, jemalloc,
  or CockroachDB setup in the Materialize repository. Use this skill even
  if the user just says "how do I run this" or "it won't compile" without
  being specific.
---

# Developing Materialize

## Compiling

Check compilation with `cargo check`.
Do not use `cargo build` or `cargo run` to build or run Materialize.
Use `bin/environmentd --build-only` to build without running.

## Running locally

Start Materialize using `bin/environmentd --optimized`.
Pass `--reset` to delete data from prior runs (clears the mzdata directory and resets Postgres schemas), useful when testing catalog changes or starting fresh.

Access Materialize using psql:
* `psql -p 6875 -h localhost -U materialize` for regular access.
* `psql -p 6877 -h localhost -U mz_system` for system access.

If it fails because CockroachDB is not running, start it:

```
docker run --name=cockroach -d -p 26257:26257 -p 26258:8080 \
  cockroachdb/cockroach:latest start-single-node --insecure --store=type=mem,size=2G
```

If the container already exists, use `docker start cockroach` instead.

Other useful flags:
* `--tokio-console` — activate the Tokio console for debugging async tasks.
* `--coverage` — build with coverage instrumentation.
* `--sanitizer address|memory|thread` — build with the specified sanitizer.

## Formatting and linting

Format Rust, Python, and Protobuf files with `bin/fmt`.
Run `bin/lint` to check for lint errors.
Run `cargo clippy --all-targets -- -D warnings` to check for Rust-specific warnings.
If Rust dependencies were added or changed, run `cargo hakari generate`.

## Log filters

Set `MZ_LOG_FILTER` to a tracing-compatible filter expression.
For example, `mz_adapter::catalog::apply=trace,warn` would enable trace logging for the `mz-adapter` crate's `catalog::apply` module, and `warn` for the rest of the system.

## jemalloc

On Linux, we link against jemalloc by default.
Compile with `--no-default-features` to use the system allocator.
