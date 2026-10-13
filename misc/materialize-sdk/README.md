# Materialize SDK

Libraries for consuming `SUBSCRIBE` correctly and for writing custom sinks. The
design is in `doc/developer/design/20260706_materialize_sdk.md`.

- `core/`: the protocol core, a Rust library with no I/O that every language
  package wraps.
- `conformance/`: language-neutral test vectors that every package runs.
- `rust/`: the Rust package, over `tokio-postgres`.

This directory is its own Cargo workspace, outside the Materialize workspace.

```
cd misc/materialize-sdk
cargo test
cargo clippy --all-targets -- -D warnings
```

The Rust package's integration tests need a running Materialize. The nightly
pipeline runs them through `test/materialize-sdk`, which also restarts
Materialize between the two phases of the restart test. From the repository
root:

```
bin/mzcompose --find materialize-sdk run default
```

Against a Materialize you started yourself, first turn on the features the
tests use, as `mz_system` on the internal port, for example with
`psql -h localhost -p 6877 -U mz_system`:

```
ALTER SYSTEM SET enable_logical_compaction_window = true;
ALTER SYSTEM SET enable_alter_table_add_column = true;
```

Then run the tests from `misc/materialize-sdk`, skipping the restart phases:

```
MATERIALIZE_URL="host=localhost port=6875 user=materialize" \
MATERIALIZE_SYSTEM_URL="host=localhost port=6877 user=mz_system" \
cargo test --tests -- --ignored --skip before_a_restart --skip after_a_restart
```

`bin/fmt` formats it with the rest of the repository.
