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

`bin/fmt` formats it with the rest of the repository.
