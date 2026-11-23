# Materialize SDK

Libraries for consuming `SUBSCRIBE` correctly and for writing custom sinks. The
design is in `doc/developer/design/20260706_materialize_sdk.md`.

- `core/`: the protocol core, a Rust library with no I/O that every language
  package wraps.
- `conformance/`: language-neutral test vectors that every package runs.
- `schema/`: the JSON Schemas of the values the core hands to the packages,
  which `codegen/` generates along with each package's types.
- `rust/`: the Rust package, over `tokio-postgres`.
- `python/`: the Python package, the core built with PyO3 under psycopg.
- `node/`: the Node package, the core built for WebAssembly under
  node-postgres.

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

The Python and Node packages' value types, and their conversion of decimal
strings to native integers, are generated from the core's types. After changing
a value the core hands out, regenerate them:

```
cargo run -p materialize-sdk-codegen
```

`cargo test` fails while a generated file is stale, and checks every value in
the conformance vectors against the schemas.

The Python package builds with maturin, in a virtual environment with
Python 3.10 or later:

```
cd misc/materialize-sdk/python
pip install "maturin>=1.9,<2"
maturin develop --extras test
python -m mypy.stubtest materialize_sdk._core
python -m pytest tests
```

`_core.pyi` describes the compiled module by hand. stubtest fails when it no
longer matches. The live tests run when `MATERIALIZE_URL` is set.

The Node package builds with wasm-pack and needs Node 22 or later:

```
cd misc/materialize-sdk/node
npm ci
npm run build
npm test
```

wasm-bindgen generates the binding's TypeScript declarations, and the build
type-checks the package against them. The live tests run when
`MATERIALIZE_URL` is set.

