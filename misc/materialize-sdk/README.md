# Materialize SDK

Libraries for consuming `SUBSCRIBE` correctly and for writing custom sinks. The
design is in `doc/developer/design/20260706_materialize_sdk.md`.

- `core/`: the protocol core, a Rust library with no I/O that every language
  package wraps.
- `conformance/`: language-neutral test vectors that every package runs.
- `rust/`: the Rust package, over `tokio-postgres`.
- `python/`: the Python package, the core built with PyO3 under psycopg.

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

## Try it

`rust/examples/postgres_sink.rs` keeps a Postgres table equal to a Materialize
table, source, or materialized view, with each batch and its checkpoint in one
Postgres transaction. Once its first snapshot has completed, stop it at any
point and start it again: it resumes from the checkpoint without a new
snapshot. From `misc/materialize-sdk`:

```
MATERIALIZE_URL="host=localhost port=6875 user=materialize" \
POSTGRES_URL="host=localhost port=5432 user=postgres" \
SOURCE=materialize.public.orders \
cargo run --example postgres_sink
```

The rows land in `mz_rows`, one per sink, snapshot generation, and distinct
row, as a JSON array of the columns' text with a count. Once the first snapshot
completes, each sink's rows equal the source. `SINK_ID` names the sink, so several sinks can
share one Postgres database. To resume after a stop, the source needs history
that outlasts the stop, for example
`ALTER TABLE orders SET (RETAIN HISTORY = FOR '1h')`. `RETAIN HISTORY` is in
private preview and needs `enable_logical_compaction_window`. Both connections
use TLS when the server offers it, as Materialize Cloud does, and verify its
certificate against the system's certificate authorities. A server with a
self-signed certificate needs its CA in `SSL_CERT_FILE`, or `sslmode=disable`
in its URL.

`turbopuffer/examples/search.rs` keeps a Materialize table, source, or
materialized view searchable in turbopuffer, by meaning with native embedding
and by words with full-text search, and searches it. From
`misc/materialize-sdk`:

```
export MATERIALIZE_URL="host=localhost port=6875 user=materialize"
export TURBOPUFFER_API_KEY=...
export SOURCE=materialize.public.articles KEY=id EMBED=body FULL_TEXT=body NAME=articles
cargo run --example search -- sink
cargo run --example search -- search "a cat on a windowsill"
```

Changes to the source show up in searches within seconds, deletes included.
Stop the sink and start it again, and it resumes from its checkpoint without
embedding everything again. The namespaces are named after `NAME`, and the
example's docs list its other settings.
