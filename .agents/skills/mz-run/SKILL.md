---
name: mz-run
description: >
  Trigger: "run Materialize locally", "start environmentd", "check compilation",
  "format code", "lint", "cargo check", "cargo fmt", "cargo clippy", "bin/fmt",
  "bin/lint", or mentions compiling, building, running, formatting, linting,
  log filters, jemalloc, CockroachDB setup in Materialize. Also "how do I run
  this" or "it won't compile".
---

# Developing Materialize

## Compiling

Check compilation with `cargo check`.
Do not use `cargo build` or `cargo run` to build or run Materialize.
Use `bin/environmentd --build-only` to build without running.

`mz_environmentd::Config` is constructed in three places, one of them in a
separate crate: `src/environmentd/src/environmentd/main.rs` (production),
`src/environmentd/src/test_util.rs` (TestHarness), and
`src/sqllogictest/src/runner.rs` (crate `mz-sqllogictest`). Adding a field means
updating all three. `cargo check -p mz-environmentd` does NOT cover the
sqllogictest constructor, so a missing field there compiles environmentd fine
but breaks `mz-sqllogictest`, cascading in CI to clippy, doc-tests, and the
environmentd image build (failing every mzcompose job). It looks like infra but
is not. Verify with `cargo check -p mz-sqllogictest --all-targets`, not only
`-p mz-environmentd`.

## Running locally

Start Materialize using `bin/environmentd --optimized`.
Pass `--reset` to delete data from prior runs (clears the mzdata directory and resets Postgres schemas), useful when testing catalog changes or starting fresh.

`--reset` runs `DROP SCHEMA ... CASCADE` (consensus, tsoracle, storage) against
the shared CockroachDB at `localhost:26257`. That persist consensus and
timestamp-oracle state is global, not per-worktree: every local `environmentd`
across all worktrees shares it (mzdata only holds the blob and environment-id).
Resetting while another instance runs wipes its state unrecoverably. Before
`--reset`, check for another instance with `ss -ltnp | grep 6875` and
`pgrep -af environmentd`. If one exists, reuse it, or point yours at a different
metadata backend with `--postgres <url>` (`bin/environmentd`, env `MZDEV_POSTGRES`)
instead of the shared default. That can be a dedicated instance you spin up
yourself, or a shared-but-different Postgres/CockroachDB you and others already
point at deliberately, which avoids both the reset hazard and the cost of
spinning up a fresh CRDB per instance. Prefer unit tests or `bin/mzcompose` (own
containerized CRDB) over a second bare-metal instance when neither applies.
`--reset` also kills every `clusterd` process owned by your user
(`_handle_lingering_services` in `misc/python/materialize/cli/run.py`),
including those of other sessions' instances.

`bin/environmentd` and `bin/sqllogictest` run the program in a forked child
process group, and the Python parent kills that group when it exits normally.
SIGTERM to the Python parent (e.g. from `timeout`) skips that cleanup and leaks
the program and its `clusterd` processes. To stop or restart an instance, kill
the program's own PID (`pgrep -f "$(git rev-parse --show-toplevel)/[m]zbuild/bin/environmentd"`,
which scopes the match to this checkout, while the brackets keep the pattern
from matching the calling shell), so the parent
exits and cleans up. Before restarting, wait until ports 6875 (SQL) and 6879
(persist pubsub) are free. `mzbuild/bin/environmentd` is a symlink into
`target/`, so a `/proc/<pid>/exe` ending in `(deleted)` means the process
still runs a binary that has since been rebuilt.

The compute memory limiter is off locally and in mzcompose. clusterd gets
`--heap-limit` only for replica sizes with `swap_enabled` and a non-zero
`disk_limit` (`src/controller/src/clusters.rs`), and no size in
`cluster_replica_size_map()` sets `swap_enabled`. To test the limiter, pass
`bin/environmentd -- --cluster-replica-sizes='<json>'` with such a size. The
last occurrence of the flag wins, so the map must contain every size you need,
including `bootstrap` and `scale=1,workers=1`, the default size of the
builtin clusters. Without the latter, environmentd panics at boot because
`mz_system` references an unknown replica size.

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

Format Rust, Python, and Protobuf files with `bin/fmt`. Takes no file
arguments — it always formats the whole tree (only flag: `--check`).
Run `bin/lint` to check for lint errors.
Run `cargo clippy --all-targets -- -D warnings` to check for Rust-specific warnings.

## Log filters

Set `MZ_LOG_FILTER` to a tracing-compatible filter expression.
For example, `mz_adapter::catalog::apply=trace,warn` would enable trace logging for the `mz-adapter` crate's `catalog::apply` module, and `warn` for the rest of the system.

## jemalloc

On Linux, we link against jemalloc by default.
Compile with `--no-default-features` to use the system allocator.
