# AGENTS.md

## Cursor Cloud specific instructions

### Overview

Materialize is a streaming SQL database (~100+ Rust crates). The core services are `environmentd` (coordinator) and `clusterd` (compute/storage). See `doc/developer/guide.md` for full developer setup details.

### Running Materialize locally

1. **Start CockroachDB** (metadata store):
   ```
   docker start cockroach || docker run --name=cockroach -d -p 127.0.0.1:26257:26257 -p 127.0.0.1:26258:8080 cockroachdb/cockroach:v23.1.11 start-single-node --insecure --store=type=mem,size=2G
   ```
2. **Start environmentd**: `bin/environmentd --optimized --reset -- --all-features --unsafe-mode`
   - `--reset` clears state from prior runs.
   - Connect: `psql -U materialize -h localhost -p 6875 materialize`
   - System access: `psql -U mz_system -h localhost -p 6877 materialize`

### Gotchas discovered during setup

- **Default compiler must be gcc/g++**: The VM ships with `cc`/`c++` pointing to clang. The Rust build uses `--target=x86_64-unknown-linux-gnu` in cmake, which breaks clang's C++ header resolution. Fix: `sudo update-alternatives --set cc /usr/bin/gcc && sudo update-alternatives --set c++ /usr/bin/g++`.
- **libstdc++ symlink**: A symlink may be missing: `sudo ln -s /usr/lib/gcc/x86_64-linux-gnu/13/libstdc++.so /usr/lib/x86_64-linux-gnu/libstdc++.so`.
- **Docker socket permissions**: After starting dockerd, run `sudo chmod 666 /var/run/docker.sock`.
- **RUSTFLAGS**: For fast builds, set `export RUSTFLAGS="-C link-arg=-fuse-ld=mold -C debuginfo=0 --cfg tokio_unstable"`. The `--cfg tokio_unstable` flag is required.
- **First build takes ~30 min** on a 2-core VM. Subsequent incremental builds are much faster.
- **Unit tests requiring metadata store**: Set `COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257`.

### Standard commands (see `doc/developer/guide.md` for details)

| Task | Command |
|------|---------|
| Build | `bin/environmentd --build-only --optimized` |
| Run | `bin/environmentd --optimized --reset -- --all-features --unsafe-mode` |
| Clippy | `cargo clippy -p <crate> --all-targets -- -D warnings` |
| Format | `bin/fmt` |
| Lint | `bin/lint` |
| Unit tests | `cargo test -p <crate>` |
| sqllogictest | `bin/sqllogictest --optimized -- <path>` |
| mzcompose | `bin/mzcompose --find <name> run default` |
