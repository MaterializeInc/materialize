---
source: src/pgwire-common/src/lib.rs
revision: 2c17d81232
---

# mz-pgwire-common

Shared PostgreSQL wire protocol primitives used by Materialize's pgwire implementation.
Provides message encoding/decoding (`codec`), connection wrapping with optional TLS and connection limiting (`conn`), the `Format` enum for text/binary encoding (`format`), all frontend message types and version constants (`message`), and the `Severity` enum for error/notice levels (`severity`).
All public items are re-exported from the crate root; consumers import directly from `mz_pgwire_common`.
