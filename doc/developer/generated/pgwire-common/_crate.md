---
source: src/pgwire-common/src/lib.rs
revision: 2daa609ac4
---

# mz-pgwire-common

Shared PostgreSQL wire protocol primitives used by Materialize's pgwire implementation.
Provides message encoding/decoding (`codec`), connection wrapping with optional TLS and connection limiting (`conn`), the `Format` enum for text/binary encoding (`format`), all frontend message types and version constants (`message`), and the `Severity` enum for error/notice levels (`severity`).
All public items are re-exported from the crate root; consumers import directly from `mz_pgwire_common`. Exported frame-size constants include `MAX_STARTUP_FRAME_SIZE` (10,000 bytes, matching PostgreSQL's `MAX_STARTUP_PACKET_LENGTH`), `MAX_FORWARDED_STARTUP_FRAME_SIZE` (`MAX_STARTUP_FRAME_SIZE` plus `FORWARDED_STARTUP_PARAM_ALLOWANCE`), `FORWARDED_STARTUP_PARAM_ALLOWANCE` (512 bytes of headroom for balancer-appended parameters), and `MAX_PREAUTH_FRAME_SIZE` (16 KiB, the ceiling for pre-authentication frames such as credentials).
