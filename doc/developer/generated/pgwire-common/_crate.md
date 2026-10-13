---
source: src/pgwire-common/src/lib.rs
revision: 0e35544577
---

# mz-pgwire-common

Shared PostgreSQL wire protocol primitives used by Materialize's pgwire implementation.
Provides message encoding/decoding (`codec`), connection wrapping with optional TLS and connection limiting (`conn`), all frontend message types and version constants (`message`), and the `Severity` enum for error/notice levels (`severity`).
The `Format` enum for text/binary encoding is defined in `mz-pgrepr-consts` and re-exported here at `mz_pgwire_common::Format` so existing call sites are unchanged.
All public items are re-exported from the crate root; consumers import directly from `mz_pgwire_common`. Exported frame-size constants include `MAX_STARTUP_FRAME_SIZE` (10,000 bytes, matching PostgreSQL's `MAX_STARTUP_PACKET_LENGTH`), `MAX_FORWARDED_STARTUP_FRAME_SIZE` (`MAX_STARTUP_FRAME_SIZE` plus `FORWARDED_STARTUP_PARAM_ALLOWANCE`), `FORWARDED_STARTUP_PARAM_ALLOWANCE` (512 bytes of headroom for balancer-appended parameters), and `MAX_PREAUTH_FRAME_SIZE` (16 KiB, the ceiling for pre-authentication frames such as credentials).
