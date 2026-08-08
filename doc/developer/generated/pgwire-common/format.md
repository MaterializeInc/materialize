---
source: src/pgwire-common/src/format.rs
revision: dd328dbda5
---

# mz-pgwire-common::format

Defines the `Format` enum (`Text` = 0, `Binary` = 1) representing the pgwire encoding format for a value.
Implements `TryFrom<u16>` (returning an `io::Error` for unknown codes) and `From<Format> for i8`.
