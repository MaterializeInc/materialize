---
source: src/pgwire-common/src/format.rs
revision: 7d6d670654
---

# mz-pgwire-common::format

Defines the `Format` enum (`Text` = 0, `Binary` = 1) representing the pgwire encoding format for a value.
Implements `TryFrom<i16>` (returning an `io::Error` for unknown codes) and `From<Format> for i8`.
