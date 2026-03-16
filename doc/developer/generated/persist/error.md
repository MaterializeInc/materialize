---
source: src/persist/src/error.rs
revision: 4ea16cafbc
---

# persist::error

Defines the `Error` enum for persistence-layer failures, covering I/O errors, quota exhaustion, unknown stream registrations, sequenced no-ops, and runtime shutdown.
Provides `From` conversions from `io::Error`, `ExternalError`, Arrow/Parquet errors, and string types so callers can use `?` uniformly.
