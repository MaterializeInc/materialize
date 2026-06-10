---
source: src/persist-client/src/error.rs
revision: 53a0a45c78
---

# persist-client::error

Defines the public error types for the crate: `InvalidUsage` (caller violated the API contract), `CodecMismatch` (requested codecs differ from those stored durably), and `UpperMismatch` (compare-and-append rejected because the expected upper did not match).
These are distinct from `ExternalError` (storage backend failures) and cover the cases where the caller needs to adjust its behavior rather than retry.
