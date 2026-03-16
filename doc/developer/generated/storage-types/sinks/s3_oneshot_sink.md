---
source: src/storage-types/src/sinks/s3_oneshot_sink.rs
revision: e757b4d11b
---

# storage-types::sinks::s3_oneshot_sink

Implements `preflight` for the S3 copy-to (one-shot) sink: verifies that the target S3 path is empty (apart from files from the same sink ID), checks `DeleteObject` permissions, and writes an `INCOMPLETE` sentinel file.
Also defines `S3KeyManager`, which encodes the naming conventions for per-replica file keys and the singleton `INCOMPLETE` sentinel key.
The sentinel-based approach ensures a single atomic notification event for downstream consumers.
