---
source: src/persist/src/location.rs
revision: db1a5ce170
---

# persist::location

Defines the core storage abstractions for `mz-persist`: the `Blob` trait (linearizable key-value byte store) and the `Consensus` trait (versioned, compare-and-set state log).
`SeqNo` identifies positions in the consensus log, while `ExternalError` (with `Determinate` and `Indeterminate` variants) classifies failures from external storage systems.
`Determinate`, `Indeterminate`, and `ExternalError` each expose a `context` method that wraps the inner error with additional context text (mirroring `anyhow::Error::context`) while preserving the determinate vs. indeterminate classification. Callers use this to annotate errors with the resource being accessed (e.g. the blob key in a GET) so the context appears everywhere the error is displayed.
The `Tasked<A>` wrapper delegates every call to a backing `Blob` or `Consensus` implementation via a new Tokio task, shielding the system from callers that do not promptly drive futures.
This module is the foundational contract that all concrete storage backends (S3, Postgres, file, in-memory) must implement.
