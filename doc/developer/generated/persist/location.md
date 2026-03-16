---
source: src/persist/src/location.rs
revision: 4267863081
---

# persist::location

Defines the core storage abstractions for `mz-persist`: the `Blob` trait (linearizable key-value byte store) and the `Consensus` trait (versioned, compare-and-set state log).
`SeqNo` identifies positions in the consensus log, while `ExternalError` (with `Determinate` and `Indeterminate` variants) classifies failures from external storage systems.
The `Tasked<A>` wrapper delegates every call to a backing `Blob` or `Consensus` implementation via a new Tokio task, shielding the system from callers that do not promptly drive futures.
This module is the foundational contract that all concrete storage backends (S3, Postgres, file, in-memory) must implement.
