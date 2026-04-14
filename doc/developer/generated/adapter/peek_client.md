---
source: src/adapter/src/peek_client.rs
revision: 00cc513fa5
---

# adapter::peek_client

Provides `PeekClient`, which bundles a `Client` handle with lazily-populated direct channels to each compute instance (`compute_instances`), lazily-populated per-timeline timestamp oracles (`oracles`), a `PersistClient`, storage-collection frontier access, a transient ID generator, optimizer metrics, and a `StatementLoggingFrontend`.
This allows the frontend peek sequencing path (and other callers) to issue peeks directly to compute instances and check persist fast-path conditions without serialising through the coordinator's main event loop.
`CollectionLookupError` enumerates errors that can occur when consulting storage or compute frontiers for timestamp selection.
