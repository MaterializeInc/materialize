---
source: src/persist-client/src/fetch.rs
revision: 161628c089
---

# persist-client::fetch

Implements reading of individual batch parts from blob storage, including `LeasedBatchPart` (a leased reference to a part), `FetchedPart` (the decoded columnar data), and `EncodedPart` (the raw bytes as fetched).
Applies time-range filtering (`FetchBatchFilter`) and optional stats-based pushdown to skip fetching parts entirely.
A semaphore-based permit system (`FETCH_SEMAPHORE_PERMIT_ADJUSTMENT`) bounds the total number of bytes being decoded in-flight to avoid memory overload.
`LeasedBatchPart` carries the `LeasedReaderId` of the reader that minted it; when a blob fetch fails, `missing_blob_diagnostics` refreshes shard state and reports whether that reader is still present, distinguishing an expired lease from a GC bug.
`BatchFetcher` exposes a `missing_blob_diagnostics` method that delegates to the same free function via its internal `SchemaCache` applier.
