---
source: src/persist-client/src/fetch.rs
revision: db1a5ce170
---

# persist-client::fetch

Implements reading of individual batch parts from blob storage, including `LeasedBatchPart` (a leased reference to a part), `FetchedPart` (the decoded columnar data), and `EncodedPart` (the raw bytes as fetched).
Applies time-range filtering (`FetchBatchFilter`) and optional stats-based pushdown to skip fetching parts entirely.
A semaphore-based permit system (`FETCH_SEMAPHORE_PERMIT_ADJUSTMENT`) bounds the total number of bytes being decoded in-flight to avoid memory overload.
`LeasedBatchPart` carries the `LeasedReaderId` of the reader that minted it; when a blob fetch fails, `missing_blob_diagnostics` refreshes shard state and reports whether that reader is still present, distinguishing an expired lease from a GC bug.
`fetch_batch_part_blob` adds context (the blob key) to any error returned by `Blob::get`, so that retry log entries produced by `retry_external` identify which blob and therefore which shard is stuck when a GET stalls indefinitely.
`BatchFetcher` exposes a `missing_blob_diagnostics` method that delegates to the same free function via its internal `SchemaCache` applier.
