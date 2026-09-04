---
source: src/persist-client/src/fetch.rs
revision: 6ebd534824
---

# persist-client::fetch

Implements reading of individual batch parts from blob storage, including `LeasedBatchPart` (a leased reference to a part), `FetchedPart` (the decoded columnar data), and `EncodedPart` (the raw bytes as fetched).
Applies time-range filtering (`FetchBatchFilter`) and optional stats-based pushdown to skip fetching parts entirely.
`FetchedBlob::stats()` decodes part stats via `try_decode()` and returns `None` on failure (undecodable stats due to version skew or corruption), causing the caller to treat the part as unconstrained and fetch it rather than discarding it.
A semaphore-based permit system (`FETCH_SEMAPHORE_PERMIT_ADJUSTMENT`) bounds the total number of bytes being decoded in-flight to avoid memory overload.
`LeasedBatchPart` carries the `LeasedReaderId` of the reader that minted it; when a blob fetch fails, `missing_blob_diagnostics` refreshes shard state and reports whether that reader is still present, distinguishing an expired lease from a GC bug.
`fetch_batch_part_blob` adds context (the blob key) to any error returned by `Blob::get`, so that retry log entries produced by `retry_external` identify which blob and therefore which shard is stuck when a GET stalls indefinitely.
`BatchFetcher` exposes a `missing_blob_diagnostics` method that delegates to the same free function via its internal `SchemaCache` applier.
`BatchFetcher` implements `Clone` manually (avoiding bounds on `K`, `V`, `D` since every field is an `Arc` or independently `Clone`); clones share the `schema_cache`, so cached schema fetches are reused across clones, enabling concurrent `fetch_leased_part` calls each on their own clone.
`LeasedBatchPart` carries a `bounds_truncated` field derived from the containing batch's run metadata (true if any run has `RunMeta::bounds_truncated()`). `try_optimize_ignored_data_fetch` checks this flag before substituting the part's write-time `diffs_sum` for a real fetch: a truncated part's blob may physically contain updates outside the registered desc that a real fetch would filter out, so substituting `diffs_sum` would fabricate data and the optimization is skipped.
