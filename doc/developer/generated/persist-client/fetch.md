---
source: src/persist-client/src/fetch.rs
revision: 530390d54e
---

# persist-client::fetch

Implements reading of individual batch parts from blob storage, including `LeasedBatchPart` (a leased reference to a part), `FetchedPart` (the decoded columnar data), and `EncodedPart` (the raw bytes as fetched).
Applies time-range filtering (`FetchBatchFilter`) and optional stats-based pushdown to skip fetching parts entirely.
A semaphore-based permit system (`FETCH_SEMAPHORE_PERMIT_ADJUSTMENT`) bounds the total number of bytes being decoded in-flight to avoid memory overload.
