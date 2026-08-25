---
source: src/persist-client/src/internal/cache.rs
revision: 5a4a36c4fd
---

# persist-client::internal::cache

Provides `BlobMemCache`, an LRU in-memory cache layered in front of the `Blob` storage backend to reduce redundant blob fetches.
Cache capacity is dynamically configurable and can optionally scale with the number of worker threads in the process.
