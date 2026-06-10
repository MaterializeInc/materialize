---
source: src/storage-operators/src/oneshot_source/util.rs
revision: 59763d1319
---

# storage-operators::oneshot_source::util

Defines the `IntoRangeHeaderValue` trait with implementations for `Range<usize>` and `RangeInclusive<usize>`, converting Rust range types into HTTP `Range` header byte-range strings (e.g. `bytes=0-1023`).
Used by both the HTTP and S3 source implementations when issuing partial-content requests.
