---
source: src/persist/src/intercept.rs
revision: 9c0755d4b3
---

# persist::intercept

Test utility that wraps a `Blob` implementation and allows post-operation closures to be installed on individual operations (currently `delete`).
`InterceptHandle` provides a shared, mutable reference to the closure registry; `InterceptBlob` is the `Blob` wrapper that consults the handle on each call.
Used in tests to inject errors or observe results after a real backend has processed an operation.
