---
source: src/persist-client/src/internal/merge.rs
revision: 951d074c87
---

# persist-client::internal::merge

Provides `MergeTree<T>`, a bounded-depth binary merge tree used during batch building to limit the number of outstanding parts by merging adjacent parts when any level exceeds the configured maximum length.
The tree guarantees insertion-order preservation and `O(log N)` merge depth, with at most `K` parts returned by `finish`.

Also provides `Pending<T>`, an enum wrapping either a `JoinHandle<T>` (`Writing` variant) or a resolved value (`Finished` variant). `block_until_ready` waits for the task and stores the result so later calls resolve without spawning additional work. It is cancel-safe: the future awaits through the `JoinHandle` borrow rather than taking the handle out, so a future dropped mid-await leaves the handle intact and a subsequent call to `block_until_ready` or `into_result` still resolves to the value.
