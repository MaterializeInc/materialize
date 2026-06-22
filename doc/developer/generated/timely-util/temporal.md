---
source: src/timely-util/src/temporal.rs
revision: f9be03fda1
---

# timely-util::temporal

Defines `BucketChain<S>` and its supporting traits `BucketTimestamp` and `Bucket` for efficiently storing and retrieving future updates indexed by timestamp.
`BucketChain` organizes buckets of exponentially increasing size in a `BTreeMap`, keeping adjacent buckets within two bits of each other (the "chain property"), which bounds both storage overhead and the cost of the `peel` (extract data up to a frontier) and `restore` (re-establish chain invariant, with fuel limiting) operations to amortized logarithmic time.
`restore` includes a fast path that checks whether the chain is already well-formed before allocating a new map: if all buckets are already within two bits of each other (with an imaginary -2-bit bucket at the start), the method returns immediately without rebuilding the map. Only when the chain is not well-formed does it iterate through buckets, moving correctly-sized ones into a new map and splitting oversized ones, stopping when fuel is exhausted and appending any remaining buckets unchanged.
`buckets()` and `buckets_mut()` return iterators over the stored bucket values in time order, without exposing the internal bucket-range keys.
