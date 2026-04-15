---
source: src/timely-util/src/temporal.rs
revision: d90aafba34
---

# timely-util::temporal

Defines `BucketChain<S>` and its supporting traits `BucketTimestamp` and `Bucket` for efficiently storing and retrieving future updates indexed by timestamp.
`BucketChain` organizes buckets of exponentially increasing size in a `BTreeMap`, keeping adjacent buckets within two bits of each other (the "chain property"), which bounds both storage overhead and the cost of the `peel` (extract data up to a frontier) and `restore` (re-establish chain invariant, with fuel limiting) operations to amortized logarithmic time.
