---
source: src/storage-types/src/read_holds.rs
revision: 248f143a50
---

# storage-types::read_holds

Defines `ReadHold`, a RAII token that prevents the since of a storage collection from advancing past the held frontier.
Communicates frontier changes back to the issuing controller via a `ChangeTx` channel; on clone, drop, and `try_downgrade` the appropriate diff is sent.
`merge_assign` combines two holds on the same collection into one by taking the join of their frontiers.
`try_clone` is a fallible clone that returns `Err(ReadHoldIssuerHungUp)` when the issuer's channel has closed; `Clone::clone` delegates to it and panics on error. `ReadHoldIssuerHungUp` carries the `GlobalId` of the affected collection and is only expected during process shutdown, when the tokio runtime drops tasks in arbitrary order.
