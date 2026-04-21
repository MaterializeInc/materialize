---
source: src/storage-types/src/read_holds.rs
revision: b55d3dee25
---

# storage-types::read_holds

Defines `ReadHold`, a RAII token that prevents the since of a storage collection from advancing past the held frontier.
Communicates frontier changes back to the issuing controller via a `ChangeTx` channel; on clone, drop, and `try_downgrade` the appropriate diff is sent.
`merge_assign` combines two holds on the same collection into one by taking the join of their frontiers.
