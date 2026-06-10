---
source: src/persist-cli/src/maelstrom/txn_list_append_single.rs
revision: b89a9e0ec5
---

# persistcli::maelstrom::txn_list_append_single

Implements the Maelstrom `txn-list-append` workload using a single persist shard to store a key→value-list map.
`Transactor` executes read-write transactions modeled after Materialize's SQL implementation: each transaction reads at `read_ts`, writes at `write_ts = read_ts+1` via `compare_and_append`, and retries on upper mismatch.
It keeps both a short-lived snapshot+listen and a long-lived listen open per transaction, cross-checking them to exercise both read paths, and advances the since lag behind the upper via a critical handle.
`TransactorService` wraps `Transactor` behind a mutex and implements the `Service` trait, wiring up `MaelstromBlob`/`MaelstromConsensus` or configurable external storage backends with optional `UnreliableBlob`/`UnreliableConsensus` fault injection.
