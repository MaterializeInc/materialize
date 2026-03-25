---
source: src/catalog/src/durable/transaction.rs
revision: 892cf626bc
---

# catalog::durable::transaction

Defines `Transaction`, which batches multiple catalog mutations and commits them atomically to the durable store.
Provides methods for every catalog DDL operation (create/update/delete for clusters, replicas, items, roles, schemas, comments, network policies, storage metadata, etc.) as well as ID allocation from the per-collection allocators.
`TransactionBatch` is the serializable representation of the accumulated changes; it is passed to `DurableCatalogState::commit_transaction` for atomic persist writes.
