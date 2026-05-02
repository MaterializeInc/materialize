---
source: src/compute-client/src/controller/error.rs
revision: 681cdf1339
---

# mz-compute-client::controller::error

Defines bespoke error types returned by each compute controller public method, following the principle that each method's error type documents exactly the failure modes that method can produce.
Covers instance lifecycle (`InstanceMissing`, `InstanceExists`), replica lifecycle (`ReplicaCreationError`, `ReplicaDropError`), collection access (`CollectionLookupError`, `CollectionUpdateError`), dataflow creation (`DataflowCreationError`), peeks (`PeekError`), read policy assignment (`ReadPolicyError`), and orphan removal (`RemoveOrphansError`).
