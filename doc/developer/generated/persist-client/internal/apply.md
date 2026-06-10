---
source: src/persist-client/src/internal/apply.rs
revision: 181b1e7efc
---

# persist-client::internal::apply

Implements `Applier`, the narrow interface through which all persist state mutations flow.
`Applier` executes compare-and-set operations against `StateVersions` (consensus), updates the in-process `StateCache`, and publishes state diffs to PubSub subscribers.
By funneling all state changes through this type, persist keeps the surface area interacting with shared state tightly controlled and auditable.
