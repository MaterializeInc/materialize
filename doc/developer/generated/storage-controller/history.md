---
source: src/storage-controller/src/history.rs
revision: 82d92a7fad
---

# storage-controller::history

Defines `CommandHistory<T>`, a reducible log of `StorageCommand`s used to rehydrate new or reconnecting storage replicas.
The history auto-reduces whenever it doubles in size, discarding dropped ingestions and sinks and retaining only the latest definition and compaction frontier for each live object.
It tracks per-command-type metrics and exposes an iterator so `Instance` can replay commands to fresh replicas.
