---
source: src/persist-client/src/internal/watch.rs
revision: b8f9449bec
---

# persist-client::internal::watch

Provides `StateWatchNotifier` and `StateWatch`, a reactive notification mechanism that allows callers to efficiently wait for the in-process `LockingTypedState` to advance past a given `SeqNo`.
`StateWatchNotifier` broadcasts new SeqNos over a Tokio broadcast channel each time state is updated under the write lock; `StateWatch` subscribes to these broadcasts and falls back to polling consensus when it lags behind.
