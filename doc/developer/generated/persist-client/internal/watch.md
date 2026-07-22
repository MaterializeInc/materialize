---
source: src/persist-client/src/internal/watch.rs
revision: b8c325f035
---

# persist-client::internal::watch

Provides `StateWatchNotifier` and `StateWatch`, a reactive notification mechanism that allows callers to efficiently wait for the in-process `LockingTypedState` to advance.
`StateWatchNotifier` holds two independent channels: a Tokio broadcast channel (`seqno_tx`) that fires on every strict seqno advance, and a Tokio watch channel (`upper_tx`) that advances only when the shard upper strictly moves forward.
`StateWatch` exposes two wait methods over these channels: `wait_for_seqno_ge` wakes on any strict seqno advance (used by lease/seqno machinery), while `wait_for_upper_past` wakes only when the shard upper advances past a given frontier, ignoring seqno bumps from GC, rollups, since-downgrades, and other writers' no-op CaAs.
`StateWatch` falls back to polling consensus when the broadcast channel lags behind.
